package io.github.kdroidfilter.seforimlibrary.common.dh

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Fills `line_dh` — the (bookId, dhText) -> lineIndex dibbur-hamatchil index
 * the Otzaria client uses to search commentaries by their opening words and
 * to render them as virtual sub-headings (dhDisplay carries the printed form).
 *
 * Runs as a late DB-writing stage (`:generator-common:buildLineDhIndex`),
 * after every book-writing stage, and is idempotent: the table is rebuilt
 * from scratch on each run.
 *
 * Automatic eligibility is deliberately semantic, not typographic: only a
 * book whose metadata declares it a commentary is considered (plus linked
 * `midrash` records, which are the commentary-like Lekach Tov volumes).
 * Audited legacy imports without that metadata can be admitted explicitly by
 * `line_dh_book_overrides.csv`; unreviewed books remain out of the index.
 *
 * An eligible book is indexed only when one extraction format dominates its content
 * lines ([MIN_COVERAGE] of them, at least [MIN_LINES] hits). A bold book in
 * which at least [LEAD_SHARE] of the bold lines run on to `כו'` is a
 * "lead" book: those lines take the full quotation, the rest keep the bold
 * prefix. This book-level
 * gate is the main false-positive defence: books that merely bold an
 * occasional word, or use a spaced dash mid-sentence here and there, never
 * reach the threshold and contribute nothing. Each format is quality-checked
 * independently, so a noisy majority format cannot hide a smaller valid one.
 *
 * A second, per-book noise gate drops books whose winning format marks
 * something other than dibburim: paragraph openers (responsa, mussar,
 * commentaries that bold the first word of every paragraph) show up as low
 * key diversity or a large share of a closed list of opener words
 * ([OPENERS]). See [NoiseRatios]. Interleaved-format corpora such as Chavruta
 * are excluded by the semantic policy rather than by fragile tag counting.
 *
 * Required system property: `dbPath`.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildLineDhIndexCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        val report = rebuildLineDhIndex(conn, logger)
        logger.i {
            "line_dh: ${report.indexed} dibburim over ${report.boldBooks} bold-format " +
                "(${report.leadBooks} of them lead-bold) + ${report.dashBooks} dash-format books " +
                "(${report.skippedBooks} books below threshold, " +
                "${report.noisyBooks} books dropped as non-dibbur marking; " +
                "eligible=${report.metadataBooks} metadata + ${report.overrideBooks} reviewed overrides, " +
                "unreviewed=${report.unreviewedBooks})"
        }
    }
}

/**
 * Atomically replaces the existing index. If extraction or insertion fails,
 * the previous table contents remain usable instead of leaving a partial or
 * empty index behind.
 */
internal fun rebuildLineDhIndex(conn: Connection, logger: Logger): LineDhIndexReport {
    check(conn.autoCommit) { "rebuildLineDhIndex requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        conn.createStatement().use { st ->
            st.execute(
                """
                CREATE TABLE IF NOT EXISTS line_dh (
                    bookId INTEGER NOT NULL,
                    dhText TEXT NOT NULL,
                    lineIndex INTEGER NOT NULL,
                    dhDisplay TEXT NOT NULL,
                    PRIMARY KEY (bookId, dhText, lineIndex)
                ) WITHOUT ROWID
                """.trimIndent(),
            )
            st.execute("DELETE FROM line_dh")
        }
        indexAllBooks(conn, logger).also { conn.commit() }
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

/** Minimum share of a book's content lines one format must cover. */
internal const val MIN_COVERAGE = 0.4

/** Minimum absolute number of extracted dibburim per book. */
internal const val MIN_LINES = 10

/** Below this share of distinct keys among a book's hits, the same word is being marked over and over. */
internal const val MIN_DISTINCT_RATIO = 0.5

/**
 * The diversity rule only applies to books whose hits are this often a single
 * word: a commentary that quotes the same verse phrase for several notes
 * (Torah Temimah) is repetitive but genuine.
 */
internal const val DISTINCT_RULE_ONE_WORD_RATIO = 0.5

/** At or above this share of [OPENERS] among a book's hits, the marks are paragraph openers. */
internal const val MAX_OPENER_RATIO = 0.25

/**
 * Words that open a paragraph of argument rather than quote a base text.
 * Compared against the printed dibbur after [normalizeOpener].
 */
internal val OPENERS: Set<String> = setOf(
    "והנה", "עוד", "אמנם", "ובזה", "ועוד", "אך", "אבל", "אלא", "ונראה", "הנה", "ומה", "וכן", "גם",
    "ויש", "ומעתה", "וראיתי", "ולפי", "ואם", "או", "ועל", "ודע", "כל", "ולכאורה", "וזהו", "וזה",
    "איברא", "ועתה", "ולפ\"ז", "ומ\"ש", "ואמנם", "וגם", "אכן", "והשתא", "ובאמת", "ולענ\"ד", "ונ\"ל",
    "ואפשר", "ואולם", "ובזה יש", "ומ\"מ", "והא", "תו", "לא", "אמר", "ומיהו", "והרי", "אין", "אי",
    "מה", "א\"כ", "ת\"ל", "שם", "מ\"ש", "מ\"מ", "שלום", "אודות",
)

private val POINTS = Regex("[\u0591-\u05c7]")

/** Same quote-mark folding as DhExtractor's marker check, so `ולפ״ז` and `ולפ"ז` both match. */
internal fun normalizeOpener(display: String): String =
    POINTS.replace(display, "")
        .replace('״', '"')
        .replace('”', '"')
        .replace('“', '"')
        .replace('׳', '\'')
        .replace('’', '\'')
        .replace('‘', '\'')
        .replace("''", "\"")
        .trim()

/**
 * Per-book shape of the winning format's hits: how many distinct keys they
 * carry, how often the printed dibbur is a single word, how often it is a
 * known paragraph opener. [isNoisy] is the gate itself. The gate intentionally
 * uses only strong corpus signals: a mild opener ratio in a genuine one-word
 * lexicon is not enough to discard the whole book.
 */
internal data class NoiseRatios(
    val distinctRatio: Double,
    val oneWordRatio: Double,
    val openerRatio: Double,
) {
    val isNoisy: Boolean
        get() = (distinctRatio < MIN_DISTINCT_RATIO && oneWordRatio >= DISTINCT_RULE_ONE_WORD_RATIO) ||
            openerRatio >= MAX_OPENER_RATIO

    companion object {
        fun of(hits: List<DhExtractor.Dh>): NoiseRatios {
            require(hits.isNotEmpty()) { "noise ratios need at least one hit" }
            val total = hits.size.toDouble()
            val distinct = hits.mapTo(HashSet()) { it.key }.size
            val oneWord = hits.count { it.display.trim().split(' ').count(String::isNotEmpty) == 1 }
            val openers = hits.count { normalizeOpener(it.display) in OPENERS }
            return NoiseRatios(distinct / total, oneWord / total, openers / total)
        }
    }
}

/**
 * Share of a book's bold hits that must run on to `כו'` for the bold to be
 * read as the first word of a longer quotation (Maharsha 0.79–0.94; Rashi,
 * Metzudot, Be'er Heitev ≤ 0.04).
 */
internal const val LEAD_SHARE = 0.75

internal data class BookOverrideKey(val source: String, val title: String)

internal enum class BookOverrideDecision { INCLUDE, EXCLUDE }

internal data class BookOverride(val decision: BookOverrideDecision, val reason: String)

private const val BOOK_OVERRIDES_RESOURCE = "/line_dh_book_overrides.csv"

/** Parses the small, reviewed RFC-4180 CSV shipped with the generator. */
internal fun parseBookOverrides(csv: String): Map<BookOverrideKey, BookOverride> {
    val records = csv.lineSequence()
        .map(String::trim)
        .filter { it.isNotEmpty() && !it.startsWith('#') }
        .toList()
    require(records.isNotEmpty()) { "$BOOK_OVERRIDES_RESOURCE is empty" }
    require(parseCsvRecord(records.first()) == listOf("source", "title", "decision", "reason")) {
        "$BOOK_OVERRIDES_RESOURCE has an invalid header"
    }
    val overrides = LinkedHashMap<BookOverrideKey, BookOverride>()
    records.drop(1).forEachIndexed { index, record ->
        val fields = parseCsvRecord(record)
        require(fields.size == 4) { "$BOOK_OVERRIDES_RESOURCE:${index + 2} must have 4 fields" }
        val key = BookOverrideKey(fields[0].trim(), fields[1].trim())
        require(key.source.isNotEmpty() && key.title.isNotEmpty()) {
            "$BOOK_OVERRIDES_RESOURCE:${index + 2} has an empty source or title"
        }
        val override = BookOverride(
            decision = BookOverrideDecision.valueOf(fields[2].trim().uppercase()),
            reason = fields[3].trim().also {
                require(it.isNotEmpty()) { "$BOOK_OVERRIDES_RESOURCE:${index + 2} has no audit reason" }
            },
        )
        require(overrides.put(key, override) == null) {
            "$BOOK_OVERRIDES_RESOURCE:${index + 2} duplicates $key"
        }
    }
    return overrides
}

private fun parseCsvRecord(record: String): List<String> {
    val fields = ArrayList<String>()
    val field = StringBuilder()
    var quoted = false
    var index = 0
    while (index < record.length) {
        val char = record[index]
        when {
            char == '"' && quoted && index + 1 < record.length && record[index + 1] == '"' -> {
                field.append('"')
                index++
            }
            char == '"' -> quoted = !quoted
            char == ',' && !quoted -> {
                fields += field.toString()
                field.setLength(0)
            }
            else -> field.append(char)
        }
        index++
    }
    require(!quoted) { "Unterminated quoted field in $BOOK_OVERRIDES_RESOURCE" }
    fields += field.toString()
    return fields
}

private fun loadBookOverrides(): Map<BookOverrideKey, BookOverride> {
    val stream = checkNotNull(object {}.javaClass.getResourceAsStream(BOOK_OVERRIDES_RESOURCE)) {
        "Missing required resource $BOOK_OVERRIDES_RESOURCE"
    }
    return stream.bufferedReader(Charsets.UTF_8).use { parseBookOverrides(it.readText()) }
}

internal data class LineDhIndexReport(
    val boldBooks: Int,
    val dashBooks: Int,
    val skippedBooks: Int,
    val noisyBooks: Int,
    val indexed: Int,
    val leadBooks: Int = 0,
    val metadataBooks: Int = 0,
    val overrideBooks: Int = 0,
    val unreviewedBooks: Int = 0,
)

private data class BookInfo(
    val id: Long,
    val title: String,
    val source: String,
    val dependenceType: String?,
    val isBaseBook: Boolean,
    val hasBaseText: Boolean,
)

private data class FormatCandidate(
    val hits: List<Pair<Long, DhExtractor.Dh>>,
    val ratios: NoiseRatios,
    val bold: Boolean,
)

/** Bold hits of a lead book, with the lead-bold quotation replacing the bare prefix where it exists. */
internal fun mergeLeadHits(
    bold: List<Pair<Long, DhExtractor.Dh>>,
    lead: List<Pair<Long, DhExtractor.Dh>>,
): List<Pair<Long, DhExtractor.Dh>> {
    val leadByLine = lead.associate { it }
    return bold.map { (lineIndex, dh) -> lineIndex to (leadByLine[lineIndex] ?: dh) }
}

internal fun indexAllBooks(
    conn: Connection,
    logger: Logger,
    overrides: Map<BookOverrideKey, BookOverride> = loadBookOverrides(),
): LineDhIndexReport {
    var boldBooks = 0
    var dashBooks = 0
    var leadBooks = 0
    var skipped = 0
    var noisy = 0
    var indexed = 0
    var metadataBooks = 0
    var overrideBooks = 0
    var unreviewedBooks = 0

    val books = ArrayList<BookInfo>()
    conn.prepareStatement(
        """
        SELECT b.id, b.title, s.name, b.dependenceType, b.isBaseBook,
               EXISTS (SELECT 1 FROM book_base_text bbt WHERE bbt.bookId = b.id)
        FROM book b
        JOIN source s ON s.id = b.sourceId
        ORDER BY b.id
        """.trimIndent(),
    ).use { ps ->
        ps.executeQuery().use { rs ->
            while (rs.next()) {
                books += BookInfo(
                    id = rs.getLong(1),
                    title = rs.getString(2),
                    source = rs.getString(3),
                    dependenceType = rs.getString(4),
                    isBaseBook = rs.getInt(5) != 0,
                    hasBaseText = rs.getInt(6) != 0,
                )
            }
        }
    }

    conn.prepareStatement(
        "INSERT OR IGNORE INTO line_dh (bookId, dhText, lineIndex, dhDisplay) VALUES (?, ?, ?, ?)",
    ).use { insert ->
        conn.prepareStatement(
            "SELECT lineIndex, content FROM line WHERE bookId = ? ORDER BY lineIndex",
        ).use { selectLines ->
            for (book in books) {
                val override = overrides[BookOverrideKey(book.source, book.title)]
                val metadataEligible =
                    (!book.isBaseBook || book.hasBaseText) &&
                        (book.dependenceType.equals("commentary", ignoreCase = true) ||
                            (book.dependenceType.equals("midrash", ignoreCase = true) && book.hasBaseText))
                when (override?.decision) {
                    BookOverrideDecision.EXCLUDE -> continue
                    BookOverrideDecision.INCLUDE -> overrideBooks++
                    null -> if (metadataEligible) metadataBooks++ else {
                        unreviewedBooks++
                        continue
                    }
                }

                var contentLines = 0
                val bold = ArrayList<Pair<Long, DhExtractor.Dh>>()
                val boldLead = ArrayList<Pair<Long, DhExtractor.Dh>>()
                val dash = ArrayList<Pair<Long, DhExtractor.Dh>>()

                selectLines.setLong(1, book.id)
                selectLines.executeQuery().use { rs ->
                    while (rs.next()) {
                        val lineIndex = rs.getLong(1)
                        val content = rs.getString(2) ?: continue
                        if (content.isBlank() || DhExtractor.isHeadingLine(content)) continue
                        contentLines++
                        DhExtractor.extract(content, DhExtractor.Format.BOLD)?.let {
                            bold += lineIndex to it
                            DhExtractor.extract(content, DhExtractor.Format.BOLD_LEAD)
                                ?.let { lead -> boldLead += lineIndex to lead }
                        }
                        DhExtractor.extract(content, DhExtractor.Format.DASH)
                            ?.let { dash += lineIndex to it }
                    }
                }

                val leadBook = bold.isNotEmpty() && boldLead.size >= LEAD_SHARE * bold.size
                val boldHits = if (leadBook) mergeLeadHits(bold, boldLead) else bold
                fun qualify(hits: List<Pair<Long, DhExtractor.Dh>>, isBold: Boolean): FormatCandidate? =
                    hits.takeIf {
                        contentLines > 0 && it.size >= MIN_LINES && it.size.toDouble() / contentLines >= MIN_COVERAGE
                    }?.let { FormatCandidate(it, NoiseRatios.of(it.map(Pair<Long, DhExtractor.Dh>::second)), isBold) }

                val qualified = listOfNotNull(qualify(boldHits, true), qualify(dash, false))
                val winner = qualified.filterNot { it.ratios.isNoisy }.maxWithOrNull(
                    compareBy<FormatCandidate> { it.hits.size }.thenBy { it.bold },
                )
                if (winner == null && qualified.isEmpty()) {
                    if (contentLines > 0) skipped++
                    continue
                }
                if (winner == null) {
                    noisy++
                    val rejected = qualified.maxBy { it.hits.size }
                    logger.i {
                        "line_dh: dropping noisy book ${book.id} '${book.title}' " +
                            "(distinct=${"%.2f".format(rejected.ratios.distinctRatio)}, " +
                            "oneWord=${"%.2f".format(rejected.ratios.oneWordRatio)}, " +
                            "opener=${"%.2f".format(rejected.ratios.openerRatio)}, hits=${rejected.hits.size})"
                    }
                    continue
                }

                for ((lineIndex, dh) in winner.hits) {
                    insert.setLong(1, book.id)
                    insert.setString(2, dh.key)
                    insert.setLong(3, lineIndex)
                    insert.setString(4, dh.display)
                    insert.addBatch()
                }
                insert.executeBatch()
                indexed += winner.hits.size
                if (winner.bold) {
                    boldBooks++
                    if (leadBook) {
                        leadBooks++
                        logger.i { "line_dh: lead-bold book ${book.id} '${book.title}' (${boldLead.size} of ${bold.size} bold lines run on to a marker)" }
                    }
                } else {
                    dashBooks++
                }
                if ((boldBooks + dashBooks) % 500 == 0) {
                    logger.i { "line_dh: ${boldBooks + dashBooks} books, $indexed dibburim" }
                }
            }
        }
    }

    return LineDhIndexReport(
        boldBooks, dashBooks, skipped, noisy, indexed, leadBooks,
        metadataBooks, overrideBooks, unreviewedBooks,
    )
}
