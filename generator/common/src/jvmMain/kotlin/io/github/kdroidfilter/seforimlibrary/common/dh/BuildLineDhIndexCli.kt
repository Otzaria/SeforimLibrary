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
 * A book is indexed only when one extraction format dominates its content
 * lines ([MIN_COVERAGE] of them, at least [MIN_LINES] hits). This book-level
 * gate is the main false-positive defence: books that merely bold an
 * occasional word, or use a spaced dash mid-sentence here and there, never
 * reach the threshold and contribute nothing. Base texts themselves are
 * excluded; a book marked as a base is retained only when `book_base_text`
 * also identifies it as dependent (some curated commentaries carry both
 * flags).
 *
 * A second, per-book noise gate drops books whose winning format marks
 * something other than dibburim: paragraph openers (responsa, mussar,
 * commentaries that bold the first word of every paragraph) show up as low
 * key diversity or a large share of a closed list of opener words
 * ([OPENERS]); interleaved bold (a Gemara quoted in bold inside a running
 * explanation, as in Chavruta) shows up as bold that mostly starts
 * mid-line. See [NoiseRatios].
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
            "line_dh: ${report.indexed} dibburim over ${report.boldBooks} bold-format + " +
                "${report.dashBooks} dash-format books (${report.skippedBooks} books below threshold, " +
                "${report.noisyBooks} books dropped as paragraph-opener noise)"
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

/**
 * In a bold-format book nearly every line that contains `<b>` opens with it.
 * Below this share the bold is interleaved quotation, not a dibbur.
 */
internal const val MIN_BOLD_START_RATIO = 0.75

/** At or above this share of [OPENERS] among a book's hits, the marks are paragraph openers. */
internal const val MAX_OPENER_RATIO = 0.25

/** A book whose hits are this often a single word is checked against the stricter opener bound. */
internal const val ONE_WORD_BOOK_RATIO = 0.9

/** Opener share that disqualifies a one-word book (see [ONE_WORD_BOOK_RATIO]). */
internal const val MAX_OPENER_RATIO_ONE_WORD_BOOK = 0.10

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
 * known paragraph opener, and (bold format only) what share of the lines
 * containing `<b>` actually open with it. [isNoisy] is the gate itself.
 */
internal data class NoiseRatios(
    val distinctRatio: Double,
    val oneWordRatio: Double,
    val openerRatio: Double,
    val boldStartRatio: Double = 1.0,
) {
    val isNoisy: Boolean
        get() = (distinctRatio < MIN_DISTINCT_RATIO && oneWordRatio >= DISTINCT_RULE_ONE_WORD_RATIO) ||
            openerRatio >= MAX_OPENER_RATIO ||
            (oneWordRatio >= ONE_WORD_BOOK_RATIO && openerRatio >= MAX_OPENER_RATIO_ONE_WORD_BOOK) ||
            boldStartRatio < MIN_BOLD_START_RATIO

    companion object {
        /** [linesWithBold] is the number of content lines containing `<b>`; pass 0 for a dash-format book. */
        fun of(hits: List<DhExtractor.Dh>, linesWithBold: Int = 0): NoiseRatios {
            require(hits.isNotEmpty()) { "noise ratios need at least one hit" }
            val total = hits.size.toDouble()
            val distinct = hits.mapTo(HashSet()) { it.key }.size
            val oneWord = hits.count { it.display.trim().split(' ').count(String::isNotEmpty) == 1 }
            val openers = hits.count { normalizeOpener(it.display) in OPENERS }
            val boldStart = if (linesWithBold > 0) minOf(1.0, hits.size / linesWithBold.toDouble()) else 1.0
            return NoiseRatios(distinct / total, oneWord / total, openers / total, boldStart)
        }
    }
}

internal data class LineDhIndexReport(
    val boldBooks: Int,
    val dashBooks: Int,
    val skippedBooks: Int,
    val noisyBooks: Int,
    val indexed: Int,
)

private fun bookTitle(conn: Connection, bookId: Long): String =
    conn.prepareStatement("SELECT title FROM book WHERE id = ?").use { ps ->
        ps.setLong(1, bookId)
        ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) ?: "" else "" }
    }

internal fun indexAllBooks(conn: Connection, logger: Logger): LineDhIndexReport {
    var boldBooks = 0
    var dashBooks = 0
    var skipped = 0
    var noisy = 0
    var indexed = 0

    val bookIds = ArrayList<Long>()
    conn.prepareStatement(
        """
        SELECT b.id
        FROM book b
        WHERE b.isBaseBook = 0
           OR EXISTS (SELECT 1 FROM book_base_text bbt WHERE bbt.bookId = b.id)
        ORDER BY b.id
        """.trimIndent(),
    ).use { ps ->
        ps.executeQuery().use { rs -> while (rs.next()) bookIds += rs.getLong(1) }
    }

    conn.prepareStatement(
        "INSERT OR IGNORE INTO line_dh (bookId, dhText, lineIndex, dhDisplay) VALUES (?, ?, ?, ?)",
    ).use { insert ->
        conn.prepareStatement(
            "SELECT lineIndex, content FROM line WHERE bookId = ? ORDER BY lineIndex",
        ).use { selectLines ->
            for (bookId in bookIds) {
                var contentLines = 0
                var linesWithBold = 0
                val bold = ArrayList<Pair<Long, DhExtractor.Dh>>()
                val dash = ArrayList<Pair<Long, DhExtractor.Dh>>()

                selectLines.setLong(1, bookId)
                selectLines.executeQuery().use { rs ->
                    while (rs.next()) {
                        val lineIndex = rs.getLong(1)
                        val content = rs.getString(2) ?: continue
                        if (content.isBlank() || DhExtractor.isHeadingLine(content)) continue
                        contentLines++
                        if (content.contains("<b>")) linesWithBold++
                        DhExtractor.extract(content, DhExtractor.Format.BOLD)
                            ?.let { bold += lineIndex to it }
                        DhExtractor.extract(content, DhExtractor.Format.DASH)
                            ?.let { dash += lineIndex to it }
                    }
                }

                val winner = if (bold.size >= dash.size) bold else dash
                if (contentLines == 0 ||
                    winner.size < MIN_LINES ||
                    winner.size.toDouble() / contentLines < MIN_COVERAGE
                ) {
                    if (contentLines > 0) skipped++
                    continue
                }

                val ratios = NoiseRatios.of(winner.map { it.second }, if (winner === bold) linesWithBold else 0)
                if (ratios.isNoisy) {
                    noisy++
                    logger.i {
                        "line_dh: dropping noisy book $bookId '${bookTitle(conn, bookId)}' " +
                            "(distinct=${"%.2f".format(ratios.distinctRatio)}, " +
                            "oneWord=${"%.2f".format(ratios.oneWordRatio)}, " +
                            "opener=${"%.2f".format(ratios.openerRatio)}, " +
                            "boldStart=${"%.2f".format(ratios.boldStartRatio)}, hits=${winner.size})"
                    }
                    continue
                }

                for ((lineIndex, dh) in winner) {
                    insert.setLong(1, bookId)
                    insert.setString(2, dh.key)
                    insert.setLong(3, lineIndex)
                    insert.setString(4, dh.display)
                    insert.addBatch()
                }
                insert.executeBatch()
                indexed += winner.size
                if (winner === bold) boldBooks++ else dashBooks++
                if ((boldBooks + dashBooks) % 500 == 0) {
                    logger.i { "line_dh: ${boldBooks + dashBooks} books, $indexed dibburim" }
                }
            }
        }
    }

    return LineDhIndexReport(boldBooks, dashBooks, skipped, noisy, indexed)
}
