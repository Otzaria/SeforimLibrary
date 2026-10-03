package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/** Site-curated book info: the generation source. */
internal const val BOOK_INFO_FILE = "book_info.csv"

/** The previous generation source, read only from an archive without [BOOK_INFO_FILE]. */
internal const val GENERATIONS_FILE = "generations.csv"

private val BOOK_INFO_HEADER =
    listOf("bookName", "authorName", "generationName", "subGenerationName", "startYear", "endYear")

/**
 * Seeds the `generation` table and links books to their generation, driven by
 * otzaria-library/ForDB/book_info.csv — the per-book info edited on the Otzaria
 * website (one PR per edit). An older ForDB archive that predates it falls back
 * to ForDB/generations.csv (`שם ספר,קבוצת דור` with header); see [loadGenerationRows].
 *
 * Runs AFTER appendOtzaria so both Sefaria- and Otzaria-stage books are
 * considered. Linking is purely by book title — no transitive author-level
 * propagation — so per-book generations in the CSV are preserved (e.g. the
 * empty-author bucket where books legitimately span eras, אברבנאל's
 * ראשונים/אחרונים split).
 *
 * Download failures are fatal because silently skipping generation seeding can
 * produce an invalid DB delta.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedGenerations -PseforimDb=/path/to/seforim.db
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SeedGenerations")

    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val dbPath = Paths.get(dbPathStr)

    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    logger.i { "Seeding generations in $dbPath" }

    val rows = loadGenerationRows(logger)

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            val result = try {
                applyGenerations(conn, rows, logger).also {
                    conn.commit()
                }
            } catch (e: Exception) {
                runCatching { conn.rollback() }.onFailure { logger.w(it) { "Rollback failed" } }
                throw e
            }

            logger.i {
                "Generations done: seeded=${result.generationsCreated} " +
                    "book links=${result.linksCreated} unmatched=${result.unmatched}"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to seed generations; aborting" }
        exitProcess(1)
    }
}

/**
 * Generation rows for [applyGenerations]: from [BOOK_INFO_FILE] when the ForDB
 * archive has it, else from [GENERATIONS_FILE]. The fallback keeps an older pinned
 * archive buildable (e.g. a delta test on the released inputs) while otzaria-library
 * moves from one file to the other. Neither file present is fatal.
 */
internal fun loadGenerationRows(logger: Logger): List<Pair<String, String>> {
    downloadOptionalForDbFile(BOOK_INFO_FILE, logger)?.let { return parseBookInfoGenerations(it, logger) }
    val legacy = checkNotNull(downloadOptionalForDbFile(GENERATIONS_FILE, logger)) {
        "ForDB archive has neither $BOOK_INFO_FILE nor $GENERATIONS_FILE"
    }
    logger.w { "$BOOK_INFO_FILE is not in this ForDB archive; reading generations from $GENERATIONS_FILE" }
    return parseGenerations(legacy, logger)
}

/**
 * [BOOK_INFO_FILE] → (book title, generation) pairs, one per title.
 *
 * The file has one row per (book, author), so a title may appear more than once;
 * rows without a generation are skipped. When a title's rows disagree, the most
 * frequent generation wins and a tie goes to the first row in the file (it is
 * sorted by book, then author), with a warning — a book is never linked to two
 * generations. Records are parsed RFC-4180 style, since fields are all quoted.
 */
internal fun parseBookInfoGenerations(lines: List<String>, logger: Logger): List<Pair<String, String>> {
    val records = parseForDbCsvRecords(lines.joinToString("\n"))
    require(records.isNotEmpty()) { "$BOOK_INFO_FILE is empty" }
    require(records.first() == BOOK_INFO_HEADER) {
        "$BOOK_INFO_FILE must start with the header ${BOOK_INFO_HEADER.joinToString(",")}"
    }

    val byTitle = LinkedHashMap<String, MutableList<String>>()
    for ((index, fields) in records.drop(1).withIndex()) {
        require(fields.size == BOOK_INFO_HEADER.size && fields[0].isNotEmpty()) {
            "$BOOK_INFO_FILE record ${index + 2} is malformed: $fields"
        }
        val generation = fields[2]
        if (generation.isEmpty()) continue
        byTitle.getOrPut(fields[0]) { mutableListOf() } += generation
    }

    val conflicts = mutableListOf<String>()
    val rows = byTitle.map { (title, generations) ->
        val counts = generations.groupingBy { it }.eachCount()
        val best = counts.values.max()
        val chosen = generations.first { counts.getValue(it) == best }
        if (counts.size > 1) conflicts += "'$title' → $chosen (${counts.keys.joinToString("/")})"
        title to chosen
    }
    if (conflicts.isNotEmpty()) {
        logger.w {
            "$BOOK_INFO_FILE: ${conflicts.size} book(s) have rows with different generations; using " +
                conflicts.take(20).joinToString()
        }
    }
    return rows
}

/**
 * `שם ספר,קבוצת דור` rows. Requires the header row, ignores blank rows with a
 * visible warning, and fails on malformed non-blank data rows.
 */
internal fun parseGenerations(lines: List<String>, logger: Logger): List<Pair<String, String>> {
    val sourceName = GENERATIONS_FILE
    val blankRows = lines.count { parseForDbCsvLine(it).all { field -> field.trim().isEmpty() } }
    if (blankRows > 0) {
        logger.w { "$sourceName: ignoring $blankRows blank row(s)" }
    }
    val nonBlank = lines.filter { it.isNotBlank() }
    require(nonBlank.isNotEmpty()) { "$sourceName is empty" }
    require("שם ספר" in nonBlank.first()) { "$sourceName must start with a שם ספר header" }
    val duplicateHeader = nonBlank.drop(1).indexOfFirst { "שם ספר" in parseForDbCsvLine(it).firstOrNull().orEmpty() }
    require(duplicateHeader < 0) {
        "$sourceName contains a duplicate header at non-blank row ${duplicateHeader + 2}"
    }
    return parseRequiredCsvRows(nonBlank.drop(1), sourceName, minFields = 2)
        .map { f -> f[0] to f[1] }
}

internal data class GenerationApplyResult(
    val generationsCreated: Int,
    val linksCreated: Int,
    val unmatched: Int,
)

/**
 * Seeds the `generation` table with distinct names and links each book to its
 * generation via `book_generation`. Matching is exact by book title; data
 * mismatches fail the task so the CSV can be corrected instead of guessed.
 * INSERT OR IGNORE keeps re-runs idempotent.
 */
internal fun applyGenerations(
    conn: Connection,
    rows: List<Pair<String, String>>,
    logger: Logger,
): GenerationApplyResult {
    if (rows.isEmpty()) return GenerationApplyResult(0, 0, 0)

    var generationsCreated = 0
    conn.prepareStatement("INSERT OR IGNORE INTO generation(name) VALUES (?)").use { stmt ->
        for (name in rows.map { it.second }.distinct()) {
            stmt.setString(1, name)
            generationsCreated += stmt.executeUpdate()
        }
    }

    val nameToId = HashMap<String, Long>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, name FROM generation").use { rs ->
            while (rs.next()) nameToId[rs.getString(2)] = rs.getLong(1)
        }
    }

    val books = ArrayList<Pair<Long, String>>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, title FROM book").use { rs ->
            while (rs.next()) books += rs.getLong(1) to rs.getString(2)
        }
    }
    val exactMap = books.groupBy({ it.second }, { it.first })

    var linksCreated = 0
    val unmatchedTitles = mutableListOf<String>()
    conn.prepareStatement("INSERT OR IGNORE INTO book_generation(bookId, generationId) VALUES (?, ?)").use { linkStmt ->
        for ((bookTitle, genName) in rows) {
            val genId = nameToId[genName] ?: continue
            val bookId = findBookIdForGeneration(exactMap, bookTitle, logger)
            if (bookId == null) {
                unmatchedTitles += bookTitle
                continue
            }
            linkStmt.setLong(1, bookId)
            linkStmt.setLong(2, genId)
            linksCreated += linkStmt.executeUpdate()
        }
    }
    // Don't abort the whole build when generations.csv references books that are
    // not in the current library (upstream otzaria-library vs ForDB drift): keep
    // every link that DID match and just warn about the rest.
    if (unmatchedTitles.isNotEmpty()) {
        logger.w {
            "Generation CSV has ${unmatchedTitles.size} unmatched book title(s) (skipped): " +
                unmatchedTitles.take(20).joinToString()
        }
    }
    // The count MUST be the one the warning above reports. It used to be
    // hard-coded to 0, so the same run printed
    // `Generation CSV has 13 unmatched book title(s)` and then
    // `Generations done: … unmatched=0` — the second line is the one a reader
    // scanning the summary sees, and it said the opposite of the truth.
    return GenerationApplyResult(generationsCreated, linksCreated, unmatchedTitles.size)
}

// `book.title` is not UNIQUE in the schema, so even an exact match can return
// more than one row. Fail rather than arbitrarily picking one.
private fun findBookIdForGeneration(
    exactMap: Map<String, List<Long>>,
    title: String,
    logger: Logger,
): Long? {
    exactMap[title]?.let { return pickOne(it, title, "exact", logger) }
    return null
}

private fun pickOne(matches: List<Long>, title: String, tier: String, logger: Logger): Long? =
    if (matches.size == 1) matches.single()
    else {
        logger.e { "Generation link: '$title' has multiple $tier matches" }
        error("Generation link '$title' has ${matches.size} $tier matches")
    }
