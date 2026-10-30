package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/** The commentator entity of books that are not from Sefaria. Optional. */
internal const val COMMENTATOR_COLLECTIVE_TITLES_FILE = "commentator_collective_titles.csv"

private val COLLECTIVE_TITLES_HEADER = listOf("bookName", "collectiveTitleHe")
private val COLLECTIVE_TITLES_HEADER_WITH_EN = COLLECTIVE_TITLES_HEADER + "collectiveTitleEn"

/** One CSV row: the final DB title of a book and its commentator entity. */
internal data class CollectiveTitleRow(val bookName: String, val he: String, val en: String?)

internal data class CollectiveTitlesResult(
    val filled: Int,
    /** Rows whose book already carries the same value (a rerun). */
    val unchanged: Int,
    /** `title: existing ≠ row` — the book already has another value, which is kept. */
    val conflicts: List<String>,
    val unmatched: List<String>,
)

/**
 * Fills `book.collectiveTitleHe` / `collectiveTitleEn` from
 * otzaria-library/ForDB/[COMMENTATOR_COLLECTIVE_TITLES_FILE] for books whose value is NULL.
 *
 * Only Sefaria books get the value at import (the schema's `collective_title`), so
 * the commentators of the other sources rely on this file. A value already in the DB
 * always wins: the Sefaria schema stays authoritative, and a row for such a book is
 * only reported. Runs after every stage that writes or renames books, since rows match
 * the final `book.title` exactly. An archive without the file fills nothing.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedCollectiveTitles -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SeedCollectiveTitles")
    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    try {
        val rows = loadCollectiveTitleRows(logger)
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false
            val result = try {
                applyCollectiveTitles(conn, rows, logger).also { conn.commit() }
            } catch (e: Exception) {
                runCatching { conn.rollback() }
                throw e
            }
            logger.i {
                "Collective titles done: rows=${rows.size} filled=${result.filled} unchanged=${result.unchanged} " +
                    "conflicts=${result.conflicts.size} unmatched=${result.unmatched.size}"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to seed collective titles; aborting" }
        exitProcess(1)
    }
}

/** Reads the optional file from the ForDB archive ([read] is for tests); absent = no rows. */
internal fun loadCollectiveTitleRows(
    logger: Logger,
    read: (String) -> List<String>? = { downloadOptionalForDbFile(it, logger) },
): List<CollectiveTitleRow> {
    val lines = read(COMMENTATOR_COLLECTIVE_TITLES_FILE)
    if (lines == null) {
        logger.w { "$COMMENTATOR_COLLECTIVE_TITLES_FILE is not in this ForDB archive; no collective title is filled" }
        return emptyList()
    }
    return parseCommentatorCollectiveTitles(forDbText(lines))
}

internal fun parseCommentatorCollectiveTitles(text: String): List<CollectiveTitleRow> {
    val records = parseForDbCsvRecords(text)
    if (records.isEmpty()) return emptyList()
    val header = records.first()
    require(header == COLLECTIVE_TITLES_HEADER || header == COLLECTIVE_TITLES_HEADER_WITH_EN) {
        "$COMMENTATOR_COLLECTIVE_TITLES_FILE must start with the header " +
            COLLECTIVE_TITLES_HEADER_WITH_EN.joinToString(",") + " (collectiveTitleEn is optional)"
    }
    val seen = HashSet<String>()
    return records.drop(1).mapIndexed { index, fields ->
        val where = "$COMMENTATOR_COLLECTIVE_TITLES_FILE record ${index + 2}"
        require(fields.size == header.size) { "$where is malformed: $fields" }
        require(fields.all { it == it.trim() }) { "$where has a value with edge spaces: $fields" }
        val (bookName, he) = fields
        require(bookName.isNotEmpty() && he.isNotEmpty()) { "$where has an empty bookName or collectiveTitleHe" }
        // The app compares ״/׳ as "/'; ASCII keeps one spelling in ForDB.
        require(fields.drop(1).none { '״' in it || '׳' in it }) { "$where writes ״/׳; use \" and '" }
        require(seen.add(bookName)) { "$where repeats bookName '$bookName'" }
        CollectiveTitleRow(bookName, he, fields.getOrNull(2)?.takeIf { it.isNotEmpty() })
    }
}

/**
 * Sets the values of every book with the row's exact title whose `collectiveTitleHe` is
 * NULL. `collectiveTitleEn` is set only when the row has one. The caller owns the transaction.
 */
internal fun applyCollectiveTitles(
    conn: Connection,
    rows: List<CollectiveTitleRow>,
    logger: Logger,
): CollectiveTitlesResult {
    if (rows.isEmpty()) return CollectiveTitlesResult(0, 0, emptyList(), emptyList())

    data class Book(val id: Long, val he: String?)
    val booksByTitle = conn.createStatement().use { st ->
        st.executeQuery("SELECT id, title, collectiveTitleHe FROM book ORDER BY id").use { rs ->
            buildList { while (rs.next()) add(rs.getString(2) to Book(rs.getLong(1), rs.getString(3))) }
        }
    }.groupBy({ it.first }, { it.second })
    requireForDbUsesDisplayTitles(
        COMMENTATOR_COLLECTIVE_TITLES_FILE,
        rows.map { it.bookName },
        booksByTitle.keys,
        loadRetitledBooks(conn),
    )

    var filled = 0
    var unchanged = 0
    val conflicts = mutableListOf<String>()
    val unmatched = mutableListOf<String>()
    conn.prepareStatement(
        "UPDATE book SET collectiveTitleHe = ?, collectiveTitleEn = COALESCE(?, collectiveTitleEn) " +
            "WHERE id = ? AND collectiveTitleHe IS NULL",
    ).use { update ->
        for (row in rows) {
            val books = booksByTitle[row.bookName]
            if (books == null) {
                unmatched += row.bookName
                continue
            }
            for (book in books) {
                when (book.he) {
                    null -> {
                        update.setString(1, row.he)
                        update.setString(2, row.en)
                        update.setLong(3, book.id)
                        filled += update.executeUpdate()
                    }
                    row.he -> unchanged++
                    else -> conflicts += "'${row.bookName}': ${book.he} ≠ ${row.he}"
                }
            }
        }
    }

    if (conflicts.isNotEmpty()) {
        logger.w {
            "$COMMENTATOR_COLLECTIVE_TITLES_FILE: ${conflicts.size} book(s) already have another collective " +
                "title, which is kept: ${conflicts.take(20).joinToString()}"
        }
    }
    if (unmatched.isNotEmpty()) {
        logger.w {
            "$COMMENTATOR_COLLECTIVE_TITLES_FILE: ${unmatched.size} row(s) matched no book (skipped): " +
                unmatched.take(20).joinToString()
        }
    }
    return CollectiveTitlesResult(filled, unchanged, conflicts, unmatched)
}
