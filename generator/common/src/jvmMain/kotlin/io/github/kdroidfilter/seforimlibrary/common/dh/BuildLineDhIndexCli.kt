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
                "${report.dashBooks} dash-format books (${report.skippedBooks} books below threshold)"
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

internal data class LineDhIndexReport(
    val boldBooks: Int,
    val dashBooks: Int,
    val skippedBooks: Int,
    val indexed: Int,
)

internal fun indexAllBooks(conn: Connection, logger: Logger): LineDhIndexReport {
    var boldBooks = 0
    var dashBooks = 0
    var skipped = 0
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
                val bold = ArrayList<Pair<Long, DhExtractor.Dh>>()
                val dash = ArrayList<Pair<Long, DhExtractor.Dh>>()

                selectLines.setLong(1, bookId)
                selectLines.executeQuery().use { rs ->
                    while (rs.next()) {
                        val lineIndex = rs.getLong(1)
                        val content = rs.getString(2) ?: continue
                        if (content.isBlank() || DhExtractor.isHeadingLine(content)) continue
                        contentLines++
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

    return LineDhIndexReport(boldBooks, dashBooks, skipped, indexed)
}
