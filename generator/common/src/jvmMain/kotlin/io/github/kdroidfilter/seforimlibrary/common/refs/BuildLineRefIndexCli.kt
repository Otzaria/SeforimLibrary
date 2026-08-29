package io.github.kdroidfilter.seforimlibrary.common.refs

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.core.refs.RefKey
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Fills `line_ref` — the canonical (bookId, refKeyHash) -> lineIndex index the
 * Otzaria client uses to resolve a typed reference to an exact line.
 *
 * Runs as the last DB-writing stage (`:generator-common:buildLineRefIndex`),
 * after every book-writing stage, and is idempotent: the table is rebuilt from
 * scratch on each run.
 *
 * Required system property: `dbPath`.
 *
 * Reports, per build:
 *  - books whose lines carry a heRef that does not start with the book title
 *    (the prefix could not be stripped — those keys stay whole-heRef);
 *  - keys that map to more than one line in the same book (ambiguous refs).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildLineRefIndexCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { st ->
            st.execute(
                """
                CREATE TABLE IF NOT EXISTS line_ref (
                    bookId INTEGER NOT NULL,
                    refKeyHash INTEGER NOT NULL,
                    lineIndex INTEGER NOT NULL,
                    PRIMARY KEY (bookId, refKeyHash, lineIndex)
                ) WITHOUT ROWID
                """.trimIndent(),
            )
            st.execute("DELETE FROM line_ref")
        }

        val report = indexAllBooks(conn, logger)
        conn.commit()

        logger.i { "line_ref: ${report.indexed} keys over ${report.books} books" }
        if (report.titleMismatchBooks.isNotEmpty()) {
            logger.w {
                "line_ref: ${report.titleMismatchBooks.size} books whose heRefs do not start " +
                    "with the book title, e.g. ${report.titleMismatchBooks.take(10)}"
            }
        }
        if (report.ambiguousKeys > 0) {
            logger.w { "line_ref: ${report.ambiguousKeys} keys resolving to more than one line" }
        }
    }
}

internal data class LineRefIndexReport(
    val books: Int,
    val indexed: Int,
    val ambiguousKeys: Int,
    val titleMismatchBooks: List<String>,
)

internal fun indexAllBooks(conn: Connection, logger: Logger): LineRefIndexReport {
    var books = 0
    var indexed = 0
    var ambiguous = 0
    val mismatched = ArrayList<String>()

    val bookRows = ArrayList<Triple<Long, String, String?>>()
    conn.prepareStatement("SELECT id, title, heRef FROM book ORDER BY id").use { ps ->
        ps.executeQuery().use { rs ->
            while (rs.next()) bookRows += Triple(rs.getLong(1), rs.getString(2), rs.getString(3))
        }
    }

    conn.prepareStatement(
        "INSERT OR IGNORE INTO line_ref (bookId, refKeyHash, lineIndex) VALUES (?, ?, ?)",
    ).use { insert ->
        conn.prepareStatement(
            "SELECT lineIndex, heRef FROM line WHERE bookId = ? AND heRef IS NOT NULL AND heRef <> '' ORDER BY lineIndex",
        ).use { selectLines ->
            for ((bookId, title, bookHeRef) in bookRows) {
                val aliases = listOfNotNull(bookHeRef, title).filter { it.isNotBlank() }
                val titlePrefixes = aliases.map { "${RefKey.tokens(it).joinToString(" ")} " }
                val seen = HashSet<Long>()
                var stripped = 0
                var total = 0

                selectLines.setLong(1, bookId)
                selectLines.executeQuery().use { rs ->
                    while (rs.next()) {
                        val lineIndex = rs.getLong(1)
                        val heRef = rs.getString(2) ?: continue
                        total++
                        val key = RefKey.ofLine(heRef, aliases) ?: continue
                        // A key still carrying the title means no alias matched.
                        if (titlePrefixes.none { key.startsWith(it) }) stripped++
                        val hash = RefKey.hash(key)
                        if (!seen.add(hash)) ambiguous++
                        insert.setLong(1, bookId)
                        insert.setLong(2, hash)
                        insert.setLong(3, lineIndex)
                        insert.addBatch()
                        indexed++
                    }
                }
                insert.executeBatch()
                books++
                if (total > 0 && stripped == 0) mismatched += title
                if (books % 1000 == 0) logger.i { "line_ref: $books books, $indexed keys" }
            }
        }
    }

    return LineRefIndexReport(books, indexed, ambiguous, mismatched)
}
