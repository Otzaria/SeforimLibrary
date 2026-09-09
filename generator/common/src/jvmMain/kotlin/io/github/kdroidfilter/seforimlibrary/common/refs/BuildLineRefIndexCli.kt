package io.github.kdroidfilter.seforimlibrary.common.refs

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
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
        val report = rebuildLineRefIndex(conn, logger)
        logger.i { "line_ref: ${report.indexed} keys over ${report.books} books" }
        if (report.titleMismatchBooks.isNotEmpty()) {
            logger.w {
                "line_ref: ${report.titleMismatchBooks.size} books whose heRefs do not start " +
                    "with the book title, e.g. ${report.titleMismatchBooks.take(10)}"
            }
        }
        if (report.ambiguousKeys > 0) {
            // "90 keys" alone is unactionable: nothing said WHICH refs collide,
            // so nobody could look at the data. The colliding refs are named
            // here (bounded) and listed in full in the report file.
            logger.w {
                "line_ref: ${report.ambiguousKeys} keys resolving to more than one line, e.g. " +
                    report.ambiguous.take(MAX_REPORTED_AMBIGUOUS).joinToString {
                        "'${it.bookTitle}' ${it.heRef} (lines ${it.firstLineIndex}, ${it.secondLineIndex})"
                    }
            }
            GeneratorReport.write("line-ref-ambiguous-keys", logger) {
                put("ambiguousKeys", report.ambiguousKeys.toLong())
                put("indexedKeys", report.indexed.toLong())
                put("books", report.books.toLong())
                putRows(
                    "ambiguous",
                    report.ambiguous.map {
                        mapOf<String, Any?>(
                            "bookId" to it.bookId,
                            "bookTitle" to it.bookTitle,
                            "heRef" to it.heRef,
                            "firstLineIndex" to it.firstLineIndex,
                            "secondLineIndex" to it.secondLineIndex,
                        )
                    },
                )
            }
        }
    }
}

/** How many colliding refs the single WARN line names before deferring to the report file. */
private const val MAX_REPORTED_AMBIGUOUS = 20

/** One ref key that two or more lines of the same book resolve to. */
internal data class AmbiguousLineRef(
    val bookId: Long,
    val bookTitle: String,
    val heRef: String,
    val firstLineIndex: Long,
    val secondLineIndex: Long,
)

internal data class LineRefIndexReport(
    val books: Int,
    val indexed: Int,
    val ambiguousKeys: Int,
    val titleMismatchBooks: List<String>,
    val ambiguous: List<AmbiguousLineRef> = emptyList(),
)

internal fun rebuildLineRefIndex(conn: Connection, logger: Logger): LineRefIndexReport {
    check(conn.autoCommit) { "rebuildLineRefIndex requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
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
        indexAllBooks(conn, logger).also { conn.commit() }
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

internal fun indexAllBooks(conn: Connection, logger: Logger): LineRefIndexReport {
    var books = 0
    var indexed = 0
    var ambiguous = 0
    val mismatched = ArrayList<String>()
    // Named collisions, in discovery order: the first line that claimed a hash
    // and the second one that collided with it.
    val ambiguousRefs = ArrayList<AmbiguousLineRef>()

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
                val seen = HashSet<Long>()
                val seenAmbiguous = HashSet<Long>()
                // hash -> (heRef, lineIndex) of the line that claimed it first,
                // so a collision can be reported with both sides named.
                val firstByHash = HashMap<Long, Pair<String, Long>>()
                var hasTitleMismatch = false

                selectLines.setLong(1, bookId)
                selectLines.executeQuery().use { rs ->
                    while (rs.next()) {
                        val lineIndex = rs.getLong(1)
                        val heRef = rs.getString(2) ?: continue
                        if (!RefKey.hasTitleAliasPrefix(heRef, aliases)) hasTitleMismatch = true
                        // ofLine strips a literal title before interpreting ranges. This matters
                        // for titles that themselves contain a hyphen or Hebrew maqaf.
                        val key = RefKey.ofLine(heRef, aliases) ?: continue
                        val hash = RefKey.hash(key)
                        if (!seen.add(hash)) {
                            if (seenAmbiguous.add(hash)) {
                                ambiguous++
                                val (firstRef, firstIndex) = firstByHash[hash] ?: (heRef to lineIndex)
                                ambiguousRefs += AmbiguousLineRef(
                                    bookId = bookId,
                                    bookTitle = title,
                                    heRef = firstRef,
                                    firstLineIndex = firstIndex,
                                    secondLineIndex = lineIndex,
                                )
                            }
                        } else {
                            firstByHash[hash] = heRef to lineIndex
                        }
                        insert.setLong(1, bookId)
                        insert.setLong(2, hash)
                        insert.setLong(3, lineIndex)
                        insert.addBatch()
                        indexed++
                    }
                }
                insert.executeBatch()
                books++
                if (hasTitleMismatch) mismatched += title
                if (books % 1000 == 0) logger.i { "line_ref: $books books, $indexed keys" }
            }
        }
    }

    return LineRefIndexReport(books, indexed, ambiguous, mismatched, ambiguousRefs)
}
