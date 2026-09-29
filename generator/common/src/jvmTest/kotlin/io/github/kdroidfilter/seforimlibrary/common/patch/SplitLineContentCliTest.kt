package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SplitLineContentCliTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `schema 5 working shape becomes schema 6 with byte-exact dedup of version text`() {
        val db = workingShapeDb("split.db")
        val before = connect(db).use { lineTexts(it, "SELECT id, content FROM line") }
        val versionBefore = connect(db).use { versionRows(it) }

        val report = connect(db).use { conn -> splitLineContent(conn, chunkRows = 2, dbFile = db) }

        assertFalse(report.alreadySplit)
        assertEquals(before.size.toLong(), report.lines)
        assertEquals(versionBefore.size.toLong(), report.versionLines)
        assertEquals(2, report.versionLinesInherited)
        connect(db).use { conn ->
            assertTrue(LineContentShape.isSplit(conn))
            assertFalse("content" in columns(conn, "line"))
            assertEquals(before, lineTexts(conn, "SELECT id, content FROM line_content"))
            val versionAfter = versionRows(conn)
            assertEquals(versionBefore.keys, versionAfter.keys, "rowids and keys survive the rebuild")
            for ((rowid, row) in versionBefore) {
                val after = versionAfter.getValue(rowid)
                assertEquals(row.copy(content = null), after.copy(content = null))
                val identical = row.content == before.getValue(row.lineId)
                if (identical) assertNull(after.content, "identical text of row $rowid is inherited")
                else assertEquals(row.content, after.content, "differing text of row $rowid is kept")
            }
            assertEquals(emptyList(), fkViolations(conn))
            assertTrue("idx_version_line_line" in indexes(conn, "version_line"))
            assertTrue("idx_line_book_index" in indexes(conn, "line"))
            stampSchemaVersion(conn, dbVersion = 30, dbSchemaVersion = 6)
        }
    }

    @Test
    fun `second run only validates and leaves the schema 6 hash unchanged`() {
        val db = workingShapeDb("idempotent.db")
        connect(db).use { splitLineContent(it, chunkRows = 3) }
        val first = connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) }

        val again = connect(db).use { splitLineContent(it, chunkRows = 3) }

        assertTrue(again.alreadySplit)
        val second = connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) }
        assertEquals(first, second)
        assertEquals(LogicalContentHasher.TABLES_SCHEMA_6, first.tableHashes.keys.toList())
    }

    @Test
    fun `an interrupted copy resumes after the last committed chunk`() {
        val db = workingShapeDb("resume.db")
        val before = connect(db).use { lineTexts(it, "SELECT id, content FROM line") }
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    "CREATE TABLE line_content (id INTEGER PRIMARY KEY NOT NULL, content TEXT NOT NULL, " +
                        "FOREIGN KEY (id) REFERENCES line(id) ON DELETE CASCADE)",
                )
                val firstTwo = "SELECT id FROM line ORDER BY id LIMIT 2"
                st.execute("INSERT INTO line_content SELECT id, content FROM line WHERE id IN ($firstTwo)")
                st.execute("UPDATE line SET content = '' WHERE id IN ($firstTwo)")
            }
        }

        connect(db).use { splitLineContent(it, chunkRows = 2) }

        connect(db).use { conn ->
            assertEquals(before, lineTexts(conn, "SELECT id, content FROM line_content"))
            validateSplit(conn)
        }
    }

    @Test
    fun `peak file size stays near the original on a larger synthetic DB`() {
        val db = workingShapeDb("measure.db", extraLines = 4_000, extraLineBytes = 3_000)
        val originalBytes = Files.size(db)

        val report = connect(db).use { splitLineContent(it, chunkRows = 500, dbFile = db) }

        println("synthetic split: original=$originalBytes bytes; ${report.describe()}")
        assertEquals(2_000L + 2, report.versionLinesInherited)
        val peakBytes = report.peakPageCount * report.pageSize
        // A chunk of headroom plus b-tree slack, never a second copy of the text.
        assertTrue(peakBytes < originalBytes * 1.05, "peak $peakBytes vs original $originalBytes")
        // The deduplicated edition text is what is left on the (zeroed) freelist.
        assertTrue(report.freelistAfter * report.pageSize > 2_000L * 3_000 * 0.8, report.describe())
    }

    // ── fixtures ────────────────────────────────────────────────────────────

    private data class VersionRow(val versionId: Long, val lineId: Long, val content: String?, val charCount: Long)

    private fun workingShapeDb(name: String, extraLines: Int = 0, extraLineBytes: Int = 0): Path {
        val db = tmp.root.toPath().resolve(name)
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'src')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'book')")
                // Non-contiguous ids, a negative one and one past 2^53.
                st.execute(
                    "INSERT INTO line(id, bookId, lineIndex, content, heRef, charCount) VALUES " +
                        "(-5, 1, 0, 'alpha', 'r0', 5), (3, 1, 1, 'Beta', 'r1', 4), (4, 1, 2, 'gamma ', NULL, 6), " +
                        "(10, 1, 3, '<b>delta</b>', 'r3', 5), (9007199254740993, 1, 4, '', 'r4', 0)",
                )
                st.execute(
                    "INSERT INTO book_version(id, bookId, versionTitle, hasContent) VALUES (1, 1, 'A', 1), (2, 1, 'B', 1)",
                )
                // Identical (twice), case-only and trailing-space differences, two plain differences.
                st.execute(
                    "INSERT INTO version_line(versionId, lineId, content, charCount) VALUES " +
                        "(1, -5, 'alpha', 5), (1, 3, 'beta', 4), (1, 4, 'gamma', 5), " +
                        "(2, 10, '<b>delta</b>', 5), (2, 3, 'other', 5), (2, 9007199254740993, 'x', 1)",
                )
            }
            if (extraLines > 0) {
                val text = "w".repeat(extraLineBytes)
                conn.autoCommit = false
                conn.prepareStatement(
                    "INSERT INTO line(id, bookId, lineIndex, content, charCount) VALUES (?, 1, ?, ?, 0)",
                ).use { ps ->
                    for (i in 0 until extraLines) {
                        ps.setLong(1, 1_000L + i); ps.setInt(2, 100 + i); ps.setString(3, "$i $text")
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
                // Half the lines get an identical edition text, a quarter a different one.
                conn.prepareStatement(
                    "INSERT INTO version_line(versionId, lineId, content) VALUES (?, ?, ?)",
                ).use { ps ->
                    for (i in 0 until extraLines) {
                        val differs = i % 4 == 1
                        if (i % 2 == 1 && !differs) continue
                        ps.setLong(1, if (differs) 2L else 1L); ps.setLong(2, 1_000L + i)
                        ps.setString(3, if (differs) "$i $text!" else "$i $text")
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
                conn.commit()
                conn.autoCommit = true
            }
        }
        return db
    }

    private fun connect(db: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}")

    private fun lineTexts(conn: Connection, sql: String): Map<Long, String> =
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs -> buildMap { while (rs.next()) put(rs.getLong(1), rs.getString(2)) } }
        }

    private fun versionRows(conn: Connection): Map<Long, VersionRow> =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT rowid, versionId, lineId, content, charCount FROM version_line").use { rs ->
                buildMap {
                    while (rs.next()) {
                        put(rs.getLong(1), VersionRow(rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getLong(5)))
                    }
                }
            }
        }

    private fun columns(conn: Connection, table: String): Set<String> =
        PatchDbSchema.readTableInfo(conn, "main", table).mapTo(HashSet()) { it.name }

    private fun indexes(conn: Connection, table: String): Set<String> =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT name FROM pragma_index_list('$table')").use { rs ->
                buildSet { while (rs.next()) add(rs.getString(1)) }
            }
        }

    private fun fkViolations(conn: Connection): List<String> =
        listOf("line_content", "version_line").flatMap { table ->
            conn.createStatement().use { st ->
                st.executeQuery("PRAGMA foreign_key_check(\"$table\")").use { rs ->
                    buildList { while (rs.next()) add("${rs.getString(1)}:${rs.getLong(2)}") }
                }
            }
        }
}
