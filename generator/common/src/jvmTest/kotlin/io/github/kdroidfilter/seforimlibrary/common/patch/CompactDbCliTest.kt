package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CompactDbCliTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `split DB is rewritten without its freelist through a scratch directory`() {
        val db = splitDb("compact.db")
        val scratch = tmp.newFolder("scratch").toPath()
        val before = connect(db).use { snapshot(it) }
        val (freelistBefore, sizeBefore) = connect(db).use { pragma(it, "freelist_count") } to Files.size(db)
        assertTrue(freelistBefore > 0, "the split must leave a freelist for this test to mean anything")

        val report = compactDatabase(db, scratch)

        assertTrue(report.compacted)
        assertEquals(freelistBefore, report.freelistPagesBefore)
        assertTrue(Files.size(db) < sizeBefore, "file shrinks: ${Files.size(db)} vs $sizeBefore")
        assertEquals(Files.size(db), report.bytesAfter)
        connect(db).use { conn ->
            assertEquals(0, pragma(conn, "freelist_count"))
            assertEquals(before, snapshot(conn), "every row and the split shape survive")
            validateSplit(conn)
        }
        assertEquals(emptyList(), Files.list(scratch).use { it.toList() }, "no scratch copy is left behind")
    }

    @Test
    fun `compact DB is left untouched`() {
        val db = splitDb("already.db")
        compactDatabase(db, tmp.newFolder("first").toPath())
        val bytes = Files.readAllBytes(db)

        val report = compactDatabase(db, tmp.newFolder("second").toPath())

        assertFalse(report.compacted)
        assertTrue(bytes.contentEquals(Files.readAllBytes(db)))
    }

    @Test
    fun `stale scratch copy from an interrupted run is replaced`() {
        val db = splitDb("stale.db")
        val scratch = tmp.newFolder("stale-scratch").toPath()
        Files.writeString(scratch.resolve("stale.db.compact"), "left over")

        compactDatabase(db, scratch)

        connect(db).use { assertEquals(0, pragma(it, "freelist_count")) }
    }

    @Test
    fun `planner statistics of an analyzed DB survive the compaction`() {
        val db = splitDb("analyzed.db")
        connect(db).use { conn -> conn.createStatement().use { it.execute("ANALYZE") } }
        val before = connect(db).use { stat1(it) }
        assertTrue(before.any { it.startsWith("line_content|") }, "ANALYZE covers the split table: $before")

        assertTrue(compactDatabase(db, tmp.newFolder("analyzed-scratch").toPath()).compacted)

        // PatchDbProducer ships these rows as stat1_snapshot, so they must not be lost here.
        assertEquals(before, connect(db).use { stat1(it) })
    }

    /** A schema-5 working-shape DB with enough text that the split frees pages. */
    private fun splitDb(name: String): Path {
        val db = tmp.root.toPath().resolve(name)
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("PRAGMA page_size = 4096")
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY)")
                st.execute("CREATE TABLE book_version (id INTEGER PRIMARY KEY, bookId INTEGER NOT NULL)")
                st.execute(
                    "CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL, " +
                        "lineIndex INTEGER NOT NULL, content TEXT NOT NULL, heRef TEXT)",
                )
                st.execute("CREATE INDEX idx_line_book_index ON line(bookId, lineIndex)")
                st.execute(
                    "CREATE TABLE version_line (versionId INTEGER NOT NULL, lineId INTEGER NOT NULL, " +
                        "content TEXT NOT NULL, charCount INTEGER NOT NULL DEFAULT 0, " +
                        "PRIMARY KEY (versionId, lineId), " +
                        "FOREIGN KEY (versionId) REFERENCES book_version(id) ON DELETE CASCADE, " +
                        "FOREIGN KEY (lineId) REFERENCES line(id) ON DELETE CASCADE)",
                )
                st.execute("CREATE INDEX idx_version_line_line ON version_line(lineId)")
                st.execute("INSERT INTO book(id) VALUES (1)")
                st.execute("INSERT INTO book_version(id, bookId) VALUES (1, 1)")
                st.execute(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) " +
                        "INSERT INTO line(id, bookId, lineIndex, content, heRef) " +
                        "SELECT i, 1, i - 1, printf('%.400c line %d', 'x', i), 'ref ' || i FROM n",
                )
                // Half the edition repeats the base text: those rows become NULL and free pages.
                st.execute(
                    "INSERT INTO version_line(versionId, lineId, content, charCount) " +
                        "SELECT 1, id, CASE WHEN id % 2 = 0 THEN content ELSE content || ' variant' END, 400 FROM line",
                )
            }
            splitLineContent(conn, chunkRows = 300)
        }
        return db
    }

    private fun snapshot(conn: Connection): List<String> = conn.createStatement().use { st ->
        st.executeQuery(
            "SELECT 'l|' || l.id || '|' || l.lineIndex || '|' || lc.content FROM line l JOIN line_content lc ON lc.id = l.id " +
                "UNION ALL SELECT 'v|' || versionId || '|' || lineId || '|' || IFNULL(content, '<inherit>') FROM version_line " +
                "ORDER BY 1",
        ).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
    }

    private fun stat1(conn: Connection): List<String> = conn.createStatement().use { st ->
        st.executeQuery("SELECT tbl || '|' || IFNULL(idx, '') || '|' || stat FROM sqlite_stat1 ORDER BY 1")
            .use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
    }

    private fun pragma(conn: Connection, name: String): Long =
        conn.createStatement().use { st -> st.executeQuery("PRAGMA $name").use { rs -> rs.next(); rs.getLong(1) } }

    private fun connect(db: Path): Connection = DriverManager.getConnection("jdbc:sqlite:$db")
}
