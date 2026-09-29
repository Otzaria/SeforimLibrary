package io.github.kdroidfilter.seforimlibrary.common.patch

import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StampSchemaVersionCliTest {

    @Test
    fun `schema 4 stamp requires both derived index tables before writing metadata`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '23'), ('db_schema_version', '3')")
                st.execute(
                    "CREATE TABLE line_ref (bookId INTEGER, refKeyHash INTEGER, lineIndex INTEGER, " +
                        "PRIMARY KEY (bookId, refKeyHash, lineIndex)) WITHOUT ROWID",
                )
            }

            val error = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 24, dbSchemaVersion = 4)
            }
            assertTrue("line_dh" in error.message.orEmpty())
            assertEquals("23", meta(conn, "db_version"))
            assertEquals("3", meta(conn, "db_schema_version"))

            conn.createStatement().use { st ->
                st.execute(
                    "CREATE TABLE line_dh (bookId INTEGER, dhText TEXT, lineIndex INTEGER, " +
                        "PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
                )
            }
            stampSchemaVersion(conn, dbVersion = 24, dbSchemaVersion = 4)
            assertEquals("24", meta(conn, "db_version"))
            assertEquals("4", meta(conn, "db_schema_version"))
            assertTrue(conn.autoCommit)
        }
    }

    @Test
    fun `schema 5 stamp requires dhDisplay TEXT NOT NULL before writing metadata`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '26'), ('db_schema_version', '4')")
                st.execute(
                    "CREATE TABLE line_ref (bookId INTEGER, refKeyHash INTEGER, lineIndex INTEGER, " +
                        "PRIMARY KEY (bookId, refKeyHash, lineIndex)) WITHOUT ROWID",
                )
                st.execute(
                    "CREATE TABLE line_dh (bookId INTEGER, dhText TEXT, lineIndex INTEGER, " +
                        "PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
                )
            }

            val error = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 27, dbSchemaVersion = 5)
            }
            assertTrue("line_dh.dhDisplay" in error.message.orEmpty())
            assertEquals("26", meta(conn, "db_version"))
            assertEquals("4", meta(conn, "db_schema_version"))

            conn.createStatement().use {
                it.execute("ALTER TABLE line_dh ADD COLUMN dhDisplay TEXT NOT NULL DEFAULT ''")
            }
            stampSchemaVersion(conn, dbVersion = 27, dbSchemaVersion = 5)
            assertEquals("27", meta(conn, "db_version"))
            assertEquals("5", meta(conn, "db_schema_version"))
            assertTrue(conn.autoCommit)
        }
    }

    @Test
    fun `schema 6 stamp requires the line_content split and a nullable version text`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
                st.execute("INSERT INTO schema_meta VALUES ('db_version', '28'), ('db_schema_version', '5')")
                st.execute("CREATE TABLE line_ref (bookId INTEGER, refKeyHash INTEGER, lineIndex INTEGER)")
                st.execute("CREATE TABLE line_dh (bookId INTEGER, dhText TEXT, lineIndex INTEGER, dhDisplay TEXT NOT NULL)")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, content TEXT NOT NULL)")
                st.execute("CREATE TABLE version_line (versionId INTEGER, lineId INTEGER, content TEXT NOT NULL)")
            }

            val missing = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 29, dbSchemaVersion = 6)
            }
            assertTrue("line_content" in missing.message.orEmpty())

            conn.createStatement().use { it.execute("CREATE TABLE line_content (id INTEGER PRIMARY KEY, content TEXT NOT NULL)") }
            val unsplit = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 29, dbSchemaVersion = 6)
            }
            assertTrue("line.content" in unsplit.message.orEmpty())

            conn.createStatement().use { it.execute("ALTER TABLE line DROP COLUMN content") }
            val notNull = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 29, dbSchemaVersion = 6)
            }
            assertTrue("version_line.content" in notNull.message.orEmpty())
            assertEquals("5", meta(conn, "db_schema_version"))

            conn.createStatement().use { st ->
                st.execute("DROP TABLE version_line")
                st.execute("CREATE TABLE version_line (versionId INTEGER, lineId INTEGER, content TEXT)")
            }
            stampSchemaVersion(conn, dbVersion = 29, dbSchemaVersion = 6)
            assertEquals("29", meta(conn, "db_version"))
            assertEquals("6", meta(conn, "db_schema_version"))
        }
    }

    @Test
    fun `schema 3 remains stampable when an unsigned line ref table is present`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
                st.execute("CREATE TABLE line_ref (bookId INTEGER, refKeyHash INTEGER, lineIndex INTEGER)")
            }
            stampSchemaVersion(conn, dbVersion = 23, dbSchemaVersion = 3)
            assertEquals("23", meta(conn, "db_version"))
            assertEquals("3", meta(conn, "db_schema_version"))
        }
    }

    private fun meta(conn: java.sql.Connection, key: String): String =
        conn.prepareStatement("SELECT value FROM schema_meta WHERE key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeQuery().use { rs -> rs.next(); rs.getString(1) }
        }
}
