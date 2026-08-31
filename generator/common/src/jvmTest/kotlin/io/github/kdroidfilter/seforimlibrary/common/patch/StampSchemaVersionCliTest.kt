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
