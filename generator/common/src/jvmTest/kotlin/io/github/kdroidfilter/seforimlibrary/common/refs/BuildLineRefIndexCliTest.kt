package io.github.kdroidfilter.seforimlibrary.common.refs

import co.touchlab.kermit.Logger
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BuildLineRefIndexCliTest {

    @Test
    fun `reports only books with line refs that do not start with a title alias`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, heRef TEXT)")
                st.execute("CREATE TABLE line (bookId INTEGER NOT NULL, lineIndex INTEGER NOT NULL, heRef TEXT)")
                st.execute(
                    "CREATE TABLE line_ref (bookId INTEGER NOT NULL, refKeyHash INTEGER NOT NULL, " +
                        "lineIndex INTEGER NOT NULL, PRIMARY KEY (bookId, refKeyHash, lineIndex)) WITHOUT ROWID",
                )
                st.execute("INSERT INTO book VALUES (1, 'ישעיהו', 'ישעיהו')")
                st.execute("INSERT INTO book VALUES (2, 'כותרת', 'כותרת')")
                st.execute("INSERT INTO book VALUES (3, 'לא תואם', 'לא תואם')")
                st.execute("INSERT INTO book VALUES (4, 'יד דוד על השס א-ב', 'יד דוד על השס א-ב')")
                st.execute("INSERT INTO book VALUES (5, 'מגדל־עז או תמת ישרים', 'מגדל־עז או תמת ישרים')")
                st.execute("INSERT INTO book VALUES (6, 'פת לחם', 'פת לחם')")
                st.execute("INSERT INTO line VALUES (1, 0, 'ישעיהו')")
                st.execute("INSERT INTO line VALUES (1, 1, 'ישעיהו לב, יא')")
                st.execute("INSERT INTO line VALUES (2, 0, 'כותרת')")
                st.execute("INSERT INTO line VALUES (3, 0, 'ברכות ב., א')")
                st.execute("INSERT INTO line VALUES (4, 10, 'יד דוד על השס א-ב 10')")
                st.execute("INSERT INTO line VALUES (5, 10, 'מגדל־עז או תמת ישרים 10')")
                st.execute("INSERT INTO line VALUES (6, 10, 'פת לחם, שער רביעי - שער הביטחון, א, א')")
                st.execute("INSERT INTO line VALUES (6, 11, 'פת לחם, שער רביעי - שער הביטחון, א, ב')")
                st.execute("INSERT INTO line VALUES (6, 12, 'פת לחם, שער רביעי - שער הביטחון, א, א!')")
                st.execute("INSERT INTO line VALUES (6, 13, 'פת לחם, שער רביעי - שער הביטחון, א, א?')")
            }

            val report = indexAllBooks(conn, Logger.withTag("BuildLineRefIndexCliTest"))

            assertEquals(listOf("לא תואם"), report.titleMismatchBooks)
            assertEquals(8, report.indexed)
            assertEquals(1, report.ambiguousKeys)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(DISTINCT refKeyHash) FROM line_ref WHERE bookId = 6").use { rs ->
                    assertEquals(2, rs.getInt(1))
                }
            }
        }
    }

    @Test
    fun `failed rebuild rolls back the previous index`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, heRef TEXT)")
                st.execute("CREATE TABLE line (bookId INTEGER NOT NULL, lineIndex INTEGER NOT NULL, heRef TEXT)")
                st.execute(
                    "CREATE TABLE line_ref (bookId INTEGER NOT NULL, refKeyHash INTEGER NOT NULL, " +
                        "lineIndex INTEGER NOT NULL, PRIMARY KEY (bookId, refKeyHash, lineIndex)) WITHOUT ROWID",
                )
                st.execute("INSERT INTO book VALUES (1, 'ישעיהו', 'ישעיהו')")
                st.execute("INSERT INTO line VALUES (1, 1, 'ישעיהו לב, יא')")
                st.execute("INSERT INTO line_ref VALUES (99, 99, 99)")
                st.execute(
                    "CREATE TRIGGER reject_line_ref BEFORE INSERT ON line_ref " +
                        "BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
                )
            }

            assertFailsWith<SQLException> {
                rebuildLineRefIndex(conn, Logger.withTag("BuildLineRefIndexCliTest"))
            }
            conn.createStatement().use { st ->
                st.executeQuery("SELECT bookId, refKeyHash, lineIndex FROM line_ref").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(99L, rs.getLong(1))
                    assertEquals(99L, rs.getLong(2))
                    assertEquals(99L, rs.getLong(3))
                    assertFalse(rs.next())
                }
            }
        }
    }
}
