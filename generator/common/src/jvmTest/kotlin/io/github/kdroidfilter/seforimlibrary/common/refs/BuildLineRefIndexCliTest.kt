package io.github.kdroidfilter.seforimlibrary.common.refs

import co.touchlab.kermit.Logger
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

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
                st.execute("INSERT INTO line VALUES (1, 0, 'ישעיהו')")
                st.execute("INSERT INTO line VALUES (1, 1, 'ישעיהו לב, יא')")
                st.execute("INSERT INTO line VALUES (2, 0, 'כותרת')")
                st.execute("INSERT INTO line VALUES (3, 0, 'ברכות ב., א')")
            }

            val report = indexAllBooks(conn, Logger.withTag("BuildLineRefIndexCliTest"))

            assertEquals(listOf("לא תואם"), report.titleMismatchBooks)
            assertEquals(2, report.indexed)
        }
    }
}
