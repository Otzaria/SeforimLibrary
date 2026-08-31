package io.github.kdroidfilter.seforimlibrary.common.dh

import co.touchlab.kermit.Logger
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BuildLineDhIndexCliTest {

    private fun withDb(block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    "CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, " +
                        "isBaseBook INTEGER NOT NULL DEFAULT 0)",
                )
                st.execute(
                    "CREATE TABLE book_base_text (bookId INTEGER NOT NULL, baseBookId INTEGER NOT NULL, " +
                        "PRIMARY KEY (bookId, baseBookId))",
                )
                st.execute("CREATE TABLE line (bookId INTEGER NOT NULL, lineIndex INTEGER NOT NULL, content TEXT NOT NULL)")
                st.execute(
                    "CREATE TABLE line_dh (bookId INTEGER NOT NULL, dhText TEXT NOT NULL, " +
                        "lineIndex INTEGER NOT NULL, PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
                )
            }
            block(conn)
        }
    }

    private fun insertLines(conn: Connection, bookId: Long, lines: List<String>, startIndex: Int = 0) {
        conn.prepareStatement("INSERT INTO line VALUES (?, ?, ?)").use { ps ->
            lines.forEachIndexed { i, content ->
                ps.setLong(1, bookId)
                ps.setInt(2, startIndex + i)
                ps.setString(3, content)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun dhRows(conn: Connection): List<Triple<Long, String, Long>> {
        val out = ArrayList<Triple<Long, String, Long>>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT bookId, dhText, lineIndex FROM line_dh ORDER BY bookId, lineIndex").use { rs ->
                while (rs.next()) out += Triple(rs.getLong(1), rs.getString(2), rs.getLong(3))
            }
        }
        return out
    }

    @Test
    fun `a dash-dominant book is indexed in dash format, headings skipped`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (1, 'רש\"י על ברכות')") }
            insertLines(
                conn, 1,
                listOf("<h1>רש\"י על ברכות</h1>", "<h2>דף ב.</h2>") +
                    List(12) { "דיבור מספר $it – פירושו של הדיבור" },
            )

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.dashBooks)
            assertEquals(0, report.boldBooks)
            assertEquals(12, report.indexed)
            assertEquals(
                (0L until 12L).map { Triple(1L, "דיבור מספר ${it}", it + 2) },
                dhRows(conn),
            )
        }
    }

    @Test
    fun `a bold-dominant book is indexed in bold format`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (2, 'רש\"י על בראשית')") }
            insertLines(conn, 2, List(10) { "<b>דיבור $it.</b> פירוש כלשהו" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.boldBooks)
            assertEquals(10, report.indexed)
        }
    }

    @Test
    fun `a book with only incidental dashes stays out of the index`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (3, 'ספר רגיל')") }
            // 2 of 20 content lines have a spaced dash — below MIN_COVERAGE.
            insertLines(
                conn, 3,
                List(18) { "שורת טקסט רגילה מספר $it בלי שום מפריד" } +
                    listOf("אחד - שניים", "שלוש - ארבע"),
            )

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.boldBooks + report.dashBooks)
            assertEquals(1, report.skippedBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `a dominant format below the absolute minimum stays out of the index`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (4, 'ספר קצר')") }
            insertLines(conn, 4, List(5) { "דיבור $it – פירוש" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.boldBooks + report.dashBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `base texts are excluded even when dash extraction would dominate`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book VALUES (7, 'בבא קמא', 1)") }
            insertLines(conn, 7, List(12) { "הצד השוה $it – המשך הסוגיה" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.boldBooks + report.dashBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `a dependent commentary remains eligible when also marked as a base book`() {
        withDb { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO book VALUES (8, 'פירוש מסומן גם כבסיס', 1)")
                st.execute("INSERT INTO book VALUES (9, 'ספר הבסיס', 1)")
                st.execute("INSERT INTO book_base_text VALUES (8, 9)")
            }
            insertLines(conn, 8, List(10) { "<b>דיבור $it.</b> פירוש" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.boldBooks)
            assertEquals(10, report.indexed)
        }
    }

    @Test
    fun `rebuilding replaces stale rows`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (5, 'ספר')") }
            insertLines(conn, 5, List(10) { "דיבור $it – פירוש" })
            conn.createStatement().use { it.execute("INSERT INTO line_dh VALUES (99, 'ישן', 99)") }

            rebuildLineDhIndex(conn, Logger.withTag("test"))

            assertEquals(
                (0L until 10L).map { Triple(5L, "דיבור $it", it) },
                dhRows(conn),
            )
        }
    }

    @Test
    fun `failed rebuild rolls back the previous index`() {
        withDb { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO book (id, title) VALUES (6, 'ספר')")
                st.execute("INSERT INTO line VALUES (6, 0, '<b>דיבור.</b> פירוש')")
                st.execute("INSERT INTO line_dh VALUES (99, 'ישן', 99)")
                st.execute(
                    "CREATE TRIGGER reject_line_dh BEFORE INSERT ON line_dh " +
                        "BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
                )
            }

            // Ten rows are needed to pass the book-level minimum and reach the
            // failing INSERT, so add the remaining distinct source rows here.
            insertLines(conn, 6, (1 until 10).map { "<b>דיבור $it.</b> פירוש" }, startIndex = 1)

            assertFailsWith<SQLException> {
                rebuildLineDhIndex(conn, Logger.withTag("test"))
            }
            assertEquals(listOf(Triple(99L, "ישן", 99L)), dhRows(conn))
        }
    }
}
