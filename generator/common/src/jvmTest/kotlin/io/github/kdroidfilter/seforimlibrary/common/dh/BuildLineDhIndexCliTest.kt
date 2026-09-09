package io.github.kdroidfilter.seforimlibrary.common.dh

import co.touchlab.kermit.Logger
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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
                        "lineIndex INTEGER NOT NULL, dhDisplay TEXT NOT NULL, " +
                        "PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
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
    fun `a bold-dominant book is indexed in bold format, display alongside the key`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (2, 'רש\"י על בראשית')") }
            insertLines(conn, 2, List(10) { "<b>דִּבּוּר $it.</b> פירוש כלשהו" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.boldBooks)
            assertEquals(10, report.indexed)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT dhText, dhDisplay FROM line_dh WHERE lineIndex = 3").use { rs ->
                    assertTrue(rs.next())
                    assertEquals("דבור 3", rs.getString(1))
                    assertEquals("דִּבּוּר 3", rs.getString(2))
                }
            }
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
    fun `a book whose bold words are paragraph openers is dropped as noise`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (10, 'שו\"ת')") }
            // Full coverage and fully distinct keys, but every mark is an opener
            // (two of them with Hebrew gershayim, which must fold to ASCII).
            val openers = listOf(
                "והנה", "ועוד", "אמנם", "ובזה", "אך", "אבל", "אלא", "ונראה",
                "הנה", "ומה", "ולפ״ז", "ולענ״ד",
            )
            insertLines(conn, 10, openers.map { "<b>$it</b> דברי המשיב בעניין השאלה" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.noisyBooks)
            assertEquals(0, report.skippedBooks)
            assertEquals(0, report.boldBooks + report.dashBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `a book that bolds the same word in most paragraphs is dropped for low diversity`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (11, 'רלב\"ג על התורה')") }
            // 18 of 20 hits share one key that is not in OPENERS, so only the
            // distinct-ratio rule can reject this book.
            insertLines(
                conn, 11,
                List(18) { "<b>ופירש</b> הכתוב עניין מספר $it" } +
                    listOf("<b>ויאמר אלהים</b> פירוש", "<b>ויהי ערב</b> פירוש"),
            )

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.noisyBooks)
            assertEquals(0, report.boldBooks + report.dashBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `a genuine one-word commentary with distinct non-opener words is kept`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (12, 'מצודת ציון')") }
            val words = listOf("בקר", "ערב", "תהום", "רקיע", "דשא", "מאור", "שרץ", "כנף", "בהמה", "רמש", "צלם", "שבת")
            insertLines(conn, 12, words.map { "<b>$it.</b> ביאור המילה" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.noisyBooks)
            assertEquals(1, report.boldBooks)
            assertEquals(12, report.indexed)
            assertEquals(words, dhRows(conn).map { it.second })
        }
    }

    @Test
    fun `a regular commentary with multi-word dibburim passes the noise gate`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (13, 'רש\"י על שמות')") }
            insertLines(conn, 13, List(12) { "<b>ואלה שמות בני $it</b> פירוש הפסוק" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.noisyBooks)
            assertEquals(1, report.boldBooks)
            assertEquals(12, report.indexed)
        }
    }

    @Test
    fun `noise ratios are computed over the winning hits`() {
        val hits = listOf(
            DhExtractor.Dh("והנה", "והנה"),
            DhExtractor.Dh("והנה", "וְהִנֵּה"),
            DhExtractor.Dh("ולפז", "ולפ״ז"),
            DhExtractor.Dh("בראשית ברא", "בראשית ברא"),
        )

        val ratios = NoiseRatios.of(hits)

        assertEquals(0.75, ratios.distinctRatio)
        assertEquals(0.75, ratios.oneWordRatio)
        assertEquals(0.75, ratios.openerRatio)
        assertTrue(ratios.isNoisy)
        assertEquals(0.5, NoiseRatios.of(hits, linesWithBold = 8).boldStartRatio)
        assertFalse(NoiseRatios(distinctRatio = 1.0, oneWordRatio = 1.0, openerRatio = 0.05).isNoisy)
        assertTrue(NoiseRatios(distinctRatio = 1.0, oneWordRatio = 0.95, openerRatio = 0.10).isNoisy)
        assertFalse(NoiseRatios(distinctRatio = 1.0, oneWordRatio = 0.5, openerRatio = 0.20).isNoisy)
        // Low diversity alone is not noise when the dibburim are phrases, not single words.
        assertFalse(NoiseRatios(distinctRatio = 0.3, oneWordRatio = 0.0, openerRatio = 0.0).isNoisy)
        assertTrue(NoiseRatios(distinctRatio = 0.3, oneWordRatio = 0.9, openerRatio = 0.0).isNoisy)
        assertTrue(NoiseRatios(distinctRatio = 1.0, oneWordRatio = 0.0, openerRatio = 0.0, boldStartRatio = 0.6).isNoisy)
    }

    @Test
    fun `a commentary that repeats the same verse phrase for several notes is kept`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (14, 'תורה תמימה')") }
            val phrases = listOf("אדם שעמלו בחכמה וגו'", "יפה בעתו", "אם קהה הברזל וגו'", "טוב מלא כף וגו'")
            insertLines(conn, 14, List(12) { "<b>${phrases[it % phrases.size]}.</b> ביאור $it" })

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.noisyBooks)
            assertEquals(1, report.boldBooks)
            assertEquals(12, report.indexed)
        }
    }

    @Test
    fun `interleaved bold quotation inside a running explanation is not a dibbur format`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (15, 'חברותא')") }
            val opening = List(10) { "<b>אבות מאי ניהו $it</b> מה הן האבות? הלוא הן <b>יציאות</b> הוצאות." }
            val midLine = List(10) { "אך מקשה הגמרא $it: והרי <b>יציאות תרי</b> שתיים בלבד <b>הויין</b> אחת." }
            insertLines(conn, 15, opening + midLine)

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.noisyBooks)
            assertEquals(0, report.boldBooks)
            assertEquals(emptyList(), dhRows(conn))
        }
    }

    @Test
    fun `a lead-bold book takes the full quotation, other bold lines keep the prefix`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (20, 'חידושי אגדות')") }
            val lead = List(8) { "<b>אין</b> שלום $it כו'. הכא ניחא דמשמע ליה" }
            val plain = List(2) { "<b>והיינו</b> דאמרי אינשי $it בין קני לאורבני" }
            insertLines(conn, 20, lead + plain)

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(1, report.leadBooks)
            assertEquals(1, report.boldBooks)
            assertEquals(10, report.indexed)
            val keys = dhRows(conn).map { it.second }
            assertEquals(List(8) { "אין שלום $it" }, keys.take(8))
            assertEquals(listOf("והיינו", "והיינו"), keys.drop(8))
        }
    }

    @Test
    fun `a bold book with an occasional nearby marker stays a plain bold book`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (21, 'רש\"י')") }
            val plain = List(19) { "<b>מאימתי קורין $it.</b> משעה שהכהנים נכנסין לאכול" }
            val marker = listOf("<b>ואמר</b> רבי כו'. פירוש")
            insertLines(conn, 21, plain + marker)

            val report = indexAllBooks(conn, Logger.withTag("test"))

            assertEquals(0, report.leadBooks)
            assertEquals(1, report.boldBooks)
            assertEquals(20, report.indexed)
            assertEquals("ואמר", dhRows(conn).last().second)
        }
    }

    @Test
    fun `rebuilding replaces stale rows`() {
        withDb { conn ->
            conn.createStatement().use { it.execute("INSERT INTO book (id, title) VALUES (5, 'ספר')") }
            insertLines(conn, 5, List(10) { "דיבור $it – פירוש" })
            conn.createStatement().use { it.execute("INSERT INTO line_dh VALUES (99, 'ישן', 99, 'ישן')") }

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
                st.execute("INSERT INTO line_dh VALUES (99, 'ישן', 99, 'ישן')")
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
