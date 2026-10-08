package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** ForDB/book_info.csv as the generation source of seedGenerations. */
class BookInfoGenerationsTest {

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }

        fun warnings(): List<String> = lines.filter { it.first == Severity.Warn }.map { it.second }
    }

    private fun capturingLogger(capture: Capture) =
        Logger(StaticConfig(Severity.Verbose, listOf(capture)), "test")

    private val header = "bookName,authorName,generationName,subGenerationName,startYear,endYear"

    @Test
    fun `quoted rows become title-generation pairs, and rows without a generation are skipped`() {
        val capture = Capture()
        val rows = parseBookInfoGenerations(
            listOf(
                header,
                "\"אבן עזרא, פירוש\",\"אברהם אבן עזרא\",\"ראשונים\",\"אחרוני הראשונים\",\"1089\",\"\"",
                "\"בראשית רבה\",\"\",\"חז\"\"ל\",\"אמוראים\",\"300\",\"500\"",
                "\"מפקתא פשיטתא\",\"\",\"\",\"\",\"\",\"\"",
                "",
            ),
            capturingLogger(capture),
        )

        assertEquals(listOf("אבן עזרא, פירוש" to "ראשונים", "בראשית רבה" to "חז\"ל"), rows)
        assertEquals(emptyList(), capture.warnings())
    }

    @Test
    fun `several authors of one book link it once, by majority and then by file order, with a warning`() {
        val capture = Capture()
        val rows = parseBookInfoGenerations(
            listOf(
                header,
                "\"נתיבות עולם\",\"יהודה ליווא בן בצלאל\",\"אחרונים\",\"\",\"1512\",\"1609\"",
                "\"נתיבות עולם\",\"משה בן יעקב מקוצי\",\"ראשונים\",\"\",\"\",\"\"",
                "\"עין יעקב\",\"יעקב אבן חביב\",\"אחרונים\",\"\",\"\",\"\"",
                "\"עין יעקב\",\"לוי בן חביב\",\"אחרונים\",\"\",\"\",\"\"",
                "\"שלושה\",\"א\",\"ראשונים\",\"\",\"\",\"\"",
                "\"שלושה\",\"ב\",\"אחרונים\",\"\",\"\",\"\"",
                "\"שלושה\",\"ג\",\"אחרונים\",\"\",\"\",\"\"",
            ),
            capturingLogger(capture),
        )

        assertEquals(
            listOf("נתיבות עולם" to "אחרונים", "עין יעקב" to "אחרונים", "שלושה" to "אחרונים"),
            rows,
        )
        val warning = capture.warnings().single()
        assertContains(warning, "2 book(s) have rows with different generations")
        assertContains(warning, "'נתיבות עולם' → אחרונים")
    }

    @Test
    fun `a wrong header or a malformed record fails instead of guessing`() {
        val logger = capturingLogger(Capture())
        assertFailsWith<IllegalArgumentException> {
            parseBookInfoGenerations(listOf("שם ספר,קבוצת דור", "בראשית,תורה שבכתב"), logger)
        }
        assertFailsWith<IllegalArgumentException> {
            parseBookInfoGenerations(listOf(header, "\"בראשית\",\"\",\"תורה שבכתב\""), logger)
        }
        assertFailsWith<IllegalArgumentException> {
            parseBookInfoGenerations(listOf(header, "\"\",\"\",\"תורה שבכתב\",\"\",\"\",\"\""), logger)
        }
    }

    @Test
    fun `book_info rows link books exactly like generations rows`() {
        java.sql.DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, heRef TEXT)")
                st.execute("CREATE TABLE generation (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
                st.execute(
                    "CREATE TABLE book_generation (bookId INTEGER NOT NULL, generationId INTEGER NOT NULL, " +
                        "PRIMARY KEY (bookId, generationId))",
                )
                st.execute("INSERT INTO book (id, title) VALUES (1, 'נתיבות עולם')")
            }
            val logger = capturingLogger(Capture())
            val rows = parseBookInfoGenerations(
                listOf(
                    header,
                    "\"נתיבות עולם\",\"יהודה ליווא בן בצלאל\",\"אחרונים\",\"\",\"\",\"\"",
                    "\"נתיבות עולם\",\"משה בן יעקב מקוצי\",\"ראשונים\",\"\",\"\",\"\"",
                ),
                logger,
            )
            val result = applyGenerations(conn, rows, logger)

            assertEquals(1, result.linksCreated)
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT g.name FROM book_generation bg JOIN generation g ON g.id = bg.generationId WHERE bg.bookId = 1",
                ).use { rs ->
                    val names = generateSequence { if (rs.next()) rs.getString(1) else null }.toList()
                    assertEquals(listOf("אחרונים"), names)
                }
            }
        }
    }
}
