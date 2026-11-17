package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HavroutaCuratedContinuationsTest {
    private val logger = Logger.withTag("HavroutaCuratedContinuationsTest")

    @Test
    fun freshPipelineRunsTheReviewedCorrectionAfterCreatingHavroutaLinks() = runBlocking {
        val folder = Files.createTempDirectory("havrouta-curated-pipeline")
        val dbPath = folder.resolve("seforim.db")
        val statePath = folder.resolve("seforim.db.buildstate")
        val sourceDir = Files.createDirectories(folder.resolve("source"))
        Files.createDirectories(sourceDir.resolve("links"))
        val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(dbPath.toString(), driver)
        val allocator = InMemoryIdAllocator.load(null)
        val bindings = IdAllocatorBindings(allocator, repo)
        val sefaria = bindings.upsertSource("Sefaria")
        val otzaria = bindings.upsertSource("ToratEmet")
        val category = bindings.upsertCategory("תלמוד", null, "תלמוד", level = 0, orderIndex = 1)
        val baseBook = bindings.insertBookStable("Sefaria", "בכורות", Book(
            0, category, sefaria, "בכורות", "בכורות", totalLines = 2, isBaseBook = true,
        ))
        val commentaryBook = bindings.insertBookStable("ToratEmet", "חברותא על בכורות", Book(
            0, category, otzaria, "חברותא על בכורות", "חברותא על בכורות", totalLines = 6, isBaseBook = false,
        ))
        bindings.insertLineStable(Line(0, baseBook, 0, "<h2>דף נב:</h2>"))
        bindings.insertLineStable(Line(0, baseBook, 1, BASE_TEXT, heRef = BASE_REF))
        val commentary = listOf("<h3>דף נב:</h3>", BEFORE) + GAP + AFTER
        commentary.forEachIndexed { i, text -> bindings.insertLineStable(Line(0, commentaryBook, i, text)) }
        ConnectionType.entries.forEach { bindings.upsertConnectionType(it.name) }

        // Actual build order: the manual-link phase has no Havrouta anchors on a clean DB.
        DatabaseGenerator(sourceDir, repo, allocator = allocator).generateLinksOnly()
        val manualPhaseConnection = driver.getConnection()
        try {
            assertEquals(0L, count(manualPhaseConnection, "link_coverage"))
        } finally {
            driver.closeConnection(manualPhaseConnection)
        }
        allocator.snapshotTo(statePath)
        repo.close()

        val previousSourceDir = System.getProperty("sourceDir")
        val previousBuildState = System.getProperty("buildStatePath")
        try {
            System.setProperty("sourceDir", sourceDir.toString())
            System.setProperty("buildStatePath", statePath.toString())
            // Invoke the real CLI: this also tests that the correction is wired after generation.
            Class.forName("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateHavroutaLinksKt")
                .getMethod("main", Array<String>::class.java)
                .invoke(null, arrayOf(dbPath.toString()) as Any)
        } finally {
            previousSourceDir?.let { System.setProperty("sourceDir", it) } ?: System.clearProperty("sourceDir")
            previousBuildState?.let { System.setProperty("buildStatePath", it) } ?: System.clearProperty("buildStatePath")
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            assertEquals(3L, count(conn, "link_coverage"))
            assertEquals(1L, count(conn, "link_range"))
            assertEquals(0, extendCuratedHavroutaContinuations(conn, logger))
        }
    }

    @Test
    fun changedOrAmbiguousSourceAndExistingMappingsAreLeftAlone() {
        val changes = listOf(
            "UPDATE line SET content = 'changed' WHERE id = 102",
            "UPDATE line SET content = '<h2>' || content || '</h2>' WHERE id = 102",
            "UPDATE line SET content = 'changed' WHERE id = 10",
            "UPDATE book SET sourceId = 1 WHERE id = 2",
            "UPDATE link SET sourceLineId = 11 WHERE id = 2",
            "INSERT INTO link_range VALUES (1, 1, 101, 1)",
            linkSql(3, 11, 102, 1),
            linkSql(3, 11, 100, 1) + "\n",
        )
        for ((index, change) in changes.withIndex()) fixture { conn ->
            conn.createStatement().use { st ->
                st.execute(change)
                if (index == changes.lastIndex) {
                    st.execute("INSERT INTO link_range VALUES (3, 1, 103, 3)")
                    st.execute("INSERT INTO link_coverage VALUES (102, 3, 1)")
                }
            }
            assertEquals(0, extendCuratedHavroutaContinuations(conn, logger), change)
        }
    }

    @Test
    fun linkerCitationsCoexistWithTheReviewedMappingAndRerunsAreNoOps() = fixture { conn ->
        conn.createStatement().use { it.execute(linkSql(3, 11, 102, 2)) }
        assertEquals(3, extendCuratedHavroutaContinuations(conn, logger))
        // Phase 2 can add another citation afterwards; it does not change the explanation.
        conn.createStatement().use { it.execute(linkSql(4, 11, 103, 2)) }
        assertEquals(0, extendCuratedHavroutaContinuations(conn, logger))
        assertEquals(3L, count(conn, "link_coverage"))
        assertEquals(4L, count(conn, "link"))
    }

    @Test
    fun failedCoverageWriteRollsBackTheRangeAsWell() = fixture { conn ->
        conn.createStatement().use { st ->
            st.execute(
                "CREATE TRIGGER reject_continuation BEFORE INSERT ON link_coverage " +
                    "WHEN NEW.lineId = 102 BEGIN SELECT RAISE(ABORT, 'probe'); END",
            )
        }
        assertFailsWith<SQLException> { extendCuratedHavroutaContinuations(conn, logger) }
        assertTrue(conn.autoCommit)
        assertEquals(0L, count(conn, "link_range"))
        assertEquals(0L, count(conn, "link_coverage"))
        conn.createStatement().use { it.execute("DROP TRIGGER reject_continuation") }
        assertEquals(3, extendCuratedHavroutaContinuations(conn, logger))
    }

    private fun fixture(check: (Connection) -> Unit) {
        JdbcSqliteDriver("jdbc:sqlite::memory:").use { driver ->
            SeforimDb.Schema.create(driver)
            val conn = driver.getConnection()
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'Sefaria'), (2, 'ToratEmet')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'תלמוד')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'בכורות'), (2, 1, 2, 'חברותא על בכורות')")
                st.execute("INSERT INTO connection_type(id, name) VALUES (1, 'COMMENTARY'), (2, 'LINKER')")
                st.execute(linkSql(1, 10, 100, 1))
                st.execute(linkSql(2, 10, 104, 1))
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content, heRef) VALUES (?, ?, ?, ?, ?)").use { st ->
                val rows = listOf(Line(10, 1, 0, BASE_TEXT, heRef = BASE_REF), Line(11, 1, 1, "other")) +
                    (listOf(BEFORE) + GAP + AFTER).mapIndexed { i, text -> Line(100L + i, 2, i, text) }
                for (line in rows) {
                    st.setLong(1, line.id); st.setLong(2, line.bookId); st.setInt(3, line.lineIndex)
                    st.setString(4, line.content); st.setString(5, line.heRef); st.addBatch()
                }
                st.executeBatch()
            }
            check(conn)
        }
    }

    private fun count(conn: Connection, table: String): Long = conn.createStatement().use { st ->
        st.executeQuery("SELECT COUNT(*) FROM $table").use { rs -> rs.next(); rs.getLong(1) }
    }

    private fun linkSql(id: Int, base: Int, commentary: Int, type: Int): String =
        "INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, targetLineIndex, " +
            "targetBookOrderIndex, connectionTypeId) VALUES ($id, 1, 2, $base, $commentary, ${commentary - 100}, 0, $type)"

    companion object {
        private const val BASE_REF = "בכורות, נב:, ה"
        private const val BASE_TEXT = "<strong><big>גְּמָ׳</big></strong> מַאי טַעְמָא דְּרַבִּי מֵאִיר? " +
            "מֶכֶר הוּא דְּאָמַר רַחֲמָנָא לֶיהְדַּר בַּיּוֹבֵל, מַתָּנָה וִירוּשָּׁה לָא. " +
            "וְהָנֵי מִיְּרוּשָּׁה — יְרוּשָּׁה, דְּמַתָּנָה — מַתָּנָה. " +
            "בְּכוֹר — ״לָתֶת לוֹ פִּי שְׁנַיִם״, מַתָּנָה קַרְיֵיהּ רַחֲמָנָא."
        private const val BEFORE = "שואלת הגמרא: <b>מאי טעמא דרבי מאיר?</b> ומפרשינן: " +
            "<b>מכר הוא דאמר רחמנא ליהדר ביובל</b> [מכר בלבד הוא שאמרה תורה שיחזור ביובל], אבל <b>מתנה וירושה לא</b>."
        private const val AFTER = "<b>בכור</b>, היינו חלק בכורה: \"<b>לתת לו פי שנים</b>\" אמר הכתוב, " +
            "הרי ש<b>מתנה קרייה רחמנא</b>, והיות ומתנה היא אינה חוזרת ביובל."
        // Exact three paragraphs in the Otzaria source, before line_note postprocessing.
        private val GAP = listOf(
            "<b>ו</b>לפיכך: <b>הני</b> ממשנתנו <b>דירושה</b> הם כדמפרש ואזיל, הרי <b>ירושה</b> הם שאינה חוזרת.",
            "והני ממשנתנו <b>דמתנה</b> הם כדמפרש ואזיל, הרי <b>מתנה</b> הם שאינה חוזרת ביובל -",
            "ומפרשת הגמרא, אלו הם ירושה ואלו הם מתנה:",
        )
    }
}
