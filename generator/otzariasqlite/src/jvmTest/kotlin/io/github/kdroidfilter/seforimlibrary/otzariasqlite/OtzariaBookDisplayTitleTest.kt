package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Otzaria/otzaria#2766: file stems lose their gershayim; the stored title gets them back. */
class OtzariaBookDisplayTitleTest {

    private val titles = BookDisplayTitles(BookDisplayTitles.parse(TEST_TITLE_ACRONYMS))

    @Test
    fun theWholeHeadingIsTakenWhenItIsTheStemWithGershayim() {
        assertEquals("שו״ת באר מרדכי", titles.displayTitle("שות באר מרדכי", "שו\"ת באר מרדכי"))
        assertEquals("מהר״ם שיק תרי״ג מצות חלק א", titles.displayTitle("מהרם שיק תריג מצות חלק א", "מהר\"ם שיק תרי\"ג מצות חלק א"))
        assertEquals("דרושי הצל״ח", titles.displayTitle("דרושי הצלח", "דרושי הצל”ח"))
    }

    @Test
    fun acronymWordsOfTheHeadingFillTheirWordsOfTheStem() {
        assertEquals("חידושי מהר״י מינץ", titles.displayTitle("חידושי מהרי מינץ", "חידושים מהר\"י מינץ"))
        // A quoted word of the heading is not an acronym.
        assertEquals("ספר שלום", titles.displayTitle("ספר שלום", "\"שלום\" ספר"))
    }

    @Test
    fun curatedAcronymsFillTheStemWithoutEvidenceInTheBook() {
        assertEquals("דברי שאול עה״ת ומגילות", titles.displayTitle("דברי שאול עהת ומגילות", "דברי שאול"))
        assertEquals("חידושי הריטב״א על הש״ס", titles.displayTitle("חידושי הריטבא על השס", null))
        assertEquals("דבש תמר (פירוש המשניות להרמב״ם)", titles.displayTitle("דבש תמר (פירוש המשניות להרמבם)", null))
        assertEquals("חידושי הר״ן על שבת [מיוחס להריטב״א]", titles.displayTitle("חידושי הרן על שבת [מיוחס להריטבא]", "חידושי הר\"ן על שבת [מיוחס להריטבא]"))
    }

    @Test
    fun curatedAcronymsAlsoFillWordsTheWholeHeadingLeftBare() {
        assertEquals("שו״ת פני יצחק ח״ב", titles.displayTitle("שות פני יצחק חב", "שות פני יצחק ח\"ב"))
    }

    @Test
    fun theForDbFileFailsClosedWhenItListsNothing() {
        assertEquals(listOf("שו״ת", "הש״ס"), BookDisplayTitles.parse(listOf("﻿# comment", "", " שו״ת ", "הש״ס")))
        assertFailsWith<IllegalStateException> { BookDisplayTitles.parse(listOf("# only a comment", "")) }
        assertFailsWith<IllegalStateException> { testDatabaseGenerator(Files.createTempDirectory("x"), repo(), titleAcronyms = emptyList()) }
    }

    @Test
    fun ambiguousWordsAreLeftAlone() {
        for (stem in listOf("קול הרמז", "שבות יעקב", "דעת תורה", "שי למורא", "מר דרור", "שולחן ערוך יוד", "פירוש החסיד יעבץ על אבות")) {
            assertEquals(stem, titles.displayTitle(stem, null))
        }
    }

    @Test
    fun aHeadingThatChangesMoreThanQuoteMarksIsIgnored() {
        assertEquals("ספר חדש", titles.displayTitle("ספר חדש", "ספר חדש מהר\"ם"))
        // Its acronym words still apply one by one.
        assertEquals("שו״ת באר מרדכי", titles.displayTitle("שות באר מרדכי", "שו\"ת באר מרדכי השלם"))
    }

    @Test
    fun theTitleOnlyEverGainsQuoteMarks() {
        assertTrue(CategoryLabels.onlyAddsQuoteMarks("שו״ת א", "שות א"))
        assertTrue(CategoryLabels.onlyAddsQuoteMarks("שו\"ת א", "שות א"))
        assertFalse(CategoryLabels.onlyAddsQuoteMarks("שו״ת ב", "שות א"))
        assertFalse(CategoryLabels.onlyAddsQuoteMarks("שו״ת", "שות א"))
        // A mark the stem had must stay.
        assertFalse(CategoryLabels.onlyAddsQuoteMarks("ר חיים", "ר' חיים"))
        // A curated form that is not the word plus gershayim never applies.
        assertEquals("שות א", BookDisplayTitles(listOf("שו״תים")).displayTitle("שות א", null))
        assertFailsWith<IllegalArgumentException> { BookDisplayTitles(listOf("שות")) }
    }

    @Test
    fun linksAndAltTocStillReachABookWhoseTitleGainedGershayim() = runBlocking {
        val root = Files.createTempDirectory("otzaria-display-title")
        val base = "שות באר מרדכי"
        val commentary = "דברי שאול עהת ומגילות"
        writeBook(root, base, "<h1>שו\"ת באר מרדכי</h1>")
        writeBook(root, commentary, "<h1>דברי שאול</h1>")
        Files.writeString(
            root.resolve("files_manifest_new.json"),
            listOf(base, commentary).joinToString(",\n", "{\n", "\n}") {
                "  \"MoreBooks/אוצריא/שות/$it.txt\": { \"hash\": \"x\" }"
            },
        )
        writeFilesManifest(root)
        val links = Files.createDirectories(root.resolve("links"))
        Files.writeString(
            links.resolve("${base}_links.json"),
            """[{"line_index_1": 2, "heRef_2": "$commentary 2", "path_2": "$commentary.txt", "line_index_2": 2, "Conection Type": "commentary"}]""",
        )
        val altToc = Files.createDirectories(root.resolve("alt_toc"))
        Files.writeString(
            altToc.resolve("${base}_alt_toc.json"),
            """[{"key": "Parts", "heTitle": "חלקים", "nodes": [{"heTitle": "חלק א", "line": 2}]}]""",
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            val allocator = InMemoryIdAllocator.load(null)
            testDatabaseGenerator(sourceDirectory = root, repository = repo, allocator = allocator, buildVersion = 1)
                .generateLinesOnly()
            testDatabaseGenerator(sourceDirectory = root, repository = repo, allocator = allocator, buildVersion = 1)
                .generateLinksOnly()

            val books = repo.getAllBooks().associateBy { it.title }
            val baseBook = books.getValue("שו״ת באר מרדכי")
            val commentaryBook = books.getValue("דברי שאול עה״ת ומגילות")
            assertEquals(baseBook.title, baseBook.heRef)
            // The id stays keyed by the file stem.
            assertEquals(allocator.peekBookId("MoreBooks", base), baseBook.id)
            assertEquals(allocator.peekBookId("MoreBooks", commentary), commentaryBook.id)
            assertEquals("שו״ת באר מרדכי 2", repo.getLines(baseBook.id, 1, 1).single().heRef)

            val baseLine = repo.getLines(baseBook.id, 1, 1).single().id
            val commentaryLine = repo.getLines(commentaryBook.id, 1, 1).single().id
            assertEquals(1, repo.getLinkIdsBetweenLines(baseLine, commentaryLine).size)
            assertEquals(1, repo.getAltTocStructuresForBook(baseBook.id).size)
            assertTrue(repo.getBook(baseBook.id)!!.hasAltStructures)
        } finally {
            repo.close()
        }
    }

    private fun repo(): SeforimRepository {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private fun writeBook(root: Path, stem: String, heading: String) {
        val dir = Files.createDirectories(root.resolve("אוצריא").resolve("שות"))
        Files.writeString(dir.resolve("$stem.txt"), (listOf(heading) + (1..3).map { "שורה $it" }).joinToString("\n"))
    }
}
