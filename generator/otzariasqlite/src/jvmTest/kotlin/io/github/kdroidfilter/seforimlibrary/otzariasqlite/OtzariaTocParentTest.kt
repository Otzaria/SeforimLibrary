package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/** A heading's TOC parent is the nearest preceding heading of a shallower level. */
class OtzariaTocParentTest {

    private fun parentsOf(bookLines: String): Map<String, String?> = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceDir = Files.createTempDirectory("otzaria-toc-parent")
        val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("מוסר"))
        Files.writeString(bookDir.resolve("ספר.txt"), bookLines)

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        val book = repo.getAllBooks().single { it.title == "ספר" }
        val toc = repo.getBookToc(book.id)
        val textById = toc.associate { it.id to it.text }
        toc.associate { it.text to it.parentId?.let(textById::get) }
    }

    @Test
    fun levelSkipAfterShallowerHeadingDoesNotAdoptEarlierSectionHeading() {
        val parents = parentsOf(
            listOf(
                "<h1>ספר</h1>",
                "<h3>הערה</h3>",
                "טקסט",
                "<h2>פרק א</h2>",
                "<h4>ברכה</h4>",
                "טקסט",
                "<h2>פרק ב</h2>",
                "<h4>ניסן</h4>",
                "טקסט",
            ).joinToString("\n"),
        )

        assertEquals("ספר", parents["הערה"])
        assertEquals("פרק א", parents["ברכה"])
        assertEquals("פרק ב", parents["ניסן"])
    }

    @Test
    fun emptyShallowerHeadingStillClosesDeeperLevels() {
        val parents = parentsOf(
            listOf(
                "<h1>ספר</h1>",
                "<h3>הערה</h3>",
                "טקסט",
                "<h2></h2>",
                "<h4>ברכה</h4>",
                "טקסט",
            ).joinToString("\n"),
        )

        assertEquals("ספר", parents["ברכה"])
    }
}
