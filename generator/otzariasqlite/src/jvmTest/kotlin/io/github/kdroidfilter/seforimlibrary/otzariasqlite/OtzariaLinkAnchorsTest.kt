package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The Otzaria links phase with word-level anchors, on the משנה ברורה →
 * שער הציון shape: the source book is Sefaria-sourced but the target is not,
 * so the link must be imported (Sefaria cannot ship links to a book it does
 * not have), carrying the JSON `start`/`end` raw offsets converted to
 * visible-char coordinates. A Sefaria→Sefaria entry in the same file must
 * still be skipped.
 */
class OtzariaLinkAnchorsTest {
    @Test
    fun anchoredLinksImportAndSefariaPairsStaySkipped() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sefariaSourceId = repo.insertSource("Sefaria")
        val tashmaSourceId = repo.insertSource("Tashma")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        fun book(id: Long, title: String, sourceId: Long, totalLines: Int) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = totalLines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "משנה ברורה", sefariaSourceId, 2))
        repo.insertBook(book(2, "שער הציון", tashmaSourceId, 2))
        repo.insertBook(book(3, "ילקוט", sefariaSourceId, 1))

        // Raw offsets: start=6 (at 'ג') → 3 visible chars before; end=12 → 5.
        val mbContent = "אב <b>ג</b> דה"
        repo.insertLinesBatch(
            listOf(
                Line(id = 100, bookId = 1, lineIndex = 0, content = "שורה ראשונה", heRef = "משנה ברורה 1"),
                Line(id = 101, bookId = 1, lineIndex = 1, content = mbContent, heRef = "משנה ברורה 2"),
                Line(id = 200, bookId = 2, lineIndex = 0, content = "הקדמה", heRef = "שער הציון 1"),
                Line(id = 201, bookId = 2, lineIndex = 1, content = "מטור", heRef = "שער הציון 2"),
                Line(id = 300, bookId = 3, lineIndex = 0, content = "ילקוט שורה", heRef = "ילקוט 1"),
            )
        )

        val sourceDir = Files.createTempDirectory("otzaria-anchors")
        val linksDir = Files.createDirectories(sourceDir.resolve("links"))
        Files.writeString(
            linksDir.resolve("משנה ברורה_links.json"),
            """
            |[
            | {"line_index_1": 2, "heRef_2": "שער הציון, סימן א", "path_2": "שער הציון.txt",
            |  "line_index_2": 2, "Conection Type": "commentary", "start": 6, "end": 12},
            | {"line_index_1": 2, "heRef_2": "ילקוט א", "path_2": "ילקוט.txt",
            |  "line_index_2": 1, "Conection Type": "commentary", "start": 1}
            |]
            """.trimMargin()
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        // Sefaria(MB) → Tashma(שער הציון): imported, with the converted anchor.
        val linkIds = repo.getLinkIdsBetweenLines(101, 201)
        assertEquals(1, linkIds.size)
        val anchors = repo.getLinkAnchors(linkIds.single())
        assertEquals(1, anchors.size)
        assertEquals(3, anchors.single().charStart)
        assertEquals(5, anchors.single().charEnd)

        // Sefaria(MB) → Sefaria(ילקוט): must stay skipped.
        assertTrue(repo.getLinkIdsBetweenLines(101, 300).isEmpty())
    }
}
