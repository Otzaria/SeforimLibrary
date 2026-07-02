package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * End-to-end check of the itag → link_anchor pass on an in-memory DB:
 * a Shulchan-Arukh-shaped base line carrying two Be'er-HaGolah-style itags
 * (one well-formed with data-order, one label-only Mishnah-Berurah-style with
 * the real-world missing opening quote) resolves to anchors on the existing
 * link rows, with visible-char offsets.
 */
class SefariaInlineAnchorsIntegrationTest {
    @Test
    fun itagsResolveToAnchorsOnStoredLinks() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        fun book(id: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = id == 1L, totalLines = 1, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "שולחן ערוך"))
        repo.insertBook(book(2, "באר הגולה"))
        repo.insertBook(book(3, "משנה ברורה"))

        // Base line: "אב " (3 visible chars) then a well-formed itag, "גד " then
        // a malformed label-only itag (missing opening quote, gematria ב = 2).
        val baseContent = "אב <i data-commentator=\"Be'er HaGolah\" data-label=\"א\" data-order=\"1\"></i>" +
            "גד <i data-commentator=Mishnah Berurah\" data-label=\"ב\"></i>הו"
        repo.insertLinesBatch(
            listOf(
                Line(id = 100, bookId = 1, lineIndex = 0, content = baseContent, heRef = "שולחן ערוך א:א"),
                Line(id = 200, bookId = 2, lineIndex = 0, content = "טור", heRef = "באר הגולה א:א"),
                Line(id = 300, bookId = 3, lineIndex = 0, content = "ס\"ק ב", heRef = "משנה ברורה א:ב"),
            )
        )
        repo.insertLinksBatch(
            listOf(
                Link(id = 7, sourceBookId = 1, targetBookId = 2, sourceLineId = 100, targetLineId = 200,
                    targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY),
                Link(id = 8, sourceBookId = 1, targetBookId = 3, sourceLineId = 100, targetLineId = 300,
                    targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY),
            )
        )

        val books = listOf(
            SefariaInlineAnchors.BookInput(
                bookId = 1, enTitle = "Shulchan Arukh, Orach Chayim", bookPath = "sa",
                lines = listOf(baseContent),
                refsByLineIndex = mapOf(0 to RefEntry("Shulchan Arukh, Orach Chayim 1:1", "שולחן ערוך א:א", "sa", 1)),
            ),
            SefariaInlineAnchors.BookInput(
                bookId = 2, enTitle = "Be'er HaGolah on Shulchan Arukh, Orach Chayim", bookPath = "bhg",
                lines = listOf("טור"),
                refsByLineIndex = mapOf(0 to RefEntry("Be'er HaGolah on Shulchan Arukh, Orach Chayim 1:1", "באר הגולה א:א", "bhg", 1)),
            ),
            SefariaInlineAnchors.BookInput(
                bookId = 3, enTitle = "Mishnah Berurah", bookPath = "mb",
                lines = listOf("ס\"ק ב"),
                refsByLineIndex = mapOf(0 to RefEntry("Mishnah Berurah 1:2", "משנה ברורה א:ב", "mb", 1)),
            ),
        )
        val bookMetaById = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            2L to BookMeta(
                isBaseBook = false, categoryLevel = 0, priorityRank = null,
                dependence = Dependence.COMMENTARY,
                sefariaDeclaredBaseTextBookIds = setOf(1L),
                collectiveTitleEn = "Be'er HaGolah",
            ),
            3L to BookMeta(
                isBaseBook = false, categoryLevel = 0, priorityRank = null,
                dependence = Dependence.COMMENTARY,
                sefariaDeclaredBaseTextBookIds = setOf(1L),
                collectiveTitleEn = null,
            ),
        )
        val refsByCanonical = books.flatMap { input ->
            input.refsByLineIndex.values.map { it.copy(path = input.bookPath) }
        }.groupBy { canonicalCitation(it.ref) }
        val lineKeyToId = mapOf("sa" to 0 to 100L, "bhg" to 0 to 200L, "mb" to 0 to 300L)

        SefariaInlineAnchors(repo, Logger.withTag("InlineAnchorsTest")).generate(
            books = books,
            bookMetaById = bookMetaById,
            refsByCanonical = refsByCanonical,
            lineKeyToId = lineKeyToId,
        )

        // Be'er HaGolah itag after "אב " → visible offset 3
        assertEquals(
            listOf(Triple(3, null as Int?, "א")),
            repo.getLinkAnchors(7).map { Triple(it.charStart, it.charEnd, it.label) },
        )
        // Mishnah Berurah label-only itag after "אב גד " → visible offset 6,
        // resolved via gematria(ב)=2 → "Mishnah Berurah 1:2"
        assertEquals(
            listOf(Triple(6, null as Int?, "ב")),
            repo.getLinkAnchors(8).map { Triple(it.charStart, it.charEnd, it.label) },
        )
    }
}
