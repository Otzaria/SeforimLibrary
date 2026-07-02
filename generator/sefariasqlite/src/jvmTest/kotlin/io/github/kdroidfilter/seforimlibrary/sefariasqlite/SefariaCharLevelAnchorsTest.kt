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
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SefariaCharLevelAnchorsTest {

    @Test
    fun parsesCharBasedCell() {
        val cell = parseCharLevelCell(
            """{"startChar":4639,"endChar":4657,"versionTitle":"Tzeror Hamor, Warsaw, 1879","language":"he"}"""
        )
        assertEquals(4639, cell?.start)
        assertEquals(4657, cell?.end)
        assertEquals("Tzeror Hamor, Warsaw, 1879", cell?.versionTitle)
        assertEquals(false, cell?.isWordBased)
    }

    @Test
    fun parsesWordBasedCell() {
        val cell = parseCharLevelCell(
            """{"startWord":0,"endWord":4,"versionTitle":"Tanach with Nikkud","language":"he"}"""
        )
        assertEquals(true, cell?.isWordBased)
        assertEquals(0, cell?.start)
        assertEquals(4, cell?.end)
    }

    @Test
    fun rejectsEmptyAndMalformedCells() {
        assertNull(parseCharLevelCell(null))
        assertNull(parseCharLevelCell(""))
        assertNull(parseCharLevelCell("not json"))
        assertNull(parseCharLevelCell("""{"versionTitle":"x"}"""))
        assertNull(parseCharLevelCell("""{"startChar":1,"endChar":2,"versionTitle":""}"""))
    }

    @Test
    fun exactGateAndOffsetConversion() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "מדרש", level = 0, order = 1))
        fun book(id: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = 1, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "בראשית"))
        repo.insertBook(book(2, "צרור המור"))

        // שורת הספר המצטט: קידומת "(א) " שנוספה בזמן הבנייה (shift=4) + תג
        // בתוך הטקסט. הציטוט "ויבז עשו" יושב באופסט גולמי 8 של הקטע המקורי.
        val quotingLine = "(א) על <b>כן</b> ויבז עשו את הבכורה"
        repo.insertLinesBatch(
            listOf(
                Line(id = 100, bookId = 1, lineIndex = 0, content = "פסוק", heRef = "בראשית כה, לד"),
                Line(id = 200, bookId = 2, lineIndex = 0, content = quotingLine, heRef = "צרור המור א"),
            )
        )
        repo.insertLinksBatch(
            listOf(
                Link(id = 9, sourceBookId = 1, targetBookId = 2, sourceLineId = 100, targetLineId = 200,
                    targetLineIndex = 0, connectionType = ConnectionType.QUOTATION),
            )
        )

        val books = listOf(
            SefariaInlineAnchors.BookInput(
                bookId = 2, enTitle = "Tzror HaMor on Torah", bookPath = "tzror",
                lines = listOf(quotingLine),
                refsByLineIndex = mapOf(0 to RefEntry("Tzror HaMor on Torah, Genesis 25:1", "צרור המור א", "tzror", 1)),
                singleVersionTitle = "Tzeror Hamor, Warsaw, 1879",
                cleanShiftByLineIndex = mapOf(0 to 4),
            ),
        )
        // הקטע הגולמי (בלי הקידומת): "על <b>כן</b> ויבז עשו את הבכורה"
        // אופסט גולמי [10:31] מכסה "ויבז עשו את הבכורה" (התגים נספרים בגולמי).
        val raw = quotingLine.substring(4)
        assertEquals("ויבז עשו את הבכורה", raw.substring(13, 31))
        val pending = listOf(
            // עובר את השער: startChar=13 גולמי → גלוי: 4 (קידומת) + "על כן ויבז..."
            PendingCharLevelAnchor(
                path = "tzror", lineIndex0 = 0, srcLineId = 100, tgtLineId = 200, side = 1,
                startChar = 13, endChar = 31,
                versionTitle = "Tzeror Hamor, Warsaw, 1879", language = "he", isWordBased = false,
            ),
            // גרסה לא תואמת — נחסם
            PendingCharLevelAnchor(
                path = "tzror", lineIndex0 = 0, srcLineId = 100, tgtLineId = 200, side = 1,
                startChar = 0, endChar = 5,
                versionTitle = "Some Other Edition", language = "he", isWordBased = false,
            ),
            // מבוסס-מילים — נחסם
            PendingCharLevelAnchor(
                path = "tzror", lineIndex0 = 0, srcLineId = 100, tgtLineId = 200, side = 0,
                startChar = 0, endChar = 4,
                versionTitle = "Tzeror Hamor, Warsaw, 1879", language = "he", isWordBased = true,
            ),
        )

        SefariaCharLevelAnchors(repo, Logger.withTag("CharLevelTest")).generate(pending, books)

        val anchors = repo.getLinkAnchors(9)
        assertEquals(1, anchors.size)
        val anchor = anchors.single()
        assertEquals(1, anchor.side)
        // גלוי: "(א) על כן " לפני הציטוט = 4 + 6 = 10... נספר: "(א) "=4, "על "=3,
        // "כן"=2 (התגים לא נספרים), " "=1 → 10.
        assertEquals(10, anchor.charStart)
        assertEquals(10 + "ויבז עשו את הבכורה".length, anchor.charEnd)
        assertTrue(anchor.label == null)
    }
}
