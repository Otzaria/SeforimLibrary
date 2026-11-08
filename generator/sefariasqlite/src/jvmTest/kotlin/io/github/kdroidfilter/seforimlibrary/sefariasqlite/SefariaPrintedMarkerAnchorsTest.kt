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
 * Chafetz Chaim prints its Be'er Mayim Chaim markers as plain "(X)" text, with no itags
 * (Otzaria/otzaria#4328). The printed marker itself must become a range anchor on the
 * stored link to its comment.
 */
class SefariaPrintedMarkerAnchorsTest {
    @Test
    fun markersSkipTagsAndParenthesizedWords() {
        val markers = parsePrintedMarkers("<b>אב</b> (א) &quot;ג&quot; (כתוב) (וכי) (יט) ד")
        assertEquals(
            listOf(PrintedMarker("א", 1, 3, 6, false), PrintedMarker("יט", 19, 24, 28, false)),
            markers,
        )
        // Ketiv and abbreviations are not numbers written canonically.
        assertEquals(emptyList(), parsePrintedMarkers("אב (יבא) גד (הג\"א) הו (שם) זח"))

        val opening = " <b>(א)</b> אב (ב) ג"
        assertEquals(
            listOf(PrintedMarker("א", 1, 1, 4, true), PrintedMarker("ב", 2, 8, 11, false)),
            parsePrintedMarkers(opening),
        )
        assertEquals(listOf(PrintedMarker("ב", 2, 8, 11, false)), parsePrintedMarkers(opening, skipBefore = 4))
    }

    @Test
    fun printedMarkersAnchorOnStoredLinks() = runBlocking {
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
        repo.insertBook(book(1, "חפץ חיים"))
        repo.insertBook(book(2, "באר מים חיים על חפץ חיים"))

        val base = listOf(
            // Comments open with their printed number.
            "אב (א) גד (ב) הו",
            // Comments print no number: a lone marker carrying the segment's number (2)
            // goes to the first comment.
            "המרגל (ב) בחברו (כתוב) זה",
            // No stored link to the comment: no anchor.
            "ועובר (ד) נמי",
            // A lone marker that is not the segment's number (4) is a citation: no anchor.
            "כן איתא (ת\"ה) בגמרא",
            // A printed marker may open the line.
            "(ה) אסור לספר",
            // The "(ו) " that cleaning prepended is the segment's number, not a marker.
            "(ו) טקסט (ז) עוד",
            // A label that cleaning prepended to the comment is not its printed number.
            "טקסט (ח) עוד",
        )
        val comments = listOf(
            "(א) ראשון", "(ב) שני",
            "<b>המרגל.</b> כל זה", "ומ\"ש דעובר",
            "(ד) רביעי",
            "כן איתא. פירוש",
            "(ה) חמישי",
            "(ו) שישי", "(ז) שביעי",
            "(ח) שמיני",
        )
        val baseRefs = (1..7).map { "Chafetz Chaim, Principle 1 $it" }
        val commentRefs = listOf("1:1", "1:2", "2:1", "2:2", "3:1", "4:1", "5:1", "6:1", "6:2", "7:1")
            .map { "Be'er Mayim Chaim, Principle 1 $it" }

        repo.insertLinesBatch(
            base.mapIndexed { i, c -> Line(id = 100L + i, bookId = 1, lineIndex = i, content = c, heRef = "") } +
                comments.mapIndexed { i, c -> Line(id = 200L + i, bookId = 2, lineIndex = i, content = c, heRef = "") }
        )
        fun link(id: Long, src: Long, tgt: Long) = Link(
            id = id, sourceBookId = 1, targetBookId = 2, sourceLineId = src, targetLineId = tgt,
            targetLineIndex = (tgt - 200).toInt(), connectionType = ConnectionType.COMMENTARY,
        )
        repo.insertLinksBatch(
            listOf(link(1, 100, 200), link(2, 100, 201), link(3, 101, 202), link(4, 101, 203), link(5, 103, 205),
                link(6, 104, 206), link(7, 105, 207), link(8, 105, 208), link(9, 106, 209))
        )

        val books = listOf(
            SefariaInlineAnchors.BookInput(
                bookId = 1, enTitle = "Chafetz Chaim", bookPath = "cc", lines = base,
                refsByLineIndex = baseRefs.withIndex().associate { (i, r) -> i to RefEntry(r, "", "cc", i + 1) },
                // A cleaned line keeps its generated prefix in the encoded shift.
                cleanShiftByLineIndex = mapOf(5 to cleanedLineShift("(ו) ".length)),
            ),
            SefariaInlineAnchors.BookInput(
                bookId = 2, enTitle = "Be'er Mayim Chaim", bookPath = "bmc", lines = comments,
                refsByLineIndex = commentRefs.withIndex().associate { (i, r) -> i to RefEntry(r, "", "bmc", i + 1) },
                cleanShiftByLineIndex = mapOf(9 to "(ח) ".length),
            ),
        )
        val bookMetaById = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            2L to BookMeta(
                isBaseBook = false, categoryLevel = 0, priorityRank = null,
                dependence = Dependence.COMMENTARY,
                sefariaDeclaredBaseTextBookIds = setOf(1L),
            ),
        )
        val lineKeyToId = base.indices.associate { ("cc" to it) to 100L + it } +
            comments.indices.associate { ("bmc" to it) to 200L + it }

        SefariaPrintedMarkerAnchors(repo, Logger.withTag("PrintedMarkerAnchorsTest")).generate(
            books = books,
            bookMetaById = bookMetaById,
            lineKeyToId = lineKeyToId,
        )

        suspend fun anchors(linkId: Long) = repo.getLinkAnchors(linkId).map { Triple(it.charStart, it.charEnd, it.label) }
        assertEquals(listOf(Triple(3, 6 as Int?, "א")), anchors(1))
        assertEquals(listOf(Triple(10, 13 as Int?, "ב")), anchors(2))
        assertEquals(listOf(Triple(6, 9 as Int?, "ב")), anchors(3))
        assertEquals(emptyList(), anchors(4))
        assertEquals(emptyList(), anchors(5))
        assertEquals(listOf(Triple(0, 3 as Int?, "ה")), anchors(6))
        assertEquals(emptyList(), anchors(7))
        assertEquals(listOf(Triple(9, 12 as Int?, "ז")), anchors(8))
        assertEquals(emptyList(), anchors(9))
    }
}
