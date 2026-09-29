package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Unlinked Sefaria segments that continue a linked comment under the same
 * parent address join that comment's commentary-side range (forum topic 1911).
 */
class SefariaContinuationParagraphsTest {
    private fun runs(
        refs: List<String>,
        linked: Map<Int, List<Long>>,
        headings: Set<Int> = emptySet(),
        lineIndexes: List<Int> = refs.indices.map { it + 1 },
    ): List<ContinuationRun> {
        val path = "Book"
        val entries = refs.zip(lineIndexes) { ref, idx -> RefEntry(ref, "", path, idx) }
        val lineKeyToId = lineIndexes.associate { (path to it - 1) to (100L + it - 1) }
        val linkedIds = linked.mapKeys { 100L + it.key }
        return findContinuationRuns(
            path = path,
            entries = entries,
            lineKeyToId = lineKeyToId,
            headingLineIds = headings.map { 100L + it }.toSet(),
            isCovered = { it in linkedIds },
            linksEndingAt = { linkedIds[it].orEmpty() },
        )
    }

    @Test
    fun unlinkedSegmentsJoinPrecedingCommentUnderSameParent() {
        val result = runs(
            refs = listOf(
                "Abarbanel on Torah, Genesis 1:1:1",
                "Abarbanel on Torah, Genesis 1:1:2",
                "Abarbanel on Torah, Genesis 1:1:3",
                "Abarbanel on Torah, Genesis 1:1:4",
                "Abarbanel on Torah, Genesis 1:1:5",
                "Abarbanel on Torah, Genesis 1:2:1",
            ),
            linked = mapOf(0 to listOf(7L, 8L), 2 to listOf(9L)),
        )
        assertEquals(
            listOf(
                ContinuationRun(linkIds = listOf(7L, 8L), lineIds = listOf(101L), endLineIndex0 = 1),
                // 1:2:1 opens another verse: it stays unlinked.
                ContinuationRun(linkIds = listOf(9L), lineIds = listOf(103L, 104L), endLineIndex0 = 4),
            ),
            result,
        )
    }

    @Test
    fun singleComponentAddressIsNeverExtended() {
        val result = runs(
            refs = listOf("Chatam Sofer on Torah, Bereshit 4", "Chatam Sofer on Torah, Bereshit 5"),
            linked = mapOf(0 to listOf(7L)),
        )
        assertEquals(emptyList(), result)
    }

    @Test
    fun headingOrRefGapEndsTheRun() {
        val heading = runs(
            refs = listOf("R on G 1:1:1", "R on G 1:1:2", "R on G 1:1:3"),
            linked = mapOf(0 to listOf(7L)),
            headings = setOf(1),
        )
        assertEquals(emptyList(), heading)

        // lineIndex 2 has no ref (e.g. an untitled heading line).
        val gap = runs(
            refs = listOf("R on G 1:1:1", "R on G 1:1:2"),
            linked = mapOf(0 to listOf(7L)),
            lineIndexes = listOf(1, 3),
        )
        assertEquals(emptyList(), gap)
    }

    @Test
    fun newCommentOpenersAreRecognized() {
        assertTrue(opensNewComment("<b>ויאמר.</b> פירוש"))
        assertTrue(opensNewComment("עד סוף האשמורה – שליש הלילה"))
        assertTrue(opensNewComment("ברש\"י בד\"ה ותיפוק ליה כו'"))
        assertTrue(opensNewComment("תוס' בד\"ה ואם יש עליו עוררין"))
        assertTrue(opensNewComment("בתד\"ה אמר ר\"נ כו'. עיין בר\"ן"))
        assertTrue(opensNewComment("פר\"ח ד\"ה ולכן נ\"ל וכו'"))
        assertTrue(opensNewComment("נ\"ב גם ממנה:"))
        assertTrue(opensNewComment("שני שמות. משום אלמנה ומשום גרושה:"))
        assertFalse(opensNewComment("השאלה הב' למה לא נזכרה"))
        assertFalse(opensNewComment("ובזה נלע\"ד לתרץ קושית הרשב\"א בחידושיו. ועוד"))
        assertFalse(opensNewComment("<b>בא\"ד</b> דלמא בתנאי מודה"))
    }

    @Test
    fun runIsCutBeforeTheFirstNewComment() {
        val run = ContinuationRun(linkIds = listOf(7L), lineIds = listOf(101L, 102L, 103L), endLineIndex0 = 3)
        assertEquals(run, run.untilNewComment { false })
        assertEquals(run.copy(lineIds = listOf(101L), endLineIndex0 = 1), run.untilNewComment { it == 102L })
        assertNull(run.untilNewComment { it == 101L })
    }

    @Test
    fun generateExtendsRangesAndCoverageInDb() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-continuation")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        // Rashi 1:1:1-2 is a range whose last segment is followed by unlinked 1:1:3;
        // Rashi 1:2:1 is linked alone and followed by unlinked 1:2:2.
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Rashi on Genesis 1:1:1-2","Genesis 1:1","Commentary"
            |"Rashi on Genesis 1:2:1","Genesis 1:2","Commentary"
            """.trimMargin()
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "תורה", level = 0, order = 1))
        fun book(id: Long, title: String, isBase: Boolean, lines: Int) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = isBase, totalLines = lines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "בראשית", isBase = true, lines = 2))
        repo.insertBook(book(2, "רש\"י על בראשית", isBase = false, lines = 6))
        repo.insertLinesBatch(
            listOf(
                Line(id = 1, bookId = 1, lineIndex = 0, content = "gen 1:1", heRef = "בראשית א, א"),
                Line(id = 2, bookId = 1, lineIndex = 1, content = "gen 1:2", heRef = "בראשית א, ב"),
            ) + (0..4).map { Line(id = 10L + it, bookId = 2, lineIndex = it, content = "rashi $it", heRef = "") } +
                Line(id = 15, bookId = 2, lineIndex = 5, content = "<b>ויאמר.</b> פירוש חדש", heRef = "")
        )

        val rashiAddresses = listOf("1:1:1", "1:1:2", "1:1:3", "1:2:1", "1:2:2", "1:2:3")
        val allRefs = listOf(
            RefEntry("Genesis 1:1", "", "Genesis", 1),
            RefEntry("Genesis 1:2", "", "Genesis", 2),
        ) + rashiAddresses.mapIndexed { i, a -> RefEntry("Rashi on Genesis $a", "", "Rashi on Genesis", i + 1) }
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefs.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }
        val refsByPath = allRefs.groupBy { it.path }
        val lineKeyToId = mapOf("Genesis" to 0 to 1L, "Genesis" to 1 to 2L) +
            (0..5).associate { ("Rashi on Genesis" to it) to 10L + it }
        val lineIdToBookId = mapOf(1L to 1L, 2L to 1L) + (0..5).associate { 10L + it to 2L }
        val bookMeta = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            2L to BookMeta(
                isBaseBook = false, categoryLevel = 1, priorityRank = null,
                dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(1L),
            ),
        )
        val bindings = io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings(
            io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator.load(path = null),
            repo,
        )
        val logger = Logger.withTag("SefariaContinuationParagraphsTest")
        SefariaLinksImporter(repo, bindings, logger).processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) },
            refsByBase = refsByBase,
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            refsByPath = refsByPath,
        )

        fun query(sql: String): List<List<Long>> =
            driver.getConnection().createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    val out = mutableListOf<List<Long>>()
                    while (rs.next()) out += (1..rs.metaData.columnCount).map { rs.getLong(it) }
                    out
                }
            }

        val generator = SefariaContinuationParagraphs(repo, logger)
        // Not a commentary book: nothing is extended.
        generator.generate(refsByPath, lineKeyToId, emptySet(), commentaryBookIds = emptySet(), lineIdToBookId)
        assertEquals(1, query("SELECT COUNT(*) FROM link_coverage").single().single().toInt())
        generator.generate(refsByPath, lineKeyToId, emptySet(), commentaryBookIds = setOf(2L), lineIdToBookId)

        assertEquals(
            listOf(
                listOf(1L, 12L, 2L, 1L, 10L), // 1:1:1-2 widened to 1:1:3
                listOf(1L, 14L, 4L, 2L, 13L), // 1:2:1 covers 1:2:2; 1:2:3 opens a new dibbur
            ),
            query(
                "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                    "FROM link_range lr JOIN link l ON l.id = lr.linkId ORDER BY lr.endLineId"
            ),
        )
        assertEquals(
            listOf(listOf(11L, 10L), listOf(12L, 10L), listOf(14L, 13L)),
            query(
                "SELECT lc.lineId, l.targetLineId FROM link_coverage lc JOIN link l ON l.id = lc.linkId " +
                    "WHERE lc.side = 1 ORDER BY lc.lineId"
            ),
        )
        repo.close()
    }
}
