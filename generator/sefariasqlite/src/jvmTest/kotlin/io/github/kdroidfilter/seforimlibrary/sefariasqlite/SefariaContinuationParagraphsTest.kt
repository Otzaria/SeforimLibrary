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
import kotlinx.serialization.json.*
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
    ): List<ContinuationRun> = runBlocking {
        val path = "Book"
        val entries = refs.zip(lineIndexes) { ref, idx -> RefEntry(ref, "", path, idx) }
        val lineKeyToId = lineIndexes.associate { (path to it - 1) to (100L + it - 1) }
        val linkedIds = linked.mapKeys { 100L + it.key }
        findContinuationRuns(
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

    private fun schema(sections: String = "Paragraph", dibbur: String = ""): JsonObject = Json.parseToJsonElement(
        """{"nodes":[{"title":"Genesis","key":"Genesis","nodes":[{"title":"","key":"default","depth":3,"sectionNames":["Chapter","Verse","$sections"],"addressTypes":["Perek","Integer","Integer"]$dibbur}]}]}"""
    ).jsonObject

    @Test
    fun onlyCuratedContinuousParagraphSchemaIsEligible() {
        assertEquals(setOf("abarbanel on torah genesis"), continuationRefPrefixes("Abarbanel on Torah", schema()))
        assertTrue(continuationRefPrefixes("Rashi on Genesis", schema()).isEmpty())
        val prefixes = continuationRefPrefixes("Abarbanel on Torah", schema())
        assertTrue(isEligibleContinuationRef("abarbanel on torah genesis 1:1:2", prefixes))
        assertFalse(isEligibleContinuationRef("abarbanel on torah genesis introduction 1:1:2", prefixes))
        assertFalse(isEligibleContinuationRef("abarbanel on torah genesis 1:1", prefixes))
        assertFalse(isEligibleContinuationRef("abarbanel on torah genesis 1:1:2:3", prefixes))
        assertTrue(continuationRefPrefixes("Abarbanel on Isaiah", schema()).isEmpty())
        assertTrue(continuationRefPrefixes("Abarbanel on Torah", schema("Comment")).isEmpty())
        assertTrue(continuationRefPrefixes("Abarbanel on Torah", schema(dibbur = """, "isSegmentLevelDiburHamatchil":true""")).isEmpty())
        assertTrue(continuationRefPrefixes("Abarbanel on Torah", schema(dibbur = """, "diburHamatchilRegexes":["period"]""")).isEmpty())
        val inherited = JsonObject(schema() + ("isSegmentLevelDiburHamatchil" to JsonPrimitive(true)))
        assertTrue(continuationRefPrefixes("Abarbanel on Torah", inherited).isEmpty())
        val riSchema = Json.parseToJsonElement("""{"depth":2,"sectionNames":["Daf","Comment"],"addressTypes":["Talmud","Integer"],"isSegmentLevelDiburHamatchil":true,"diburHamatchilRegexes":["^(.*?)\\."]}""").jsonObject
        // The real unlinked 29a:4 is a separate women/parents comment, though
        // its eight-word opener bypasses the old generic marker heuristic.
        assertFalse(opensNewComment("נשים פטורות וכי הבת פטורה מכבוד אב ואם. שנים שהזהיר הבן והבת."))
        assertTrue(continuationRefPrefixes("Tosafot Ri HaZaken on Kiddushin", riSchema).isEmpty())
    }

    @Test
    fun boundedChunksStopAfterNewCommentWithoutReadingTheTail() = runBlocking {
        val path = "Book"
        val entries = (1..2000).map { RefEntry("Abarbanel on Torah, Genesis 1:1:$it", "", path, it) }
        val ids = (0..1999).associate { (path to it) to it.toLong() }
        val chunks = mutableListOf<ContinuationRun>()
        walkContinuationRuns(path, entries, ids, emptySet(), { it == 0L }, { _, _ -> listOf(7L) },
            isEligibleRef = { true }, maxRunSize = 500) { chunks += it; false }
        assertEquals(1, chunks.size)
        assertEquals(500, chunks.single().lineIds.size)
        assertEquals(500, chunks.single().endLineIndex0)
    }

    @Test
    fun generateExtendsRangesAndCoverageInDb() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-continuation")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        // Abarbanel 1:1:1-2 is a range whose last segment is followed by unlinked 1:1:3;
        // Abarbanel 1:2:1 is linked alone and followed by unlinked 1:2:2.
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Abarbanel on Torah, Genesis 1:1:1-2","Genesis 1:1","Commentary"
            |"Abarbanel on Torah, Genesis 1:2:1","Genesis 1:2","Commentary"
            """.trimMargin()
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
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
            repo.insertBook(book(2, "אברבנאל על תורה", isBase = false, lines = 6))
            repo.insertLinesBatch(
                listOf(
                    Line(id = 1, bookId = 1, lineIndex = 0, content = "gen 1:1", heRef = "בראשית א, א"),
                    Line(id = 2, bookId = 1, lineIndex = 1, content = "gen 1:2", heRef = "בראשית א, ב"),
                ) + (0..4).map { Line(id = 10L + it, bookId = 2, lineIndex = it, content = "paragraph $it", heRef = "") } +
                    Line(id = 15, bookId = 2, lineIndex = 5, content = "<b>ויאמר.</b> פירוש חדש", heRef = "")
            )

            val paragraphAddresses = listOf("1:1:1", "1:1:2", "1:1:3", "1:2:1", "1:2:2", "1:2:3")
            val allRefs = listOf(
                RefEntry("Genesis 1:1", "", "Genesis", 1),
                RefEntry("Genesis 1:2", "", "Genesis", 2),
            ) + paragraphAddresses.mapIndexed { i, a -> RefEntry("Abarbanel on Torah, Genesis $a", "", "Abarbanel on Torah, Genesis", i + 1) }
            val refsByBase = mutableMapOf<String, RefEntry>()
            allRefs.forEach { e ->
                val base = canonicalBase(e.ref)
                val existing = refsByBase[base]
                if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
            }
            val refsByPath = allRefs.groupBy { it.path }
            val lineKeyToId = mapOf("Genesis" to 0 to 1L, "Genesis" to 1 to 2L) +
                (0..5).associate { ("Abarbanel on Torah, Genesis" to it) to 10L + it }
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
            generator.generate(refsByPath, lineKeyToId, emptySet(), eligibleRefPrefixesByBookId = emptyMap(), lineIdToBookId)
            assertEquals(1, query("SELECT COUNT(*) FROM link_coverage").single().single().toInt())
            generator.generate(refsByPath, lineKeyToId, emptySet(), eligibleRefPrefixesByBookId = mapOf(2L to setOf(canonicalCitation("Abarbanel on Torah, Genesis"))), lineIdToBookId)

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
            // Repetition must not widen beyond the same new-comment boundary or duplicate rows.
            val before = query("SELECT linkId, side, endLineId, endLineIndex FROM link_range ORDER BY linkId, side")
            val coverageBefore = query("SELECT lineId, linkId, side FROM link_coverage ORDER BY lineId, linkId, side")
            generator.generate(refsByPath, lineKeyToId, emptySet(), mapOf(2L to setOf(canonicalCitation("Abarbanel on Torah, Genesis"))), lineIdToBookId)
            assertEquals(before, query("SELECT linkId, side, endLineId, endLineIndex FROM link_range ORDER BY linkId, side"))
            assertEquals(coverageBefore, query("SELECT lineId, linkId, side FROM link_coverage ORDER BY lineId, linkId, side"))
            // A hidden commentary side must never be used as an extension anchor.
            driver.getConnection().createStatement().use { it.execute("INSERT INTO link_suppressed_side SELECT id, 1, 1 FROM link") }
            assertTrue(repo.selectTargetSideEnds(listOf("COMMENTARY"), 2L).first.isEmpty())
            assertTrue(repo.selectTargetSideEnds(listOf("COMMENTARY"), 1L).first.isEmpty())
        } finally {
            repo.close()
            Files.walk(tempDir).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    @Test
    fun multipleAnchorsCrossContentAndWriteBatchBoundariesSafely() = runBlocking {
        val driver = JdbcSqliteDriver("jdbc:sqlite::memory:")
        val repo = SeforimRepository(":memory:", driver)
        try {
            val source = repo.insertSource("Sefaria-Test")
            val category = repo.insertCategory(Category(0, null, "תורה", 0, 1))
            driver.getConnection().createStatement().use { st ->
                st.execute("INSERT INTO book(id,categoryId,sourceId,title) VALUES(1,$category,$source,'base'),(2,$category,$source,'paragraphs')")
                st.execute("INSERT INTO connection_type(id,name) VALUES(1,'COMMENTARY')")
            }
            repo.insertLinesBatch((0..1300).map { index ->
                Line(100L + index, 2L, index, if (index == 1123) "<b>מאמר חדש.</b> דבר" else "המשך הדברים כאן", "")
            } + (1..4).map { Line(it.toLong(), 1L, it - 1, "base $it", "") })
            driver.getConnection().createStatement().use { st ->
                for (id in 1..7) {
                    val sourceId = if (id == 5) 2 else 1
                    st.execute("INSERT INTO link(id,sourceBookId,targetBookId,sourceLineId,targetLineId,targetLineIndex,targetBookOrderIndex,connectionTypeId) VALUES($id,1,2,$sourceId,100,0,999,1)")
                }
                // An incidental same-corpus verse, a whole-unit range without
                // coverage and a hidden side must retain their explicit shape.
                st.execute("INSERT INTO link_range VALUES(6,1,1200,1100)")
                st.execute("INSERT INTO link_suppressed_side VALUES(7,1,1)")
            }
            val path = "paragraphs"
            val refs = (0..1300).map { RefEntry("Abarbanel on Torah, Genesis 1:1:${it + 1}", "", path, it + 1) }
            val baseRefs = (1..4).map { RefEntry("Genesis 1:$it", "", "base", it) }
            val keys = (0..1300).associate { (path to it) to (100L + it) } + (1..4).associate { ("base" to it - 1) to it.toLong() }
            val books = (0..1300).associate { (100L + it) to 2L }
            val generator = SefariaContinuationParagraphs(repo, Logger.withTag("bounded"))
            suspend fun generate() = generator.generate(mapOf(path to refs, "base" to baseRefs), keys, emptySet(), mapOf(2L to setOf("abarbanel on torah genesis")), books)
            fun rows(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
                st.executeQuery(sql).use { rs -> buildList {
                    while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) })
                } }
            }
            generate()
            assertEquals((1L..4L).map { listOf(it, 1222L, 1122L) }, rows("SELECT linkId,endLineId,endLineIndex FROM link_range WHERE linkId!=6 ORDER BY linkId"))
            assertEquals(listOf(listOf(6L, 1200L, 1100L)), rows("SELECT linkId,endLineId,endLineIndex FROM link_range WHERE linkId=6"))
            assertTrue(rows("SELECT linkId FROM link_coverage WHERE linkId IN (5,6,7)").isEmpty())
            assertEquals(listOf(listOf(4488L, 101L, 1222L)), rows("SELECT count(*),min(lineId),max(lineId) FROM link_coverage"))
            assertTrue(rows("SELECT linkId FROM link_coverage WHERE lineId>=1223").isEmpty())
            val before = rows("SELECT lineId,linkId,side FROM link_coverage ORDER BY lineId,linkId,side")
            generate()
            assertEquals(before, rows("SELECT lineId,linkId,side FROM link_coverage ORDER BY lineId,linkId,side"))
        } finally { repo.close() }
    }
}
