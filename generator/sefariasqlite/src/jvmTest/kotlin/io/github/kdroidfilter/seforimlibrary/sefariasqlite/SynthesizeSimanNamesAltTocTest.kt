package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ExtractSimanNameTest {

    @Test
    fun `strips seif marker, count tail and commentator markers`() {
        assertEquals(
            "דין השכמת הבוקר",
            extractSimanName(
                "(א) <b>דין השכמת הבוקר. ובו ט סעיפים:</b> " +
                    "<i data-commentator=\"Be'er HaGolah\" data-label=\"א\" data-order=\"1\"></i>יתגבר כארי",
            ),
        )
    }

    @Test
    fun `single seif tail`() {
        assertEquals("דין ברכת הלבנה", extractSimanName("(א) <b>דין ברכת הלבנה. ובו סעיף אחד:</b> text"))
    }

    @Test
    fun `commentator markers before and inside the bold segment`() {
        assertEquals(
            "דין לבישת בגדים",
            extractSimanName(
                "(א) <i data-commentator=\"Turei Zahav\" data-order=\"1\"></i><b>דין לבישת " +
                    "<i data-commentator=\"Magen Avraham\" data-order=\"2\"></i>בגדים. ובו ו סעיפים:</b> x",
            ),
        )
    }

    @Test
    fun `Aruch HaShulchan bracketed forms`() {
        assertEquals(
            "דין שחיטת כותי ושחיטת רשע",
            extractSimanName("<b>[דין שחיטת כותי ושחיטת רשע ובו כ\"א סעיפים].</b> שנו חכמים"),
        )
        assertEquals("דיני שחיטה", extractSimanName("<b>[דיני שחיטה ובו ג' סעי']</b> x"))
        assertEquals("דיני טריפות", extractSimanName("<b>[דיני טריפות ובו ובו ז' סעיפים]:</b> x"))
        assertEquals("דין הבדיקה", extractSimanName("<b>[דין הבדיקה ובו ה' סעיפים[3]]</b> x"))
    }

    @Test
    fun `an editorial small-note inside the name is dropped, leaving no stray bracket`() {
        assertEquals(
            "דיני הגוסס (ואמירת צידוק הדין) ומה הם הסימנים היפים",
            extractSimanName(
                "(א) <b>דיני הגוסס (ואמירת צידוק הדין) ומה הם הסימנים היפים " +
                    "<small>[מהסימנים נשמטו בשו\"ע ונמצאו בטור יו\"ד ע\"ש]</small>. ובו ה' סעיפים:</b> הגוסס",
            ),
        )
    }

    @Test
    fun `bare bold opening without seif marker or punctuation`() {
        assertEquals("איך נתינת פאה", extractSimanName("<b>איך נתינת פאה ובו כ' סעיפים</b> עיקר"))
    }

    @Test
    fun `no name without a leading bold segment or without the count tail`() {
        assertNull(extractSimanName("(א) <i data-commentator=\"Be'er HaGolah\"></i>שיעור כזית יש אומרים"))
        assertNull(extractSimanName("(א) לעבודת בוראו – <b>ובו</b> text"))
        assertNull(extractSimanName("(א) <b>הקדמה.</b> text"))
        assertNull(extractSimanName("(א) text <b>דין פלוני. ובו ג סעיפים:</b>"))
    }

    @Test
    fun `label round trip`() {
        assertEquals("[סימן א] דין השכמת הבוקר", simanLabel("סימן א", "דין השכמת הבוקר"))
        assertEquals("סימן א", simanLabel("סימן א", null))
        assertEquals("סימן א", simanLabelHeading("[סימן א] דין השכמת הבוקר"))
        assertEquals("סימן תפו", simanLabelHeading("סימן תפו"))
        assertEquals("כא", simanNumberKey("סימן כ\"א"))
    }
}

class PlanSimanNamesTest {

    private fun siman(tocId: Long, anchor: Long, content: String?, parent: Long? = 1) = SimanHeadingRow(
        tocId = tocId, parentTocId = parent, text = "סימן $tocId", headingLineIndex = anchor - 1,
        anchorLineId = anchor * 10, anchorLineIndex = anchor, endLineIndex = anchor + 2, firstContent = content,
    )

    private fun root(id: Long, line: Long) =
        TopicEntryRow(id, null, 0, "הלכות $id", line * 10, line, hasChildren = false)

    @Test
    fun `a sibling group below the hit rate keeps no names`() {
        val named = "(א) <b>דין פלוני. ובו ג סעיפים:</b>"
        assertEquals(
            emptyMap(),
            ownSimanNames(listOf(siman(1, 2, named), siman(2, 4, "x"), siman(3, 6, "y"))),
        )
        assertEquals(
            setOf(1L, 2L),
            ownSimanNames(listOf(siman(1, 2, named), siman(2, 4, named), siman(3, 6, "y"))).keys,
        )
    }

    @Test
    fun `each siman nests under the last Topic entry starting at or before its content`() {
        val children = planNestedChildren(
            roots = listOf(root(100, 2), root(200, 7)),
            simanim = listOf(siman(1, 0, null), siman(2, 2, null), siman(3, 5, null), siman(4, 7, null)),
            namesByTocId = mapOf(2L to "דין"),
        )
        assertEquals(
            listOf(100L to "[סימן 2] דין", 100L to "סימן 3", 200L to "סימן 4"),
            children.map { it.parentId to it.text },
        )
    }

    private fun book(structureId: Long?, simanim: List<SimanHeadingRow>, entries: List<TopicEntryRow>) =
        SimanNamesBookSnapshot(1, "ספר", structureId, simanim, entries)

    @Test
    fun `a Topic whose entries are simanim is relabeled, never nested into`() {
        val simanim = listOf(siman(1, 2, null))
        val kitzurShape = TopicEntryRow(10, null, 0, "[סימן 1] דין", 20, 2, hasChildren = false)
        assertEquals(SimanNamesMode.RELABEL, classifyTopicStructure(book(5, simanim, listOf(kitzurShape))))
        assertEquals(SimanNamesMode.NEST, classifyTopicStructure(book(5, simanim, listOf(root(10, 2)))))
        assertNull(classifyTopicStructure(book(null, simanim, emptyList())))
    }

    @Test
    fun `siman coverage counts simanim holding a linked line`() {
        val simanim = listOf(siman(1, 2, null), siman(2, 4, null), siman(3, 6, null)) // spans [1,4) [3,6) [5,8)
        assertEquals(1.0 / 3, linkedSimanShare(simanim, listOf(2L, 2L)))
        assertEquals(1.0, linkedSimanShare(simanim, listOf(2L, 4L, 7L)))
        assertEquals(0.0, linkedSimanShare(simanim, emptyList()))
    }

    @Test
    fun `borrowed roots follow the source by siman number and drop roots holding none, anchored at headings`() {
        fun numbered(tocId: Long, number: String, anchor: Long) = siman(tocId, anchor, null).copy(text = "סימן $number")
        val source = book(
            5,
            listOf(numbered(1, "א", 2), numbered(2, "ב", 4), numbered(3, "ג", 6)),
            // "mid" starts inside siman ב and ends where ג starts: it holds no siman.
            listOf(root(100, 2), root(150, 5), root(200, 6)),
        )
        val commentary = book(null, listOf(numbered(11, "א", 12), numbered(13, "ג", 16)), emptyList())
        assertEquals(
            listOf(CreatedRoot("הלכות 100", 120, 11), CreatedRoot("הלכות 200", 160, 15)),
            borrowedRoots(source, commentary),
        )
    }

    @Test
    fun `the app's visibility check — bracket prefix stripped, first three words near a line start`() {
        val sa = "(א) <i data-commentator=\"Be'er HaGolah\"></i><b>דין השכמת הבוקר. ובו ט סעיפים:</b> יתגבר"
        assertTrue(isInlineHeadingVisible("[סימן א] דין השכמת הבוקר", listOf(sa, "<h2>סימן א</h2>", null)))
        assertTrue(isInlineHeadingVisible("סימן א", listOf("(א) יתגבר", "<h2>סימן א</h2>", null)))
        // A borrowed name is not in the commentary's text: the app would inject it.
        assertTrue(!isInlineHeadingVisible("[סימן א] דין השכמת הבוקר", listOf("(א) יתגבר", "<h2>סימן א</h2>", null)))
        // Deep in the line does not count ("index <= 8").
        assertTrue(!isInlineHeadingVisible("דין השכמת הבוקר", listOf("הקדמה ארוכה מאוד מאוד דין השכמת הבוקר")))
        // ו/י and nikud are ignored on both sides.
        assertTrue(isInlineHeadingVisible("[סימן ו] דיני ציצית", listOf("<b>[דִּינֵי צִצִת ובו ג סעיפים]</b>")))
    }

    @Test
    fun `a Topic child the app would inject falls back to the bare heading or is skipped`() {
        val visible = siman(1, 2, "(א) <b>דין א. ובו ג סעיפים:</b>").copy(text = "סימן א", linesAboveAnchor = listOf("<h2>סימן א</h2>", null))
        val hidden = siman(2, 5, "(א) טקסט אחר").copy(text = "סימן ב", linesAboveAnchor = listOf("<h2>סימן ב</h2>", null))
        val noHeading = siman(3, 7, "(א) טקסט").copy(text = "סימן ג", linesAboveAnchor = listOf("שורה", "שורה"))
        val children = planNestedChildren(
            roots = listOf(root(100, 2)),
            simanim = listOf(visible, hidden, noHeading),
            namesByTocId = mapOf(1L to "דין א", 2L to "דין ב", 3L to "דין ג"),
            inlineVisibleOnly = true,
        )
        assertEquals(listOf("[סימן א] דין א", "סימן ב"), children.map { it.text })
    }

    @Test
    fun `bounded owners expire at gaps and restore roots without resurrecting expired children`() {
        val owners = nearestPrecedingOwners(
            (0L..12).map { (100 + it) to it },
            listOf(Triple(1L, 0, 1L), Triple(2L, 1, 2L), Triple(5L, 1, 3L), Triple(9L, 0, 4L), Triple(9L, 1, 5L)),
            mapOf(2L to 4L, 3L to 7L, 5L to 11L),
        )
        assertNull(owners[100])
        assertEquals(listOf(1L, 2L, 2L, 1L, 3L, 3L, 1L, 1L, 5L, 5L, 4L, 4L), (101L..112).map { owners[it] })
        assertEquals(
            mapOf(102L to 2L, 103L to 2L, 105L to 3L, 106L to 3L),
            nearestPrecedingOwners(
                (0L..8).map { (100 + it) to it },
                listOf(Triple(2L, 0, 2L), Triple(5L, 0, 3L)),
                mapOf(2L to 4L, 3L to 7L),
            ),
        )
    }

    @Test
    fun `line owner is the nearest preceding anchor, deeper wins on a shared line`() {
        val lines = (0L..8L).map { it * 10 to it }
        val owners = nearestPrecedingOwners(
            lines,
            listOf(Triple(2L, 0, 100L), Triple(2L, 1, 1L), Triple(5L, 1, 2L), Triple(7L, 0, 200L), Triple(7L, 1, 3L)),
        )
        assertEquals(
            listOf(null, null, 1L, 1L, 1L, 2L, 2L, 3L, 3L),
            lines.map { owners[it.first] },
        )
    }
}

/** End-to-end over a real (temp-file) DB shaped like SA OC, Biur Halacha and Aruch HaShulchan. */
class SynthesizeSimanNamesAltTocIntegrationTest {

    private val dbFile = Files.createTempFile("siman-names-test", ".db")
    private val stateFile: Path = Files.createTempFile("siman-names-state", ".db").also { Files.delete(it) }
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
        Files.deleteIfExists(stateFile)
    }

    private data class Seeded(
        val saTopic: Long,
        val bhTopic: Long,
        val ahTopic: Long,
        val noiseTopic: Long,
        val mb: Long,
        val mbSeifim: Long,
        val sparse: Long,
        val heAtid: Long,
        val keset: Long,
    )

    private suspend fun book(catId: Long, sourceId: Long, title: String) =
        repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = title, heRef = title))

    /** Lines by content; main-TOC headings at the given line indices (level 1 under a level-0 title). */
    private suspend fun lines(bookId: Long, contents: List<String>): List<Long> =
        contents.mapIndexed { i, c -> repo.insertLine(Line(bookId = bookId, lineIndex = i, content = c)) }

    private suspend fun simanToc(bookId: Long, lineIds: List<Long>, headings: Map<Int, String>, parentLine: Int = 0) {
        val parent = repo.insertTocEntry(
            TocEntry(bookId = bookId, parentId = null, text = "כותר $bookId", level = 0, lineId = lineIds[parentLine]),
        )
        for ((line, text) in headings) {
            repo.insertTocEntry(TocEntry(bookId = bookId, parentId = parent, text = text, level = 1, lineId = lineIds[line]))
        }
    }

    /** A flat Sefaria-style Topic structure and its line_alt_toc (nearest preceding root). */
    private suspend fun flatTopic(bookId: Long, lineIds: List<Long>, roots: List<Pair<String, Int>>): Long {
        val structureId = repo.upsertAltTocStructure(
            AltTocStructure(bookId = bookId, key = TOPIC_STRUCTURE_KEY, title = "Topic", heTitle = "נושאים"),
        )
        val rootIds = roots.mapIndexed { i, (text, line) ->
            repo.insertAltTocEntry(
                AltTocEntry(
                    structureId = structureId, text = text, level = 0, lineId = lineIds[line],
                    isLastChild = i == roots.lastIndex,
                ),
            )
        }
        for ((i, lineId) in lineIds.withIndex()) {
            val owner = roots.indices.lastOrNull { roots[it].second <= i } ?: continue
            repo.upsertLineAltToc(lineId, structureId, rootIds[owner])
        }
        return structureId
    }

    private fun seed(): Seeded = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val sourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        val topics = listOf("הלכות הנהגת האדם בבוקר", "הלכות ציצית")

        val sa = book(catId, sourceId, "שולחן ערוך, אורח חיים")
        val saLines = lines(
            sa,
            listOf(
                "<h1>שולחן ערוך, אורח חיים</h1>",
                "<h2>סימן א</h2>",
                "(א) <b>דין השכמת הבוקר. ובו ט סעיפים:</b> <i data-commentator=\"Mishnah Berurah\" data-label=\"א\"></i>יתגבר",
                "(ב) המשך",
                "<h2>סימן ב</h2>",
                "(א) <i data-commentator=\"Be'er HaGolah\" data-label=\"א\"></i><b>דין לבישת בגדים. ובו סעיף אחד:</b> x",
                "<h2>סימן ג</h2>",
                "(א) בלי שם",
                "(ב) עוד",
            ),
        )
        simanToc(sa, saLines, mapOf(1 to "סימן א", 4 to "סימן ב", 6 to "סימן ג"))
        val saTopic = flatTopic(sa, saLines, listOf(topics[0] to 2, topics[1] to 7))

        val bh = book(catId, sourceId, "ביאור הלכה")
        val bhLines = lines(bh, listOf("<h1>ביאור הלכה</h1>", "<h2>סימן א</h2>", "(א) יתגבר כארי", "<h2>סימן ג</h2>", "(א) דבור"))
        simanToc(bh, bhLines, mapOf(1 to "סימן א", 3 to "סימן ג"))
        val bhTopic = flatTopic(bh, bhLines, listOf(topics[0] to 2, topics[1] to 4))
        for ((saLine, bhLine) in listOf(saLines[2] to bhLines[2], saLines[7] to bhLines[4])) {
            repo.insertLink(
                Link(
                    sourceBookId = sa, targetBookId = bh, sourceLineId = saLine, targetLineId = bhLine,
                    targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }

        // Topic + simanim, but neither names nor a SA source: untouched.
        val noise = book(catId, sourceId, "ספר אחר")
        val noiseLines = lines(noise, listOf("<h1>ספר אחר</h1>", "<h2>סימן א</h2>", "(א) <b>פתיחה.</b> x"))
        simanToc(noise, noiseLines, mapOf(1 to "סימן א"))
        val noiseTopic = flatTopic(noise, noiseLines, listOf("נושא" to 2))

        // Aruch HaShulchan shape: the Topic tree already holds siman entries.
        val ah = book(catId, sourceId, "ערוך השולחן")
        val ahLines = lines(
            ah,
            listOf(
                "<h2>יורה דעה</h2>",
                "<h3>סימן א</h3>",
                "<b>[דין שחיטת כותי ובו כ\"א סעיפים].</b> שנו",
                "<h3>סימן ב</h3>",
                "<b>[דין אחר ובו ג' סעי']</b> x",
            ),
        )
        simanToc(ah, ahLines, mapOf(1 to "סימן א", 3 to "סימן ב"))
        val ahTopic = repo.upsertAltTocStructure(
            AltTocStructure(bookId = ah, key = TOPIC_STRUCTURE_KEY, title = "Topic", heTitle = "נושאים"),
        )
        val part = repo.insertAltTocEntry(AltTocEntry(structureId = ahTopic, text = "יורה דעה", level = 0, hasChildren = true))
        val halachot = repo.insertAltTocEntry(
            AltTocEntry(structureId = ahTopic, parentId = part, text = "הלכות שחיטה", level = 1, lineId = ahLines[2], hasChildren = true),
        )
        repo.insertAltTocEntry(AltTocEntry(structureId = ahTopic, parentId = halachot, text = "סימן א", level = 2, lineId = ahLines[2]))
        repo.insertAltTocEntry(AltTocEntry(structureId = ahTopic, parentId = halachot, text = "סימן ב", level = 2, lineId = ahLines[4]))

        fun commentaryLink(saLine: Long, target: Long, targetLine: Long) = runBlocking {
            repo.insertLink(
                Link(
                    sourceBookId = sa, targetBookId = target, sourceLineId = saLine, targetLineId = targetLine,
                    targetLineIndex = 0, connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }

        // Mishnah Berurah shape: no Topic, a Seifim structure that must stay as it is.
        val mb = book(catId, sourceId, "משנה ברורה")
        val mbLines = lines(
            mb,
            listOf("<h1>משנה ברורה</h1>", "<h2>סימן א</h2>", "(א) יתגבר", "<h2>סימן ב</h2>", "(א) לבישה", "<h2>סימן ג</h2>", "(א) עוד"),
        )
        simanToc(mb, mbLines, mapOf(1 to "סימן א", 3 to "סימן ב", 5 to "סימן ג"))
        for ((saLine, mbLine) in listOf(saLines[2] to mbLines[2], saLines[5] to mbLines[4], saLines[7] to mbLines[6])) {
            commentaryLink(saLine, mb, mbLine)
        }
        val mbSeifim = repo.upsertAltTocStructure(
            AltTocStructure(bookId = mb, key = SEIFIM_STRUCTURE_KEY, title = "Seifim", heTitle = "סעיפים"),
        )
        val mbSeifimEntry = repo.insertAltTocEntry(AltTocEntry(structureId = mbSeifim, text = "סימן א", level = 0, lineId = mbLines[1]))
        repo.upsertLineAltToc(mbLines[2], mbSeifim, mbSeifimEntry)

        // Every link from the SA, but only one of its three simanim linked: not a commentary on it.
        val sparse = book(catId, sourceId, "ספר מועט")
        val sparseLines = lines(sparse, listOf("<h2>סימן א</h2>", "(א) x", "<h2>סימן ב</h2>", "(א) y", "<h2>סימן ג</h2>", "(א) z"))
        simanToc(sparse, sparseLines, mapOf(0 to "סימן א", 2 to "סימן ב", 4 to "סימן ג"))
        commentaryLink(saLines[2], sparse, sparseLines[1])

        // ערוך השולחן העתיד shape: own names, no Topic, simanim under subheadings.
        val heAtid = book(catId, sourceId, "ערוך השולחן העתיד")
        val heAtidLines = lines(
            heAtid,
            listOf(
                "<h1>ערוך השולחן העתיד</h1>",
                "<h2>דיני זרעים</h2>",
                "<h3>סימן א</h3>",
                "<b>הלכות פאה ובו י' סעיפים</b> כתיב",
                "<h3>סימן ב</h3>",
                "<b>איך נתינת פאה ובו כ' סעיפים</b> עיקר",
            ),
        )
        val heAtidTitle = repo.insertTocEntry(
            TocEntry(bookId = heAtid, parentId = null, text = "ערוך השולחן העתיד", level = 0, lineId = heAtidLines[0]),
        )
        val zeraim = repo.insertTocEntry(
            TocEntry(bookId = heAtid, parentId = heAtidTitle, text = "דיני זרעים", level = 1, lineId = heAtidLines[1]),
        )
        for ((line, text) in listOf(2 to "סימן א", 4 to "סימן ב")) {
            repo.insertTocEntry(TocEntry(bookId = heAtid, parentId = zeraim, text = text, level = 2, lineId = heAtidLines[line]))
        }

        // קסת הסופר shape: own names, simanim straight under the title — they become the roots.
        val keset = book(catId, sourceId, "קסת הסופר")
        val kesetLines = lines(
            keset,
            listOf("<h1>קסת הסופר</h1>", "<h2>סימן א</h2>", "<b>דין הקלף ובו ג סעיפים</b> x", "<h2>סימן ב</h2>", "<b>דין הדיו ובו ד סעיפים</b> y"),
        )
        simanToc(keset, kesetLines, mapOf(1 to "סימן א", 3 to "סימן ב"))

        Seeded(saTopic, bhTopic, ahTopic, noiseTopic, mb, mbSeifim, sparse, heAtid, keset)
    }

    private fun structureOf(bookId: Long, key: String): Long? = dump(
        "SELECT id FROM alt_toc_structure WHERE bookId = $bookId AND key = '$key'",
    ).singleOrNull()?.let { (it[0] as Number).toLong() }

    private fun namesOf(bookId: Long): Long? = structureOf(bookId, SIMAN_NAMES_STRUCTURE_KEY)

    /** Empty build state plus the root paths the Sefaria builder would have recorded. */
    private fun attachState(conn: Connection, seeded: Seeded, create: Boolean) {
        if (create) BuildStateWriter().write(BuildStateSnapshot.empty(), stateFile)
        conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
            st.setString(1, stateFile.toString())
            st.execute()
        }
        if (!create) return
        for (structureId in listOf(seeded.saTopic, seeded.bhTopic, seeded.noiseTopic)) {
            val roots = conn.createStatement().use { st ->
                st.executeQuery("SELECT id FROM alt_toc_entry WHERE structureId = $structureId ORDER BY id").use { rs ->
                    buildList { while (rs.next()) add(rs.getLong(1)) }
                }
            }
            roots.forEachIndexed { i, id ->
                conn.createStatement().use {
                    it.executeUpdate(
                        "INSERT INTO seifim_state.id_alt_toc_entry(structure_id, ancestor_path, id) VALUES ($structureId, '${i + 1}', $id)",
                    )
                }
            }
        }
    }

    private fun run(seeded: Seeded, createState: Boolean): SimanNamesResult =
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            attachState(conn, seeded, createState)
            val books = readSimanNamesSnapshots(conn)
            writeSimanNames(conn, planSimanNames(books, readBorrowEvidence(conn, books)), AttachedBuildStateIds(conn))
        }

    private fun dump(sql: String): List<List<Any?>> = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val n = rs.metaData.columnCount
                buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
            }
        }
    }

    private fun tree(structureId: Long) = dump(
        """
        SELECT e.id, e.parentId, e.level, t.text, l.lineIndex, e.isLastChild, e.hasChildren
        FROM alt_toc_entry e JOIN tocText t ON t.id = e.textId LEFT JOIN line l ON l.id = e.lineId
        WHERE e.structureId = $structureId ORDER BY e.id
        """.trimIndent(),
    )

    private fun owners(structureId: Long) = dump(
        """
        SELECT l.lineIndex, t.text FROM line_alt_toc m
        JOIN line l ON l.id = m.lineId JOIN alt_toc_entry e ON e.id = m.altTocEntryId JOIN tocText t ON t.id = e.textId
        WHERE m.structureId = $structureId ORDER BY l.lineIndex
        """.trimIndent(),
    ).map { (it[0] as Number).toLong() to it[1] as String }

    @Test
    fun `nests named siman children under the Shulchan Aruch Topic entries`() {
        val seeded = seed()
        val rootsBefore = tree(seeded.saTopic)
        val result = run(seeded, createState = true)
        assertEquals(
            SimanNamesResult(books = 6, children = 3, named = 2, relabeled = 2, separateStructures = 4, separateEntries = 14),
            result,
        )

        val after = tree(seeded.saTopic)
        val roots = after.filter { it[1] == null }
        assertEquals(rootsBefore.map { it.take(6) }, roots.map { it.take(6) }, "Topic entries keep id, text, line and isLastChild")
        assertEquals(listOf(1, 1), roots.map { (it[6] as Number).toInt() })
        val children = after.filter { it[1] != null }.map { listOf(it[1], it[2], it[3], it[4], it[5], it[6]) }
        val (t1, t2) = roots.map { it[0] }
        assertEquals(
            listOf(
                listOf(t1, 1, "[סימן א] דין השכמת הבוקר", 2, 0, 0),
                listOf(t1, 1, "[סימן ב] דין לבישת בגדים", 5, 1, 0),
                listOf(t2, 1, "סימן ג", 7, 1, 0),
            ),
            children,
        )
        assertEquals(
            listOf(
                2L to "[סימן א] דין השכמת הבוקר",
                3L to "[סימן א] דין השכמת הבוקר",
                4L to "הלכות הנהגת האדם בבוקר",
                5L to "[סימן ב] דין לבישת בגדים",
                6L to "הלכות הנהגת האדם בבוקר",
                7L to "סימן ג",
                8L to "סימן ג",
            ),
            owners(seeded.saTopic),
        )
    }

    /** (id, textId, text, lineIndex, bookId) of every Topic entry. */
    private fun topicEntries() = dump(
        """
        SELECT e.id, e.textId, t.text, l.lineIndex, s.bookId
        FROM alt_toc_entry e JOIN alt_toc_structure s ON s.id = e.structureId
        JOIN tocText t ON t.id = e.textId LEFT JOIN line l ON l.id = e.lineId
        WHERE s.key = '$TOPIC_STRUCTURE_KEY' ORDER BY e.id
        """.trimIndent(),
    )

    @Test
    fun `no Topic entry this stage adds or relabels is one the app would inject into the text`() {
        val seeded = seed()
        val before = topicEntries().map { it[0] to it[1] }.toSet()
        run(seeded, createState = true)
        val touched = topicEntries().filter { (it[0] to it[1]) !in before }
        assertEquals(5, touched.size, "3 SA children + 2 Aruch HaShulchan relabels")
        val invisible = touched.filterNot { row ->
            val bookId = (row[4] as Number).toLong()
            val index = (row[3] as Number).toLong()
            val window = (0L..2L).map { back ->
                dump("SELECT content FROM line WHERE bookId = $bookId AND lineIndex = ${index - back}").singleOrNull()?.get(0) as String?
            }
            isInlineHeadingVisible(row[2] as String, window)
        }
        assertEquals(emptyList(), invisible)
    }

    @Test
    fun `a borrowing commentary gets a separate structure, its Topic untouched`() {
        val seeded = seed()
        val bhTopicBefore = tree(seeded.bhTopic) to owners(seeded.bhTopic)
        val noiseBefore = tree(seeded.noiseTopic) to owners(seeded.noiseTopic)
        run(seeded, createState = true)
        assertEquals(bhTopicBefore, tree(seeded.bhTopic) to owners(seeded.bhTopic))
        assertEquals(noiseBefore, tree(seeded.noiseTopic) to owners(seeded.noiseTopic))
        val bhNames = namesOf(dump("SELECT bookId FROM alt_toc_structure WHERE id = ${seeded.bhTopic}").single()[0].let { (it as Number).toLong() })!!
        assertEquals(
            listOf(
                listOf(0, "הלכות הנהגת האדם בבוקר", 1),
                listOf(0, "הלכות ציצית", 3),
                listOf(1, "[סימן א] דין השכמת הבוקר", 1),
                listOf(1, "סימן ג", 3),
            ),
            tree(bhNames).map { listOf(it[2], it[3], it[4]) },
        )
        assertEquals(
            listOf(listOf(SIMAN_NAMES_STRUCTURE_KEY, SIMAN_NAMES_STRUCTURE_TITLE_HE_HALACHOT)),
            dump("SELECT key, heTitle FROM alt_toc_structure WHERE id = $bhNames"),
        )
    }

    @Test
    fun `existing siman entries are relabeled in place`() {
        val seeded = seed()
        val before = tree(seeded.ahTopic)
        run(seeded, createState = true)
        val after = tree(seeded.ahTopic)
        assertEquals(before.map { it[0] }, after.map { it[0] })
        assertEquals(
            listOf("יורה דעה", "הלכות שחיטה", "[סימן א] דין שחיטת כותי", "[סימן ב] דין אחר"),
            after.map { it[3] },
        )
    }

    @Test
    fun `a commentary without a Topic gets a SimanNames structure from its source, Seifim untouched`() {
        val seeded = seed()
        val seifim = "SELECT * FROM alt_toc_entry WHERE structureId = ${seeded.mbSeifim} ORDER BY id"
        val seifimMap = "SELECT * FROM line_alt_toc WHERE structureId = ${seeded.mbSeifim} ORDER BY lineId"
        val seifimBefore = dump(seifim) to dump(seifimMap)
        run(seeded, createState = true)

        assertNull(structureOf(seeded.mb, TOPIC_STRUCTURE_KEY), "never a new Topic")
        val names = namesOf(seeded.mb)!!
        assertEquals(
            listOf(
                listOf(false, 0, "הלכות הנהגת האדם בבוקר", 1, 0, 1),
                listOf(false, 0, "הלכות ציצית", 5, 1, 1),
                listOf(true, 1, "[סימן א] דין השכמת הבוקר", 1, 0, 0),
                listOf(true, 1, "[סימן ב] דין לבישת בגדים", 3, 1, 0),
                listOf(true, 1, "סימן ג", 5, 1, 0),
            ),
            tree(names).map { listOf(it[1] != null, it[2], it[3], it[4], it[5], it[6]) },
        )
        assertEquals(
            listOf(1L, 2L, 3L, 4L, 5L, 6L).zip(
                listOf(
                    "[סימן א] דין השכמת הבוקר", "[סימן א] דין השכמת הבוקר",
                    "[סימן ב] דין לבישת בגדים", "[סימן ב] דין לבישת בגדים", "סימן ג", "סימן ג",
                ),
            ),
            owners(names),
        )
        assertEquals(seifimBefore, dump(seifim) to dump(seifimMap))
        assertTrue(runBlocking { repo.getBook(seeded.mb)!!.hasAltStructures })
        assertNull(namesOf(seeded.sparse), "a book linked in one siman of three is not a commentary")
    }

    @Test
    fun `a book naming its own simanim without a Topic gets a SimanNames structure from its main TOC`() {
        val seeded = seed()
        run(seeded, createState = true)
        assertNull(structureOf(seeded.heAtid, TOPIC_STRUCTURE_KEY))
        assertEquals(
            listOf(
                listOf(null, 0, "דיני זרעים", 2),
                listOf(0, 1, "[סימן א] הלכות פאה", 2),
                listOf(0, 1, "[סימן ב] איך נתינת פאה", 4),
            ),
            tree(namesOf(seeded.heAtid)!!).let { rows ->
                val rootId = rows.first()[0]
                rows.map { listOf(it[1]?.let { p -> if (p == rootId) 0 else p }, it[2], it[3], it[4]) }
            },
        )
        assertEquals(
            listOf(listOf(0, "[סימן א] דין הקלף", 1, 0), listOf(0, "[סימן ב] דין הדיו", 3, 1)),
            tree(namesOf(seeded.keset)!!).map { listOf(it[2], it[3], it[4], it[5]) },
        )
    }

    @Test
    fun `Topic children stop at external sections and reruns repair earlier leaked ownership`() {
        val seeded = seed()
        val saId = (dump("SELECT bookId FROM alt_toc_structure WHERE id = ${seeded.saTopic}").single()[0] as Number).toLong()
        val appended = runBlocking {
            val contents = listOf(
                "<h2>סדר הגט</h2>", "פסקה ראשונה", "<h3>פרק א</h3>", "פסקה שניה",
                "<h2>סימן ד</h2>", "<b>דין נוסף ובו ג סעיפים</b> x", "<h3>סעיף א</h3>", "המשך הדין",
                "<h2>סדר חליצה</h2>", "נספח",
            )
            val ids = contents.mapIndexed { i, c -> repo.insertLine(Line(bookId = saId, lineIndex = i + 9, content = c)) }
            val title = (dump("SELECT id FROM tocEntry WHERE bookId = $saId AND parentId IS NULL").single()[0] as Number).toLong()
            val get = repo.insertTocEntry(TocEntry(bookId = saId, parentId = title, text = "סדר הגט", level = 1, lineId = ids[0]))
            repo.insertTocEntry(TocEntry(bookId = saId, parentId = get, text = "פרק א", level = 2, lineId = ids[2]))
            val siman = repo.insertTocEntry(TocEntry(bookId = saId, parentId = title, text = "סימן ד", level = 1, lineId = ids[4]))
            repo.insertTocEntry(TocEntry(bookId = saId, parentId = siman, text = "סעיף א", level = 2, lineId = ids[6]))
            repo.insertTocEntry(TocEntry(bookId = saId, parentId = title, text = "סדר חליצה", level = 1, lineId = ids[8]))
            // The original Topic root mapping is authoritative outside generated children.
            val root = (tree(seeded.saTopic).last()[0] as Number).toLong()
            ids.forEach { repo.upsertLineAltToc(it, seeded.saTopic, root) }
            ids
        }
        val originalLines = dump("SELECT * FROM line ORDER BY id")
        val originalToc = dump("SELECT * FROM tocEntry ORDER BY id")
        run(seeded, createState = true)
        val byIndex = owners(seeded.saTopic).toMap()
        for (index in listOf(9L, 10L, 11L, 12L, 13L, 17L, 18L)) assertEquals("הלכות ציצית", byIndex[index])
        for (index in 14L..16L) assertEquals("[סימן ד] דין נוסף", byIndex[index], "descendant headings remain in the siman")
        val firstTree = tree(seeded.saTopic)
        val firstOwners = owners(seeded.saTopic)
        val lastChild = (firstTree.last { it[1] != null }[0] as Number).toLong()
        // Simulate a previously released unbounded owner map, including both gaps.
        runBlocking { appended.filterIndexed { i, _ -> i < 4 || i >= 8 }.forEach { repo.upsertLineAltToc(it, seeded.saTopic, lastChild) } }
        run(seeded, createState = false)
        assertEquals(firstTree, tree(seeded.saTopic), "all entry IDs survive repair")
        assertEquals(firstOwners, owners(seeded.saTopic))
        assertEquals(originalLines, dump("SELECT * FROM line ORDER BY id"))
        assertEquals(originalToc, dump("SELECT * FROM tocEntry ORDER BY id"))
        assertTrue(dump("PRAGMA foreign_key_check").isEmpty())
    }

    @Test
    fun `separate flat and grouped structures exclude appendices and rejected intervening groups`() {
        val seeded = seed()
        runBlocking {
            val flatAppendix = listOf("<h2>סדר הגט</h2>", "נספח").mapIndexed { i, c ->
                repo.insertLine(Line(bookId = seeded.keset, lineIndex = 5 + i, content = c))
            }
            val flatTitle = (dump("SELECT id FROM tocEntry WHERE bookId = ${seeded.keset} AND parentId IS NULL").single()[0] as Number).toLong()
            repo.insertTocEntry(TocEntry(bookId = seeded.keset, parentId = flatTitle, text = "סדר הגט", level = 1, lineId = flatAppendix[0]))
            val contents = listOf(
                "<h2>דיני שמיטה</h2>", "<h3>סימן א</h3>", "בלי שם", "<h3>סימן ב</h3>", "בלי שם",
                "<h2>דיני תרומות</h2>", "<h3>סימן א</h3>", "<b>דין תרומה ובו ג סעיפים</b> x",
                "<h3>סימן ב</h3>", "<b>דין מעשר ובו ד סעיפים</b> x", "<h2>נספח</h2>", "נספח",
            )
            val ids = contents.mapIndexed { i, c -> repo.insertLine(Line(bookId = seeded.heAtid, lineIndex = 6 + i, content = c)) }
            val title = (dump("SELECT id FROM tocEntry WHERE bookId = ${seeded.heAtid} AND parentId IS NULL").single()[0] as Number).toLong()
            for ((parentIndex, simanIndices) in listOf(0 to listOf(1, 3), 5 to listOf(6, 8))) {
                val parent = repo.insertTocEntry(TocEntry(bookId = seeded.heAtid, parentId = title, text = if (parentIndex == 0) "דיני שמיטה" else "דיני תרומות", level = 1, lineId = ids[parentIndex]))
                simanIndices.forEachIndexed { i, index -> repo.insertTocEntry(TocEntry(bookId = seeded.heAtid, parentId = parent, text = "סימן ${if (i == 0) "א" else "ב"}", level = 2, lineId = ids[index])) }
            }
            repo.insertTocEntry(TocEntry(bookId = seeded.heAtid, parentId = title, text = "נספח", level = 1, lineId = ids[10]))
        }
        run(seeded, createState = true)
        assertEquals(listOf(1L, 2L, 3L, 4L), owners(namesOf(seeded.keset)!!).map { it.first })
        assertEquals(listOf(2L, 3L, 4L, 5L, 12L, 13L, 14L, 15L), owners(namesOf(seeded.heAtid)!!).map { it.first })
        assertEquals(6, tree(namesOf(seeded.heAtid)!!).size, "two roots and four children; rejected group has no children")
        val tableSql = listOf("SELECT * FROM alt_toc_entry ORDER BY id", "SELECT * FROM line_alt_toc ORDER BY lineId, structureId")
        val before = tableSql.map(::dump)
        run(seeded, createState = false)
        assertEquals(before, tableSql.map(::dump))
        assertTrue(dump("PRAGMA foreign_key_check").isEmpty())
    }

    @Test
    fun `a failed child insert rolls back owner maps text rows and allocator changes`() {
        val seeded = seed()
        val tables = listOf("book", "line", "tocEntry", "tocText", "alt_toc_structure", "alt_toc_entry", "line_alt_toc")
        val before = tables.map { dump("SELECT * FROM $it ORDER BY 1, 2") }
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            attachState(conn, seeded, create = true)
            val stateTables = listOf("id_alt_toc_entry", "id_alt_toc_structure", "id_lookup", "id_counters")
            fun stateRows() = stateTables.map { table ->
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT * FROM seifim_state.$table ORDER BY 1, 2").use { rs ->
                        buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getObject(it) }) }
                    }
                }
            }
            val stateBefore = stateRows()
            // Abort the last SA child after earlier entries/text IDs have been written.
            val lastLine = (dump("SELECT lineId FROM alt_toc_entry WHERE structureId = ${seeded.saTopic} ORDER BY id DESC LIMIT 1").single()[0] as Number).toLong()
            conn.createStatement().use { st ->
                st.executeUpdate(
                    """
                    CREATE TRIGGER fail_siman_child BEFORE INSERT ON alt_toc_entry
                    WHEN NEW.structureId = ${seeded.saTopic} AND NEW.parentId IS NOT NULL AND NEW.lineId = $lastLine
                    BEGIN SELECT RAISE(ABORT, 'intentional QA insert failure'); END
                    """.trimIndent(),
                )
            }
            val books = readSimanNamesSnapshots(conn)
            val failure = assertFailsWith<Exception> {
                writeSimanNames(conn, planSimanNames(books, readBorrowEvidence(conn, books)), AttachedBuildStateIds(conn))
            }
            assertTrue(failure.message.orEmpty().contains("intentional QA insert failure"))
            assertTrue(conn.autoCommit, "caller regains JDBC ownership")
            assertEquals(stateBefore, stateRows(), "attached allocator participates in rollback")
        }
        assertEquals(before, tables.map { dump("SELECT * FROM $it ORDER BY 1, 2") })
        assertTrue(dump("PRAGMA foreign_key_check").isEmpty())
    }

    @Test
    fun `a rerun reproduces every row and id`() {
        val seeded = seed()
        val first = run(seeded, createState = true)
        val all = "SELECT * FROM alt_toc_entry ORDER BY id"
        val map = "SELECT * FROM line_alt_toc ORDER BY lineId, structureId"
        val others = listOf(
            "SELECT * FROM tocText ORDER BY id",
            "SELECT * FROM alt_toc_structure ORDER BY id",
            "SELECT id, hasAltStructures FROM book ORDER BY id",
        )
        val rows = listOf(dump(all), dump(map)) + others.map(::dump)
        val second = run(seeded, createState = false)
        assertEquals(rows, listOf(dump(all), dump(map)) + others.map(::dump))
        assertEquals(first.copy(relabeled = 0, relabelUnchanged = 2), second, "the relabels are already in place")
    }

    @Test
    fun `heTitle says halachot only when the roots are halachot`() {
        val seeded = seed()
        run(seeded, createState = true)
        fun heTitle(bookId: Long) = dump("SELECT heTitle FROM alt_toc_structure WHERE id = ${namesOf(bookId)}").single()[0]
        assertEquals(SIMAN_NAMES_STRUCTURE_TITLE_HE_HALACHOT, heTitle(seeded.mb))
        assertEquals(SIMAN_NAMES_STRUCTURE_TITLE_HE, heTitle(seeded.heAtid))
        assertEquals(SIMAN_NAMES_STRUCTURE_TITLE_HE, heTitle(seeded.keset))
    }

    @Test
    fun `a book losing its only structure has hasAltStructures cleared`() {
        val seeded = seed()
        run(seeded, createState = true)
        assertTrue(runBlocking { repo.getBook(seeded.keset)!!.hasAltStructures })
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.createStatement().use { it.executeUpdate("UPDATE line SET content = 'x' WHERE bookId = ${seeded.keset} AND content LIKE '<b>%'") }
        }
        run(seeded, createState = false)
        assertNull(namesOf(seeded.keset))
        assertTrue(!runBlocking { repo.getBook(seeded.keset)!!.hasAltStructures })
        assertTrue(runBlocking { repo.getBook(seeded.mb)!!.hasAltStructures }, "MB keeps its Seifim structure and flag")
    }

    @Test
    fun `child ids come from the build state, keyed under the Topic entry path`() {
        val seeded = seed()
        run(seeded, createState = true)
        val state = DriverManager.getConnection("jdbc:sqlite:$stateFile").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery(
                    "SELECT ancestor_path, id FROM id_alt_toc_entry WHERE structure_id = ${seeded.saTopic} ORDER BY ancestor_path",
                ).use { rs -> buildMap { while (rs.next()) put(rs.getString(1), rs.getLong(2)) } }
            }
        }
        val children = tree(seeded.saTopic).filter { it[1] != null }.map { (it[0] as Number).toLong() }
        assertEquals(listOf("1", "1/1", "1/2", "2", "2/1"), state.keys.toList())
        assertEquals(children, listOf(state.getValue("1/1"), state.getValue("1/2"), state.getValue("2/1")))
        assertTrue(children.all { it > 0 })
    }
}

/** One focused test per borrow gate: each flips a single input of an otherwise accepted borrower. */
class BorrowGatesTest {

    private val letters = listOf("א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י")

    private fun simanim(bookId: Long, numbers: List<String>, named: Boolean) = numbers.mapIndexed { i, n ->
        val anchor = 3L * i + 2
        SimanHeadingRow(
            tocId = bookId * 100 + i, parentTocId = bookId * 100 - 1, text = "סימן $n", headingLineIndex = anchor - 1,
            anchorLineId = bookId * 1000 + anchor, anchorLineIndex = anchor, endLineIndex = anchor + 2,
            firstContent = if (named) "(א) <b>דין $n. ובו ג סעיפים:</b> x" else "(א) x",
            headingLineId = bookId * 1000 + anchor - 1,
        )
    }

    private fun topic(bookId: Long, text: String) = listOf(TopicEntryRow(bookId * 10, null, 0, text, bookId * 1000 + 2, 2, false))

    private val source = SimanNamesBookSnapshot(10, "שו\"ע", 5, simanim(10, letters.take(5), named = true), topic(10, "הלכות פלוניות"))

    private fun borrower(
        numbers: List<String> = letters.take(5),
        topicTitle: String? = null,
    ) = SimanNamesBookSnapshot(20, "מפרש", topicTitle?.let { 7L }, simanim(20, numbers, named = false), topicTitle?.let { topic(20, it) } ?: emptyList())

    private fun planFor(
        book: SimanNamesBookSnapshot = borrower(),
        src: SimanNamesBookSnapshot = source,
        evidence: BorrowEvidence = BorrowEvidence(10, 1.0, 1.0),
    ) = planSimanNames(listOf(src, book), mapOf(20L to evidence)).singleOrNull { it.book.bookId == 20L }

    @Test
    fun `an accepted borrower gets a separate structure with the source's names`() {
        val plan = planFor()!!
        assertEquals(SimanNamesMode.SEPARATE, plan.mode)
        assertEquals(10L, plan.nameSource)
        assertEquals(listOf("דין א", "דין ב", "דין ג", "דין ד", "דין ה"), plan.namesByTocId.toSortedMap().values.toList())
        assertEquals(listOf("הלכות פלוניות"), plan.createdRoots.map { it.text })
    }

    @Test
    fun `rejected when under 90 percent of its COMMENTARY links come from the source`() {
        assertNull(planFor(evidence = BorrowEvidence(10, 0.89, 1.0)))
    }

    @Test
    fun `rejected when the source's links reach under half of its simanim`() {
        assertNull(planFor(evidence = BorrowEvidence(10, 1.0, 0.49)))
    }

    @Test
    fun `rejected when under 90 percent of its siman numbers exist in the source`() {
        // 4 of 5 numbers (80%) are the source's.
        assertNull(planFor(book = borrower(numbers = listOf("א", "ב", "ג", "ד", "ט"))))
    }

    @Test
    fun `rejected when its own Topic titles are not the source's`() {
        assertNull(planFor(book = borrower(topicTitle = "הלכות אחרות")))
        assertEquals(SimanNamesMode.SEPARATE, planFor(book = borrower(topicTitle = "הלכות פלוניות"))!!.mode)
    }

    @Test
    fun `rejected when the source repeats a siman number`() {
        val repeated = source.copy(simanim = simanim(10, listOf("א", "ב", "ג", "ד", "א"), named = true))
        assertNull(planFor(src = repeated))
    }
}
