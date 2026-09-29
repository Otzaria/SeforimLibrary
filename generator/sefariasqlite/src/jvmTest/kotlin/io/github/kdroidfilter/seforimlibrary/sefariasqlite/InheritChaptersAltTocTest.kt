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
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InheritChaptersParsingTest {

    @Test
    fun `daf headings in every amud notation`() {
        assertEquals(AmudSpan(26, 26), parseDafHeading("דף יג."))
        assertEquals(AmudSpan(27, 27), parseDafHeading("יג:"))
        assertEquals(AmudSpan(26, 26), parseDafHeading("דף יג ע\"א"))
        assertEquals(AmudSpan(27, 27), parseDafHeading("דף יג ע״ב"))
        assertEquals(AmudSpan(27, 27), parseDafHeading("דף יג עמוד ב"))
        assertEquals(AmudSpan(26, 27), parseDafHeading("דף יג"))
        assertEquals(AmudSpan(30, 30), parseDafHeading("﻿דף טו. - תוספת"))
        assertEquals(AmudSpan(44, 44), parseDafHeading("דף כ\"ב."))
    }

    @Test
    fun `other headings are not daf headings`() {
        for (text in listOf("הקדמה", "פרק ב", "דף הקדמה", "אמת ליעקב על ברכות", "דף יי.", "ע\"א")) {
            assertNull(parseDafHeading(text), text)
        }
    }

    @Test
    fun `base heRef gives the chapter start amud`() {
        assertEquals(26, parseHeRefAmud("ברכות, יג., טז"))
        assertEquals(35, parseHeRefAmud("ברכות, יז:, יב"))
        assertNull(parseHeRefAmud("ספרי במדבר, א, ב"))
        assertNull(parseHeRefAmud(null))
    }

    @Test
    fun `longest non-decreasing run drops strays on both sides`() {
        val values = listOf(5, 0, 0, 1, 0, 1, 2, 2, 0)
        assertEquals(listOf(1, 2, 3, 5, 6, 7), longestNonDecreasingRun(values))
    }

    @Test
    fun `title base takes the longest matching suffix`() {
        val bases = mapOf("ברכות" to 1L, "רש\"י על ברכות" to 2L, "חולין" to 3L)
        assertEquals(1L, titleChaptersBase("מאירי על ברכות", bases))
        assertEquals(2L, titleChaptersBase("מהרש\"א על רש\"י על ברכות", bases))
        assertEquals(3L, titleChaptersBase("קרן אורה על מסכת חולין", bases))
        assertNull(titleChaptersBase("משנה ברכות", bases))
    }

    /** Bases 1 (ברכות) and 2 (רי"ף ברכות) share chapters; 3 (תוספות על ב"ב) and 4 (רשב"ם) share others. */
    private val groups = mapOf(1L to "B", 2L to "B", 3L to "BB", 4L to "BB")
    private val baseOf = mapOf(1L to 1L, 2L to 2L, 3L to 3L, 4L to 4L)
    private val baseIds = groups.keys

    private fun choose(
        title: Long?,
        declared: List<Long>,
        links: Map<Long, Int>,
        groupOf: Map<Long, String> = groups,
        baseOfSource: Map<Long, Long> = baseOf,
    ) = chooseChaptersBase(title, declared, links, groupOf, baseOfSource, baseIds)

    @Test
    fun `base choice follows the data before the title`() {
        // Links point at the declared Rif base, not the tractate the title names.
        assertEquals(ChaptersBaseChoice(2L, ChaptersBaseRule.LINKS, setOf(1L, 2L)), choose(1L, listOf(2L), mapOf(2L to 348, 1L to 3)))
        // Links only to a non-Chapters book (Yerushalmi): the title is wrong.
        assertNull(choose(1L, listOf(9L), mapOf(9L to 10)))
        // A minority share is not enough.
        assertNull(choose(1L, emptyList(), mapOf(9L to 1500, 1L to 80)))
        // A declared base that the links contradict.
        assertNull(choose(1L, listOf(9L), mapOf(1L to 80)))
        assertEquals(ChaptersBaseChoice(1L, ChaptersBaseRule.LINKS, setOf(1L)), choose(1L, emptyList(), mapOf(8L to 3400, 1L to 3100)))
        assertEquals(ChaptersBaseChoice(2L, ChaptersBaseRule.DECLARED), choose(null, listOf(2L), emptyMap()))
        assertNull(choose(1L, listOf(9L), emptyMap()))
        assertEquals(ChaptersBaseChoice(1L, ChaptersBaseRule.TITLE), choose(1L, emptyList(), emptyMap()))
    }

    @Test
    fun `a tie between bases with the same chapters is resolved`() {
        // 7 links from Tosafot, 7 from Rashbam on the same tractate: one group, lowest id wins.
        assertEquals(ChaptersBaseChoice(3L, ChaptersBaseRule.LINKS, setOf(3L, 4L)), choose(null, emptyList(), mapOf(3L to 7, 4L to 7)))
        assertEquals(4L, choose(4L, emptyList(), mapOf(3L to 7, 4L to 7))?.baseId, "the title's base breaks the tie")
        // Different chapter lists still tie out.
        assertNull(choose(null, emptyList(), mapOf(1L to 7, 3L to 7)))
    }

    @Test
    fun `credited commentary links count for their base`() {
        // 37 links from a commentary (20) the first pass gave Rif chapters, 1 from the Rif.
        val links = mapOf(20L to 37, 2L to 1)
        assertNull(choose(2L, listOf(2L, 20L), links), "first pass: 1 of 38 links")
        val credited = choose(2L, listOf(2L, 20L), links, groups + (20L to "B"), baseOf + (20L to 2L))
        assertEquals(ChaptersBaseChoice(2L, ChaptersBaseRule.LINKS, setOf(2L, 20L)), credited)
    }

    @Test
    fun `daf children fall in their chapter by line`() {
        val anchors = listOf(ChapterAnchor(chapters[0], 1, 0), ChapterAnchor(chapters[1], 6, 1))
        val headings = listOf(
            ChaptersHeading(0, "ספר", id = 1),
            ChaptersHeading(1, "דף ב.", id = 2, parentId = 1),
            ChaptersHeading(2, "הגהות", id = 3, parentId = 2),
            ChaptersHeading(4, "דף ג.", id = 4, parentId = 1),
            ChaptersHeading(5, "חדושים", id = 5, parentId = 4),
            ChaptersHeading(7, "חדושים", id = 6, parentId = 4),
            ChaptersHeading(8, "דף ג:", id = 7, parentId = 1),
            ChaptersHeading(9, "סיום", id = 8, parentId = 1),
        )
        val dafs = buildDafChildren(anchors, headings)
        assertEquals(
            listOf(
                listOf(DafNode("דף ב.", 1, listOf(DafNode("הגהות", 2))), DafNode("דף ג.", 4, listOf(DafNode("חדושים", 5)))),
                // "דף ג." opens above the chapter-2 anchor, so it and its in-range heading stay in chapter 1.
                listOf(DafNode("דף ג:", 8)),
            ),
            dafs,
        )
    }

    @Test
    fun `daf headings above the first anchor go to the first chapter`() {
        // Book-title heading, then dafs; the first linked line comes after "דף לח.".
        val anchors = listOf(ChapterAnchor(chapters[0], 3, 0), ChapterAnchor(chapters[1], 6, 1))
        val headings = listOf(
            ChaptersHeading(0, "קרן לדוד על סוכה", id = 1),
            ChaptersHeading(1, "דף לח.", id = 2, parentId = 1),
            ChaptersHeading(4, "דף לט.", id = 3, parentId = 1),
            ChaptersHeading(7, "דף מ.", id = 4, parentId = 1),
        )
        assertEquals(
            listOf(listOf(DafNode("דף לח.", 1), DafNode("דף לט.", 4)), listOf(DafNode("דף מ.", 7))),
            buildDafChildren(anchors, headings),
        )
    }

    @Test
    fun `a numbered perek heading must sit in that chapter`() {
        val anchors = listOf(ChapterAnchor(chapters[1], 10, 1), ChapterAnchor(chapters[2], 14, 2))
        // "פרק ב" inside chapter 2: consistent; a lone title-like "פרק" name is ignored.
        assertTrue(perekHeadingsAgree(anchors, listOf(ChaptersHeading(10, "פרק ב"), ChaptersHeading(2, "פרק מאימתי"))))
        // Rosh on Yoma: "פרק ח" (here "פרק ג") on a line the structure gives to chapter 2.
        assertFalse(perekHeadingsAgree(anchors, listOf(ChaptersHeading(10, "פרק ג"))))
        // Above the first anchor nothing covers the heading.
        assertTrue(perekHeadingsAgree(anchors, listOf(ChaptersHeading(1, "פרק א"))))
    }

    @Test
    fun `chapter names in the main TOC mark it as already naming chapters`() {
        assertTrue(hasOwnChapterHeadings(listOf(ChaptersHeading(0, "מאימתי קורין"), ChaptersHeading(9, "היה קורא בתורה")), chapters))
        assertTrue(hasOwnChapterHeadings(listOf(ChaptersHeading(0, "פרק א"), ChaptersHeading(9, "פרק ב")), chapters))
        assertFalse(hasOwnChapterHeadings(listOf(ChaptersHeading(0, "דף ב."), ChaptersHeading(9, "ד\"ה מאימתי")), chapters))
    }

    private val chapters = listOf(
        BaseChapter("מאימתי", lineIndex = 2, amud = 4),
        BaseChapter("היה קורא", lineIndex = 8, amud = 6),
        BaseChapter("מי שמתו", lineIndex = 12, amud = 8),
    )

    /** Commentary line → base line, as chapter numbers. */
    private fun byChapter(baseLineByLine: Map<Long, Long>) =
        baseLineByLine.mapValues { chapterOfBaseLine(chapters, it.value) }

    @Test
    fun `link anchors snap to daf headings and ignore stray back-links`() {
        val headings = listOf(
            ChaptersHeading(1, "דף ב."),
            ChaptersHeading(4, "דף ג."),
            ChaptersHeading(10, "דף ד."),
        )
        val links = mapOf(2L to 2L, 3L to 3L, 5L to 7L, 6L to 8L, 8L to 3L, 9L to 10L, 11L to 12L, 12L to 13L)
        val anchors = computeChapterAnchors(chapters, (0L..12L).toList(), headings, byChapter(links))
        assertEquals(listOf("מאימתי" to 1L, "היה קורא" to 6L, "מי שמתו" to 10L), anchors.map { it.chapter.text to it.lineIndex })
    }

    @Test
    fun `link anchors climb non-daf heading lines and drop chapters without material`() {
        val headings = listOf(ChaptersHeading(3, "ד\"ה מאימתי"), ChaptersHeading(4, "בא"))
        val links = mapOf(5L to 2L, 6L to 13L)
        val anchors = computeChapterAnchors(chapters, (0L..6L).toList(), headings, byChapter(links))
        assertEquals(listOf("מאימתי" to 3L, "מי שמתו" to 6L), anchors.map { it.chapter.text to it.lineIndex })
    }

    @Test
    fun `full daf headings beat sparse links`() {
        val headings = listOf(ChaptersHeading(1, "דף ב."), ChaptersHeading(4, "דף ג."), ChaptersHeading(10, "דף ד."))
        val anchors = computeChapterAnchors(chapters, (0L..12L).toList(), headings, byChapter(mapOf(2L to 2L)))
        assertEquals(listOf("מאימתי" to 1L, "היה קורא" to 4L, "מי שמתו" to 10L), anchors.map { it.chapter.text to it.lineIndex })
    }

    @Test
    fun `daf fallback anchors a mid-amud chapter at its amud heading`() {
        val headings = listOf(
            ChaptersHeading(0, "אמת ליעקב על ברכות"),
            ChaptersHeading(1, "דף ב ע\"ב"),
            ChaptersHeading(3, "דף ג עמוד א"),
            ChaptersHeading(5, "דף ג:"),
        )
        val anchors = computeChapterAnchors(chapters, (0L..6L).toList(), headings, emptyMap())
        // Chapter 3 (4a) is past the commentary's last heading: partial coverage.
        assertEquals(listOf("מאימתי" to 1L, "היה קורא" to 3L), anchors.map { it.chapter.text to it.lineIndex })
    }

    @Test
    fun `daf fallback skips a chapter the commentary does not reach`() {
        val headings = listOf(ChaptersHeading(1, "דף ב."), ChaptersHeading(2, "דף ד."), ChaptersHeading(3, "דף ה"))
        val anchors = computeChapterAnchors(chapters, (0L..4L).toList(), headings, emptyMap())
        assertEquals(listOf("מאימתי" to 1L, "מי שמתו" to 2L), anchors.map { it.chapter.text to it.lineIndex })
    }

    @Test
    fun `no daf fallback for a base without amud refs`() {
        val offDaf = chapters.map { it.copy(amud = null) }
        assertTrue(computeChapterAnchors(offDaf, (0L..3L).toList(), listOf(ChaptersHeading(1, "דף ב.")), emptyMap()).isEmpty())
    }
}

/** End-to-end over a temp-file DB: candidate read, write, rerun. */
class InheritChaptersAltTocIntegrationTest {

    private val dbFile = Files.createTempFile("inherit-chapters-test", ".db")
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository
    private val tempFiles = mutableListOf<Path>()

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
        tempFiles.forEach { Files.deleteIfExists(it) }
    }

    private data class Seeded(val baseId: Long, val linkedId: Long, val dafId: Long, val ownAltId: Long, val perekId: Long)

    /**
     * Base "ברכות" with a Sefaria Chapters structure (2a, mid-3a, 4a); a linked
     * commentary; an Otzaria commentary with daf headings only; one with its
     * own (JSON-style) Topic structure; one whose TOC already names chapters.
     */
    private fun seed(): Seeded = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val sefaria = repo.insertSource("Sefaria")
        val otzaria = repo.insertSource("Otzaria")
        val cat = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))

        val baseId = repo.insertBook(Book(categoryId = cat, sourceId = sefaria, title = "ברכות", heRef = "ברכות"))
        val baseRefs = listOf(
            null, null, "ברכות, ב., א", "ברכות, ב., ב", null, "ברכות, ב:, א", null,
            "ברכות, ג., א", "ברכות, ג., ב", null, "ברכות, ג:, א", null, "ברכות, ד., א", "ברכות, ד., ב",
        )
        val baseLines = baseRefs.mapIndexed { i, ref ->
            repo.insertLine(Line(bookId = baseId, lineIndex = i, content = "שורה $i", heRef = ref))
        }
        val structureId = repo.upsertAltTocStructure(
            AltTocStructure(bookId = baseId, key = "Chapters", title = "Berakhot", heTitle = "ברכות"),
        )
        for ((text, line) in listOf("מאימתי" to 2, "היה קורא" to 8, "מי שמתו" to 12)) {
            repo.insertAltTocEntry(AltTocEntry(structureId = structureId, text = text, level = 0, lineId = baseLines[line]))
        }
        repo.updateHasAltStructures(baseId, true)

        suspend fun book(title: String, source: Long) =
            repo.insertBook(Book(categoryId = cat, sourceId = source, title = title, heRef = title))

        suspend fun lines(bookId: Long, count: Int, headings: Map<Int, String>): List<Long> {
            val ids = (0 until count).map { i ->
                repo.insertLine(Line(bookId = bookId, lineIndex = i, content = headings[i] ?: "פירוש $i"))
            }
            for ((i, text) in headings) {
                repo.insertTocEntry(TocEntry(bookId = bookId, parentId = null, text = text, level = 1, lineId = ids[i]))
            }
            return ids
        }

        val linkedId = book("מפרש על ברכות", sefaria)
        repo.insertBookBaseText(linkedId, baseId)
        val linkedLines = lines(linkedId, 13, mapOf(1 to "דף ב.", 4 to "דף ג.", 10 to "דף ד."))
        val links = mapOf(2 to 2, 3 to 3, 5 to 7, 6 to 8, 8 to 3, 9 to 10, 11 to 12, 12 to 13)
        for ((commentaryLine, baseLine) in links) {
            repo.insertLink(
                Link(
                    sourceBookId = baseId, targetBookId = linkedId,
                    sourceLineId = baseLines[baseLine], targetLineId = linkedLines[commentaryLine],
                    targetLineIndex = commentaryLine, connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }

        val dafId = book("אמת ליעקב על ברכות", otzaria)
        lines(dafId, 7, mapOf(0 to "אמת ליעקב על ברכות", 1 to "דף ב ע\"ב", 3 to "דף ג עמוד א", 5 to "דף ג:"))

        val ownAltId = book("הגהות על ברכות", otzaria)
        val ownLines = lines(ownAltId, 4, mapOf(1 to "דף ב.", 2 to "דף ג."))
        val topic = repo.upsertAltTocStructure(AltTocStructure(bookId = ownAltId, key = "Topic", heTitle = "נושאים"))
        repo.insertAltTocEntry(AltTocEntry(structureId = topic, text = "נושא", level = 0, lineId = ownLines[1]))
        repo.updateHasAltStructures(ownAltId, true)

        val perekId = book("ביאור על ברכות", otzaria)
        lines(perekId, 5, mapOf(0 to "פרק ראשון - מאימתי", 1 to "דף ב.", 3 to "פרק שני - היה קורא", 4 to "דף יג."))

        Seeded(baseId, linkedId, dafId, ownAltId, perekId)
    }

    private fun attachEmptyBuildState(conn: Connection, existing: Path? = null): Path {
        val state = existing ?: Files.createTempFile("inherit-chapters-buildstate", ".db").also {
            Files.delete(it)
            BuildStateWriter().write(BuildStateSnapshot.empty(), it)
            tempFiles.add(it)
        }
        conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
            st.setString(1, state.toString())
            st.execute()
        }
        return state
    }

    private fun run(state: Path? = null): Pair<InheritChaptersResult, Path?> =
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val attached = state?.let { attachEmptyBuildState(conn, it) }
            val ids = attached?.let { AttachedBuildStateIds(conn) }
            synthesizeInheritedChapters(conn, readInheritChaptersSnapshots(conn), ids) to attached
        }

    private fun entries(bookId: Long): List<AltTocEntry> = runBlocking {
        val structure = repo.getAltTocStructuresForBook(bookId).single { it.key == CHAPTERS_STRUCTURE_KEY }
        repo.getAltTocEntriesForStructure(structure.id).sortedBy { it.id }
    }

    private fun lineIndexOf(entry: AltTocEntry): Long = runBlocking { repo.getLine(entry.lineId!!)!!.lineIndex.toLong() }

    /** (entry text, anchor lineIndex) of the book's chapters. */
    private fun chapters(bookId: Long): List<Pair<String, Long>> =
        entries(bookId).filter { it.parentId == null }.map { it.text to lineIndexOf(it) }

    /** Chapter text → its daf entries' (text, lineIndex). */
    private fun dafs(bookId: Long): Map<String, List<Pair<String, Long>>> {
        val all = entries(bookId)
        return all.filter { it.parentId == null }.associate { chapter ->
            chapter.text to all.filter { it.parentId == chapter.id }.map { it.text to lineIndexOf(it) }
        }
    }

    /** lineIndex → owning entry text, for every mapped line of [bookId]. */
    private fun lineOwners(bookId: Long): Map<Long, String> =
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.prepareStatement(
                """
                SELECT l.lineIndex, t.text FROM line_alt_toc m
                JOIN line l ON l.id = m.lineId
                JOIN alt_toc_entry e ON e.id = m.altTocEntryId
                JOIN tocText t ON t.id = e.textId
                WHERE l.bookId = ?
                """.trimIndent(),
            ).use { st ->
                st.setLong(1, bookId)
                st.executeQuery().use { rs ->
                    buildMap { while (rs.next()) put(rs.getLong(1), rs.getString(2)) }
                }
            }
        }

    private fun dump(): List<String> = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
        listOf(
            "SELECT * FROM alt_toc_structure ORDER BY id",
            "SELECT * FROM alt_toc_entry ORDER BY id",
            "SELECT * FROM line_alt_toc ORDER BY lineId, structureId",
            "SELECT * FROM tocText ORDER BY id",
            "SELECT id, hasAltStructures FROM book ORDER BY id",
        ).flatMap { sql ->
            conn.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    val columns = rs.metaData.columnCount
                    buildList { while (rs.next()) add((1..columns).joinToString("|") { "${rs.getObject(it)}" }) }
                }
            }
        }
    }

    @Test
    fun `linked and daf-heading commentaries inherit the base chapters`() = runBlocking {
        val seeded = seed()
        val (result, _) = run()
        assertEquals(InheritChaptersResult(structures = 2, entries = 11), result)

        assertEquals(listOf("מאימתי" to 1L, "היה קורא" to 6L, "מי שמתו" to 10L), chapters(seeded.linkedId))
        assertEquals(listOf("מאימתי" to 1L, "היה קורא" to 3L), chapters(seeded.dafId))

        val structure = repo.getAltTocStructuresForBook(seeded.linkedId).single()
        assertEquals(INHERITED_CHAPTERS_TITLE_EN, structure.title)
        assertEquals("מפרש על ברכות", structure.heTitle)
        val roots = entries(seeded.linkedId).filter { it.parentId == null }
        assertTrue(roots.all { it.level == 0 })
        assertEquals(listOf(true, false, true), roots.map { it.hasChildren })
        assertEquals(listOf(false, false, true), roots.map { it.isLastChild })

        // The book's own daf headings, verbatim, under the chapter whose range holds their line.
        assertEquals(
            mapOf(
                "מאימתי" to listOf("דף ב." to 1L, "דף ג." to 4L),
                "היה קורא" to emptyList(),
                "מי שמתו" to listOf("דף ד." to 10L),
            ),
            dafs(seeded.linkedId),
        )
        assertEquals(
            mapOf("מאימתי" to listOf("דף ב ע\"ב" to 1L), "היה קורא" to listOf("דף ג עמוד א" to 3L, "דף ג:" to 5L)),
            dafs(seeded.dafId),
        )
        val dafEntries = entries(seeded.dafId).filter { it.parentId != null }
        assertTrue(dafEntries.all { it.level == 1 && !it.hasChildren })
        assertEquals(listOf(true, false, true), dafEntries.map { it.isLastChild })

        assertTrue(repo.getBook(seeded.linkedId)!!.hasAltStructures)
        assertTrue(repo.getBook(seeded.dafId)!!.hasAltStructures)
        assertFalse(repo.getBook(seeded.perekId)!!.hasAltStructures)
    }

    @Test
    fun `every line maps to the nearest preceding anchor`() = runBlocking {
        val seeded = seed()
        run()
        // A daf entry sharing its chapter's line is deeper, so it owns that line.
        val expected = buildMap {
            (1L..3L).forEach { put(it, "דף ב.") }
            (4L..5L).forEach { put(it, "דף ג.") }
            (6L..9L).forEach { put(it, "היה קורא") }
            (10L..12L).forEach { put(it, "דף ד.") }
        }
        assertEquals(expected, lineOwners(seeded.linkedId))
        val bab = "דף ב ע\"ב"
        assertEquals(
            mapOf(1L to bab, 2L to bab, 3L to "דף ג עמוד א", 4L to "דף ג עמוד א", 5L to "דף ג:", 6L to "דף ג:"),
            lineOwners(seeded.dafId),
        )
    }

    @Test
    fun `books with a structure, the base, and books naming their own chapters are untouched`() = runBlocking {
        val seeded = seed()
        run()
        assertEquals(listOf("Topic"), repo.getAltTocStructuresForBook(seeded.ownAltId).map { it.key })
        assertEquals(listOf("Berakhot"), repo.getAltTocStructuresForBook(seeded.baseId).map { it.title })
        assertTrue(repo.getAltTocStructuresForBook(seeded.perekId).isEmpty())
    }

    @Test
    fun `a commentary on a commentary is credited in the second pass`() = runBlocking {
        val seeded = seed()
        val linked = repo.getBook(seeded.linkedId)!!
        val notesId = repo.insertBook(
            Book(categoryId = linked.categoryId, sourceId = linked.sourceId, title = "הערות על ברכות", heRef = "הערות על ברכות"),
        )
        val notesLines = (0 until 6).map { repo.insertLine(Line(bookId = notesId, lineIndex = it, content = "הערה $it")) }
        // Five links from the first-pass commentary, one from the tractate: 1/6 < 1/3.
        val sources = listOf(
            seeded.baseId to 2, seeded.linkedId to 2, seeded.linkedId to 3,
            seeded.linkedId to 6, seeded.linkedId to 9, seeded.linkedId to 11,
        )
        sources.forEachIndexed { i, (book, line) ->
            repo.insertLink(
                Link(
                    sourceBookId = book, targetBookId = notesId,
                    sourceLineId = repo.getLineByIndex(book, line)!!.id, targetLineId = notesLines[i],
                    targetLineIndex = i, connectionType = ConnectionType.COMMENTARY,
                ),
            )
        }
        val stats = InheritChaptersStats()
        val snapshots = DriverManager.getConnection("jdbc:sqlite:$dbFile").use { readInheritChaptersSnapshots(it, stats) }
        val notes = snapshots.single { it.bookId == notesId }
        assertTrue(notes.credited)
        assertEquals(1, stats.recoveredByCredit)
        assertEquals(
            listOf("מאימתי" to 0L, "היה קורא" to 3L, "מי שמתו" to 5L),
            notes.anchors.map { it.chapter.text to it.lineIndex },
        )
    }

    @Test
    fun `chapter ids are keyed by base chapter and reserved in order`() = runBlocking {
        val seeded = seed()
        val state = Files.createTempFile("inherit-chapters-buildstate", ".db").also {
            Files.delete(it)
            BuildStateWriter().write(BuildStateSnapshot.empty(), it)
            tempFiles.add(it)
        }
        run(state)
        val first = entries(seeded.dafId).filter { it.parentId == null }.associate { it.text to it.id }
        assertEquals(listOf("מאימתי", "היה קורא"), first.keys.toList())

        // Next build: the commentary gains a heading on the third chapter's amud.
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.createStatement().use { st ->
                val ours = "SELECT id FROM alt_toc_structure WHERE title = '$INHERITED_CHAPTERS_TITLE_EN'"
                st.executeUpdate("DELETE FROM line_alt_toc WHERE structureId IN ($ours)")
                st.executeUpdate("DELETE FROM alt_toc_entry WHERE structureId IN ($ours)")
                st.executeUpdate("DELETE FROM alt_toc_structure WHERE id IN ($ours)")
            }
        }
        repo.insertTocEntry(
            TocEntry(bookId = seeded.dafId, parentId = null, text = "דף ד.", level = 1, lineId = repo.getLineByIndex(seeded.dafId, 6)!!.id),
        )
        run(state)
        val second = entries(seeded.dafId).filter { it.parentId == null }.associate { it.text to it.id }
        assertEquals(listOf("מאימתי", "היה קורא", "מי שמתו"), second.keys.toList())
        assertEquals(first["מאימתי"], second["מאימתי"])
        assertEquals(first["היה קורא"], second["היה קורא"])
        assertEquals(second.getValue("היה קורא") + 1, second["מי שמתו"], "the new chapter takes its reserved id, in order")
    }

    @Test
    fun `a rerun is a no-op`() = runBlocking {
        seed()
        val state = Files.createTempFile("inherit-chapters-buildstate", ".db").also {
            Files.delete(it)
            BuildStateWriter().write(BuildStateSnapshot.empty(), it)
            tempFiles.add(it)
        }
        run(state)
        val first = dump()
        val (second, _) = run(state)
        assertEquals(InheritChaptersResult(0, 0), second)
        assertEquals(first, dump())
    }

    @Test
    fun `a rebuilt DB gets the same ids from the build state`() = runBlocking {
        val seeded = seed()
        val state = Files.createTempFile("inherit-chapters-buildstate", ".db").also {
            Files.delete(it)
            BuildStateWriter().write(BuildStateSnapshot.empty(), it)
            tempFiles.add(it)
        }
        run(state)
        val first = dump()
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            conn.createStatement().use { st ->
                val ours = "SELECT id FROM alt_toc_structure WHERE title = '$INHERITED_CHAPTERS_TITLE_EN'"
                st.executeUpdate("DELETE FROM line_alt_toc WHERE structureId IN ($ours)")
                st.executeUpdate("DELETE FROM alt_toc_entry WHERE structureId IN ($ours)")
                st.executeUpdate("DELETE FROM alt_toc_structure WHERE id IN ($ours)")
                // A foreign row far above the issued ids must not shift them.
                st.executeUpdate(
                    "INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren) " +
                        "VALUES (9000, 8888, NULL, 1, 0, 1, 0, 0)",
                )
                st.executeUpdate("UPDATE book SET hasAltStructures = 0 WHERE id IN (${seeded.linkedId}, ${seeded.dafId})")
            }
        }
        run(state)
        val rebuilt = dump().filterNot { it.startsWith("9000|") }
        assertEquals(first, rebuilt)
    }
}
