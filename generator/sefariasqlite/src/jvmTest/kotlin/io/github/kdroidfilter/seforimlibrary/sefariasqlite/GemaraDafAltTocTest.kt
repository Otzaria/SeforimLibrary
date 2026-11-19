package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GemaraDafAltTocTest {

    private val header = "bookName,structureKey,structureHeTitle"
    private val rule = GemaraDafRule("רי\"ף ברכות", "GemaraDapim", "דפי הגמרא")

    @Test
    fun `parses rules, names with gershayim need no quoting`() {
        assertEquals(listOf(rule), parseGemaraDafRules(listOf("﻿$header", "רי\"ף ברכות,GemaraDapim,דפי הגמרא")))
        assertEquals(emptyList(), parseGemaraDafRules(listOf(header)))
        assertFailsWith<IllegalArgumentException> { parseGemaraDafRules(listOf(header, "רי\"ף ברכות,GemaraDapim")) }
        assertFailsWith<IllegalArgumentException> { parseGemaraDafRules(listOf(header, "רי\"ף ברכות,Topic,דפי הגמרא")) }
        assertFailsWith<IllegalArgumentException> { parseGemaraDafRules(listOf(header, "רי\"ף ברכות,דפים,דפי הגמרא")) }
        assertFailsWith<IllegalArgumentException> {
            parseGemaraDafRules(listOf(header, "רי\"ף ברכות,GemaraDapim,דפי הגמרא", "רי\"ף ברכות,Dapim,דפים"))
        }
        assertFailsWith<IllegalArgumentException> { parseGemaraDafRules(listOf("bookName,marker", "רי\"ף ברכות,x")) }
    }

    @Test
    fun `accepts only a bare daf and amud`() {
        fun texts(content: String) = findGemaraDafMarkers(content).map { it.text }
        assertEquals(listOf("דף ב.", "דף ב:"), texts("<b><big>גמ'</big></b> (דף ב.) תנו רבנן (דף ב:) עוד"))
        assertEquals(listOf("דף קעו."), texts("(דף קעו.)"))
        assertEquals(listOf("דף יז.", "דף ג:"), texts("(דף יז ע\"א) ו(דף ג ע\"ב)"))
        for (reference in listOf(
            "(דף ח: ע\"ש)", "(דף לעיל מח:)", "(דף לקמן יח.)", "(דף יד: טו.)", "(דף ו. דף לג.)",
            "(דף כא. ובפ\"א דחלה)", "(דף נ. ע\"ש (ג\"א)", "(דף ס)", "(דף קל)", "(ברכות דף יב.)", "(דף ח.]",
            "[דף ח ע\"א]", "([דף ח ע\"א])", "לקמן ((דף י\"ט ע\"ב)", "(דף יז  ע\"א)",
        )) {
            assertEquals(emptyList(), texts(reference), reference)
        }
    }

    @Test
    fun `orders amudim by gematria, amud a first`() {
        val keys = listOf("(דף קעו.)", "(דף י.)", "(דף ט:)", "(דף ט.)").map { findGemaraDafMarkers(it).single() }
        assertEquals(listOf("דף ט.", "דף ט:", "דף י.", "דף קעו."), keys.sortedBy { it.key }.map { it.text })
    }

    /** Title (sole root) › Rif pages; Gemara amud ח: is reached twice from page א. and once from ג:. */
    private val toc = listOf(
        MarkerTocRow(1, null, "רי\"ף ברכות", 100, 0),
        MarkerTocRow(2, 1, "דף א.", 101, 1),
        MarkerTocRow(3, 1, "דף ג:", 105, 5),
    )
    private val lines = listOf(
        MarkerLineRow(100, 0, "<h1>רי\"ף ברכות</h1>", 1),
        MarkerLineRow(101, 1, "<h2>דף א.</h2>", 2),
        MarkerLineRow(102, 2, "(דף ב.) מאימתי (דף ח:) קורין (דף ח:) עוד", 2),
        MarkerLineRow(103, 3, "(דף ח:) המשך באותו דף רי\"ף", 2),
        MarkerLineRow(104, 4, "(דף ט.) ו(דף ב:)", 2),
        MarkerLineRow(105, 5, "<h2>דף ג:</h2>", 3),
        MarkerLineRow(106, 6, "<b>גמ'</b> (דף ח:) תנו רבנן", 3),
    )

    @Test
    fun `an amud reached from two Rif pages gets one child per page`() {
        val plan = planGemaraDafEntries(lines, toc)
        assertEquals(
            listOf(
                PlannedMarkerEntry("1", null, 0, "דף ב.", 102, 2, isMarker = true),
                PlannedMarkerEntry("2", null, 0, "דף ב:", 104, 4, isMarker = true),
                PlannedMarkerEntry("3", null, 0, "דף ח:", 102, 2, isMarker = false),
                PlannedMarkerEntry("3/1", "3", 1, "דף ח: – רי\"ף דף א.", 102, 2, isMarker = true),
                PlannedMarkerEntry("3/2", "3", 1, "דף ח: – רי\"ף דף ג:", 106, 6, isMarker = true),
                PlannedMarkerEntry("4", null, 0, "דף ט.", 104, 4, isMarker = true),
            ),
            plan.entries,
        )
        assertEquals(mapOf(2 to "3/1", 3 to "3/1", 4 to "2", 6 to "3/2"), plan.ownerPathByLineIndex)
    }

    @Test
    fun `an amud reached twice from one Rif page is a single leaf`() {
        val plan = planGemaraDafEntries(lines.filter { it.index != 6 }, toc)
        assertEquals(PlannedMarkerEntry("3", null, 0, "דף ח:", 102, 2, isMarker = true), plan.entries.single { it.text == "דף ח:" })
        assertEquals(mapOf(2 to "3", 3 to "3", 4 to "2"), plan.ownerPathByLineIndex)
    }

    @Test
    fun `each entry is spelled like the marker it points to`() {
        val spelled = listOf(
            MarkerLineRow(102, 2, "(דף יט:) א", 2),
            MarkerLineRow(106, 6, "(דף י\"ט ע\"ב) ב", 3),
        )
        val entries = planGemaraDafEntries(spelled, toc).entries
        assertEquals(listOf("דף יט:", "דף יט: – רי\"ף דף א.", "דף י\"ט: – רי\"ף דף ג:"), entries.map { it.text })
        for (entry in entries) {
            val marker = entry.text.substringBefore(" – ")
            val content = spelled.single { it.index == entry.lineIndex }.content
            val amudForm = "(${marker.dropLast(1)} ע\"${if (marker.last() == '.') 'א' else 'ב'})"
            assert("($marker)" in content || amudForm in content) { "${entry.text} not found in '$content'" }
        }
    }

    @Test
    fun `markers past the tractate's last daf are dropped`() {
        val plan = planGemaraDafEntries(lines, toc, lastDaf = 8)
        assertEquals(listOf("דף ב.", "דף ב:", "דף ח:"), plan.entries.filter { it.parentPath == null }.map { it.text })
        assertEquals(listOf("דף ט." to 4), plan.dropped.map { (amud, index) -> amud.text to index })
        assertEquals("2", plan.ownerPathByLineIndex[4])
    }

    @Test
    fun `an occurrence without a Rif page heading is numbered`() {
        val flat = lines.map { it.copy(tocEntryId = 1) }
        val texts = planGemaraDafEntries(flat, toc).entries.map { it.text }
        assertEquals(listOf("דף ח: – 1", "דף ח: – 2", "דף ח: – 3"), texts.filter { it.startsWith("דף ח: –") })
    }

    private val dbFile = Files.createTempFile("gemara-daf-alt-toc", ".db")
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
    }

    @Test
    fun `replaces a structure of the same key and maps every line to its occurrence`() = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val catId = repo.insertCategory(Category(0, null, "ראשונים", level = 0, order = 1))
        val bookId = repo.insertBook(Book(categoryId = catId, sourceId = repo.insertSource("Sefaria"), title = rule.bookTitle))
        val lineIds = lines.map { repo.insertLine(Line(bookId = bookId, lineIndex = it.index, content = it.content)) }
        for (row in toc) {
            repo.insertTocEntry(
                TocEntry(id = row.id, bookId = bookId, parentId = row.parentId, text = row.text, level = 0, lineId = lineIds[row.lineIndex!!]),
            )
        }
        for (line in lines) repo.upsertLineToc(lineIds[line.index], line.tocEntryId!!)
        val old = repo.upsertAltTocStructure(AltTocStructure(bookId = bookId, key = "GemaraDapim", title = "x", heTitle = "x"))
        repo.insertAltTocEntry(AltTocEntry(structureId = old, text = "ישן", level = 0, lineId = lineIds[2]))

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertEquals(mapOf(rule.bookTitle to 6), writeGemaraDafAltTocs(conn, listOf(rule)))
            val entries = conn.createStatement().use { st ->
                st.executeQuery(
                    """
                    SELECT tt.text, e.level, l.lineIndex, e.hasChildren, e.isLastChild
                    FROM alt_toc_entry e JOIN tocText tt ON tt.id = e.textId JOIN line l ON l.id = e.lineId
                    ORDER BY e.id
                    """.trimIndent(),
                ).use { rs -> buildList { while (rs.next()) add(listOf(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5))) } }
            }
            assertEquals(
                listOf(
                    listOf("דף ב.", 0, 2, 0, 0),
                    listOf("דף ב:", 0, 4, 0, 0),
                    listOf("דף ח:", 0, 2, 1, 0),
                    listOf("דף ח: – רי\"ף דף א.", 1, 2, 0, 0),
                    listOf("דף ח: – רי\"ף דף ג:", 1, 6, 0, 1),
                    listOf("דף ט.", 0, 4, 0, 1),
                ),
                entries,
            )
            val owners = conn.createStatement().use { st ->
                st.executeQuery(
                    """
                    SELECT l.lineIndex, tt.text FROM line_alt_toc m
                    JOIN line l ON l.id = m.lineId JOIN alt_toc_entry e ON e.id = m.altTocEntryId JOIN tocText tt ON tt.id = e.textId
                    ORDER BY l.lineIndex
                    """.trimIndent(),
                ).use { rs -> buildList { while (rs.next()) add(rs.getInt(1) to rs.getString(2)) } }
            }
            assertEquals(
                listOf(
                    2 to "דף ח: – רי\"ף דף א.", 3 to "דף ח: – רי\"ף דף א.", 4 to "דף ב:", 5 to "דף ב:",
                    6 to "דף ח: – רי\"ף דף ג:",
                ),
                owners,
            )
            val heTitle = conn.createStatement().use { st ->
                st.executeQuery("SELECT group_concat(heTitle) FROM alt_toc_structure").use { rs -> rs.next(); rs.getString(1) }
            }
            assertEquals("דפי הגמרא", heTitle)
        }
    }

    @Test
    fun `the last daf comes from the one Bavli tractate of the same name`() = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val catId = repo.insertCategory(Category(0, null, "תלמוד בבלי", level = 0, order = 1))
        val source = repo.insertSource("Sefaria")
        suspend fun addBook(title: String, headings: List<String>) {
            val bookId = repo.insertBook(Book(categoryId = catId, sourceId = source, title = title))
            headings.forEachIndexed { i, text ->
                val lineId = repo.insertLine(Line(bookId = bookId, lineIndex = i, content = text))
                repo.insertTocEntry(TocEntry(bookId = bookId, text = text, level = 1, lineId = lineId))
            }
        }
        addBook("ברכות", listOf("ברכות", "דף ב.", "דף סד.", "דף י:"))
        addBook("רי\"ף ברכות", listOf("דף קה:"))
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertEquals(64, bavliLastDaf(conn, "רי\"ף ברכות"))
            assertEquals(null, bavliLastDaf(conn, "רי\"ף שבת"))
            assertEquals(null, bavliLastDaf(conn, "ברכות"))
            // A second book of that title (the repository dedupes titles, so copy the rows).
            conn.createStatement().use { st ->
                for (sql in listOf(
                    "CREATE TEMP TABLE b AS SELECT * FROM book WHERE title = 'ברכות'",
                    "UPDATE b SET id = 999",
                    "INSERT INTO book SELECT * FROM b",
                    "CREATE TEMP TABLE t AS SELECT * FROM tocEntry WHERE bookId = (SELECT id FROM book WHERE title = 'ברכות' AND id <> 999)",
                    "UPDATE t SET id = id + 1000, bookId = 999, parentId = NULL",
                    "INSERT INTO tocEntry SELECT * FROM t",
                )) st.execute(sql)
            }
            assertEquals(null, bavliLastDaf(conn, "רי\"ף ברכות"))
        }
    }

    @Test
    fun `a listed book without markers fails`() = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val catId = repo.insertCategory(Category(0, null, "ראשונים", level = 0, order = 1))
        val bookId = repo.insertBook(Book(categoryId = catId, sourceId = repo.insertSource("Sefaria"), title = rule.bookTitle))
        repo.insertLine(Line(bookId = bookId, lineIndex = 0, content = "(דף ח: ע\"ש)"))
        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            assertFailsWith<IllegalStateException> { writeGemaraDafAltTocs(conn, listOf(rule)) }
            assertFailsWith<IllegalStateException> { writeGemaraDafAltTocs(conn, listOf(rule.copy(bookTitle = "אין"))) }
        }
        Unit
    }
}
