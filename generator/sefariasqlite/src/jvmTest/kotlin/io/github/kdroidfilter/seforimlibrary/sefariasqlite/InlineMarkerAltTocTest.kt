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

class InlineMarkerAltTocTest {

    private val header = "bookName,marker,structureKey,structureHeTitle,groupBy"
    private val rule = InlineMarkerRule("מרדכי", "רמז", "Remez", "רמזים", "פרק")

    @Test
    fun `parses rules and rejects keys the app gives another meaning`() {
        assertEquals(listOf(rule), parseInlineMarkerRules(listOf("﻿$header", "מרדכי,רמז,Remez,רמזים,פרק")))
        assertEquals(emptyList(), parseInlineMarkerRules(listOf(header)))
        assertFailsWith<IllegalArgumentException> { parseInlineMarkerRules(listOf(header, "מרדכי,רמז,Topic,רמזים,פרק")) }
        assertFailsWith<IllegalArgumentException> { parseInlineMarkerRules(listOf(header, "מרדכי,רמז,Remez,רמזים")) }
        assertFailsWith<IllegalArgumentException> {
            parseInlineMarkerRules(listOf(header, "מרדכי,רמז,Remez,רמזים,פרק", "מרדכי,סימן,Simanim2,סימנים,"))
        }
        assertFailsWith<IllegalArgumentException> { parseInlineMarkerRules(listOf("bookName,marker", "מרדכי,רמז")) }
    }

    /** Title (sole root) › part › chapter › daf: markers group under part › chapter, not the daf. */
    private val toc = listOf(
        MarkerTocRow(1, null, "מרדכי בבא מציעא", 100, 0),
        MarkerTocRow(2, 1, "מרדכי", 101, 1),
        MarkerTocRow(3, 2, "פרק שנים אוחזין", 102, 2),
        MarkerTocRow(4, 3, "דף ב.", 103, 3),
        MarkerTocRow(5, 1, "הגהות", 106, 6),
        MarkerTocRow(6, 5, "פרק שנים אוחזין", 107, 7),
    )
    private val lines = listOf(
        MarkerLineRow(100, 0, "<h1>מרדכי בבא מציעא</h1>", 1),
        MarkerLineRow(101, 1, "<h2>מרדכי</h2>", 2),
        MarkerLineRow(102, 2, "<h3>פרק שנים אוחזין</h3>", 3),
        MarkerLineRow(103, 3, "<h4>דף ב.</h4>", 4),
        MarkerLineRow(104, 4, "א <small>[רמז רטז]</small> ב <small>[רמז ריז]</small>", 4),
        MarkerLineRow(105, 5, "המשך בלי רמז", 4),
        MarkerLineRow(106, 6, "<h2>הגהות</h2>", 5),
        MarkerLineRow(107, 7, "<h3>פרק שנים אוחזין</h3>", 6),
        MarkerLineRow(108, 8, "<small>[רמז תיז]</small> הגה", 6),
    )

    @Test
    fun `markers nest under part and chapter, several on one line each get an entry`() {
        val planned = planInlineMarkerEntries(rule, lines, toc)
        assertEquals(
            listOf(
                PlannedMarkerEntry("1", null, 0, "מרדכי", 101, 1, isMarker = false),
                PlannedMarkerEntry("1/1", "1", 1, "פרק שנים אוחזין", 102, 2, isMarker = false),
                PlannedMarkerEntry("1/1/1", "1/1", 2, "רמז רטז", 104, 4, isMarker = true),
                PlannedMarkerEntry("1/1/2", "1/1", 2, "רמז ריז", 104, 4, isMarker = true),
                PlannedMarkerEntry("2", null, 0, "הגהות", 106, 6, isMarker = false),
                PlannedMarkerEntry("2/1", "2", 1, "פרק שנים אוחזין", 107, 7, isMarker = false),
                PlannedMarkerEntry("2/1/1", "2/1", 2, "רמז תיז", 108, 8, isMarker = true),
            ),
            planned,
        )
    }

    @Test
    fun `a blank groupBy gives a flat list`() {
        val planned = planInlineMarkerEntries(rule.copy(groupBy = ""), lines, toc)
        assertEquals(listOf("1", "2", "3"), planned.map { it.path })
        assertEquals(listOf("רמז רטז", "רמז ריז", "רמז תיז"), planned.map { it.text })
    }

    private val dbFile = Files.createTempFile("inline-marker-alt-toc", ".db")
    private val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbFile")
    private lateinit var repo: SeforimRepository

    @AfterTest
    fun tearDown() {
        if (::repo.isInitialized) repo.close()
        Files.deleteIfExists(dbFile)
    }

    @Test
    fun `replaces a structure of the same key and maps every line to its marker`() = runBlocking {
        SeforimDb.Schema.create(driver)
        repo = SeforimRepository(dbFile.toString(), driver)
        val catId = repo.insertCategory(Category(0, null, "ראשונים", level = 0, order = 1))
        val bookId = repo.insertBook(Book(categoryId = catId, sourceId = repo.insertSource("Sefaria"), title = "מרדכי"))
        val lineIds = lines.map { repo.insertLine(Line(bookId = bookId, lineIndex = it.index, content = it.content)) }
        for (row in toc) {
            repo.insertTocEntry(
                TocEntry(id = row.id, bookId = bookId, parentId = row.parentId, text = row.text, level = 0, lineId = lineIds[row.lineIndex!!]),
            )
        }
        for (line in lines) repo.upsertLineToc(lineIds[line.index], line.tocEntryId!!)
        // Sefaria's ten-remez range, to be replaced.
        val old = repo.upsertAltTocStructure(AltTocStructure(bookId = bookId, key = "Remez", title = "Remez", heTitle = "רמזים"))
        repo.insertAltTocEntry(AltTocEntry(structureId = old, text = "רמז רטז עד רכה", level = 0, lineId = lineIds[4]))

        DriverManager.getConnection("jdbc:sqlite:$dbFile").use { conn ->
            val written = writeInlineMarkerAltTocs(conn, listOf(rule))
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
                    listOf("מרדכי", 0, 1, 1, 0),
                    listOf("פרק שנים אוחזין", 1, 2, 1, 1),
                    listOf("רמז רטז", 2, 4, 0, 0),
                    listOf("רמז ריז", 2, 4, 0, 1),
                    listOf("הגהות", 0, 6, 1, 1),
                    listOf("פרק שנים אוחזין", 1, 7, 1, 1),
                    listOf("רמז תיז", 2, 8, 0, 1),
                ),
                entries,
            )
            assertEquals(mapOf("מרדכי" to 7), written)
            val owners = conn.createStatement().use { st ->
                st.executeQuery(
                    """
                    SELECT l.lineIndex, tt.text FROM line_alt_toc m
                    JOIN line l ON l.id = m.lineId JOIN alt_toc_entry e ON e.id = m.altTocEntryId JOIN tocText tt ON tt.id = e.textId
                    ORDER BY l.lineIndex
                    """.trimIndent(),
                ).use { rs -> buildList { while (rs.next()) add(rs.getInt(1) to rs.getString(2)) } }
            }
            assertEquals(listOf(4 to "רמז ריז", 5 to "רמז ריז", 6 to "רמז ריז", 7 to "רמז ריז", 8 to "רמז תיז"), owners)
            val structures = conn.createStatement().use { st ->
                st.executeQuery("SELECT count(*) FROM alt_toc_structure").use { rs -> rs.next(); rs.getInt(1) }
            }
            assertEquals(1, structures)
        }
    }
}
