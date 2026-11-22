package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Ranged Otzaria links via the JSON `line_index_1_end`/`line_index_2_end`
 * fields: the link row stays anchored at the start line, one link_range row
 * records the last line, and coverage rows mark every covered line after the
 * first (headings excluded). Reversed, degenerate and out-of-range ends must
 * not produce range rows while the base link is kept.
 */
class OtzariaRangedLinksTest {
    @Test
    fun rangedJsonEntriesProduceRangeAndCoverageRows() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Tashma")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        fun book(id: Long, title: String, totalLines: Int) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = totalLines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "פירוש", 4))
        repo.insertBook(book(2, "גמרא", 6))

        // Target line idx 2 (id 202) is a heading — excluded from coverage.
        repo.insertLinesBatch(
            listOf(
                Line(id = 100, bookId = 1, lineIndex = 0, content = "פירוש א", heRef = "פירוש 1"),
                Line(id = 101, bookId = 1, lineIndex = 1, content = "פירוש ב", heRef = "פירוש 2"),
                Line(id = 102, bookId = 1, lineIndex = 2, content = "פירוש ג", heRef = "פירוש 3"),
                Line(id = 103, bookId = 1, lineIndex = 3, content = "פירוש ד", heRef = "פירוש 4"),
                Line(id = 200, bookId = 2, lineIndex = 0, content = "דף ב", heRef = "גמרא 1"),
                Line(id = 201, bookId = 2, lineIndex = 1, content = "דף ג", heRef = "גמרא 2"),
                Line(id = 202, bookId = 2, lineIndex = 2, content = "<h2>פרק שני</h2>", heRef = "גמרא 3"),
                Line(id = 203, bookId = 2, lineIndex = 3, content = "דף ד", heRef = "גמרא 4"),
                Line(id = 204, bookId = 2, lineIndex = 4, content = "דף ה", heRef = "גמרא 5"),
                Line(id = 205, bookId = 2, lineIndex = 5, content = "דף ו", heRef = "גמרא 6"),
            )
        )

        val sourceDir = Files.createTempDirectory("otzaria-ranged")
        val linksDir = Files.createDirectories(sourceDir.resolve("links"))
        // Row 1: target-side range crossing the heading. Row 2: source-side range.
        // Row 3: reversed end. Row 4: degenerate (end == start). Row 5: end out of range.
        Files.writeString(
            linksDir.resolve("פירוש_links.json"),
            """
            |[
            | {"line_index_1": 1, "heRef_2": "גמרא ב", "path_2": "גמרא.txt",
            |  "line_index_2": 2, "line_index_2_end": 5, "Conection Type": "commentary"},
            | {"line_index_1": 2, "line_index_1_end": 4, "heRef_2": "גמרא א", "path_2": "גמרא.txt",
            |  "line_index_2": 1, "Conection Type": "commentary"},
            | {"line_index_1": 3, "line_index_1_end": 2, "heRef_2": "גמרא ו", "path_2": "גמרא.txt",
            |  "line_index_2": 6, "Conection Type": "commentary"},
            | {"line_index_1": 4, "heRef_2": "גמרא ד", "path_2": "גמרא.txt",
            |  "line_index_2": 4, "line_index_2_end": 4, "Conection Type": "commentary"},
            | {"line_index_1": 1, "heRef_2": "גמרא ו", "path_2": "גמרא.txt",
            |  "line_index_2": 6, "line_index_2_end": 99, "Conection Type": "commentary"}
            |]
            """.trimMargin()
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        fun query(sql: String): List<List<Long>> {
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    val out = mutableListOf<List<Long>>()
                    val cols = rs.metaData.columnCount
                    while (rs.next()) out += (1..cols).map { rs.getLong(it) }
                    return out
                }
            }
        }

        // All five links import; only rows 1-2 carry a valid range.
        assertEquals(5, query("SELECT COUNT(*) FROM link").single().single().toInt())
        val ranges = query(
            "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                "FROM link_range lr JOIN link l ON l.id = lr.linkId ORDER BY lr.side"
        )
        assertEquals(
            listOf(
                listOf(0L, 103L, 3L, 101L, 200L),  // source-side range פירוש 2-4
                listOf(1L, 204L, 4L, 100L, 201L),  // target-side range גמרא 2-5
            ),
            ranges,
        )

        // Coverage: source side lines 102,103; target side 203,204 — the heading
        // (202) and all lines of the reversed/degenerate/out-of-range rows absent.
        val coverage = query(
            "SELECT lc.lineId, lc.side FROM link_coverage lc ORDER BY lc.lineId"
        )
        assertEquals(
            listOf(
                listOf(102L, 0L),
                listOf(103L, 0L),
                listOf(203L, 1L),
                listOf(204L, 1L),
            ),
            coverage,
        )

        // Rerun with the target range shortened (5→4) and the source range removed:
        // the import is authoritative for re-processed links — stale rows must go.
        Files.writeString(
            linksDir.resolve("פירוש_links.json"),
            """
            |[
            | {"line_index_1": 1, "heRef_2": "גמרא ב", "path_2": "גמרא.txt",
            |  "line_index_2": 2, "line_index_2_end": 4, "Conection Type": "commentary"},
            | {"line_index_1": 2, "heRef_2": "גמרא א", "path_2": "גמרא.txt",
            |  "line_index_2": 1, "Conection Type": "commentary"}
            |]
            """.trimMargin()
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        // Same stable link ids → still 5 link rows, no duplicates.
        assertEquals(5, query("SELECT COUNT(*) FROM link").single().single().toInt())
        assertEquals(
            listOf(listOf(1L, 203L, 3L, 100L, 201L)),
            query(
                "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                    "FROM link_range lr JOIN link l ON l.id = lr.linkId"
            ),
        )
        assertEquals(
            listOf(listOf(203L, 1L)),
            query("SELECT lc.lineId, lc.side FROM link_coverage lc"),
        )

        repo.close()
    }
}
