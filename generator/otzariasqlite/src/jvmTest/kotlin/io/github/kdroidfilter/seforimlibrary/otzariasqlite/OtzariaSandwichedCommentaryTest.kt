package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Unlinked dependant paragraphs between two paragraphs linked to the same base line
 * join the first paragraph's link (otzaria#5230). A gap between different base lines,
 * a gap with a heading and a gap with any link to the base book stay as they are.
 */
class OtzariaSandwichedCommentaryTest {
    @Test
    fun sandwichedParagraphsExtendTheFirstLink() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("ToratEmet")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        fun book(id: Long, title: String, totalLines: Int, isBase: Boolean) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = isBase, totalLines = totalLines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "חברותא על גמרא", 10, isBase = false))
        repo.insertBook(book(2, "גמרא", 3, isBase = true))
        val depContents = listOf(
            "מאי טעמא", "ולפיכך הני", "ומפרשת הגמרא", "בכור", "בין קטעים",
            "היורש", "הפניה", "והמייבם", "<h2>פרק</h2>", "סוף",
        )
        repo.insertLinesBatch(
            depContents.mapIndexed { i, text ->
                Line(id = 100L + i, bookId = 1, lineIndex = i, content = text, heRef = "חברותא ${i + 1}")
            } + (0..2).map { i ->
                Line(id = 200L + i, bookId = 2, lineIndex = i, content = "גמרא $i", heRef = "גמרא ${i + 1}")
            }
        )

        val sourceDir = Files.createTempDirectory("otzaria-sandwich")
        val linksDir = Files.createDirectories(sourceDir.resolve("links"))
        fun link(dep: Int, base: Int, type: String = "commentary") =
            """{"line_index_1": $dep, "heRef_2": "גמרא", "path_2": "גמרא.txt", "line_index_2": $base, "Conection Type": "$type"}"""
        Files.writeString(
            linksDir.resolve("חברותא על גמרא_links.json"),
            listOf(
                link(1, 1), link(4, 1), // lines 2-3 sandwiched by base line 1
                link(6, 2), // line 5 sits between base lines 1 and 2
                link(7, 2, "reference"), link(8, 2), // line 7 linked with the base
                link(10, 2), // line 9 is a heading
            ).joinToString(",\n", "[\n", "\n]"),
        )

        fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) }) }
            }
        }

        repeat(2) {
            DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
            assertEquals(
                listOf(listOf(1L, 102L, 2L, 200L, 100L)),
                query(
                    "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                        "FROM link_range lr JOIN link l ON l.id = lr.linkId"
                ),
            )
            assertEquals(
                listOf(listOf(101L, 1L), listOf(102L, 1L)),
                query("SELECT lineId, side FROM link_coverage ORDER BY lineId"),
            )
        }

        repo.close()
    }

    @Test
    fun explicitRangesAndSefariaDependantsAreLeftAlone() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val otzariaSource = repo.insertSource("ToratEmet")
        val sefariaSource = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        fun book(id: Long, title: String, sourceId: Long, totalLines: Int, isBase: Boolean) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = isBase, totalLines = totalLines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "חברותא על גמרא", otzariaSource, 9, isBase = false))
        repo.insertBook(book(2, "גמרא", otzariaSource, 3, isBase = true))
        repo.insertBook(book(3, "פירוש על גמרא", sefariaSource, 3, isBase = false))
        repo.insertLinesBatch(
            (0..8).map { i -> Line(id = 100L + i, bookId = 1, lineIndex = i, content = "חברותא $i", heRef = "חברותא ${i + 1}") } +
                (0..2).map { i -> Line(id = 200L + i, bookId = 2, lineIndex = i, content = "גמרא $i", heRef = "גמרא ${i + 1}") } +
                (0..2).map { i -> Line(id = 300L + i, bookId = 3, lineIndex = i, content = "פירוש $i", heRef = "פירוש ${i + 1}") }
        )

        val sourceDir = Files.createTempDirectory("otzaria-sandwich-explicit")
        val linksDir = Files.createDirectories(sourceDir.resolve("links"))
        fun link(dep: Int, base: Int, end: Int? = null) =
            """{"line_index_1": $dep, ${end?.let { "\"line_index_1_end\": $it, " } ?: ""}"heRef_2": "גמרא", """ +
                """"path_2": "גמרא.txt", "line_index_2": $base, "Conection Type": "commentary"}"""
        Files.writeString(
            linksDir.resolve("חברותא על גמרא_links.json"),
            listOf(
                link(1, 1, end = 2), link(4, 1), // the explicit range 1-2 stays as authored
                link(6, 2), link(6, 3, end = 8), link(9, 2), // lines 7-8 belong to base line 3
            ).joinToString(",\n", "[\n", "\n]"),
        )
        Files.writeString(
            linksDir.resolve("פירוש על גמרא_links.json"),
            listOf(link(1, 1), link(3, 1)).joinToString(",\n", "[\n", "\n]"),
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        fun query(sql: String): List<List<Long>> = driver.getConnection().createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildList { while (rs.next()) add((1..rs.metaData.columnCount).map { rs.getLong(it) }) }
            }
        }
        assertEquals(7, query("SELECT COUNT(*) FROM link").single().single().toInt())
        assertEquals(
            listOf(listOf(1L, 101L, 1L, 200L, 100L), listOf(1L, 107L, 7L, 202L, 105L)),
            query(
                "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                    "FROM link_range lr JOIN link l ON l.id = lr.linkId ORDER BY l.targetLineId, l.sourceLineId"
            ),
        )
        assertEquals(
            listOf(listOf(101L, 1L), listOf(106L, 1L), listOf(107L, 1L)),
            query("SELECT lineId, side FROM link_coverage ORDER BY lineId"),
        )

        repo.close()
    }
}
