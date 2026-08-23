package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
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
 * A citation that IS a whole perek keeps its `link_range` row but gets no
 * `link_coverage`, so it surfaces only at the perek's first line instead of on
 * every segment (Otzaria/otzaria: Migdal Oz on Hilchot Gezeilah 5:1 showing up
 * in every segment of Bava Batra's Chezkat HaBatim).
 *
 * A narrow range in the same book is untouched — the rule is exact ref
 * membership, never a width threshold.
 */
class SefariaWholeUnitCoverageTest {

    private data class Imported(
        val links: Int,
        /** (side, endLineIndex, sourceLineId, targetLineId) per link_range row. */
        val ranges: List<List<Long>>,
        /** (lineId, side, sourceLineId, targetLineId) per link_coverage row. */
        val coverage: List<List<Long>>,
    )

    /** Whole perek: Bava Batra 28a:1 through 28b:1 (lines 1..4 of the fixture). */
    private val perekRef = "Bava Batra 28a:1-28b:1"

    private fun importWith(wholeUnitCitations: Set<String>): Imported = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-whole-unit-coverage")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        // Row 1: whole perek as Citation 1. Row 2: two segments inside it.
        // Row 3: whole perek as Citation 2, with a genuine range on Citation 1 —
        // covers the target side (37% of the corpus' matches sit there).
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"$perekRef","Migdal Oz 5:1","reference"
            |"Bava Batra 28a:1-28a:2","Migdal Oz 5:3","reference"
            |"Migdal Oz 5:1-5:3","$perekRef","reference"
            """.trimMargin()
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        fun book(id: Long, title: String, isBase: Boolean, lines: Int) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = isBase, totalLines = lines, hasAltStructures = true,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "בבא בתרא", isBase = true, lines = 4))
        repo.insertBook(book(2, "מגדל עוז", isBase = false, lines = 2))

        repo.insertLinesBatch(
            listOf(
                Line(id = 1, bookId = 1, lineIndex = 0, content = "bb 28a:1", heRef = "בבא בתרא כח., א"),
                Line(id = 2, bookId = 1, lineIndex = 1, content = "bb 28a:2", heRef = "בבא בתרא כח., ב"),
                Line(id = 3, bookId = 1, lineIndex = 2, content = "bb 28a:3", heRef = "בבא בתרא כח., ג"),
                Line(id = 4, bookId = 1, lineIndex = 3, content = "bb 28b:1", heRef = "בבא בתרא כח:, א"),
            )
        )
        repo.insertLinesBatch(
            listOf(
                Line(id = 10, bookId = 2, lineIndex = 0, content = "mo 5:1", heRef = "מגדל עוז ה, א"),
                Line(id = 11, bookId = 2, lineIndex = 1, content = "mo 5:3", heRef = "מגדל עוז ה, ג"),
            )
        )

        val allRefs = listOf(
            RefEntry("Bava Batra 28a:1", "בבא בתרא כח., א", "Bava Batra", 1),
            RefEntry("Bava Batra 28a:2", "בבא בתרא כח., ב", "Bava Batra", 2),
            RefEntry("Bava Batra 28a:3", "בבא בתרא כח., ג", "Bava Batra", 3),
            RefEntry("Bava Batra 28b:1", "בבא בתרא כח:, א", "Bava Batra", 4),
            RefEntry("Migdal Oz 5:1", "מגדל עוז ה, א", "Migdal Oz", 1),
            RefEntry("Migdal Oz 5:3", "מגדל עוז ה, ג", "Migdal Oz", 2),
        )
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefs.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }

        val importer = SefariaLinksImporter(
            repo,
            IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo),
            Logger.withTag("SefariaWholeUnitCoverageTest"),
        )
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) },
            refsByBase = refsByBase,
            lineKeyToId = mapOf(
                "Bava Batra" to 0 to 1L, "Bava Batra" to 1 to 2L,
                "Bava Batra" to 2 to 3L, "Bava Batra" to 3 to 4L,
                "Migdal Oz" to 0 to 10L, "Migdal Oz" to 1 to 11L,
            ),
            lineIdToBookId = mapOf(1L to 1L, 2L to 1L, 3L to 1L, 4L to 1L, 10L to 2L, 11L to 2L),
            bookMetaById = mapOf(
                1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
                2L to BookMeta(isBaseBook = false, categoryLevel = 1, priorityRank = null),
            ),
            refsByPath = allRefs.groupBy { it.path },
            wholeUnitCitations = wholeUnitCitations,
        )

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

        val result = Imported(
            links = query("SELECT COUNT(*) FROM link").single().single().toInt(),
            ranges = query(
                "SELECT lr.side, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                    "FROM link_range lr JOIN link l ON l.id = lr.linkId " +
                    "ORDER BY lr.endLineIndex, lr.side, l.sourceLineId"
            ),
            coverage = query(
                "SELECT lc.lineId, lc.side, l.sourceLineId, l.targetLineId " +
                    "FROM link_coverage lc JOIN link l ON l.id = lc.linkId " +
                    "ORDER BY lc.lineId, lc.side, l.targetLineId"
            ),
        )
        repo.close()
        result
    }

    @Test
    fun wholePerekCitationKeepsItsRangeButGetsNoCoverage() {
        val imported = importWith(setOf(canonicalCitation(perekRef)))

        assertEquals(3, imported.links)
        // Every range is still recorded, on both sides.
        assertEquals(
            listOf(
                listOf(0L, 1L, 1L, 11L),   // BB 28a:1-28a:2  → ends at line 2
                listOf(0L, 1L, 10L, 1L),   // Migdal Oz 5:1-5:3 → ends at line 11
                listOf(0L, 3L, 1L, 10L),   // perek as Citation 1 → source side
                listOf(1L, 3L, 10L, 1L),   // perek as Citation 2 → target side
            ),
            imported.ranges,
        )
        // Only the genuine ranges cover their extra lines; neither perek side does.
        assertEquals(
            listOf(
                listOf(2L, 0L, 1L, 11L),
                listOf(11L, 0L, 10L, 1L),
            ),
            imported.coverage,
        )
    }

    /**
     * Control: the same three rows with an empty set. Identical links and ranges,
     * but both perek sides now cover every segment — the behaviour reported from
     * the app. Pins the rule as the only difference.
     */
    @Test
    fun withoutTheRuleThePerekCitationCoversEverySegment() {
        val imported = importWith(emptySet())

        assertEquals(3, imported.links)
        assertEquals(
            listOf(
                listOf(0L, 1L, 1L, 11L),
                listOf(0L, 1L, 10L, 1L),
                listOf(0L, 3L, 1L, 10L),
                listOf(1L, 3L, 10L, 1L),
            ),
            imported.ranges,
        )
        assertEquals(
            listOf(
                listOf(2L, 0L, 1L, 10L),
                listOf(2L, 0L, 1L, 11L),
                listOf(2L, 1L, 10L, 1L),
                listOf(3L, 0L, 1L, 10L),
                listOf(3L, 1L, 10L, 1L),
                listOf(4L, 0L, 1L, 10L),
                listOf(4L, 1L, 10L, 1L),
                listOf(11L, 0L, 10L, 1L),
            ),
            imported.coverage,
        )
    }
}
