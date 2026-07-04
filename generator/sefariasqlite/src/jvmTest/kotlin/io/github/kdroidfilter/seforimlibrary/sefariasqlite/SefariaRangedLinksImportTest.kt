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
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * End-to-end import of ranged citations: a range must produce one link row
 * anchored at the range's first line, one link_range row with the range's
 * last line, and link_coverage rows for every covered line after the first
 * (heading lines excluded).
 */
class SefariaRangedLinksImportTest {
    @Test
    fun rangedCitationsProduceRangeAndCoverageRows() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-ranged-links")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))

        // Row 1: source-side leaf range (Rashi comments 1:1:1-2 on Genesis 1:1).
        // Row 2: target-side cross-section range (Genesis 1:1-2:1) — crosses a
        //        heading line that must be excluded from coverage.
        // Row 3: source-side INTERMEDIATE range (Rashi 1:1-2 = verses 1:1
        //        through 1:2) — the shape that was formerly dropped entirely.
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Rashi on Genesis 1:1:1-2","Genesis 1:1","Commentary"
            |"Rashi on Genesis 1:2:1","Genesis 1:1-2:1","Commentary"
            |"Rashi on Genesis 1:1-2","Genesis 1:2","Commentary"
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
        repo.insertBook(book(1, "בראשית", isBase = true, lines = 5))
        repo.insertBook(book(2, "רש\"י על בראשית", isBase = false, lines = 3))

        // Genesis: 1:1, 1:2, 1:3, <heading>, 2:1 (heading at lineIndex 3 has no ref).
        repo.insertLinesBatch(
            listOf(
                Line(id = 1, bookId = 1, lineIndex = 0, content = "gen 1:1", heRef = "בראשית א, א"),
                Line(id = 2, bookId = 1, lineIndex = 1, content = "gen 1:2", heRef = "בראשית א, ב"),
                Line(id = 3, bookId = 1, lineIndex = 2, content = "gen 1:3", heRef = "בראשית א, ג"),
                Line(id = 4, bookId = 1, lineIndex = 3, content = "<h2>פרק ב</h2>", heRef = ""),
                Line(id = 5, bookId = 1, lineIndex = 4, content = "gen 2:1", heRef = "בראשית ב, א"),
            )
        )
        // Rashi: 1:1:1, 1:1:2, 1:2:1.
        repo.insertLinesBatch(
            listOf(
                Line(id = 10, bookId = 2, lineIndex = 0, content = "rashi 1:1:1", heRef = "רש\"י א, א, א"),
                Line(id = 11, bookId = 2, lineIndex = 1, content = "rashi 1:1:2", heRef = "רש\"י א, א, ב"),
                Line(id = 12, bookId = 2, lineIndex = 2, content = "rashi 1:2:1", heRef = "רש\"י א, ב, א"),
            )
        )

        val genesisRefs = listOf(
            RefEntry("Genesis 1:1", "בראשית א, א", "Genesis", 1),
            RefEntry("Genesis 1:2", "בראשית א, ב", "Genesis", 2),
            RefEntry("Genesis 1:3", "בראשית א, ג", "Genesis", 3),
            RefEntry("Genesis 2:1", "בראשית ב, א", "Genesis", 5),
        )
        val rashiRefs = listOf(
            RefEntry("Rashi on Genesis 1:1:1", "", "Rashi on Genesis", 1),
            RefEntry("Rashi on Genesis 1:1:2", "", "Rashi on Genesis", 2),
            RefEntry("Rashi on Genesis 1:2:1", "", "Rashi on Genesis", 3),
        )
        val allRefs = genesisRefs + rashiRefs
        val refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) }
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefs.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }
        val refsByPath = allRefs.groupBy { it.path }

        val lineKeyToId = mapOf(
            "Genesis" to 0 to 1L, "Genesis" to 1 to 2L, "Genesis" to 2 to 3L,
            "Genesis" to 3 to 4L, "Genesis" to 4 to 5L,
            "Rashi on Genesis" to 0 to 10L, "Rashi on Genesis" to 1 to 11L,
            "Rashi on Genesis" to 2 to 12L,
        )
        val lineIdToBookId = mapOf(
            1L to 1L, 2L to 1L, 3L to 1L, 4L to 1L, 5L to 1L,
            10L to 2L, 11L to 2L, 12L to 2L,
        )
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
        val importer = SefariaLinksImporter(repo, bindings, Logger.withTag("SefariaRangedLinksImportTest"))
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refsByCanonical,
            refsByBase = refsByBase,
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            headingLineIds = setOf(4L),
            refsByPath = refsByPath,
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

        // 3 CSV rows → 3 stored links (Rashi is the declared dependant, so all
        // rows are stored base→dependant: Genesis line = source side).
        assertEquals(3, query("SELECT COUNT(*) FROM link").single().single().toInt())

        // Each ranged side produced exactly one link_range row.
        // Row 1: range on Rashi (stored target side) ending at 1:1:2 (line 11).
        // Row 2: range on Genesis (stored source side) ending at 2:1 (line 5).
        // Row 3: intermediate range on Rashi ending at last leaf of 1:2 (line 12).
        val ranges = query(
            "SELECT lr.side, lr.endLineId, lr.endLineIndex, l.sourceLineId, l.targetLineId " +
                "FROM link_range lr JOIN link l ON l.id = lr.linkId ORDER BY lr.endLineId"
        )
        assertEquals(
            listOf(
                listOf(0L, 5L, 4L, 1L, 12L),   // Genesis 1:1-2:1, anchored at line 1
                listOf(1L, 11L, 1L, 1L, 10L),  // Rashi 1:1:1-2, anchored at line 10
                listOf(1L, 12L, 2L, 2L, 10L),  // Rashi 1:1-2, anchored at first leaf (line 10)
            ),
            ranges,
        )

        // Coverage: every covered line after the range's first, heading excluded.
        // Genesis 1:1-2:1 → lines 2,3,5 (line 4 is a heading); side 0.
        // Rashi 1:1:1-2   → line 11; side 1.
        // Rashi 1:1-2     → lines 11,12; side 1.
        val coverage = query(
            "SELECT lc.lineId, lc.side, l.sourceLineId, l.targetLineId " +
                "FROM link_coverage lc JOIN link l ON l.id = lc.linkId ORDER BY lc.lineId, l.sourceLineId"
        )
        assertEquals(
            listOf(
                listOf(2L, 0L, 1L, 12L),
                listOf(3L, 0L, 1L, 12L),
                listOf(5L, 0L, 1L, 12L),
                listOf(11L, 1L, 1L, 10L),
                listOf(11L, 1L, 2L, 10L),
                listOf(12L, 1L, 2L, 10L),
            ),
            coverage,
        )

        repo.close()
    }
}
