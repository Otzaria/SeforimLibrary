package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.SuppressionReason
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Stage 2: SefariaExport ships a per-side visibility verdict, the importer maps
 * it onto the stored direction and AND-s the rows that merge onto one link id.
 *
 * The merge rule is the sharp edge — `linkId` is
 * (sourceLineId, targetLineId, connectionTypeId), so distinct CSV rows collapse
 * (~78K for OTHER alone in the 2026-08 export). A side may only stay hidden for
 * the reasons every contributor agrees on.
 */
class SefariaSuppressedSideTest {

    private data class Imported(
        /** (linkId, side, reasonMask) */
        val suppressed: List<List<Long>>,
        val coverage: Int,
    )

    private val perekRef = "Bava Batra 28a:1-28b:1"

    /** [rows] are raw CSV lines appended after the header. */
    private fun importWith(header: String, vararg rows: String): Imported = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-suppressed-side")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        Files.writeString(linksDir.resolve("links0.csv"), (listOf(header) + rows).joinToString("\n"))

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

        SefariaLinksImporter(
            repo,
            IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo),
            Logger.withTag("SefariaSuppressedSideTest"),
        ).processLinksInParallel(
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
            suppressed = query(
                "SELECT linkId, side, reasonMask FROM link_suppressed_side ORDER BY linkId, side"
            ),
            coverage = query("SELECT COUNT(*) FROM link_coverage").single().single().toInt(),
        )
        repo.close()
        result
    }

    private companion object {
        const val HEADER_WITH_MASKS =
            "Citation 1,Citation 2,Conection Type,Text 1,Text 2,Category 1,Category 2," +
                "Char Level Data 1,Char Level Data 2,Suppression Mask 1,Suppression Mask 2"
        const val HEADER_LEGACY = "Citation 1,Citation 2,Conection Type"
        fun row(c1: String, c2: String, m1: Int, m2: Int) =
            """"$c1","$c2","reference","","","","","","",$m1,$m2"""
    }

    @Test
    fun exportedVerdictHidesOnlyTheCitedSide() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
        )
        // Citation 1 is the stored source, so the hidden side is 0.
        assertEquals(1, imported.suppressed.size)
        assertEquals(listOf(0L, SuppressionReason.WHOLE_PEREK.toLong()), imported.suppressed[0].drop(1))
        // Whole-unit reasons also suppress coverage, exactly as in stage 1.
        assertEquals(0, imported.coverage)
    }

    @Test
    fun oneVisibleContributionClearsAMergedSide() {
        // Both rows resolve to the same (src, tgt, type) and so to one linkId.
        // Sefaria hides the first but shows the second — the side must show.
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
            row("Bava Batra 28a:1", "Migdal Oz 5:1", 0, 0),
        )
        assertTrue(
            imported.suppressed.isEmpty(),
            "a visible contribution must clear the side, got ${imported.suppressed}",
        )
    }

    @Test
    fun mergedSideKeepsOnlyTheReasonsAllContributorsAgreeOn() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK or SuppressionReason.ANCHOR_NOT_SEGMENT, 0),
            row("Bava Batra 28a:1", "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
        )
        assertEquals(1, imported.suppressed.size)
        assertEquals(SuppressionReason.WHOLE_PEREK.toLong(), imported.suppressed[0][2])
    }

    @Test
    fun contributorsSuppressedForDisjointReasonsClearTheSide() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
            row("Bava Batra 28a:1", "Migdal Oz 5:1", SuppressionReason.ANCHOR_NOT_SEGMENT, 0),
        )
        assertTrue(imported.suppressed.isEmpty(), "disagreement is not agreement: ${imported.suppressed}")
    }

    @Test
    fun exportWithoutTheColumnsFallsBackToStageOne() {
        // No mask columns: no suppressed sides, and coverage still follows the
        // locally derived whole-unit rule (here: nothing declared, so covered).
        val imported = importWith(
            HEADER_LEGACY,
            """"Bava Batra 28a:1-28a:2","Migdal Oz 5:3","reference"""",
        )
        assertTrue(imported.suppressed.isEmpty())
        assertEquals(1, imported.coverage)
    }

    @Test
    fun unparsableMaskFailsLoudly() {
        val error = assertFailsWith<IllegalStateException> {
            parseSuppressionMask("not-a-number")
        }
        assertTrue(error.message!!.contains("Unparsable suppression mask"), error.message!!)
    }

    @Test
    fun maskOutsideKnownReasonsFailsLoudly() {
        assertFailsWith<IllegalArgumentException> { parseSuppressionMask("64") }
    }

    @Test
    fun blankMaskMeansDisplayed() {
        assertEquals(0, parseSuppressionMask(""))
        assertEquals(0, parseSuppressionMask(null))
    }
}
