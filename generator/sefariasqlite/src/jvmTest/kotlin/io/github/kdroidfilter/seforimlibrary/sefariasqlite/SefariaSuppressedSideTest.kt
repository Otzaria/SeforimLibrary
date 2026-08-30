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
 * it onto the stored direction and aggregates rows that merge onto one link id.
 *
 * The merge rule is the sharp edge — `linkId` is
 * (sourceLineId, targetLineId, connectionTypeId), so distinct CSV rows collapse
 * (~78K for OTHER alone in the 2026-08 export). A side stays hidden when every
 * contributor is hidden; their diagnostic reasons are OR-ed.
 */
class SefariaSuppressedSideTest {

    private data class Imported(
        /** (linkId, side, reasonMask) */
        val suppressed: List<List<Long>>,
        val coverage: Int,
    )

    private val perekRef = "Bava Batra 28a:1-28b:1"

    /** [rows] are raw CSV lines appended after the header. */
    private fun importWith(
        header: String,
        vararg rows: String,
        requireExportedVisibility: Boolean = header.contains("Suppression Mask"),
    ): Imported = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-suppressed-side")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        Files.writeString(linksDir.resolve("links0.csv"), (listOf(header) + rows).joinToString("\n"))
        if (requireExportedVisibility) {
            val visibilityRows = if (
                header.contains("Suppression Mask 1") && header.contains("Suppression Mask 2")
            ) rows.toList() else emptyList()
            writeVisibilityMetadata(tempDir, visibilityRows)
        }

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
                2L to BookMeta(
                    isBaseBook = false,
                    categoryLevel = 1,
                    priorityRank = null,
                    dependence = Dependence.COMMENTARY,
                    baseTextBookIds = setOf(1L),
                    sefariaDeclaredBaseTextBookIds = setOf(1L),
                ),
            ),
            refsByPath = allRefs.groupBy { it.path },
            requireExportedVisibility = requireExportedVisibility,
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
        fun row(c1: String, c2: String, m1: Int, m2: Int, type: String = "reference") =
            """"$c1","$c2","$type","","","","","","",$m1,$m2"""

        fun writeVisibilityMetadata(root: java.nio.file.Path, rows: List<String>) {
            fun sha256(value: String): String = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value.toByteArray())
                .joinToString("") { "%02x".format(it) }
            val perek = "Bava Batra 28a:1-28b:1"
            val parasha = "Exodus 25:1-27:19"
            val masks = rows.map { parseCsvLine(it).takeLast(2).map(String::toInt) }
            fun suppressed(side: Int) = masks.count { it[side - 1] != 0 }
            fun reason(side: Int, bit: Int) = masks.count { it[side - 1] and bit != 0 }
            val metadata = Files.createDirectories(root.resolve("metadata"))
                .resolve("link-visibility-v1.json")
            Files.writeString(
                metadata,
                """
                {
                  "schema_version": 1,
                  "sefaria_project_sha": "${"a".repeat(40)}",
                  "mask_bits": {"1":"anchor_not_segment_level","2":"other_side_too_coarse","4":"whole_talmud_perek","8":"whole_parasha"},
                  "counts": {
                    "perek_refs":1,"parasha_refs":1,
                    "suppressed_side_1":${suppressed(1)},"suppressed_side_2":${suppressed(2)},
                    "suppressed_by_side_and_bit": {
                      "1":{"anchor_not_segment_level":${reason(1, 1)},"other_side_too_coarse":${reason(1, 2)},"whole_parasha":${reason(1, 8)},"whole_talmud_perek":${reason(1, 4)}},
                      "2":{"anchor_not_segment_level":${reason(2, 1)},"other_side_too_coarse":${reason(2, 2)},"whole_parasha":${reason(2, 8)},"whole_talmud_perek":${reason(2, 4)}}
                    }
                  },
                  "perek_refs_sha256": "${sha256(perek)}",
                  "parasha_refs_sha256": "${sha256(parasha)}",
                  "perek_refs": ["$perek"],
                  "parasha_refs": ["$parasha"]
                }
                """.trimIndent(),
            )
        }
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
    fun exportedSidesFollowTheStoredDirectionAfterLinkSwap() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(
                "Migdal Oz 5:1",
                "Bava Batra 28a:1",
                SuppressionReason.ANCHOR_NOT_SEGMENT,
                0,
                type = "commentary",
            ),
        )

        // The dependant→base CSV row is stored base→dependant. Citation 1's
        // verdict must therefore land on stored side 1, not side 0.
        assertEquals(1, imported.suppressed.size)
        assertEquals(
            listOf(1L, SuppressionReason.ANCHOR_NOT_SEGMENT.toLong()),
            imported.suppressed[0].drop(1),
        )
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
    fun mergedSideKeepsAllReasonsFromHiddenContributors() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK or SuppressionReason.ANCHOR_NOT_SEGMENT, 0),
            row("Bava Batra 28a:1", "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
        )
        assertEquals(1, imported.suppressed.size)
        assertEquals(
            (SuppressionReason.WHOLE_PEREK or SuppressionReason.ANCHOR_NOT_SEGMENT).toLong(),
            imported.suppressed[0][2],
        )
    }

    @Test
    fun contributorsSuppressedForDisjointReasonsKeepTheSideHidden() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(perekRef, "Migdal Oz 5:1", SuppressionReason.WHOLE_PEREK, 0),
            row("Bava Batra 28a:1", "Migdal Oz 5:1", SuppressionReason.ANCHOR_NOT_SEGMENT, 0),
        )
        assertEquals(1, imported.suppressed.size)
        assertEquals(
            (SuppressionReason.WHOLE_PEREK or SuppressionReason.ANCHOR_NOT_SEGMENT).toLong(),
            imported.suppressed[0][2],
        )
    }

    @Test
    fun exportWithoutTheColumnsFallsBackToStageOne() {
        // No mask columns: no suppressed sides, and coverage still follows the
        // locally derived whole-unit rule (here: nothing declared, so covered).
        val imported = importWith(
            HEADER_LEGACY,
            """"Bava Batra 28a:1-28a:2","Migdal Oz 5:3","reference"""",
            requireExportedVisibility = false,
        )
        assertTrue(imported.suppressed.isEmpty())
        assertEquals(1, imported.coverage)
    }

    @Test
    fun unparsableMaskFailsLoudly() {
        val error = assertFailsWith<IllegalStateException> {
            parseSuppressionMask("not-a-number", "test:2 side 1")
        }
        assertTrue(error.message!!.contains("Unparsable suppression mask"), error.message!!)
    }

    @Test
    fun maskOutsideKnownReasonsFailsLoudly() {
        assertFailsWith<IllegalArgumentException> { parseSuppressionMask("64", "test") }
    }

    @Test
    fun blankOrMissingMaskFailsClosed() {
        assertFailsWith<IllegalArgumentException> { parseSuppressionMask("", "test") }
        assertFailsWith<IllegalArgumentException> { parseSuppressionMask(null, "test") }
    }

    @Test
    fun schemaThreeProductionRejectsLegacyExport() {
        assertFailsWith<IllegalArgumentException> {
            importWith(
                HEADER_LEGACY,
                """"Bava Batra 28a:1","Migdal Oz 5:1","reference"""",
                requireExportedVisibility = true,
            )
        }
    }

    @Test
    fun oneSuppressionHeaderFailsClosed() {
        val partial = "$HEADER_LEGACY,Suppression Mask 1"
        assertFailsWith<IllegalArgumentException> {
            importWith(
                partial,
                """"Bava Batra 28a:1","Migdal Oz 5:1","reference",0"""",
            )
        }
    }
}
