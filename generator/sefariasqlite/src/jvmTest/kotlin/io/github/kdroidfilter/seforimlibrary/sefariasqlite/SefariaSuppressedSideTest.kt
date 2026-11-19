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
        val linkCount: Int,
        val relatedCount: Int,
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
        val talmud = repo.insertCategory(Category(0, null, "תלמוד בבלי", level = 0, order = 1))
        val halakha = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 2))
        val yerushalmi = repo.insertCategory(Category(0, null, "תלמוד ירושלמי", level = 0, order = 3))
        fun book(id: Long, title: String, isBase: Boolean, lines: Int, category: Long) = Book(
            id = id, categoryId = category, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = isBase, totalLines = lines, hasAltStructures = true,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "בבא בתרא", isBase = true, lines = 4, category = talmud))
        repo.insertBook(book(2, "מגדל עוז", isBase = false, lines = 2, category = talmud))
        repo.insertBook(book(3, "טור", isBase = true, lines = 2, category = halakha))
        repo.insertBook(book(4, "בית יוסף", isBase = false, lines = 3, category = halakha))
        repo.insertBook(book(5, "ירושלמי כתובות", isBase = true, lines = 1, category = yerushalmi))

        val allRefs = listOf(
            RefEntry("Bava Batra 28a:1", "בבא בתרא כח., א", "Bava Batra", 1),
            RefEntry("Bava Batra 28a:2", "בבא בתרא כח., ב", "Bava Batra", 2),
            RefEntry("Bava Batra 28a:3", "בבא בתרא כח., ג", "Bava Batra", 3),
            RefEntry("Bava Batra 28b:1", "בבא בתרא כח:, א", "Bava Batra", 4),
            RefEntry("Migdal Oz 5:1", "מגדל עוז ה, א", "Migdal Oz", 1),
            RefEntry("Migdal Oz 5:3", "מגדל עוז ה, ג", "Migdal Oz", 2),
            RefEntry("Tur, Choshen Mishpat 157:1", "טור, חושן משפט, קנז, א", "Tur", 1),
            RefEntry("Tur, Even HaEzer 111:1", "טור, אבן העזר, קיא, א", "Tur", 2),
            RefEntry("Beit Yosef, Choshen Mishpat 157:1:1", "בית יוסף, חושן משפט, קנז, א, א", "Beit Yosef", 1),
            RefEntry("Beit Yosef, Choshen Mishpat 157:1:2", "בית יוסף, חושן משפט, קנז, א, ב", "Beit Yosef", 2),
            RefEntry("Beit Yosef, Choshen Mishpat 157:2:1", "בית יוסף, חושן משפט, קנז, ב, א", "Beit Yosef", 3),
            RefEntry("Jerusalem Talmud Ketubot 10:1:1", "תלמוד ירושלמי כתובות, י, א, א", "Jerusalem Talmud Ketubot", 1),
        )
        val bookIds = mapOf(
            "Bava Batra" to 1L, "Migdal Oz" to 2L, "Tur" to 3L, "Beit Yosef" to 4L, "Jerusalem Talmud Ketubot" to 5L,
        )
        val lines = allRefs.mapIndexed { index, ref ->
            Line(
                id = index + 1L,
                bookId = bookIds.getValue(ref.path),
                lineIndex = ref.lineIndex - 1,
                content = ref.ref,
                heRef = ref.heRef,
            )
        }
        repo.insertLinesBatch(lines)
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefs.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }

        val importer = SefariaLinksImporter(
            repo,
            IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo),
            Logger.withTag("SefariaSuppressedSideTest"),
        )
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) },
            refsByBase = refsByBase,
            lineKeyToId = allRefs.zip(lines).associate { (ref, line) -> (ref.path to line.lineIndex) to line.id },
            lineIdToBookId = lines.associate { it.id to it.bookId },
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
                3L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 1),
                4L to BookMeta(
                    isBaseBook = false, categoryLevel = 1, priorityRank = null, dependence = Dependence.COMMENTARY,
                ),
                5L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            ),
            refsByPath = allRefs.groupBy { it.path },
            requireExportedVisibility = requireExportedVisibility,
        )
        repo.rebuildCategoryClosure()
        importer.demoteCrossCorpusDependantLinks()
        importer.showTurCommentarySectionLinks()

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
            linkCount = query("SELECT COUNT(*) FROM link").single().single().toInt(),
            relatedCount = query("SELECT COUNT(*) FROM link l JOIN connection_type c ON l.connectionTypeId = c.id WHERE c.name = 'RELATED'")
                .single().single().toInt(),
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

        fun turSectionRow(mask: Int = SuppressionReason.ANCHOR_NOT_SEGMENT) =
            row("Beit Yosef, Choshen Mishpat 157:1", "Tur, Choshen Mishpat 157:1", mask, 0, type = "commentary")

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
                SuppressionReason.OTHER_TOO_COARSE,
                0,
                type = "commentary",
            ),
        )

        // The dependant→base CSV row is stored base→dependant. Citation 1's
        // verdict must therefore land on stored side 1, not side 0.
        assertEquals(1, imported.suppressed.size)
        assertEquals(
            listOf(1L, SuppressionReason.OTHER_TOO_COARSE.toLong()),
            imported.suppressed[0].drop(1),
        )
    }

    @Test
    fun turSectionLevelCommentaryCitationStaysVisibleOnTheCommentary() {
        val imported = importWith(HEADER_WITH_MASKS, turSectionRow())
        assertEquals(1, imported.linkCount)
        assertTrue(imported.suppressed.isEmpty(), "got ${imported.suppressed}")
    }

    @Test
    fun turSectionLevelRangeContributionKeepsTheSideHidden() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            turSectionRow(),
            row(
                "Beit Yosef, Choshen Mishpat 157:1-2", "Tur, Choshen Mishpat 157:1",
                SuppressionReason.ANCHOR_NOT_SEGMENT, 0, type = "commentary",
            ),
        )
        assertEquals(1, imported.linkCount)
        assertEquals(2, imported.coverage)
        assertEquals(listOf(1L, SuppressionReason.ANCHOR_NOT_SEGMENT.toLong()), imported.suppressed.single().drop(1))
    }

    @Test
    fun turSectionLevelCommentaryKeepsOtherSuppressionReasons() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            turSectionRow(),
            turSectionRow(SuppressionReason.OTHER_TOO_COARSE),
        )
        assertEquals(1, imported.linkCount)
        val combined = SuppressionReason.ANCHOR_NOT_SEGMENT or SuppressionReason.OTHER_TOO_COARSE
        assertEquals(listOf(1L, combined.toLong()), imported.suppressed.single().drop(1))
    }

    @Test
    fun unrelatedSectionLevelCommentaryCitationStaysHidden() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row("Migdal Oz 5:1", "Bava Batra 28a:1", SuppressionReason.ANCHOR_NOT_SEGMENT, 0, type = "commentary"),
        )
        assertEquals(1, imported.linkCount)
        assertEquals(listOf(1L, SuppressionReason.ANCHOR_NOT_SEGMENT.toLong()), imported.suppressed.single().drop(1))
    }

    @Test
    fun crossCorpusCommentaryCitationStaysHiddenAfterDemotion() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row(
                "Jerusalem Talmud Ketubot 10:1:1", "Tur, Even HaEzer 111",
                0, SuppressionReason.ANCHOR_NOT_SEGMENT, type = "commentary",
            ),
        )
        assertEquals(1, imported.relatedCount)
        assertEquals(listOf(1L, SuppressionReason.ANCHOR_NOT_SEGMENT.toLong()), imported.suppressed.single().drop(1))
    }

    @Test
    fun sectionLevelReferenceCitationStaysHidden() {
        val imported = importWith(
            HEADER_WITH_MASKS,
            row("Bava Batra 28a:1", "Migdal Oz 5:1", 0, SuppressionReason.ANCHOR_NOT_SEGMENT),
        )
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
