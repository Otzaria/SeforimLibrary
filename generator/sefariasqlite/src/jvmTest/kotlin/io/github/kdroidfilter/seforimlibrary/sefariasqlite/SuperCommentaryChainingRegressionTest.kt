package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * רגרסיות מול v28 שנגרמו מתזוזת ספירות הקישורים בייצוא ספריא (links_by_book.csv
 * של 2026-09-30 מול זה של 2026-09-18). כל הספירות כאן הן הספירות האמיתיות של
 * הייצוא המוצמד, והמטא-דאטה היא זו של הסכמות שלו.
 */
class SuperCommentaryChainingRegressionTest {
    private val log = Logger.withTag("test")

    private fun Pair<Long, Long>.norm(): Pair<Long, Long> = if (first < second) this else second to first

    private fun counts(vararg rows: Triple<Long, Long, Int>): Map<Pair<Long, Long>, Int> =
        rows.associate { (a, b, n) -> (a to b).norm() to n }

    private fun primary() = BookMeta(false, 0, null)

    private fun dependant(
        bases: Set<Long>,
        collective: String?,
        authors: Set<String>,
        declared: Boolean = true,
    ) = BookMeta(
        false, 1, null,
        dependence = Dependence.COMMENTARY,
        baseTextBookIds = bases,
        sefariaDeclaredBaseTextBookIds = if (declared) bases else emptySet(),
        inferredBaseTextBookIds = if (declared) emptySet() else bases,
        collectiveTitleEn = collective,
        authorKeys = authors,
    )

    private fun runDensityPipeline(meta: MutableMap<Long, BookMeta>, counts: Map<Pair<Long, Long>, Int>) {
        inferPrimaryBasesForEmptyDeclaredBookmeta(meta, counts, log)
        completeDeclaredBaseFamilies(meta, counts, log)
        applyLinkDensitySiblingChaining(meta, counts, log)
    }

    /** The stored direction of a CSV row typed `commentary`, as the importer writes it. */
    private fun storedCommentary(c1: Long, c2: Long, meta: Map<Long, BookMeta>): Pair<Long, Long>? {
        val (forward, _) = resolveDirectionalConnectionTypesForMeta(
            ConnectionType.COMMENTARY, c1, c2, meta[c1], meta[c2],
        )
        return when (forward) {
            ConnectionType.SOURCE -> c2 to c1
            ConnectionType.COMMENTARY -> c1 to c2
            else -> null
        }
    }

    // ── v28#2: מחוקקי יהודה ↔ אבן עזרא על שמות (והפירוש הקצר) ─────────────────

    private val genesis = 1L
    private val exodus = 2L
    private val ibnEzraGenesis = 3613L
    private val ibnEzraExodus = 3619L
    private val ibnEzraHaKatzar = 3621L
    private val yahelOhr = 3509L
    private val karneiOhr = 3510L

    private fun mechokekeiYehudah(): MutableMap<Long, BookMeta> {
        val ibnEzra = setOf("אבן עזרא")
        val krinsky = setOf("יהודה לייב קרינסקי")
        // Sefaria lists Ibn Ezra on Genesis/Leviticus/Numbers/Deuteronomy for both books,
        // but not Ibn Ezra on Exodus nor HaKatzar (two Torah books suffice here).
        val mechokekeiBases = setOf(genesis, exodus, ibnEzraGenesis)
        return hashMapOf(
            genesis to primary(),
            exodus to primary(),
            ibnEzraGenesis to dependant(setOf(genesis), "Ibn Ezra", ibnEzra),
            ibnEzraExodus to dependant(setOf(exodus), "Ibn Ezra", ibnEzra),
            // No base_text_titles: Exodus comes from the "… on Exodus" title.
            ibnEzraHaKatzar to dependant(setOf(exodus), "Ibn Ezra HaKatzar", ibnEzra, declared = false),
            yahelOhr to dependant(mechokekeiBases, "Mechokekei Yehudah; Yahel Ohr", krinsky),
            karneiOhr to dependant(mechokekeiBases, "Mechokekei Yehudah; Karnei Ohr", krinsky),
        )
    }

    private val mechokekeiCounts = counts(
        Triple(exodus, yahelOhr, 6745), // was 235 in the 2026-09-18 export
        Triple(ibnEzraExodus, yahelOhr, 6513),
        Triple(ibnEzraGenesis, yahelOhr, 4906),
        Triple(genesis, yahelOhr, 4885),
        Triple(ibnEzraHaKatzar, yahelOhr, 2889),
        Triple(exodus, ibnEzraExodus, 2337),
        Triple(genesis, ibnEzraGenesis, 1801),
        Triple(genesis, karneiOhr, 1640),
        Triple(ibnEzraGenesis, karneiOhr, 1558),
        Triple(exodus, ibnEzraHaKatzar, 1186),
        Triple(exodus, karneiOhr, 966), // was 98
        Triple(ibnEzraExodus, karneiOhr, 871),
        Triple(ibnEzraHaKatzar, karneiOhr, 246),
        Triple(genesis, ibnEzraExodus, 355),
    )

    @Test
    fun mechokekeiYehudahIsStoredUnderIbnEzraOnExodusAndHaKatzar() {
        val meta = mechokekeiYehudah()
        runDensityPipeline(meta, mechokekeiCounts)

        for (superCommentary in listOf(yahelOhr, karneiOhr)) {
            for (ibnEzra in listOf(ibnEzraExodus, ibnEzraHaKatzar)) {
                assertTrue(ibnEzra in meta.getValue(superCommentary).baseTextBookIds, "$superCommentary must depend on $ibnEzra")
                assertFalse(superCommentary in meta.getValue(ibnEzra).baseTextBookIds, "$ibnEzra must not depend on $superCommentary")
                // Sefaria's row order: Citation 1 = Ibn Ezra, Citation 2 = Mechokekei Yehudah.
                assertEquals(ibnEzra to superCommentary, storedCommentary(ibnEzra, superCommentary, meta))
                assertEquals(ibnEzra to superCommentary, storedCommentary(superCommentary, ibnEzra, meta))
            }
        }
    }

    @Test
    fun ibnEzraOnGenesisNoLongerGetsYahelOhrChainedBackOverItsDeclaration() {
        val meta = mechokekeiYehudah()
        runDensityPipeline(meta, mechokekeiCounts)
        // 4,906 links that were OTHER in v28 and in every build since: the chaining made the
        // declared pair mutual (Genesis–Yahel Ohr 4885 > Genesis–Ibn Ezra 1801, ratio 2.7).
        assertFalse(yahelOhr in meta.getValue(ibnEzraGenesis).baseTextBookIds)
        assertEquals(ibnEzraGenesis to yahelOhr, storedCommentary(ibnEzraGenesis, yahelOhr, meta))
        assertEquals(ibnEzraGenesis to karneiOhr, storedCommentary(ibnEzraGenesis, karneiOhr, meta))
    }

    @Test
    fun familyCompletionNeedsTheNoiseFloorASharedBaseAndTheFamily() {
        val meta = mechokekeiYehudah()
        val stranger = 9000L
        // Another Exodus commentary, same link count, but neither the collective nor the author.
        meta[stranger] = dependant(setOf(exodus), "Ramban", setOf("רמב\"ן"))
        val sparse = counts(
            Triple(ibnEzraExodus, yahelOhr, LINK_DENSITY_BASE_FLOOR - 1),
            Triple(ibnEzraHaKatzar, yahelOhr, 2889),
            Triple(stranger, yahelOhr, 2889),
        )
        completeDeclaredBaseFamilies(meta, sparse, log)
        val bases = meta.getValue(yahelOhr).baseTextBookIds
        assertFalse(ibnEzraExodus in bases, "below the floor")
        assertTrue(ibnEzraHaKatzar in bases, "same author, base Exodus is declared")
        assertFalse(stranger in bases, "not of the declared family")
        // Provenance sets are untouched: book_base_text and baseProvenance stay as declared.
        assertFalse(ibnEzraHaKatzar in meta.getValue(yahelOhr).sefariaDeclaredBaseTextBookIds)
        assertFalse(ibnEzraHaKatzar in meta.getValue(yahelOhr).inferredBaseTextBookIds)
    }

    @Test
    fun familyCompletionDoesNotReachIntoBasesTheSuperCommentaryDoesNotDeclare() {
        val meta = mechokekeiYehudah()
        val leviticus = 3L
        val ibnEzraLeviticus = 3615L
        meta[leviticus] = primary()
        meta[ibnEzraLeviticus] = dependant(setOf(leviticus), "Ibn Ezra", setOf("אבן עזרא"))
        // The fixture's Mechokekei Yehudah declares only Genesis and Exodus.
        completeDeclaredBaseFamilies(meta, counts(Triple(ibnEzraLeviticus, yahelOhr, 3000)), log)
        assertFalse(ibnEzraLeviticus in meta.getValue(yahelOhr).baseTextBookIds)
    }

    // ── chaining never overrides a declaration ─────────────────────────────

    @Test
    fun machatzitHaShekelStaysUnderMagenAvrahamWhichItDeclares() {
        val shulchanArukhOC = 382L
        val magenAvraham = 4040L
        val machatzitHaShekel = 4041L
        val meta = hashMapOf(
            shulchanArukhOC to primary(),
            magenAvraham to dependant(setOf(shulchanArukhOC), "Magen Avraham", setOf("אברהם אבלי גומבינר")),
            machatzitHaShekel to dependant(
                setOf(shulchanArukhOC, magenAvraham), "Machatzit HaShekel", setOf("שמואל הלוי קעלין"),
            ),
        )
        val real = counts(
            Triple(machatzitHaShekel, shulchanArukhOC, 13721),
            Triple(machatzitHaShekel, magenAvraham, 12442),
            Triple(magenAvraham, shulchanArukhOC, 8587),
        )
        runDensityPipeline(meta, real)
        // 13721 > 8587 and 12442/8587 = 1.45: the old rule chained Machatzit HaShekel as a base
        // of Magen Avraham, the pair became mutual, and all 12,442 links were stored as OTHER.
        assertFalse(machatzitHaShekel in meta.getValue(magenAvraham).baseTextBookIds)
        assertEquals(magenAvraham to machatzitHaShekel, storedCommentary(machatzitHaShekel, magenAvraham, meta))
    }

    @Test
    fun edgesThatWouldPointBothWaysInOnePassAreDropped() {
        val p1 = 1L
        val p2 = 2L
        val a = 10L
        val b = 11L
        val meta = hashMapOf(
            p1 to primary(),
            p2 to primary(),
            a to dependant(setOf(p1, p2), null, emptySet()),
            b to dependant(setOf(p1, p2), null, emptySet()),
        )
        // Through p1, b is the denser sibling; through p2, a is.
        val c = counts(
            Triple(a, p1, 100), Triple(b, p1, 400),
            Triple(a, p2, 400), Triple(b, p2, 100),
            Triple(a, b, 500),
        )
        applyLinkDensitySiblingChaining(meta, c, log)
        assertFalse(b in meta.getValue(a).baseTextBookIds)
        assertFalse(a in meta.getValue(b).baseTextBookIds)
    }

    // ── v28#3: מהר"ם שיף ↔ תוספות (חולין, וכתובות שכבר לפני v28) ─────────────

    private data class Tractate(
        val name: String,
        val gemara: Long,
        val tosafot: Long,
        val maharamSchiff: Long,
        val tosafotGemara: Int,
        val maharamGemara: Int,
        val maharamTosafot: Int,
    )

    private val maharamSchiffTractates = listOf(
        Tractate("Chullin", 100, 200, 300, 1545, 386, 305), // 329 in the 2026-09-18 export
        Tractate("Ketubot", 101, 201, 301, 1544, 942, 740),
        Tractate("Bava Metzia", 102, 202, 302, 1384, 416, 662),
        Tractate("Gittin", 103, 203, 303, 1177, 504, 817),
        Tractate("Bava Kamma", 104, 204, 304, 1568, 133, 232),
        Tractate("Beitzah", 105, 205, 305, 298, 132, 156),
        Tractate("Zevachim", 106, 206, 306, 1466, 116, 78),
    )

    private fun maharamSchiff(tractates: List<Tractate>): Pair<MutableMap<Long, BookMeta>, Map<Pair<Long, Long>, Int>> {
        val meta = HashMap<Long, BookMeta>()
        val rows = ArrayList<Triple<Long, Long, Int>>()
        for (t in tractates) {
            meta[t.gemara] = primary()
            meta[t.tosafot] = dependant(setOf(t.gemara), "Tosafot", setOf("תוספות"))
            meta[t.maharamSchiff] = dependant(setOf(t.gemara), "Maharam Schiff", setOf("מאיר הכהן שיף"))
            rows += Triple(t.tosafot, t.gemara, t.tosafotGemara)
            rows += Triple(t.maharamSchiff, t.gemara, t.maharamGemara)
            rows += Triple(t.maharamSchiff, t.tosafot, t.maharamTosafot)
        }
        return meta to counts(*rows.toTypedArray())
    }

    @Test
    fun maharamSchiffChainsToTosafotOnEveryTractateThroughTheWholeWork() {
        val (meta, c) = maharamSchiff(maharamSchiffTractates)
        runDensityPipeline(meta, c)
        // 305/386 = 0.79 (Chullin), 740/942 = 0.79 (Ketubot), 78/116 = 0.67 (Zevachim) miss the
        // per-volume 0.8, but Maharam Schiff → Tosafot is 3,490/3,059 = 1.14 over the work.
        for (t in maharamSchiffTractates) {
            assertTrue(t.tosafot in meta.getValue(t.maharamSchiff).baseTextBookIds, "${t.name}: Maharam Schiff → Tosafot")
            assertFalse(t.maharamSchiff in meta.getValue(t.tosafot).baseTextBookIds, "${t.name}: no reverse edge")
            assertEquals(t.tosafot to t.maharamSchiff, storedCommentary(t.maharamSchiff, t.tosafot, meta), t.name)
        }
    }

    @Test
    fun aWorkBelowTheThresholdKeepsOnlyItsPassingVolumes() {
        // Gilyon HaShas → Tosafot shape: two volumes pass, the work as a whole (0.36) does not.
        val tractates = listOf(
            Tractate("A", 100, 200, 300, 1000, 100, 90),
            Tractate("B", 101, 201, 301, 1000, 100, 85),
            Tractate("C", 102, 202, 302, 1000, 300, 60),
            Tractate("D", 103, 203, 303, 1000, 300, 50),
        )
        val (meta, c) = maharamSchiff(tractates)
        applyLinkDensitySiblingChaining(meta, c, log)
        val chained = tractates.filter { it.tosafot in meta.getValue(it.maharamSchiff).baseTextBookIds }.map { it.name }
        assertEquals(listOf("A", "B"), chained)
    }

    @Test
    fun aSingleVolumeWorkIsJudgedOnItsOwnRatio() {
        // Bartenura on Torah ↔ Rashi: one volume, below the threshold — no aggregate rescue.
        val (meta, c) = maharamSchiff(listOf(Tractate("Chullin", 100, 200, 300, 1545, 386, 305)))
        applyLinkDensitySiblingChaining(meta, c, log)
        assertFalse(200L in meta.getValue(300L).baseTextBookIds)
        assertEquals(null, storedCommentary(300L, 200L, meta), "still OTHER without a base edge")
    }
}
