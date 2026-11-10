package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Commentary links derived where Sefaria has none (Otzaria/otzaria#3854, #3264, #3806). */
class SefariaDerivedCommentaryLinksTest {
    private val base = 1L
    private val commentary = 2L

    /** Builds a commentary and its base from addresses; line ids are 1000+i (base) and 2000+i. */
    private class Fixture(baseAddresses: List<String>, segmentAddresses: List<String>, mapping: String?) {
        val baseLines = baseAddresses.mapIndexed { i, a -> AddressedLine(1000L + i, i, a) }
        val segments = segmentAddresses.mapIndexed { i, a -> AddressedLine(2000L + i, i, a) }
        val book = AddressedCommentary(2L, 1L, mapping, segments, baseLines)
        val links = CommentaryBaseLinks(
            listOf(book),
            (baseLines.associate { it.lineId to 1L } + segments.associate { it.lineId to 2L }),
        )

        fun baseLine(address: String) = baseLines.single { it.address == address }
        fun segment(address: String) = segments.single { it.address == address }

        fun link(segmentAddress: String, baseAddress: String, type: ConnectionType = ConnectionType.COMMENTARY) {
            val s = segment(segmentAddress)
            links.record(Link(0, 1L, 2L, baseLine(baseAddress).lineId, s.lineId, s.lineIndex, type))
        }
    }

    private fun DerivedCommentaryLink.addresses(f: Fixture) =
        f.baseLines.single { it.lineId == baseLineId }.address to f.segments.single { it.lineId == commentaryLineId }.address

    // --- Stage 1: the segment address names the base line -------------------------------

    /** Maggid Mishneh, Shabbat 17: every segment linked but 17:16:2, whose only Sefaria row is a self-link. */
    private fun maggidMishneh(mapping: String? = "many_to_one"): Fixture {
        val halakhot = (1..30).map { "17:$it" }
        val segments = halakhot.flatMap { listOf("$it:1") } + "17:16:2"
        return Fixture(halakhot, segments.sortedWith(compareBy({ it.split(':')[1].toInt() }, { it })), mapping).apply {
            halakhot.forEach { link("$it:1", it) }
        }
    }

    @Test
    fun issue3854SelfLinkedSegmentGetsItsHalakha() {
        val f = maggidMishneh()
        val derived = deriveSegmentAddressLinks(f.book, f.links)
        assertEquals(listOf("17:16" to "17:16:2"), derived.map { it.addresses(f) })
        assertEquals(DerivedLinkStage.SEGMENT_ADDRESS, derived.single().stage)
        assertEquals(null, derived.single().range)
    }

    @Test
    fun oneToOneMapsTheSameAddress() {
        val addresses = (1..7).map { "1:10:$it" }
        val f = Fixture(addresses, addresses, "one_to_one")
        addresses.filter { it != "1:10:4" }.forEach { f.link(it, it) }
        assertEquals(listOf("1:10:4" to "1:10:4"), deriveSegmentAddressLinks(f.book, f.links).map { it.addresses(f) })
    }

    @Test
    fun segmentWithAnyLinkToTheBaseIsLeftAlone() {
        val f = maggidMishneh()
        f.link("17:16:2", "17:18", ConnectionType.OTHER)
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun segmentInsideAStoredRangeIsLeftAlone() {
        val f = maggidMishneh()
        f.links.recordCoveredLine(f.segment("17:16:2").lineId)
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun noMappingOrAnUnknownOneDerivesNothing() {
        listOf(null, "many_to_one_default_only", "commentary_increment_base_text_depth").forEach { mapping ->
            val f = maggidMishneh(mapping)
            assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty(), "mapping $mapping")
        }
    }

    @Test
    fun missingBaseLineDerivesNothing() {
        val halakhot = (1..30).map { "17:$it" }
        val f = Fixture(halakhot - "17:16", halakhot.map { "$it:1" } + "17:16:2", "many_to_one")
        (halakhot - "17:16").forEach { f.link("$it:1", it) }
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun bookWhoseLinksDisagreeWithItsAddressesDerivesNothing() {
        val halakhot = (1..30).map { "17:$it" }
        val f = Fixture(halakhot, halakhot.map { "$it:1" } + "17:16:2", "many_to_one")
        // Sefaria links each segment to the next halakha: the address does not name the base line.
        halakhot.zipWithNext().forEach { (h, next) -> f.link("$h:1", next) }
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun siblingLinkedAwayFromTheAddressBlocksIt() {
        val f = maggidMishneh()
        val other = f.segment("17:16:1")
        // Relink 17:16:1 to 17:15 only: its own address is then contradicted.
        f.links.anchors[other.lineId] = mutableSetOf(f.baseLine("17:15").lineId)
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).none { it.addresses(f).first == "17:16" })
    }

    // --- Stage 2: daf-only Talmud commentaries ------------------------------------------

    private fun daf(amud: String, lines: Int) = (1..lines).map { "$amud:$it" }

    /** Meiri on Chullin 96a: 96a:3 sits between two segments linked to 96a:6; 96b keeps the book ordered. */
    private fun meiri(): Fixture {
        val f = Fixture(daf("96a", 10) + daf("96b", 10), daf("96a", 5) + daf("96b", 4), null)
        f.link("96a:1", "96a:5")
        f.link("96a:2", "96a:6")
        f.link("96a:4", "96a:6")
        f.link("96a:5", "96a:9")
        listOf(1 to 2, 2 to 4, 3 to 6, 4 to 9).forEach { (s, b) -> f.link("96b:$s", "96b:$b") }
        return f
    }

    @Test
    fun issue3264SegmentBetweenTwoLinksToTheSameLine() {
        val f = meiri()
        val derived = deriveTalmudNeighbourLinks(f.book, f.links)
        assertEquals(listOf("96a:6" to "96a:3"), derived.map { it.addresses(f) })
        assertEquals(DerivedLinkStage.TALMUD_NEIGHBOURS, derived.single().stage)
        assertEquals(null, derived.single().range)
    }

    /** Chidushei Halachot on Eruvin 31b: 31b:2 is the amud's last segment, after one linked to 31b:15. */
    @Test
    fun issue3806LastSegmentOfTheAmudGetsARangeToItsEnd() {
        val f = Fixture(daf("31b", 20) + daf("32a", 20), daf("31b", 2) + daf("32a", 6), null)
        f.link("31b:1", "31b:15")
        listOf(1 to 1, 2 to 3, 3 to 5, 4 to 8, 5 to 10, 6 to 15).forEach { (s, b) -> f.link("32a:$s", "32a:$b") }

        val derived = deriveTalmudNeighbourLinks(f.book, f.links).single()

        assertEquals("31b:15" to "31b:2", derived.addresses(f))
        assertEquals(DerivedLinkStage.TALMUD_NEIGHBOURS_RANGE, derived.stage)
        assertEquals(f.baseLine("31b:20").lineId, derived.range?.endLineId)
        assertEquals((16..20).map { f.baseLine("31b:$it").lineId }, derived.range?.coveredLineIds)
    }

    @Test
    fun runBetweenDifferentLinesGetsTheRangeBetweenThem() {
        val f = meiri()
        f.links.linked.remove(f.segment("96b:2").lineId)
        f.links.anchors.remove(f.segment("96b:2").lineId)
        val derived = deriveTalmudNeighbourLinks(f.book, f.links).single { it.addresses(f).second == "96b:2" }
        assertEquals("96b:2", f.baseLines.single { it.lineId == derived.baseLineId }.address)
        assertEquals(f.baseLine("96b:6").lineId, derived.range?.endLineId)
    }

    @Test
    fun amudWithoutLinkedSegmentsDerivesNothing() {
        val f = meiri()
        val g = Fixture(f.baseLines.map { it.address } + daf("97a", 5), f.segments.map { it.address } + daf("97a", 3), null)
        f.links.anchors.forEach { (lineId, bases) ->
            val s = f.segments.single { it.lineId == lineId }
            bases.forEach { b -> g.link(s.address, f.baseLines.single { it.lineId == b }.address) }
        }
        assertTrue(deriveTalmudNeighbourLinks(g.book, g.links).none { it.addresses(g).second.startsWith("97a") })
    }

    @Test
    fun neighbourLinkedOnlyOutsideTheAmudBlocksTheRun() {
        val f = meiri()
        // 96a:4 now points to the next amud only: the run 96a:3 has no known upper bound.
        f.links.anchors[f.segment("96a:4").lineId] = mutableSetOf(f.baseLine("96b:1").lineId)
        assertTrue(deriveTalmudNeighbourLinks(f.book, f.links).none { it.addresses(f).second == "96a:3" })
    }

    @Test
    fun neighboursAgainstTheBaseOrderDeriveNothing() {
        val f = meiri()
        f.links.anchors[f.segment("96a:2").lineId] = mutableSetOf(f.baseLine("96a:8").lineId)
        assertTrue(deriveTalmudNeighbourLinks(f.book, f.links).none { it.addresses(f).second == "96a:3" })
    }

    @Test
    fun rangeLongerThanTheCapIsNotDerived() {
        val f = Fixture(daf("31b", 30) + daf("32a", 20), daf("31b", 2) + daf("32a", 6), null)
        f.link("31b:1", "31b:5")
        listOf(1 to 1, 2 to 3, 3 to 5, 4 to 8, 5 to 10, 6 to 15).forEach { (s, b) -> f.link("32a:$s", "32a:$b") }
        assertTrue(deriveTalmudNeighbourLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun runNeverCrossesIntoTheNextAmud() {
        val f = meiri()
        f.links.linked.remove(f.segment("96a:5").lineId)
        f.links.anchors.remove(f.segment("96a:5").lineId)
        val derived = deriveTalmudNeighbourLinks(f.book, f.links).single { it.addresses(f).second == "96a:5" }
        assertEquals("96a:6", f.baseLines.single { it.lineId == derived.baseLineId }.address)
        assertEquals(f.baseLine("96a:10").lineId, derived.range?.endLineId)
    }

    @Test
    fun bookOutOfBaseOrderDerivesNothing() {
        val f = Fixture(daf("96a", 10) + daf("96b", 10), daf("96a", 5) + daf("96b", 4), null)
        listOf(1 to 9, 2 to 6, 4 to 6, 5 to 2).forEach { (s, b) -> f.link("96a:$s", "96a:$b") }
        listOf(1 to 9, 2 to 6, 3 to 4, 4 to 1).forEach { (s, b) -> f.link("96b:$s", "96b:$b") }
        assertTrue(deriveTalmudNeighbourLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun segmentAddressedBelowTheDafIsNotStage2() {
        val f = Fixture(daf("96a", 10), (1..6).map { "96a:$it:1" }, null)
        assertTrue(deriveTalmudNeighbourLinks(f.book, f.links).isEmpty())
    }

    // --- Collecting -----------------------------------------------------------------------

    @Test
    fun collectsSingleBaseCommentariesWithTheirMainNodeLines() {
        val refs = listOf(
            RefEntry("Chullin 96a:1", "", "chullin", 1),
            RefEntry("Chullin 96a:2", "", "chullin", 2),
            RefEntry("Meiri on Chullin, Introduction 1", "", "meiri", 1),
            RefEntry("Meiri on Chullin,  96a:1", "", "meiri", 2),
            RefEntry("Meiri on Chullin,  96a:2", "", "meiri", 3),
            RefEntry("Two Bases 1:1", "", "two", 1),
        )
        val lineKeyToId = mapOf(
            ("chullin" to 0) to 10L, ("chullin" to 1) to 11L,
            ("meiri" to 0) to 20L, ("meiri" to 1) to 21L, ("meiri" to 2) to 22L,
            ("two" to 0) to 30L,
        )
        val lineIdToBookId = mapOf(10L to 1L, 11L to 1L, 20L to 2L, 21L to 2L, 22L to 2L, 30L to 3L)
        val meta = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0, enTitle = "Chullin"),
            2L to BookMeta(
                isBaseBook = false, categoryLevel = 1, priorityRank = null, dependence = Dependence.COMMENTARY,
                baseTextBookIds = setOf(1L), sefariaDeclaredBaseTextBookIds = setOf(1L), enTitle = "Meiri on Chullin",
            ),
            3L to BookMeta(
                isBaseBook = false, categoryLevel = 1, priorityRank = null, dependence = Dependence.COMMENTARY,
                sefariaDeclaredBaseTextBookIds = setOf(1L, 2L), enTitle = "Two Bases", baseTextMapping = "many_to_one",
            ),
        )

        val collected = collectAddressedCommentaries(refs.groupBy { it.path }, lineKeyToId, lineIdToBookId, setOf(22L), meta)

        assertEquals(
            listOf(AddressedCommentary(2L, 1L, null, listOf(AddressedLine(21L, 1, "96a:1")), listOf(
                AddressedLine(10L, 0, "96a:1"), AddressedLine(11L, 1, "96a:2"),
            ))),
            collected,
        )
    }
}
