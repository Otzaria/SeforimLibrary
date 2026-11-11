package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Commentary links derived from Sefaria segment addresses (Otzaria/otzaria#3854). */
class SefariaDerivedCommentaryLinksTest {
    /** Builds a commentary and its base from addresses; line ids are 1000+i (base) and 2000+i. */
    private class Fixture(baseAddresses: List<String>, segmentAddresses: List<String>, mapping: String?) {
        val baseLines = baseAddresses.mapIndexed { i, a -> AddressedLine(1000L + i, i, a) }
        val segments = segmentAddresses.mapIndexed { i, a -> AddressedLine(2000L + i, i, a) }
        val book = AddressedCommentary(2L, 1L, mapping, segments, baseLines)
        val links = CommentaryBaseLinks(listOf(book))

        fun baseLine(address: String) = baseLines.single { it.address == address }
        fun segment(address: String) = segments.single { it.address == address }

        fun link(
            segmentAddress: String,
            baseAddress: String,
            type: ConnectionType = ConnectionType.COMMENTARY,
            nonExact: Boolean = false,
        ) {
            val s = segment(segmentAddress)
            val link = Link(0, 1L, 2L, baseLine(baseAddress).lineId, s.lineId, s.lineIndex, type)
            links.record(link)
            if (nonExact) links.recordNonExactCommentary(link)
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
        val first = f.segment("17:16:1").lineIndex
        f.links.recordRange(f.book.commentaryBookId, first, first + 2)
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
        val halakhot = (12..20).map { "17:$it" }
        val f = Fixture(halakhot, halakhot.map { "$it:1" } + "17:16:2", "many_to_one")
        (halakhot - "17:16").forEach { f.link("$it:1", it) }
        f.link("17:16:1", "17:15")
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
    }

    @Test
    fun rangeStartsDoNotCountAsCalibrationSamples() {
        val halakhot = (12..16).map { "17:$it" }
        val f = Fixture(halakhot, halakhot.map { "$it:1" } + "17:16:2", "many_to_one")
        (halakhot - "17:12").forEach { f.link("$it:1", it) }
        f.link("17:12:1", "17:12", nonExact = true)
        assertTrue(deriveSegmentAddressLinks(f.book, f.links).isEmpty())
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
                baseTextMapping = "one_to_one",
            ),
            3L to BookMeta(
                isBaseBook = false, categoryLevel = 1, priorityRank = null, dependence = Dependence.COMMENTARY,
                sefariaDeclaredBaseTextBookIds = setOf(1L, 2L), enTitle = "Two Bases", baseTextMapping = "many_to_one",
            ),
        )

        val collected = collectAddressedCommentaries(refs.groupBy { it.path }, lineKeyToId, lineIdToBookId, setOf(22L), meta)

        assertEquals(
            listOf(AddressedCommentary(2L, 1L, "one_to_one", listOf(AddressedLine(21L, 1, "96a:1")), listOf(
                AddressedLine(10L, 0, "96a:1"), AddressedLine(11L, 1, "96a:2"),
            ))),
            collected,
        )
    }
}
