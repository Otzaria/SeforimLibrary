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

    /** Texts of the fixture: a base line has three words of its own, a segment says what [Fixture.say] told. */
    private class FixtureTexts : LineTextSource {
        val segmentText = HashMap<Int, String>()

        private fun word(n: Int): String {
            val letters = ('א'..'ת').toList()
            return "ק" + letters[n % 22] + letters[(n / 22) % 22] + "ת"
        }

        fun baseText(lineIndex: Int) = (0..2).joinToString(" ") { word(lineIndex * 3 + it) }

        override fun text(path: String, lineIndex: Int) = if (path == "b") baseText(lineIndex) else segmentText[lineIndex]
    }

    /** Builds a commentary and its base from addresses; line ids are 1000+i (base) and 2000+i. */
    private class Fixture(baseAddresses: List<String>, segmentAddresses: List<String>, mapping: String?) {
        val baseLines = baseAddresses.mapIndexed { i, a -> AddressedLine(1000L + i, i, a) }
        val segments = segmentAddresses.mapIndexed { i, a -> AddressedLine(2000L + i, i, a) }
        val book = AddressedCommentary(2L, 1L, mapping, segments, baseLines, commentaryPath = "c", basePath = "b")
        val texts = FixtureTexts()
        val links = CommentaryBaseLinks(
            listOf(book),
            (baseLines.associate { it.lineId to 1L } + segments.associate { it.lineId to 2L }),
        )

        fun baseLine(address: String) = baseLines.single { it.address == address }
        fun segment(address: String) = segments.single { it.address == address }

        /** The segment's text quotes the words of [baseAddress]. */
        fun say(segmentAddress: String, baseAddress: String) {
            texts.segmentText[segment(segmentAddress).lineIndex] = texts.baseText(baseLine(baseAddress).lineIndex)
        }

        fun link(segmentAddress: String, baseAddress: String, type: ConnectionType = ConnectionType.COMMENTARY) {
            val s = segment(segmentAddress)
            links.record(Link(0, 1L, 2L, baseLine(baseAddress).lineId, s.lineId, s.lineIndex, type))
            say(segmentAddress, baseAddress)
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

    private val amudim = listOf("96a", "96b", "97a", "97b", "98a", "98b")

    /**
     * Meiri on Chullin: six amudim of 10 base lines and 5 segments. Every segment is linked and
     * its text is its base line's, except 96a:3, which sits between two segments linked to 96a:6.
     */
    private fun meiri(): Fixture {
        val f = Fixture(amudim.flatMap { daf(it, 10) }, amudim.flatMap { daf(it, 5) }, null)
        amudim.forEach { a ->
            listOf(1 to 5, 2 to 6, 4 to 6, 5 to 9).forEach { (s, b) -> f.link("$a:$s", "$a:$b") }
            if (a != "96a") f.link("$a:3", "$a:6")
        }
        f.say("96a:3", "96a:6")
        return f
    }

    private fun derive(f: Fixture) = deriveTalmudNeighbourLinks(f.book, f.links, f.texts)

    @Test
    fun issue3264SegmentBetweenTwoLinksToTheSameLine() {
        val f = meiri()
        val derived = derive(f)
        assertEquals(listOf("96a:6" to "96a:3"), derived.map { it.addresses(f) })
        assertEquals(DerivedLinkStage.TALMUD_NEIGHBOURS, derived.single().stage)
    }

    @Test
    fun runOfTwoBetweenTheSameLineGetsItWhenBothTextsAgree() {
        val f = meiri()
        f.links.anchors[f.segment("96a:5").lineId] = mutableSetOf(f.baseLine("96a:6").lineId)
        f.links.linked.remove(f.segment("96a:4").lineId)
        f.links.anchors.remove(f.segment("96a:4").lineId)
        f.say("96a:4", "96a:6")
        assertEquals(setOf("96a:3", "96a:4"), derive(f).map { it.addresses(f).second }.toSet())
    }

    /** Chidushei Halachot on Eruvin 31b:2: the last segment of the amud, one linked neighbour only. */
    @Test
    fun issue3806SegmentAtTheEdgeOfTheAmudDerivesNothing() {
        val f = meiri()
        f.links.linked.remove(f.segment("98b:5").lineId)
        f.links.anchors.remove(f.segment("98b:5").lineId)
        f.say("98b:5", "98b:9")
        assertTrue(derive(f).none { it.addresses(f).second == "98b:5" })
    }

    @Test
    fun segmentBeforeTheFirstLinkedOneDerivesNothing() {
        val f = meiri()
        f.links.linked.remove(f.segment("97a:1").lineId)
        f.links.anchors.remove(f.segment("97a:1").lineId)
        f.say("97a:1", "97a:5")
        assertTrue(derive(f).none { it.addresses(f).second == "97a:1" })
    }

    @Test
    fun neighboursOnDifferentLinesDeriveNothing() {
        val f = meiri()
        f.links.anchors[f.segment("96a:4").lineId] = mutableSetOf(f.baseLine("96a:8").lineId)
        assertTrue(derive(f).none { it.addresses(f).second == "96a:3" })
    }

    @Test
    fun neighbourLinkedToSeveralLinesDerivesNothing() {
        val f = meiri()
        f.links.anchors.getValue(f.segment("96a:4").lineId) += f.baseLine("96a:7").lineId
        assertTrue(derive(f).none { it.addresses(f).second == "96a:3" })
    }

    @Test
    fun neighbourLinkedOnlyOutsideTheAmudDerivesNothing() {
        val f = meiri()
        f.links.anchors[f.segment("96a:4").lineId] = mutableSetOf(f.baseLine("96b:6").lineId)
        f.links.anchors[f.segment("96a:2").lineId] = mutableSetOf(f.baseLine("96b:6").lineId)
        assertTrue(derive(f).none { it.addresses(f).second == "96a:3" })
    }

    @Test
    fun segmentWhoseTextPointsElsewhereDerivesNothing() {
        val f = meiri()
        f.say("96a:3", "96a:9")
        assertTrue(derive(f).isEmpty())
    }

    @Test
    fun segmentWithoutAClearBestLineDerivesNothing() {
        val f = meiri()
        f.say("96a:3", "96a:6")
        f.texts.segmentText[f.segment("96a:3").lineIndex] = f.texts.baseText(f.baseLine("96a:6").lineIndex).split(' ').first()
        assertTrue(derive(f).isEmpty())
    }

    @Test
    fun bookWhoseTextsDoNotMatchItsOwnLinksDerivesNothing() {
        val f = meiri()
        // Every other linked segment now says something else: the gate sees 50% of Sefaria's links agree.
        f.segments.filter { it.address != "96a:3" }.filterIndexed { i, _ -> i % 2 == 0 }.forEach {
            f.texts.segmentText[it.lineIndex] = f.texts.baseText(f.baseLines.first { b -> b.address.startsWith("98b") }.lineIndex)
        }
        assertTrue(derive(f).isEmpty())
    }

    @Test
    fun bookWithTooFewLinkedSegmentsDerivesNothing() {
        val f = Fixture(daf("96a", 10) + daf("96b", 10), daf("96a", 5) + daf("96b", 4), null)
        listOf(1 to 5, 2 to 6, 4 to 6, 5 to 9).forEach { (s, b) -> f.link("96a:$s", "96a:$b") }
        listOf(1 to 2, 2 to 4, 3 to 6, 4 to 9).forEach { (s, b) -> f.link("96b:$s", "96b:$b") }
        f.say("96a:3", "96a:6")
        assertTrue(derive(f).isEmpty())
    }

    @Test
    fun bookOutOfBaseOrderDerivesNothing() {
        val f = meiri()
        amudim.forEach { a ->
            f.links.anchors[f.segment("$a:2").lineId] = mutableSetOf(f.baseLine("$a:1").lineId)
            f.links.anchors[f.segment("$a:5").lineId] = mutableSetOf(f.baseLine("$a:2").lineId)
        }
        assertTrue(derive(f).isEmpty())
    }

    @Test
    fun segmentAddressedBelowTheDafIsNotStage2() {
        val f = Fixture(daf("96a", 10), (1..6).map { "96a:$it:1" }, null)
        assertTrue(derive(f).isEmpty())
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
            ), commentaryPath = "meiri", basePath = "chullin")),
            collected,
        )
    }
}
