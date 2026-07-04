package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaRangeRefsTest {

    // ───── parseCitationRange ─────

    @Test
    fun lastComponentRange() {
        val r = parseCitationRange("rashi on genesis 18:10:2-3")
        assertEquals("rashi on genesis 18:10:2", r?.startCanonical)
        assertEquals("rashi on genesis 18:10:3", r?.endCanonical)
    }

    @Test
    fun crossSectionRange() {
        val r = parseCitationRange("exodus 1:1-6:1")
        assertEquals("exodus 1:1", r?.startCanonical)
        assertEquals("exodus 6:1", r?.endCanonical)
    }

    @Test
    fun intermediateLevelRangeInheritsLeadingComponents() {
        // Verses 1-6 of chapter 6 in a depth-3 commentary.
        val r = parseCitationRange("ibn ezra on genesis 6:1-6")
        assertEquals("ibn ezra on genesis 6:1", r?.startCanonical)
        assertEquals("ibn ezra on genesis 6:6", r?.endCanonical)
    }

    @Test
    fun dafRanges() {
        val whole = parseCitationRange("berakhot 2a-2b")
        assertEquals("berakhot 2a", whole?.startCanonical)
        assertEquals("berakhot 2b", whole?.endCanonical)

        val segments = parseCitationRange("bava metzia 58b:7-8")
        assertEquals("bava metzia 58b:7", segments?.startCanonical)
        assertEquals("bava metzia 58b:8", segments?.endCanonical)

        val crossDaf = parseCitationRange("tosafot on shabbat 96a:4-102a:13")
        assertEquals("tosafot on shabbat 96a:4", crossDaf?.startCanonical)
        assertEquals("tosafot on shabbat 102a:13", crossDaf?.endCanonical)
    }

    @Test
    fun deepPartialEndRange() {
        val r = parseCitationRange("jerusalem talmud megillah 1:4:11-5:7")
        assertEquals("jerusalem talmud megillah 1:4:11", r?.startCanonical)
        assertEquals("jerusalem talmud megillah 1:5:7", r?.endCanonical)
    }

    @Test
    fun nonRangesReturnNull() {
        assertNull(parseCitationRange("genesis 1:1"))
        assertNull(parseCitationRange("genesis"))
        // Dash inside a title word is not an address range.
        assertNull(parseCitationRange("ein ayeh-berakhot 1:5"))
        // Degenerate "ranges" that resolve to the start itself.
        assertNull(parseCitationRange("genesis 1:1-1"))
        assertNull(parseCitationRange("genesis 1:1-1:1"))
        // End with more components than the start is malformed.
        assertNull(parseCitationRange("genesis 5-1:2"))
    }

    // ───── addressPrefixes ─────

    @Test
    fun addressPrefixesAtEveryDepth() {
        assertEquals(
            listOf("rashi on genesis 13", "rashi on genesis 13:11", "rashi on genesis 13:11:1"),
            addressPrefixes("rashi on genesis 13:11:1"),
        )
        assertEquals(listOf("berakhot 2a", "berakhot 2a:4"), addressPrefixes("berakhot 2a:4"))
        assertEquals(listOf("no trailing address"), addressPrefixes("no trailing address"))
    }

    // ───── PathRefPrefixIndex ─────

    private fun entry(ref: String, lineIndex: Int) =
        RefEntry(ref = ref, heRef = ref, path = "Rashi on Genesis", lineIndex = lineIndex)

    @Test
    fun lastUnderResolvesLeafAndIntermediateLevels() {
        val index = PathRefPrefixIndex.build(
            listOf(
                entry("Rashi on Genesis 1:1:1", 1),
                entry("Rashi on Genesis 1:1:2", 2),
                entry("Rashi on Genesis 1:2:1", 3),
                entry("Rashi on Genesis 2:1:1", 4),
            )
        )
        // Exact leaf.
        assertEquals(2, index.lastUnder("rashi on genesis 1:1:2")?.lineIndex)
        // Verse level → last comment on that verse.
        assertEquals(2, index.lastUnder("rashi on genesis 1:1")?.lineIndex)
        // Chapter level → last line of the chapter.
        assertEquals(3, index.lastUnder("rashi on genesis 1")?.lineIndex)
        assertEquals(4, index.lastUnder("rashi on genesis 2")?.lineIndex)
        assertNull(index.lastUnder("rashi on genesis 9"))
    }

    // ───── resolveRefs: intermediate-level range start (the former drop bug) ─────

    @Test
    fun rangeStartAtIntermediateLevelResolvesToFirstLeaf() {
        val leaves = listOf(
            RefEntry("Ibn Ezra on Genesis 6:1:1", "", "Ibn Ezra on Genesis", 1),
            RefEntry("Ibn Ezra on Genesis 6:1:2", "", "Ibn Ezra on Genesis", 2),
            RefEntry("Ibn Ezra on Genesis 6:2:1", "", "Ibn Ezra on Genesis", 3),
        )
        val refsByCanonical = leaves.groupBy { canonicalCitation(it.ref) }
        val refsByBase = mutableMapOf<String, RefEntry>()
        leaves.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }

        // Formerly dropped: canonicalBase("… 6:1") stripped one level too many.
        val resolved = resolveRefs("Ibn Ezra on Genesis 6:1-6", refsByCanonical, refsByBase)
        assertEquals(1, resolved.size)
        assertEquals(1, resolved.single().lineIndex)

        // Leaf-anchored range starts keep resolving through refsByCanonical.
        val leafStart = resolveRefs("Ibn Ezra on Genesis 6:1:1-2", refsByCanonical, refsByBase)
        assertEquals(1, leafStart.single().lineIndex)
    }
}
