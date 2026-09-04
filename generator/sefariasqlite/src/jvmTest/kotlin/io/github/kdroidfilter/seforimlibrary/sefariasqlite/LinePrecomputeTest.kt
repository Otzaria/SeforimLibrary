package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * [precomputeLineData] hoists per-line derivations out of the serial insert loop
 * in [SefariaDirectImporter] onto the parallel parse workers. The loop's id
 * allocation is keyed on those values, so any divergence from the expressions
 * the loop used to evaluate inline would silently renumber lines across builds.
 * These tests pin the hoisted values to the original inline expressions.
 */
class LinePrecomputeTest {

    private val lines = listOf(
        "<h1>בְּרֵאשִׁ֖ית</h1>",                 // heading, nikud + teamim
        "בְּרֵאשִׁית בָּרָא אֱלֹהִים",             // plain, nikud only
        "<p>ויאמר אלהים &nbsp; יהי אור</p>",     // entity + tags
        "<h3>פרק ב</h3>",                        // h3 heading
        "",                                       // empty line
        "<h4>סימן א</h4><b>טקסט</b>",             // h4 heading + markup
    )

    private fun payload(
        lines: List<String> = this.lines,
        refEntries: List<RefEntry> = listOf(
            // lineIndex is 1-based in RefEntry; the loop keyed on lineIndex - 1.
            RefEntry(ref = "Genesis 1:1", heRef = "בראשית א׳:א׳", path = "p", lineIndex = 2),
            RefEntry(ref = "Genesis 1:2", heRef = "בראשית א׳:ב׳", path = "p", lineIndex = 3),
        ),
    ) = BookPayload(
        heTitle = "ספר בדיקה", enTitle = "Test Book", categoriesHe = listOf("תנך"),
        lines = lines, refEntries = refEntries, headings = emptyList(), authors = emptyList(),
        description = null, heShortDesc = null, pubDates = emptyList(), altStructures = emptyList(),
    )

    @Test
    fun hoistedValuesMatchTheInlineExpressions() {
        val p = payload()
        assertSame(p, p.precomputeLineData(), "precomputeLineData must return the same payload instance")
        val pre = requireNotNull(p.precomputed)

        // refsByLineIndex: keyed on lineIndex - 1, exactly as the loop built it.
        assertEquals(p.refEntries.associateBy { it.lineIndex - 1 }, pre.refsByLineIndex)
        assertEquals(setOf(1, 2), pre.refsByLineIndex.keys)

        assertEquals(p.lines.size, pre.lineCount)
        val hashes = requireNotNull(pre.lineKeyHashes)
        val charCounts = requireNotNull(pre.lineCharCounts)
        val isHeading = requireNotNull(pre.lineIsHeading)
        assertEquals(p.lines.size, hashes.size)
        assertEquals(p.lines.size, charCounts.size)
        assertEquals(p.lines.size, isHeading.size)

        p.lines.forEachIndexed { idx, content ->
            val refEntry = pre.refsByLineIndex[idx]
            assertContentEquals(
                IdAllocatorBindings.lineNaturalKeyHash(content, refEntry?.heRef),
                hashes[idx],
                "hash mismatch at line $idx",
            )
            assertEquals(20, hashes[idx].size, "natural key hash must stay 20 bytes at line $idx")
            assertEquals(countVisibleChars(content), charCounts[idx], "charCount mismatch at line $idx")
            assertEquals(
                content.contains("<h1>") || content.contains("<h2>") ||
                    content.contains("<h3>") || content.contains("<h4>"),
                isHeading[idx],
                "heading flag mismatch at line $idx",
            )
        }
        // Sanity: the fixture actually exercises both branches.
        assertTrue(isHeading[0] && isHeading[3] && isHeading[5])
        assertTrue(!isHeading[1] && !isHeading[2] && !isHeading[4])

        val (teamim, nekudot) = detectTeamimAndNekudot(p.lines)
        assertEquals(teamim, pre.hasTeamim)
        assertEquals(nekudot, pre.hasNekudot)
        assertTrue(pre.hasTeamim && pre.hasNekudot, "fixture should carry both teamim and nekudot")
    }

    @Test
    fun heRefIsPreferredOverContentForLinesThatHaveOne() {
        val p = payload().precomputeLineData()
        val hashes = requireNotNull(p.precomputed).lineKeyHashes!!
        // Line 1 has a heRef → keyed on the ref, not the content.
        assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash("anything", "בראשית א׳:א׳"), hashes[1])
        // Line 0 has none → keyed on content.
        assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(p.lines[0], null), hashes[0])
    }

    @Test
    fun emptyBookProducesEmptyArrays() {
        val p = payload(lines = emptyList(), refEntries = emptyList()).precomputeLineData()
        val pre = requireNotNull(p.precomputed)
        assertEquals(0, pre.lineCount)
        assertEquals(0, pre.lineKeyHashes!!.size)
        assertEquals(0, pre.lineCharCounts!!.size)
        assertEquals(0, pre.lineIsHeading!!.size)
    }

    @Test
    fun releaseDropsPerLineArraysButKeepsRefsAndCount() {
        val p = payload().precomputeLineData()
        val pre = requireNotNull(p.precomputed)
        val refs = pre.refsByLineIndex
        val count = pre.lineCount

        pre.release()

        assertNull(pre.lineKeyHashes)
        assertNull(pre.lineCharCounts)
        assertNull(pre.lineIsHeading)
        // The inline-anchor pass holds refsByLineIndex for the whole build, and
        // the insert loop asserts lineCount against payload.lines.size.
        assertSame(refs, pre.refsByLineIndex)
        assertEquals(count, pre.lineCount)
        assertEquals(p.lines.size, pre.lineCount)
        // Release is idempotent.
        pre.release()
        assertNull(pre.lineKeyHashes)
    }

    @Test
    fun precomputedIsExcludedFromDataClassIdentity() {
        val a = payload()
        val b = payload()
        a.precomputeLineData()
        assertEquals(a, b, "precomputed must not participate in equals")
        assertEquals(a.hashCode(), b.hashCode(), "precomputed must not participate in hashCode")
        // copy() carries constructor properties only — a copied payload has no
        // precompute and would trip the insert loop's guard.
        assertNull(a.copy().precomputed)
    }

    @Test
    fun missingPrecomputeIsDetectable() {
        // The insert loop's guard: a payload that never went through
        // readBooksInParallel has no precompute at all.
        assertNull(payload().precomputed)
        val released = payload().precomputeLineData().precomputed!!
        released.release()
        assertFailsWith<NullPointerException> { released.lineKeyHashes!! }
    }

    @Test
    fun concurrentPrecomputeMatchesSerialPrecompute() = runBlocking {
        // The hoisted helpers (lineNaturalKeyHash / countVisibleChars /
        // detectTeamimAndNekudot) now run on up to FILE_PARALLELISM workers.
        val serial = payload().precomputeLineData().precomputed!!
        val parallel = coroutineScope {
            (0 until 64).map { async(Dispatchers.IO) { payload().precomputeLineData().precomputed!! } }.awaitAll()
        }
        for (pre in parallel) {
            assertEquals(serial.hasTeamim, pre.hasTeamim)
            assertEquals(serial.hasNekudot, pre.hasNekudot)
            assertEquals(serial.refsByLineIndex, pre.refsByLineIndex)
            assertContentEquals(serial.lineCharCounts, pre.lineCharCounts)
            assertContentEquals(serial.lineIsHeading, pre.lineIsHeading)
            serial.lineKeyHashes!!.forEachIndexed { idx, expected ->
                assertContentEquals(expected, pre.lineKeyHashes!![idx], "hash mismatch at line $idx")
            }
        }
    }
}
