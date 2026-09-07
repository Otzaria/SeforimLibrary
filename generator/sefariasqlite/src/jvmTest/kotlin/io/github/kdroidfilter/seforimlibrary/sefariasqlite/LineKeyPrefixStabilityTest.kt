package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.common.ids.LineOccurrenceCounter
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/**
 * Issue #1211, second half: the importer injects "(א) ", "(ב) "… into
 * `line.content`, so hashing the stored text would renumber a whole chapter
 * whenever one verse is inserted at its top. The key hashes the raw segment.
 */
class LineKeyPrefixStabilityTest {

    /** "(א) " — the generated prefix the reader records in cleanShiftByLineIndex. */
    private val prefixLen = 4

    private fun chapter(verses: List<String>, withRefs: Boolean = true): BookPayload {
        val lines = verses.mapIndexed { idx, v -> "(${'א' + idx}) $v" }
        return BookPayload(
            heTitle = "ספר בדיקה", enTitle = "Test Book", categoriesHe = listOf("תנך"),
            lines = lines,
            refEntries = if (!withRefs) emptyList() else lines.indices.map {
                RefEntry(ref = "Test 1:${it + 1}", heRef = "בדיקה א׳:${'א' + it}", path = "p", lineIndex = it + 1)
            },
            headings = emptyList(), authors = emptyList(),
            description = null, heShortDesc = null, pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = lines.indices.associateWith { prefixLen },
        ).precomputeLineData()
    }

    /** Allocates the book's line ids exactly the way the insert loop does. */
    private fun allocate(allocator: InMemoryIdAllocator, bookId: Long, payload: BookPayload): List<Long> {
        val pre = requireNotNull(payload.precomputed)
        val occ = LineOccurrenceCounter()
        val legacyOcc = LineOccurrenceCounter()
        return payload.lines.indices.map { idx ->
            val hash = pre.lineKeyHashes!![idx]
            val legacyHash = pre.legacyLineKeyHashes!![idx]
            allocator.lineId(
                bookId, hash, occ.next(bookId, hash),
                LegacyLineKey(legacyHash, legacyOcc.next(bookId, legacyHash)),
            )
        }
    }

    @Test
    fun `the natural key ignores the generated prefix`() {
        val pre = requireNotNull(chapter(listOf("פסוק אלף")).precomputed)
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash("פסוק אלף"),
            pre.lineKeyHashes!![0],
            "the key must hash the raw segment, not \"(א) פסוק אלף\"",
        )
        // The legacy array is the pre-#1211 shape and still sees the rendered line.
        assertContentEquals(
            LegacyLineKey.hash("(א) פסוק אלף", "בדיקה א׳:א"),
            pre.legacyLineKeyHashes!![0],
        )
    }

    @Test
    fun `inserting a verse at the top keeps the ids of every verse after it`() {
        val allocator = InMemoryIdAllocator.load(null)
        val bookId = allocator.bookId("Sefaria", "ספר בדיקה")
        val before = allocate(allocator, bookId, chapter(listOf("פסוק אלף", "פסוק בית", "פסוק גימל")))

        // Same three verses, now prefixed (ב)(ג)(ד) because one was inserted.
        val after = allocate(
            allocator,
            bookId,
            chapter(listOf("פסוק חדש", "פסוק אלף", "פסוק בית", "פסוק גימל")),
        )
        assertEquals(before, after.drop(1), "a head-insert must not renumber the rest of the chapter")
        assertEquals(4, after.toSet().size)
        assertEquals(0, allocator.legacyLineKeysMigrated(), "no seed, so nothing to migrate")
    }

    @Test
    fun `hashing the rendered line would have renumbered them`() {
        // Pins WHY the shift is stripped: the rendered text really does differ.
        val v1 = chapter(listOf("פסוק אלף", "פסוק בית")).lines[1]
        val v2 = chapter(listOf("פסוק חדש", "פסוק אלף", "פסוק בית")).lines[2]
        assertNotEquals(v1, v2)
        assertEquals("פסוק בית", v1.substring(prefixLen))
        assertEquals("פסוק בית", v2.substring(prefixLen))
    }

    @Test
    fun `a legacy prefixed line without an heRef still migrates`() {
        // Build 1: pre-#1211 keys — no heRef, so the rendered (prefixed) content.
        val build1 = InMemoryIdAllocator.load(null)
        val bookId = build1.bookId("Sefaria", "ספר בדיקה")
        val old = chapter(listOf("פסוק אלף", "פסוק בית"), withRefs = false)
        val legacyOcc = LineOccurrenceCounter()
        val legacyIds = old.lines.indices.map { idx ->
            val h = requireNotNull(old.precomputed).legacyLineKeyHashes!![idx]
            build1.lineId(bookId, h, legacyOcc.next(bookId, h))
        }

        // Build 2 keys on the raw segment; the shim must carry both ids over.
        val statePath = Files.createTempDirectory("legacy-prefix").resolve("build_state.db")
        build1.snapshotTo(statePath)
        val build2 = InMemoryIdAllocator.load(statePath)
        val newIds = allocate(build2, build2.bookId("Sefaria", "ספר בדיקה"), chapter(listOf("פסוק אלף", "פסוק בית"), withRefs = false))
        assertEquals(legacyIds, newIds)
        assertEquals(2, build2.legacyLineKeysMigrated())
        assertEquals(0, build2.legacyLineKeysMissed())
    }
}
