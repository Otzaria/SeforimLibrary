package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LineKey
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Issue #1211: line ids must survive an heRef-only edit, and a build_state
 * written under the old heRef-based key must seed the new scheme in place.
 */
class LegacyLineKeyMigrationTest {

    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val content = listOf("פסוק א", "פסוק ב", "פסוק ג")

    /** Runs one Sefaria-shaped book through the allocator with the given heRefs. */
    private fun allocate(
        allocator: InMemoryIdAllocator,
        bookId: Long,
        lines: List<String>,
        heRefs: List<String?>,
    ): List<Long> {
        val occ = LineOccurrenceCounter()
        val legacyOcc = LineOccurrenceCounter()
        return lines.mapIndexed { idx, text ->
            val hash = IdAllocatorBindings.lineNaturalKeyHash(text)
            val legacyHash = LegacyLineKey.hash(text, heRefs[idx])
            allocator.lineId(
                bookId,
                hash,
                occ.next(bookId, hash),
                LegacyLineKey(legacyHash, legacyOcc.next(bookId, legacyHash)),
            )
        }
    }

    @Test
    fun `same content with a changed heRef keeps its id across builds`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")
        val build1 = InMemoryIdAllocator.load(null)
        val bookId = build1.bookId("Sefaria", "בראשית")
        val ids1 = allocate(build1, bookId, content, listOf("בראשית א׳:א׳", "בראשית א׳:ב׳", "בראשית א׳:ג׳"))
        build1.snapshotTo(statePath)

        // Commit 4e286b9's shape: a comma inserted into every heRef.
        val build2 = InMemoryIdAllocator.load(statePath)
        val ids2 = allocate(
            build2,
            build2.bookId("Sefaria", "בראשית"),
            content,
            listOf("בראשית, א׳:א׳", "בראשית, א׳:ב׳", "בראשית, א׳:ג׳"),
        )
        assertEquals(ids1, ids2, "an heRef edit must not renumber lines")
    }

    @Test
    fun `a legacy snapshot seeds the new scheme with identical ids`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")
        val heRefs = listOf<String?>("בראשית א׳:א׳", "בראשית א׳:ב׳", null)

        // Build 1 emulates a pre-#1211 generator: keys carry the heRef hash.
        val build1 = InMemoryIdAllocator.load(null)
        val bookId = build1.bookId("Sefaria", "בראשית")
        val legacyOcc = LineOccurrenceCounter()
        val legacyIds = content.mapIndexed { idx, text ->
            val h = LegacyLineKey.hash(text, heRefs[idx])
            build1.lineId(bookId, h, legacyOcc.next(bookId, h))
        }
        build1.snapshotTo(statePath)
        val seeded = BuildStateReader().read(statePath)
        assertEquals(3, seeded.lines.size)

        // Build 2 uses the new key and must reuse every id via the shim.
        val build2 = InMemoryIdAllocator.load(statePath)
        val newIds = allocate(build2, build2.bookId("Sefaria", "בראשית"), content, heRefs)
        assertEquals(legacyIds, newIds)
        assertEquals(2, build2.legacyLineKeysMigrated(), "the heRef-less line needed no migration")

        // The written snapshot is fully migrated: new keys only, one per id.
        val statePath2 = tmp.newFolder().toPath().resolve("build_state.db")
        build2.snapshotTo(statePath2)
        val migrated = BuildStateReader().read(statePath2)
        assertEquals(3, migrated.lines.size)
        assertEquals(newIds.size, migrated.lines.values.toSet().size, "no id may sit under two keys")
        content.forEachIndexed { idx, text ->
            val key = LineKey(bookId, IdAllocatorBindings.lineNaturalKeyHash(text), 0)
            assertEquals(newIds[idx], migrated.lines[key])
        }
        heRefs.filterNotNull().forEach { ref ->
            assertNull(
                migrated.lines[LineKey(bookId, LegacyLineKey.hash(content[0], ref), 0)],
                "legacy key $ref must be gone from the written snapshot",
            )
        }

        // Build 3 sees no legacy keys and still resolves the same ids.
        val build3 = InMemoryIdAllocator.load(statePath2)
        assertEquals(newIds, allocate(build3, build3.bookId("Sefaria", "בראשית"), content, heRefs))
        assertEquals(0, build3.legacyLineKeysMigrated())
    }

    @Test
    fun `duplicate content lines get distinct occurrence ids`() {
        val allocator = InMemoryIdAllocator.load(null)
        val bookId = allocator.bookId("Sefaria", "ספר")
        val ids = allocate(
            allocator,
            bookId,
            listOf("אמן", "אמן", "אמן"),
            listOf("ר׳ א", "ר׳ ב", "ר׳ ג"),
        )
        assertEquals(3, ids.toSet().size, "identical content must still get distinct ids")
    }

    @Test
    fun `duplicate content under legacy distinct refs migrates one id per line`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")
        val lines = listOf("אמן", "אמן")
        val heRefs = listOf<String?>("ר׳ א", "ר׳ ב")

        val build1 = InMemoryIdAllocator.load(null)
        val bookId = build1.bookId("Sefaria", "ספר")
        val legacyOcc = LineOccurrenceCounter()
        val legacyIds = lines.mapIndexed { idx, text ->
            val h = LegacyLineKey.hash(text, heRefs[idx])
            build1.lineId(bookId, h, legacyOcc.next(bookId, h))
        }
        assertNotEquals(legacyIds[0], legacyIds[1])
        build1.snapshotTo(statePath)

        val build2 = InMemoryIdAllocator.load(statePath)
        val newIds = allocate(build2, build2.bookId("Sefaria", "ספר"), lines, heRefs)
        assertEquals(legacyIds, newIds, "occurrence order must map legacy keys onto new ones 1:1")
        assertEquals(2, build2.legacyLineKeysMigrated())

        val statePath2 = tmp.newFolder().toPath().resolve("build_state.db")
        build2.snapshotTo(statePath2)
        val migrated = BuildStateReader().read(statePath2)
        assertEquals(2, migrated.lines.size)
        assertTrue(migrated.lines.values.containsAll(newIds))
    }
}
