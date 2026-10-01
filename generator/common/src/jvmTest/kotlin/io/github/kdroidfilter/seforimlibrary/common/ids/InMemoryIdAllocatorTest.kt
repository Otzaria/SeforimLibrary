package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InMemoryIdAllocatorTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun sha1(s: String): ByteArray =
        MessageDigest.getInstance("SHA-1").digest(s.toByteArray())

    @Test
    fun `fresh allocator hands out sequential ids per table starting at 1`() {
        val allocator = InMemoryIdAllocator.load(path = null)

        assertEquals(1L, allocator.sourceId("Sefaria"))
        assertEquals(2L, allocator.sourceId("Otzaria"))
        assertEquals(1L, allocator.authorId("Rashi")) // independent counter
        assertEquals(1L, allocator.bookId("Sefaria", "בראשית"))
        assertEquals(2L, allocator.bookId("Sefaria", "שמות"))
    }

    @Test
    fun `ensureCounterAtLeast raises the next id but never lowers it`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        allocator.ensureCounterAtLeast(IdTable.LINK, 100L)
        assertEquals(100L, allocator.linkId(1L, 2L, 1L))   // fresh id starts at the floor
        allocator.ensureCounterAtLeast(IdTable.LINK, 50L)  // lower floor is a no-op
        assertEquals(101L, allocator.linkId(1L, 3L, 1L))
        assertEquals(100L, allocator.linkId(1L, 2L, 1L))   // existing key untouched
    }

    @Test
    fun `reallocateCategoryId drops the squatted id and hands out a fresh one`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val path = "תלמוד בבלי/אחרונים/אור הישר/סדר נזיקין"
        val stale = allocator.categoryId(path)

        // renameCategories has since taken every rowid up to 1477 (including `stale`)
        // with implicit-rowid inserts; the Otzaria stage raises the floor past the DB.
        allocator.ensureCounterAtLeast(IdTable.CATEGORY, 1478L)
        val fresh = allocator.reallocateCategoryId(path)

        assertNotEquals(stale, fresh)
        assertEquals(1478L, fresh)
        assertEquals(fresh, allocator.categoryId(path)) // the new id sticks
    }

    @Test
    fun `pointCategoryKey moves a key onto an id another key holds and persists it`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val path = "מחשבת ישראל/אחרונים/רמחל"
        val leafKey = BOOK_MOVE_LEAF_KEY_PREFIX + path
        assertNull(allocator.peekCategoryId(path))
        val stale = allocator.categoryId(path)
        val leaf = allocator.categoryId(leafKey)
        assertNull(allocator.peekCategoryId("אחר"), "peek does not allocate")

        assertTrue(allocator.pointCategoryKey(path, leaf))
        assertEquals(leaf, allocator.categoryId(path))
        assertEquals(mapOf(path to leaf, leafKey to leaf), allocator.categoryKeys())
        // The id the key left is not handed out again.
        assertNotEquals(stale, allocator.categoryId("חדש"))

        val file = tmp.root.toPath().resolve("point.buildstate")
        allocator.snapshotTo(file)
        val reloaded = InMemoryIdAllocator.load(file)
        assertEquals(leaf, reloaded.peekCategoryId(path))
        assertEquals(leaf, reloaded.peekCategoryId(leafKey))
    }

    @Test
    fun `same natural key returns same id across calls`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val first = allocator.bookId("Sefaria", "בראשית")
        val second = allocator.bookId("Sefaria", "בראשית")
        assertEquals(first, second)
    }

    @Test
    fun `ids survive a snapshot+reload roundtrip`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")

        val first = InMemoryIdAllocator.load(path = null)
        val sefariaId = first.sourceId("Sefaria")
        val genesisId = first.bookId("Sefaria", "בראשית")
        val exodusId = first.bookId("Sefaria", "שמות")
        val hash1 = sha1("hello world").copyOf(20)
        val hash2 = sha1("second line").copyOf(20)
        val lineGenesis1 = first.lineId(genesisId, hash1, 0)
        val lineExodus1 = first.lineId(exodusId, hash2, 0)
        val tocId = first.tocEntryId(genesisId, "1/2")
        val typeId = first.connectionTypeId("commentary")
        val linkId = first.linkId(lineGenesis1, lineExodus1, typeId)
        first.snapshotTo(statePath, mapOf("build_version" to "1"))

        val second = InMemoryIdAllocator.load(statePath)
        assertEquals(sefariaId, second.sourceId("Sefaria"))
        assertEquals(genesisId, second.bookId("Sefaria", "בראשית"))
        assertEquals(exodusId, second.bookId("Sefaria", "שמות"))
        assertEquals(lineGenesis1, second.lineId(genesisId, hash1, 0))
        assertEquals(lineExodus1, second.lineId(exodusId, hash2, 0))
        assertEquals(tocId, second.tocEntryId(genesisId, "1/2"))
        assertEquals(typeId, second.connectionTypeId("commentary"))
        assertEquals(linkId, second.linkId(lineGenesis1, lineExodus1, typeId))

        // A new book gets the next free id, not a colliding one.
        val newBookId = second.bookId("Sefaria", "ויקרא")
        assertNotEquals(genesisId, newBookId)
        assertNotEquals(exodusId, newBookId)
        assertTrue(newBookId > exodusId)
    }

    @Test
    fun `book alias preserves id under new natural key`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val oldId = allocator.bookId("Sefaria", "Old Title")

        val oldKey = BookKey("Sefaria", "Old Title")
        val newKey = BookKey("Sefaria", "New Title")
        allocator.registerBookAlias(oldKey, newKey, atVersion = 5)

        // Lookup with new title returns the original id.
        assertEquals(oldId, allocator.bookId("Sefaria", "New Title"))
        // Old title still resolves too (alias indirection).
        assertEquals(oldId, allocator.bookId("Sefaria", "Old Title"))
    }

    @Test
    fun `concurrent lookups of same key never split into two ids`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val pool = Executors.newFixedThreadPool(16)
        val ids = java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
        val tasks = (1..10_000).map {
            Runnable { ids.add(allocator.bookId("Sefaria", "racy")) }
        }
        tasks.forEach { pool.submit(it) }
        pool.shutdown()
        pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)
        assertEquals(1, ids.size, "racy key was assigned multiple ids: $ids")
    }

    @Test
    fun `counters skip used ids after partial snapshot`() {
        // Simulate a snapshot where someone manually inserted a high id externally:
        // counter says 5 but we already have id 100 mapped — allocator must avoid 100.
        val state = io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot(
            schemaVersion = io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSchema.CURRENT_VERSION,
            meta = emptyMap(),
            counters = mapOf(IdTable.BOOK to 5L),
            lookups = emptyMap(),
            books = mapOf(BookKey("Sefaria", "X") to 100L),
            lines = emptyMap(),
            tocEntries = emptyMap(),
            altTocStructures = emptyMap(),
            altTocEntries = emptyMap(),
            links = emptyMap(),
            bookAliases = emptyList(),
            sourceHashes = emptyMap(),
        )
        val allocator = InMemoryIdAllocator.fromSnapshot(state)
        val freshId = allocator.bookId("Sefaria", "Y")
        assertTrue(freshId > 100L, "fresh id $freshId must not collide with existing 100")
    }

    @Test
    fun `peekBookId returns null for unknown key`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        allocator.bookId("Sefaria", "Known")
        assertNull(allocator.peekBookId("Sefaria", "Unknown"))
    }

    @Test
    fun `stats track reused vs fresh`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        allocator.bookId("Sefaria", "A") // fresh
        allocator.bookId("Sefaria", "B") // fresh
        allocator.bookId("Sefaria", "A") // reused
        val stats = allocator.stats().perTable.getValue(IdTable.BOOK)
        assertEquals(2L, stats.freshlyAllocated)
        assertEquals(1L, stats.reused)
        assertEquals(3L, stats.total)
    }
}
