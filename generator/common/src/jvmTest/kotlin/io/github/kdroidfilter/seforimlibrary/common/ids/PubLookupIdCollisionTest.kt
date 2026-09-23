package io.github.kdroidfilter.seforimlibrary.common.ids

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

/**
 * Regression test for seedAllMetadata driven by an allocator that does not describe
 * the DB (absent or foreign build_state): `insertPubDateWithId` is INSERT OR IGNORE,
 * so a book used to be linked to a stranger's date — or to a row that never existed.
 */
class PubLookupIdCollisionTest {

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    @Test
    fun `a foreign row on the allocated id is never handed out as our date`() = runBlocking {
        val repo = newRepo()
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        // An empty allocator restarts pub_date ids at 1, where this DB already holds 1900.
        repo.insertPubDateWithId(1L, "1900")

        assertFailsWith<IllegalStateException> { bindings.upsertPubDate("2000") }
        assertEquals(null, repo.getPubDateByDate("2000"))
        assertEquals(1L, assertNotNull(repo.getPubDateByDate("1900")).id)
    }

    @Test
    fun `a date already stored under another id fails instead of dangling`() = runBlocking {
        val repo = newRepo()
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        repo.insertPubDateWithId(7L, "1900")

        // UNIQUE(date) swallows the INSERT OR IGNORE, so id 1 holds no row at all.
        assertFailsWith<IllegalStateException> { bindings.upsertPubDate("1900") }
        assertEquals(7L, assertNotNull(repo.getPubDateByDate("1900")).id)
    }

    @Test
    fun `a foreign row on the allocated id is never handed out as our place`() = runBlocking {
        val repo = newRepo()
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        repo.insertPubPlaceWithId(1L, "ונציה")

        assertFailsWith<IllegalStateException> { bindings.upsertPubPlace("וילנא") }
        assertEquals(null, repo.getPubPlaceByName("וילנא"))
    }

    @Test
    fun `an allocator that matches the DB keeps the idempotent contract`() = runBlocking {
        val repo = newRepo()
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        val first = bindings.upsertPubDate("1922")
        val second = bindings.upsertPubDate("1922")
        val place = bindings.upsertPubPlace("וילנא")

        assertEquals(first, second)
        assertEquals(first, assertNotNull(repo.getPubDateByDate("1922")).id)
        assertEquals(place, assertNotNull(repo.getPubPlaceByName("וילנא")).id)
    }
}
