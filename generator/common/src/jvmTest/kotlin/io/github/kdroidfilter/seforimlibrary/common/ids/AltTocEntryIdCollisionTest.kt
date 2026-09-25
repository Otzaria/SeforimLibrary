package io.github.kdroidfilter.seforimlibrary.common.ids

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFails

/**
 * Both ways an `alt_toc_entry` id can be claimed twice must abort the build.
 * There is no dedup, no retry and no "take the next free id" path here: after the
 * three writers were wired, every row of the table is allocator-issued, so a
 * foreign occupant is a defect, not a state to heal.
 */
class AltTocEntryIdCollisionTest {

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private fun entry(structureId: Long, text: String) =
        AltTocEntry(structureId = structureId, parentId = null, text = text, level = 0, lineId = null)

    @Test
    fun `reusing a natural key inside one build names the colliding path`() = runBlocking {
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), newRepo())

        bindings.insertAltTocEntryStable(entry(7L, "first"), ancestorPath = "2/5")
        val failure = assertFails {
            runBlocking { bindings.insertAltTocEntryStable(entry(7L, "second"), ancestorPath = "2/5") }
        }
        assertContains(failure.message!!, "structure=7")
        assertContains(failure.message!!, "path=2/5")
    }

    @Test
    fun `a foreign row on the allocated id aborts instead of being ignored`() = runBlocking {
        val repo = newRepo()
        val allocator = InMemoryIdAllocator.load(path = null)
        val bindings = IdAllocatorBindings(allocator, repo)

        // The key is memoised, so this is the very id the binding will try to use.
        val reserved = allocator.altTocEntryId(structureId = 7L, ancestorPath = "1")
        repo.insertAltTocEntry(entry(99L, "squatter").copy(id = reserved))

        // INSERT OR IGNORE would swallow this and hand the builder an id whose row
        // belongs to structure 99 — a parentId, a line_alt_toc target, all wrong.
        assertFails { runBlocking { bindings.insertAltTocEntryStable(entry(7L, "ours"), ancestorPath = "1") } }

        val stored = repo.getAltTocEntry(reserved)
        assertEquals(99L, stored!!.structureId)
    }
}
