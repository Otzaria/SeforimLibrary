package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** seedAllMetadata must not change a book's source (set from the manifest at import). */
class SeedAllMetadataSourceTest {
    @Test
    fun applyMetadata_keepsBookSource() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val natlId = repo.insertSource("National-LibraryToOtzaria")
        repo.insertSource("DictaToOtzaria")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        val bookId = repo.insertBook(
            Book(categoryId = catId, sourceId = natlId, title = "תבונה", heRef = "תבונה"),
        )

        val bulk = mapOf("תבונה" to BulkMetadata(pubDates = listOf(1900), pubPlaceHe = "ירושלים"))
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        val result = applyMetadata(repo, bindings, bulk, emptyMap(), Logger.withTag("test"))

        assertEquals(1, result.updated)
        assertEquals(natlId, repo.getBook(bookId)?.sourceId, "seedAllMetadata must not overwrite the book source")
    }

    /**
     * `unmatched=1116` named none of the 1,116, so a benign superset and a
     * title-normalisation regression looked identical in the log. The result now
     * carries the titles themselves.
     */
    @Test
    fun applyMetadata_namesTheRecordsThatMatchedNoBook() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        repo.insertBook(Book(categoryId = catId, sourceId = sourceId, title = "תבונה", heRef = "תבונה"))

        val bulk = linkedMapOf(
            "תבונה" to BulkMetadata(pubDates = listOf(1900), pubPlaceHe = "ירושלים"),
            "אבן הראשה" to BulkMetadata(pubDates = emptyList(), pubPlaceHe = null),
            "קול התור" to BulkMetadata(pubDates = emptyList(), pubPlaceHe = null),
        )
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)

        val result = applyMetadata(repo, bindings, bulk, emptyMap(), Logger.withTag("test"))

        assertEquals(1, result.updated)
        assertEquals(2, result.unmatched)
        assertEquals(listOf("אבן הראשה", "קול התור"), result.unmatchedTitles)
        assertEquals(result.unmatched, result.unmatchedTitles.size, "the count must be the list's size")
    }
}
