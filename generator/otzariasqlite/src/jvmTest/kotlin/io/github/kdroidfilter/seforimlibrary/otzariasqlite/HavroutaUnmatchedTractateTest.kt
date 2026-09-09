package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `Found 38 Havrouta books` followed by 37 `Processing:` lines was the only
 * trace that one book had been dropped — no name, no reason, no closing count.
 * The gap is now stated outright and the drop carries its cause.
 */
class HavroutaUnmatchedTractateTest {

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }
    }

    private fun run(havroutaTitles: List<String>, talmudTitles: List<String>): Capture = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bavli = repo.insertSource("Sefaria")
        val otzaria = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        // The tractate filter keys on sourceId == 1, which is the first source row.
        assertEquals(1L, bavli, "the Talmud side is selected by sourceId == 1")

        talmudTitles.forEach {
            repo.insertBook(Book(categoryId = catId, sourceId = bavli, title = it, heRef = it))
        }
        havroutaTitles.forEach {
            repo.insertBook(Book(categoryId = catId, sourceId = otzaria, title = it, heRef = it))
        }

        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.entries.forEach { bindings.upsertConnectionType(it.name) }

        val capture = Capture()
        generateHavroutaLinks(repo, bindings, Logger(StaticConfig(Severity.Verbose, listOf(capture)), "t"))
        capture
    }

    @Test
    fun `a Havrouta book with no matching tractate is named, with its reason, and counted`() {
        val capture = run(
            havroutaTitles = listOf("חברותא על ברכות", "חברותא על מסכת שאיננה"),
            talmudTitles = listOf("ברכות"),
        )
        val warnings = capture.lines.filter { it.first == Severity.Warn }.map { it.second }

        val perBook = warnings.single { it.startsWith("No Talmud match for:") }
        assertContains(perBook, "חברותא על מסכת שאיננה")
        assertContains(perBook, "no book titled 'מסכת שאיננה' among the Bavli tractates")
        assertContains(perBook, "no links created for it")

        assertEquals(
            "Havrouta-Talmud: 1/2 books processed, 1 skipped with no matching Talmud tractate: " +
                "חברותא על מסכת שאיננה",
            warnings.single { it.startsWith("Havrouta-Talmud:") },
            "the found-vs-processed gap must be closed explicitly, not left to be inferred",
        )
    }

    @Test
    fun `a fully matched run closes the count at INFO with nothing to warn about`() {
        val capture = run(
            havroutaTitles = listOf("חברותא על ברכות"),
            talmudTitles = listOf("ברכות"),
        )
        assertEquals(
            listOf("Havrouta-Talmud: 1/1 books processed"),
            capture.lines.filter { it.second.startsWith("Havrouta-Talmud:") }.map { it.second },
        )
        assertTrue(
            capture.lines.none { it.first == Severity.Warn },
            "nothing to warn about: ${capture.lines.filter { it.first == Severity.Warn }}",
        )
    }
}
