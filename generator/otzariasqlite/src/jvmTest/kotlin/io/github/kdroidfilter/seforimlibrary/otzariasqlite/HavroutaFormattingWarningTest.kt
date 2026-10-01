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
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * v30 lost 6009 of the 6400 Talmud links of חברותא על זבחים and 494 of חולין, with
 * nothing but an INFO "Created N links": a formatting tag left open in the book file
 * had wrapped thousands of lines. The figures below are the real ones of both builds.
 */
class HavroutaFormattingWarningTest {

    @Test
    fun `bold spans the matcher cannot read are reported (זבחים, v30)`() {
        val warnings = havroutaFormattingWarnings("חברותא על זבחים", HavroutaPairStats(391, 6781, 7))
        val w = warnings.single()
        assertContains(w, "חברותא על זבחים: only 391 of 6781 lines with bold Talmud text were linked (5%")
    }

    @Test
    fun `a bold that covers whole lines is reported (חולין, v30)`() {
        val warnings = havroutaFormattingWarnings("חברותא על חולין", HavroutaPairStats(8930, 10053, 2874))
        val w = warnings.single()
        assertContains(w, "חברותא על חולין: 2874 of 10053 bold lines (28%")
    }

    @Test
    fun `sound books stay quiet`() {
        // v29: זבחים, חולין, and the lowest link share and highest whole-line share of the 37
        assertEquals(emptyList(), havroutaFormattingWarnings("חברותא על זבחים", HavroutaPairStats(6400, 6631, 108)))
        assertEquals(emptyList(), havroutaFormattingWarnings("חברותא על חולין", HavroutaPairStats(9424, 9673, 356)))
        assertEquals(emptyList(), havroutaFormattingWarnings("חברותא על תמורה", HavroutaPairStats(2228, 2358, 86)))
        assertEquals(emptyList(), havroutaFormattingWarnings("חברותא על מגילה", HavroutaPairStats(2731, 2791, 231)))
        // too small a book to judge
        assertEquals(emptyList(), havroutaFormattingWarnings("חברותא על קטנה", HavroutaPairStats(0, 99, 99)))
    }

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }
    }

    /** The generator counts the lines itself: a book file whose Talmud quotes are all `<b><small>`. */
    private fun run(quote: (String) -> String): Capture = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bavli = repo.insertSource("Sefaria")
        val otzaria = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        val words = listOf("אמר", "רב", "יהודה", "שמואל", "תנא", "דבי", "רבי", "ישמעאל", "מנא", "הני", "מילי")
        val quotes = (0 until 120).map { i -> (0 until 4).joinToString(" ") { words[(i * 7 + it * 3) % words.size] } + " $i" }
        val talmud = listOf("<h2>דף ב.</h2>") + quotes
        val havrouta = listOf("<h3>דף ב.</h3>") + quotes.map { "${quote(it)}, כלומר שהוא מבאר את הדברים" }
        val tId = repo.insertBook(Book(id = 1, categoryId = catId, sourceId = bavli, title = "זבחים", heRef = "זבחים", totalLines = talmud.size))
        val hId = repo.insertBook(Book(id = 2, categoryId = catId, sourceId = otzaria, title = "חברותא על זבחים", heRef = "חברותא על זבחים", totalLines = havrouta.size))
        repo.insertLinesBatch(talmud.mapIndexed { i, c -> Line(id = 1L + i, bookId = tId, lineIndex = i, content = c) })
        repo.insertLinesBatch(havrouta.mapIndexed { i, c -> Line(id = 1000L + i, bookId = hId, lineIndex = i, content = c) })
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.entries.forEach { bindings.upsertConnectionType(it.name) }
        val capture = Capture()
        generateHavroutaLinks(repo, bindings, Logger(StaticConfig(Severity.Verbose, listOf(capture)), "t"))
        driver.close()
        capture
    }

    @Test
    fun `the generator warns when the book's bold is unreadable`() {
        val capture = run { "<b><small>$it</small></b>" }
        val warn = capture.lines.filter { it.first == Severity.Warn }.map { it.second }
        assertContains(warn.single(), "חברותא על זבחים: only 0 of 120 lines with bold Talmud text were linked")
    }

    @Test
    fun `the generator stays quiet on a sound book`() {
        val capture = run { "<b>$it</b>" }
        assertTrue(capture.lines.any { it.second == "  Created 120 links" }, capture.lines.toString())
        assertTrue(capture.lines.none { it.first == Severity.Warn }, capture.lines.toString())
    }
}
