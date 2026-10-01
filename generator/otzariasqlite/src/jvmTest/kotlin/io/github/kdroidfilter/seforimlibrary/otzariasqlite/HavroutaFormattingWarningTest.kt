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
 * had wrapped thousands of lines. The link and bold figures below are the real ones of both
 * builds, the line and daf counts those of the v30 candidate.
 */
class HavroutaFormattingWarningTest {

    /** A book of [lines] lines whose [dafs] daf headings all have a counterpart in the Talmud. */
    private fun sound(links: Int, lines: Int, bold: Int, whole: Int, dafs: Int) =
        HavroutaPairStats(links, lines, bold, whole, HavroutaSections(HavroutaSectionKind.DAF, dafs, dafs, dafs))

    private fun warnings(tractate: String, stats: HavroutaPairStats) =
        havroutaFormattingWarnings("חברותא על $tractate", tractate, stats)

    @Test
    fun `bold spans the matcher cannot read are reported (זבחים, v30)`() {
        val w = warnings("זבחים", sound(391, 7783, 6781, 7, 238)).single()
        assertContains(w, "חברותא על זבחים: only 391 of 6781 lines with bold Talmud text were linked (5%")
    }

    @Test
    fun `a bold that covers whole lines is reported (חולין, v30)`() {
        val w = warnings("חולין", sound(8930, 11280, 10053, 2874, 281)).single()
        assertContains(w, "חברותא על חולין: 2874 of 10053 bold lines (28%")
    }

    @Test
    fun `sound books stay quiet`() {
        // v29: זבחים, חולין, and the lowest link share and highest whole-line share of the 37
        assertEquals(emptyList(), warnings("זבחים", sound(6400, 7783, 6631, 108, 238)))
        assertEquals(emptyList(), warnings("חולין", sound(9424, 11280, 9673, 356, 281)))
        assertEquals(emptyList(), warnings("תמורה", sound(2228, 3353, 2358, 86, 65)))
        assertEquals(emptyList(), warnings("מגילה", sound(2731, 3141, 2791, 231, 61)))
        // the fewest links per daf of the 37 (21; its whole-line count is made up)
        assertEquals(emptyList(), warnings("מעילה", sound(870, 1625, 921, 30, 41)))
        // too few bold lines for the shares, and links enough for its dafs
        assertEquals(emptyList(), warnings("קטנה", sound(60, 150, 99, 99, 2)))
        // a book with no lines has nothing to lose
        assertEquals(emptyList(), warnings("ריקה", HavroutaPairStats(0, 0, 0, 0, HavroutaSections(HavroutaSectionKind.DAF, 0, 0, 0))))
    }

    @Test
    fun `a book none of whose daf headings is read is reported, whatever its bold count`() {
        // חברותא על מגילה with its <h3>דף headings demoted to <h4>: every line was skipped
        // before it was counted, so the result was 0 links, 0 bold lines and no warning.
        val stats = HavroutaPairStats(0, 3141, 0, 0, HavroutaSections(HavroutaSectionKind.DAF, 0, 61, 0))
        val w = warnings("מגילה", stats).single()
        assertContains(w, "חברותא על מגילה: none of its 3141 lines is a <h3>דף …</h3> heading")
        assertContains(w, "0 links were created")
    }

    @Test
    fun `daf headings that no longer match the Talmud's are reported instead of the link share`() {
        val stats = HavroutaPairStats(1300, 3141, 2791, 231, HavroutaSections(HavroutaSectionKind.DAF, 61, 61, 30))
        val w = warnings("מגילה", stats).single()
        assertContains(w, "חברותא על מגילה: only 30 of the 61 dafs of מגילה have a heading of the same name in the book")
    }

    @Test
    fun `a Talmud book without daf headings is reported`() {
        val stats = HavroutaPairStats(0, 3141, 2791, 231, HavroutaSections(HavroutaSectionKind.DAF, 61, 0, 0))
        assertContains(warnings("מגילה", stats).single(), "מגילה has no <h2>דף …</h2> heading")
    }

    @Test
    fun `a book whose quotes lost their bold markup is reported by its links per daf`() {
        // Markup the counter does not know either: 0 bold lines, so no share to judge by.
        val w = warnings("מגילה", sound(0, 3141, 0, 0, 61)).single()
        assertContains(w, "חברותא על מגילה: only 0 links over the 61 dafs it shares with מגילה")
    }

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }
    }

    /**
     * The generator counts the lines itself, on a book whose quotes are written by [quote] under
     * the daf heading [heading]. Every annotation it would print in GitHub Actions is collected
     * in [annotations].
     */
    private fun run(
        heading: String = "<h3>דף ב.</h3>",
        annotations: MutableList<String> = mutableListOf(),
        quote: (String) -> String,
    ): Capture = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val bavli = repo.insertSource("Sefaria")
        val otzaria = repo.insertSource("Otzaria")
        val catId = repo.insertCategory(Category(0, null, "תלמוד", level = 0, order = 1))
        val words = listOf("אמר", "רב", "יהודה", "שמואל", "תנא", "דבי", "רבי", "ישמעאל", "מנא", "הני", "מילי")
        val quotes = (0 until 120).map { i -> (0 until 4).joinToString(" ") { words[(i * 7 + it * 3) % words.size] } + " $i" }
        val talmud = listOf("<h2>דף ב.</h2>") + quotes
        val havrouta = listOf(heading) + quotes.map { "${quote(it)}, כלומר שהוא מבאר את הדברים" }
        val tId = repo.insertBook(Book(id = 1, categoryId = catId, sourceId = bavli, title = "זבחים", heRef = "זבחים", totalLines = talmud.size))
        val hId = repo.insertBook(Book(id = 2, categoryId = catId, sourceId = otzaria, title = "חברותא על זבחים", heRef = "חברותא על זבחים", totalLines = havrouta.size))
        repo.insertLinesBatch(talmud.mapIndexed { i, c -> Line(id = 1L + i, bookId = tId, lineIndex = i, content = c) })
        repo.insertLinesBatch(havrouta.mapIndexed { i, c -> Line(id = 1000L + i, bookId = hId, lineIndex = i, content = c) })
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repo)
        ConnectionType.entries.forEach { bindings.upsertConnectionType(it.name) }
        val capture = Capture()
        generateHavroutaLinks(repo, bindings, Logger(StaticConfig(Severity.Verbose, listOf(capture)), "t")) {
            annotations += it
        }
        driver.close()
        capture
    }

    private val Capture.warnings get() = lines.filter { it.first == Severity.Warn }.map { it.second }

    @Test
    fun `the generator warns when the bold quotes are not in the Talmud, and annotates it`() {
        val annotations = mutableListOf<String>()
        val capture = run(annotations = annotations) { "<b>${it.reversed()}</b>" }
        assertContains(capture.warnings.single(), "חברותא על זבחים: only 0 of 120 lines with bold Talmud text were linked")
        assertEquals(capture.warnings, annotations, "every warning must reach the CI annotation too")
    }

    @Test
    fun `the generator warns when the daf headings are no longer read (h4 instead of h3)`() {
        val annotations = mutableListOf<String>()
        val capture = run(heading = "<h4>דף ב.</h4>", annotations = annotations) { "<b>$it</b>" }
        assertTrue(capture.lines.any { it.second == "  Created 0 links" }, capture.lines.toString())
        assertContains(capture.warnings.single(), "חברותא על זבחים: none of its 121 lines is a <h3>דף …</h3> heading")
        assertEquals(capture.warnings, annotations)
    }

    @Test
    fun `the generator warns when the daf headings are no longer the Talmud's`() {
        val capture = run(heading = "<h3>דף ב</h3>") { "<b>$it</b>" }
        assertContains(capture.warnings.single(), "חברותא על זבחים: only 0 of the 1 dafs of זבחים have a heading of the same name")
    }

    @Test
    fun `bold the matcher does not read counts as bold that was not linked`() {
        for (open in listOf("<strong>", "<b class=\"q\">")) {
            val close = if (open == "<strong>") "</strong>" else "</b>"
            val capture = run { "$open$it$close" }
            assertContains(
                capture.warnings.single(),
                "חברותא על זבחים: only 0 of 120 lines with bold Talmud text were linked",
                message = "$open: ${capture.lines}",
            )
        }
    }

    @Test
    fun `quotes marked in a way nothing counts are caught by the links per daf`() {
        val capture = run { "<span class=\"bold\">$it</span>" }
        assertContains(capture.warnings.single(), "חברותא על זבחים: only 0 links over the 1 dafs it shares with זבחים")
    }

    @Test
    fun `the annotation is a GitHub workflow command, escaped`() {
        assertEquals(
            "::warning title=Havrouta links::a%25b%0D%0Ac: d, e",
            githubWarningCommand("Havrouta links", "a%b\r\nc: d, e"),
        )
        assertEquals("::warning title=a%3A b%2C c::x", githubWarningCommand("a: b, c", "x"))
    }

    @Test
    fun `bold that holds another tag is read, so the v30 זבחים shape links in full`() {
        val capture = run { "<b><small>$it</small></b>" }
        assertTrue(capture.lines.any { it.second == "  Created 120 links" }, capture.lines.toString())
        assertTrue(capture.lines.none { it.first == Severity.Warn }, capture.lines.toString())
    }

    @Test
    fun `the generator stays quiet on a sound book`() {
        val capture = run { "<b>$it</b>" }
        assertTrue(capture.lines.any { it.second == "  Created 120 links" }, capture.lines.toString())
        assertTrue(capture.lines.none { it.first == Severity.Warn }, capture.lines.toString())
    }
}
