package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Cadence + formatting of the generator's progress output.
 *
 * These gates replace ~18k INFO lines per build with a throttled progress line
 * and a summary, so their behaviour is pinned here against a fake clock rather
 * than against a 35-minute build.
 */
class GeneratorProgressTest {

    /** Fake clock: advances only when the test says so. */
    private class FakeClock(var nowMs: Long = 0L) {
        val reader: () -> Long = { nowMs }
        fun advance(ms: Long) { nowMs += ms }
    }

    // ─── formatters ────────────────────────────────────────────────────────

    @Test
    fun formatCountIsCompactAndLocaleIndependent() {
        assertEquals("0", formatCount(0))
        assertEquals("934", formatCount(934))
        assertEquals("9999", formatCount(9_999))
        assertEquals("10.0K", formatCount(10_000))
        assertEquals("372.9K", formatCount(372_904))
        assertEquals("999.9K", formatCount(999_999))
        assertEquals("1.0M", formatCount(1_000_000))
        assertEquals("5.2M", formatCount(5_234_567))
        assertEquals("2.2M", formatCount(2_250_736))
    }

    @Test
    fun formatDurationUsesMmSsBelowAnHourAndHMmSsAbove() {
        assertEquals("00:00", formatDuration(0))
        assertEquals("00:00", formatDuration(-5))
        assertEquals("00:09", formatDuration(9_400))
        assertEquals("06:00", formatDuration(360_000))
        assertEquals("27:10", formatDuration(1_630_000))
        assertEquals("59:59", formatDuration(3_599_000))
        assertEquals("1:00:00", formatDuration(3_600_000))
        assertEquals("2:05:03", formatDuration(7_503_000))
    }

    @Test
    fun formatPercentKeepsOneDecimalAndToleratesUnknownTotals() {
        assertEquals("82.2", formatPercent(1234, 1501))
        assertEquals("0.0", formatPercent(0, 1501))
        assertEquals("100.0", formatPercent(1501, 1501))
        assertEquals("?", formatPercent(3, 0))
        assertEquals("?", formatPercent(3, -1))
    }

    // ─── book progress cadence ─────────────────────────────────────────────

    @Test
    fun bookProgressEmitsEveryNBooksAndNotInBetween() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 100, everyMs = 60_000)
        reporter.start(totalBooks = 1501)

        val emitted = mutableListOf<String>()
        repeat(250) { reporter.onBookFinished(lines = 10)?.let(emitted::add) }

        // 100th and 200th book only — 248 of the 250 books stay silent.
        assertEquals(2, emitted.size)
        assertTrue(emitted[0].startsWith("books 100/1501 (6.6%)"), emitted[0])
        assertTrue(emitted[1].startsWith("books 200/1501 (13.3%)"), emitted[1])
    }

    @Test
    fun bookProgressAlsoEmitsOnTheTimeCadenceForSlowBooks() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 100, everyMs = 60_000)
        reporter.start(totalBooks = 1000)

        // 10 very slow books: never 100 books, but each crosses a minute.
        val emitted = mutableListOf<String>()
        repeat(10) {
            clock.advance(61_000)
            reporter.onBookFinished(lines = 1)?.let(emitted::add)
        }
        assertEquals(10, emitted.size)

        // The time budget resets on every emission: fast books stay silent again.
        val quiet = (1..50).mapNotNull { reporter.onBookFinished(lines = 1) }
        assertTrue(quiet.isEmpty(), "expected silence, got $quiet")
    }

    @Test
    fun bookProgressLineCarriesCountersElapsedAndEta() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 1000, everyMs = 0)
        reporter.start(totalBooks = 1501)
        repeat(1234) {
            clock.advance(1_320) // 1234 books ≈ 27:08
            reporter.onBookFinished(lines = 4_216, tocEntries = 244)
        }
        val line = reporter.progressLine()
        assertEquals(
            "books 1234/1501 (82.2%) · lines 5.2M · toc 301.0K · elapsed 27:08 · eta ~05:52",
            line,
        )
    }

    @Test
    fun bookProgressOmitsEtaWhenTotalIsUnknownOrComplete() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 1, everyMs = 0)

        reporter.start(totalBooks = 0)
        clock.advance(1_000)
        val unknown = assertNotNull(reporter.onBookFinished(lines = 5))
        assertEquals("books 1/0 (?%) · lines 5 · toc 0 · elapsed 00:01", unknown)

        reporter.start(totalBooks = 2)
        clock.advance(1_000)
        reporter.onBookFinished(lines = 5)
        clock.advance(1_000)
        val complete = assertNotNull(reporter.onBookFinished(lines = 5))
        assertEquals("books 2/2 (100.0%) · lines 10 · toc 0 · elapsed 00:02", complete)
    }

    @Test
    fun summaryLineCarriesExactCountersAndExtrasInOrder() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 100_000, everyMs = 0)
        reporter.start(totalBooks = 1501)
        reporter.onBookFinished(lines = 2_250_736, tocEntries = 372_904)
        clock.advance(211_000)

        assertEquals(
            "Otzaria book import (phase 1): books=1/1501 lines=2250736 tocEntries=372904 " +
                "directories=270 skipped=66 acronymBooks=715 hearotMerged=60 elapsed=03:31",
            reporter.summaryLine(
                "Otzaria book import (phase 1)",
                mapOf(
                    "directories" to 270L,
                    "skipped" to 66L,
                    "acronymBooks" to 715L,
                    "hearotMerged" to 60L,
                ),
            ),
        )
    }

    @Test
    fun startResetsCountersBetweenPhases() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 100_000, everyMs = 0)
        reporter.start(totalBooks = 10)
        reporter.onBookFinished(lines = 7, tocEntries = 3)
        clock.advance(5_000)

        reporter.start(totalBooks = 20)
        assertEquals(0, reporter.booksDone)
        assertEquals(0L, reporter.linesInserted)
        assertEquals(0L, reporter.tocEntries)
        assertEquals(0L, reporter.elapsedMs())
        assertEquals(20, reporter.totalBooks)
    }

    // ─── per-book line ticks ───────────────────────────────────────────────

    @Test
    fun lineTicksNeverFireAtIndexZero() {
        // The old `lineIndex % 1000 == 0` gate emitted `0/N (0%)` for every
        // single book — 1,496 of 3,239 ticks in the audited build.
        val clock = FakeClock()
        val small = LineTickGate(totalLines = 869, nowMs = clock.reader)
        val large = LineTickGate(totalLines = 190_760, nowMs = clock.reader)
        assertFalse(small.shouldTick(0))
        assertFalse(large.shouldTick(0))
    }

    @Test
    fun smallBooksNeverTick() {
        val clock = FakeClock()
        val gate = LineTickGate(totalLines = 5_000, nowMs = clock.reader)
        assertFalse(gate.enabled)
        val ticks = (0 until 5_000).count { gate.shouldTick(it) }
        assertEquals(0, ticks)
    }

    @Test
    fun largeBooksTickOnThousandsAtLeastTenPercentApart() {
        val clock = FakeClock()
        val gate = LineTickGate(totalLines = 50_000, nowMs = clock.reader)
        assertTrue(gate.enabled)
        val ticked = (0 until 50_000).filter { gate.shouldTick(it) }
        // 10% of 50k is 5k: ticks at 5000, 10000, … 45000 — nine, not fifty.
        assertEquals(listOf(5_000, 10_000, 15_000, 20_000, 25_000, 30_000, 35_000, 40_000, 45_000), ticked)
    }

    @Test
    fun justAboveTheThresholdTicksOnEveryThousand() {
        // 10% of 5,001 is below the 1,000 step, so the step is the floor.
        val clock = FakeClock()
        val gate = LineTickGate(totalLines = 5_001, nowMs = clock.reader)
        val ticked = (0 until 5_001).filter { gate.shouldTick(it) }
        assertEquals(listOf(1_000, 2_000, 3_000, 4_000, 5_000), ticked)
    }

    @Test
    fun aSlowBookStillTicksOnTheTimeCadence() {
        val clock = FakeClock()
        val gate = LineTickGate(totalLines = 100_000, nowMs = clock.reader, everyMs = 60_000)
        // 10% is 10k lines, but a minute passes every 1,000 lines.
        val ticked = (0 until 20_000).filter { index ->
            if (index % 1_000 == 0) clock.advance(61_000)
            gate.shouldTick(index)
        }
        assertEquals(19, ticked.size)
        assertEquals(1_000, ticked.first())
        assertEquals(19_000, ticked.last())
    }

    @Test
    fun tickLineKeepsTheHistoricalFormat() {
        val gate = LineTickGate(totalLines = 190_760, nowMs = { 0L })
        assertEquals(
            "Book 5848 'דרך החיים': 19000/190760 lines (9%)",
            gate.tickLine(5848L, "דרך החיים", 19_000),
        )
    }

    @Test
    fun aZeroLineBookIsSilentAndNeverDividesByZero() {
        val gate = LineTickGate(totalLines = 0, nowMs = { 0L })
        assertFalse(gate.enabled)
        assertFalse(gate.shouldTick(0))
        assertEquals("Book 1 'X': 0/0 lines (0%)", gate.tickLine(1L, "X", 0))
    }

    // ─── whole-corpus behaviour ────────────────────────────────────────────

    @Test
    fun aWholeCorpusOfBooksTicksOnlyForTheLargeOnes() {
        // Shape only, not the audited run's exact sizes: a long tail of small
        // books (the real median is 414 lines) plus a handful of large ones.
        // The measured cut on the real distribution of run 34024655297 —
        // 3,239 ticks down to 507 — is recorded in the task notes.
        val sizes = (1..1_400).map { 100 + (it * 7) % 4_500 } +
            listOf(190_760, 137_000, 91_000, 60_000, 25_000, 12_000, 8_000, 5_400, 5_001)

        val oldTicks = sizes.sumOf { n -> (n - 1) / 1_000 + 1 }
        val newTicks = sizes.sumOf { n ->
            val gate = LineTickGate(totalLines = n, nowMs = { 0L })
            (0 until n).count { gate.shouldTick(it) }
        }
        assertTrue(newTicks * 6 < oldTicks, "expected a >6x cut, got $oldTicks -> $newTicks")

        // Every book used to emit `0/N (0%)`; none does now.
        assertEquals(0, sizes.count { n -> LineTickGate(totalLines = n, nowMs = { 0L }).shouldTick(0) })

        // Books at or below the threshold are completely silent.
        assertEquals(
            0,
            sizes.filter { it <= LINE_TICK_MIN_BOOK_LINES }.sumOf { n ->
                val gate = LineTickGate(totalLines = n, nowMs = { 0L })
                (0 until n).count { gate.shouldTick(it) }
            },
        )
    }

    @Test
    fun fifteenHundredBooksProduceFifteenProgressLinesNotFifteenHundred() {
        val clock = FakeClock()
        val reporter = BookProgressReporter(nowMs = clock.reader, everyNBooks = 100, everyMs = 60_000)
        reporter.start(totalBooks = 1_501)
        // 1,501 books over the ~211 s the book loop took in run 34024655297.
        val emitted = (1..1_501).mapNotNull {
            clock.advance(140)
            reporter.onBookFinished(lines = 1_500, tocEntries = 249)
        }
        // The count cadence carries it: no book here takes a minute.
        assertEquals(15, emitted.size)
        assertEquals("books 100/1501 (6.6%)", emitted.first().substringBefore(" · "))
        assertEquals("books 1500/1501 (99.9%)", emitted.last().substringBefore(" · "))
        assertNull(reporter.onBookFinished(lines = 0))
    }
}
