package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import kotlin.time.TimeSource

/**
 * Cadence gates and formatters for the generator's progress output.
 *
 * The book import used to emit ~15 INFO lines per book (a 6-line lifecycle
 * block, a duplicated header, a `Books progress:` line and a line tick that
 * always fired at index 0). For ~1,500 books that is ~18k lines of the
 * "Generate Seforim Database" step. Everything per-book now goes to DEBUG
 * (the CLI entry points pin kermit at [co.touchlab.kermit.Severity.Info], so
 * DEBUG is genuinely off in CI) and the operator-facing INFO signal is a
 * throttled progress line produced here.
 *
 * Both gates are pure apart from the injected clock, so their cadence is unit
 * tested against a fake clock instead of against a 35-minute build.
 */

/** Emit a book progress line at most every N finished books… */
internal const val BOOK_PROGRESS_EVERY_N_BOOKS: Int = 100

/** …or every this many milliseconds, whichever comes first. */
internal const val BOOK_PROGRESS_EVERY_MS: Long = 60_000L

/** Books at or below this many lines never emit a line tick. */
internal const val LINE_TICK_MIN_BOOK_LINES: Int = 5_000

/** Line ticks are only ever considered on multiples of this index. */
internal const val LINE_TICK_STEP: Int = 1_000

/** A tick needs at least this much of the book to have passed since the last one… */
internal const val LINE_TICK_MIN_PERCENT: Int = 10

/** …unless this long has passed, which keeps a slow huge book observable. */
internal const val LINE_TICK_EVERY_MS: Long = 60_000L

private val processStart = TimeSource.Monotonic.markNow()

/** Monotonic milliseconds since class load; injected so tests can fake it. */
internal val monotonicMillis: () -> Long = { processStart.elapsedNow().inWholeMilliseconds }

private fun oneDecimal(value: Long, unit: Long, suffix: String): String {
    val whole = value / unit
    val tenths = (value % unit) * 10 / unit
    return "$whole.$tenths$suffix"
}

/** `934`, `372.9K`, `5.2M` — locale independent, integer arithmetic only. */
internal fun formatCount(n: Long): String = when {
    n >= 1_000_000L -> oneDecimal(n, 1_000_000L, "M")
    n >= 10_000L -> oneDecimal(n, 1_000L, "K")
    else -> n.toString()
}

private fun pad2(v: Long): String = if (v < 10) "0$v" else "$v"

/** `27:10`, `1:05:03` — mm:ss below an hour, h:mm:ss above it. */
internal fun formatDuration(ms: Long): String {
    val totalSeconds = (if (ms < 0) 0 else ms) / 1000
    val hours = totalSeconds / 3600
    val minutes = (totalSeconds % 3600) / 60
    val seconds = totalSeconds % 60
    return if (hours > 0) "$hours:${pad2(minutes)}:${pad2(seconds)}" else "${pad2(minutes)}:${pad2(seconds)}"
}

/** `82.2` for 1234/1501; `?` when the total is unknown. */
internal fun formatPercent(done: Int, total: Int): String {
    if (total <= 0) return "?"
    val tenths = done.toLong() * 1000L / total
    return "${tenths / 10}.${tenths % 10}"
}

/**
 * Throttles and formats the book-loop progress line:
 *
 *     books 1234/1501 (82.2%) · lines 5.2M · toc 301.4K · elapsed 27:10 · eta ~6:00
 *
 * A line is produced every [everyNBooks] finished books or every [everyMs],
 * whichever comes first. This replaces the per-book `Books progress: n/N (p%)`
 * line, which fired 1,501 times in a single build.
 */
internal class BookProgressReporter(
    private val nowMs: () -> Long = monotonicMillis,
    private val everyNBooks: Int = BOOK_PROGRESS_EVERY_N_BOOKS,
    private val everyMs: Long = BOOK_PROGRESS_EVERY_MS,
) {
    /** Planned book count; set once the source tree has been walked. */
    var totalBooks: Int = 0

    var booksDone: Int = 0
        private set
    var linesInserted: Long = 0L
        private set
    var tocEntries: Long = 0L
        private set

    private var startedAtMs: Long = 0L
    private var lastEmitMs: Long = 0L
    private var lastEmitBooks: Int = 0
    private var started: Boolean = false

    /** Starts (or restarts) the elapsed/ETA clock. Idempotent per phase. */
    fun start(totalBooks: Int = this.totalBooks) {
        this.totalBooks = totalBooks
        val now = nowMs()
        startedAtMs = now
        lastEmitMs = now
        lastEmitBooks = 0
        booksDone = 0
        linesInserted = 0L
        tocEntries = 0L
        started = true
    }

    fun elapsedMs(): Long = if (started) nowMs() - startedAtMs else 0L

    /**
     * Records one finished book (skipped books count too, with zero content)
     * and returns the INFO line to log, or `null` when this book is not due.
     */
    fun onBookFinished(lines: Int = 0, tocEntries: Int = 0): String? {
        if (!started) start()
        booksDone += 1
        linesInserted += lines.toLong()
        this.tocEntries += tocEntries.toLong()

        val now = nowMs()
        val dueByCount = everyNBooks > 0 && booksDone - lastEmitBooks >= everyNBooks
        val dueByTime = everyMs > 0 && now - lastEmitMs >= everyMs
        if (!dueByCount && !dueByTime) return null

        lastEmitBooks = booksDone
        lastEmitMs = now
        return progressLine(now)
    }

    /** The same line, unconditionally — used to close out a phase. */
    fun progressLine(now: Long = nowMs()): String {
        val elapsed = now - startedAtMs
        return buildString {
            append("books ").append(booksDone).append('/').append(totalBooks)
            append(" (").append(formatPercent(booksDone, totalBooks)).append("%)")
            append(" · lines ").append(formatCount(linesInserted))
            append(" · toc ").append(formatCount(tocEntries))
            append(" · elapsed ").append(formatDuration(elapsed))
            val remaining = totalBooks - booksDone
            if (totalBooks > 0 && booksDone > 0 && remaining > 0) {
                val eta = elapsed / booksDone * remaining
                append(" · eta ~").append(formatDuration(eta))
            }
        }
    }

    /**
     * End-of-phase summary. [extras] are appended as `key=value` in iteration
     * order, so pass a [LinkedHashMap] (`mapOf(...)` already is one).
     */
    fun summaryLine(label: String, extras: Map<String, Long> = emptyMap()): String = buildString {
        append(label)
        append(": books=").append(booksDone).append('/').append(totalBooks)
        append(" lines=").append(linesInserted)
        append(" tocEntries=").append(tocEntries)
        for ((key, value) in extras) {
            append(' ').append(key).append('=').append(value)
        }
        append(" elapsed=").append(formatDuration(elapsedMs()))
    }
}

/**
 * Per-book line-tick cadence. Construct one per book, then ask it about every
 * line index.
 *
 * Old behaviour: `if (lineIndex % 1000 == 0)` — which fires at index 0, so
 * every book (median 414 lines) emitted a useless `0/N (0%)`; 1,496 of the
 * 3,239 ticks in the audited build carried no information at all.
 *
 * New behaviour: nothing at all for books of [minBookLines] lines or fewer,
 * and for larger books a tick only on a multiple of [step] that is also at
 * least [minPercent] of the book past the previous tick — or [everyMs] later,
 * so a genuinely slow book still reports.
 */
internal class LineTickGate(
    private val totalLines: Int,
    private val nowMs: () -> Long = monotonicMillis,
    minBookLines: Int = LINE_TICK_MIN_BOOK_LINES,
    private val step: Int = LINE_TICK_STEP,
    minPercent: Int = LINE_TICK_MIN_PERCENT,
    private val everyMs: Long = LINE_TICK_EVERY_MS,
) {
    /** Books at or below the threshold never tick — checked before any clock read. */
    val enabled: Boolean = totalLines > minBookLines

    private val minLinesBetweenTicks: Int =
        if (!enabled) 0 else maxOf(step, (totalLines.toLong() * minPercent / 100L).toInt())

    private var lastTickLine: Int = 0
    private var lastTickMs: Long = if (enabled) nowMs() else 0L

    /**
     * Whether [lineIndex] should emit a tick. Advances internal state when it
     * returns `true`, so call it exactly once per line index.
     */
    fun shouldTick(lineIndex: Int): Boolean {
        if (!enabled) return false
        if (lineIndex <= 0) return false
        if (lineIndex % step != 0) return false
        val now = nowMs()
        val dueByLines = lineIndex - lastTickLine >= minLinesBetweenTicks
        val dueByTime = everyMs > 0 && now - lastTickMs >= everyMs
        if (!dueByLines && !dueByTime) return false
        lastTickLine = lineIndex
        lastTickMs = now
        return true
    }

    /** `Book 5848 'X': 12000/190760 lines (6%)` — unchanged from the old format. */
    fun tickLine(bookId: Long, bookTitle: String, lineIndex: Int): String {
        val pct = if (totalLines > 0) (lineIndex.toLong() * 100L / totalLines) else 0L
        return "Book $bookId '$bookTitle': $lineIndex/$totalLines lines (${pct}%)"
    }
}
