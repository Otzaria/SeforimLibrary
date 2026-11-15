package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import java.util.concurrent.atomic.AtomicInteger

/**
 * Marks the Shulchan Arukh se'ifim inside the Tur (Otzaria/otzaria#4377). Sefaria keeps most Tur
 * simanim as one long segment, so a se'if cannot be found in it. [RESOURCE] lists where each
 * se'if's topic starts, keyed by the segment's heRef; the main walk prints a bold "[סעיף X]"
 * there, the app's convention for division markers. The Tur's order is its own, so a later
 * se'if may come first.
 *
 * Each point carries the words that follow it. A point whose words moved is looked up nearby
 * and otherwise skipped, so a changed Sefaria text never puts a marker inside the wrong sentence.
 */
internal object SefariaTurSeifMarkers {

    const val BOOK_HE_TITLE = "טור"
    const val RESOURCE = "tur_seifim.tsv"

    /** How far a point may have moved before it is dropped. */
    private const val MAX_DRIFT = 400

    internal data class Point(val offset: Int, val seif: String, val context: String)

    private val TAG = Regex("<[^>]+>")
    private val SPACES = Regex("""\s+""")
    private val TRAILING_ITAGS = Regex("""(?:<i data-commentator[^>]*></i>\s*)+$""")

    private val placed = AtomicInteger()
    private val skipped = AtomicInteger()

    private val pointsByRef: Map<String, List<Point>> by lazy {
        loadPoints(SefariaTurSeifMarkers::class.java.classLoader)
    }

    /** Returns the Tur segment [line] of [heRef] with its se'if markers, or [line] unchanged. */
    fun apply(bookHeTitle: String, heRef: String, line: String, recordStats: Boolean = true): String {
        if (bookHeTitle != BOOK_HE_TITLE) return line
        val points = pointsByRef[heRef] ?: return line
        return insert(line, points, recordStats)
    }

    /** Inserts the markers of [points] into [line]; points that no longer match are skipped. */
    internal fun insert(line: String, points: List<Point>, recordStats: Boolean = true): String {
        val labelsAt = sortedMapOf<Int, MutableList<String>>()
        for (point in points) {
            val at = locate(line, point)
            if (at == null) {
                if (recordStats) skipped.incrementAndGet()
                continue
            }
            labelsAt.getOrPut(insertionPoint(line, at)) { mutableListOf() } += point.seif
            if (recordStats) placed.incrementAndGet()
        }
        if (labelsAt.isEmpty()) return line
        val out = StringBuilder(line)
        for ((at, labels) in labelsAt.entries.reversed()) {
            val needsSpace = at > 0 && !out[at - 1].isWhitespace()
            out.insert(at, (if (needsSpace) " " else "") + marker(labels) + " ")
        }
        return out.toString()
    }

    internal fun marker(labels: List<String>): String {
        val sorted = labels.sortedBy(::gematria)
        return if (sorted.size == 1) "<b>[סעיף ${sorted[0]}]</b>" else "<b>[סעיפים ${sorted.joinToString(", ")}]</b>"
    }

    private fun gematria(label: String): Int = label.sumOf { HEBREW_VALUES[it] ?: 0 }

    private val HEBREW_VALUES: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצקרשת".withIndex().associate { (i, c) ->
        c to when {
            i < 10 -> i + 1
            i < 19 -> (i - 8) * 10
            else -> (i - 17) * 100
        }
    }

    /** The offset of [point] in [line]: its own offset when the words still follow it, else the nearest match. */
    private fun locate(line: String, point: Point): Int? {
        if (point.offset in 0..line.length && isWordStart(line, point.offset) && wordsAt(line, point.offset, point.context)) {
            return point.offset
        }
        val first = point.context.substringBefore(' ')
        var best: Int? = null
        var from = maxOf(0, point.offset - MAX_DRIFT)
        while (true) {
            val i = line.indexOf(first, from)
            if (i < 0 || i > point.offset + MAX_DRIFT) break
            if (isWordStart(line, i) && wordsAt(line, i, point.context) &&
                (best == null || kotlin.math.abs(i - point.offset) < kotlin.math.abs(best - point.offset))
            ) {
                best = i
            }
            from = i + 1
        }
        return best
    }

    private fun isWordStart(line: String, at: Int): Boolean =
        at == 0 || line[at - 1].isWhitespace() || line[at - 1] in ">.,;:!?()[]{}"

    private fun wordsAt(line: String, at: Int, context: String): Boolean {
        val n = context.split(' ').size
        val visible = TAG.replace(line.substring(at, minOf(line.length, at + 600)), " ")
        return SPACES.split(visible.trim()).take(n).joinToString(" ") == context
    }

    /** Puts the marker before the commentator markers that open the word, not between them and it. */
    private fun insertionPoint(line: String, at: Int): Int =
        TRAILING_ITAGS.find(line.substring(0, at))?.range?.first ?: at

    internal fun loadPoints(classLoader: ClassLoader?): Map<String, List<Point>> {
        val stream = classLoader?.getResourceAsStream(RESOURCE)
            ?: throw IllegalStateException("$RESOURCE missing from the sefariasqlite resources")
        val byRef = LinkedHashMap<String, MutableList<Point>>()
        stream.bufferedReader(Charsets.UTF_8).useLines { lines ->
            lines.filter { it.isNotBlank() && !it.startsWith("#") }.forEach { row ->
                val cols = row.split('\t')
                require(cols.size == 4) { "$RESOURCE: expected 4 columns in '$row'" }
                byRef.getOrPut(cols[0]) { mutableListOf() } += Point(cols[1].toInt(), cols[2], cols[3])
            }
        }
        return byRef
    }

    /** Starts a fresh per-import summary. */
    fun resetSummary() {
        placed.set(0)
        skipped.set(0)
    }

    fun logSummary(logger: Logger) {
        logger.i { "Tur se'if markers: ${placed.get()} placed, ${skipped.get()} skipped (text moved)" }
    }
}
