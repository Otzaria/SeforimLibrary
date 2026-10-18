package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars

/**
 * Splits non-heading lines over [Config.minLineChars] visible chars into pieces of ~[Config.targetChars] (#1941),
 * deterministically (the links phase re-runs it), cutting only where no tag but a whole-line wrapper is open.
 */
internal object LongLineSplit {

    /** Mixed into a split book's source hash; bump whenever the produced pieces change. */
    const val VERSION = 1

    data class Config(
        val minLineChars: Int = 50_000,
        val targetChars: Int = 20_000,
        val maxChars: Int = 30_000,
        val minTailChars: Int = 10_000,
    )

    /** Where a raw offset range of a source line lands in the stored lines. */
    data class PiecePosition(val lineIndex: Int, val charStart: Int, val charEnd: Int?)

    class Result internal constructor(
        val source: List<String>,
        val lines: List<String>,
        // firstIndex[src] = first stored line of source line src; size source.size + 1.
        private val firstIndex: IntArray?,
        // Visible-char offset of each stored line inside its source line.
        private val visibleStart: IntArray?,
    ) {
        val isSplit: Boolean get() = firstIndex != null

        /** First stored line of 0-based source line [src]; -1 when out of range. */
        fun firstIndex(src: Int): Int = when {
            firstIndex == null -> src
            src in source.indices -> firstIndex[src]
            else -> -1
        }

        /** Last stored line of 0-based source line [src]; -1 when out of range. */
        fun lastIndex(src: Int): Int = when {
            firstIndex == null -> src
            src in source.indices -> firstIndex[src + 1] - 1
            else -> -1
        }

        fun pieceCount(src: Int): Int = if (src in source.indices) lastIndex(src) - firstIndex(src) + 1 else 0

        /** 0-based source line of stored line [index]. */
        fun sourceIndex(index: Int): Int {
            val starts = firstIndex ?: return index
            var lo = 0
            var hi = source.size - 1
            while (lo < hi) {
                val mid = (lo + hi + 1) ushr 1
                if (starts[mid] <= index) lo = mid else hi = mid - 1
            }
            return lo
        }

        /** The whole source line when stored line [index] is the first piece of a split one. */
        fun splitSourceOfFirstPiece(index: Int): String? {
            if (firstIndex == null) return null
            val src = sourceIndex(index)
            return if (firstIndex[src] == index && pieceCount(src) > 1) source[src] else null
        }

        /**
         * Maps raw offsets into source line [src] to the piece holding [rawStart], in that
         * piece's visible-char space. An end past the piece is clamped to the piece's end.
         */
        fun locate(src: Int, rawStart: Int, rawEnd: Int?): PiecePosition? {
            if (src !in source.indices) return null
            val content = source[src]
            if (rawStart < 0 || rawStart > content.length) return null
            val start = countVisibleChars(content, rawStart)
            val end = rawEnd?.takeIf { it in rawStart..content.length }?.let { countVisibleChars(content, it) }
            val first = firstIndex(src)
            val last = lastIndex(src)
            if (visibleStart == null || first == last) return PiecePosition(first, start, end)
            var piece = first
            while (piece < last && visibleStart[piece + 1] <= start) piece++
            val pieceEnd = if (piece < last) visibleStart[piece + 1] else countVisibleChars(content)
            val offset = visibleStart[piece]
            return PiecePosition(piece, start - offset, end?.let { minOf(it, pieceEnd) - offset })
        }
    }

    fun unsplit(lines: List<String>): Result = Result(lines, lines, null, null)

    fun split(lines: List<String>, config: Config = Config()): Result {
        var output: MutableList<String>? = null
        var firstIndex: IntArray? = null
        var visibleStart: MutableList<Int>? = null
        for ((src, line) in lines.withIndex()) {
            val pieces = if (line.length > config.minLineChars && !isHeading(line)) splitLine(line, config) else null
            if (pieces != null && output == null) {
                output = ArrayList<String>(lines.size + 16).apply { addAll(lines.subList(0, src)) }
                firstIndex = IntArray(lines.size + 1) { it }
                visibleStart = MutableList(src) { 0 }
            }
            val out = output ?: continue
            firstIndex!![src] = out.size
            if (pieces == null) {
                out += line
                visibleStart!! += 0
            } else {
                for (piece in pieces) {
                    out += piece.content
                    visibleStart!! += piece.visibleStart
                }
            }
        }
        val out = output ?: return Result(lines, lines, null, null)
        firstIndex!![lines.size] = out.size
        return Result(lines, out, firstIndex, visibleStart!!.toIntArray())
    }

    private class Piece(val content: String, val visibleStart: Int)

    // [seq]: push order in the line, so both scans name the same tag alike.
    private class OpenTag(val name: String, val open: String, val seq: Int)

    // Lower rank wins inside the cut window.
    private enum class CutKind { BR, COLON, PERIOD, SPACE }

    // [end]: where the previous piece's content stops; [next]: where the next one starts.
    private class Cut(val end: Int, val next: Int, val visible: Int, val kind: CutKind)

    private val VOID_TAGS = setOf(
        "br", "hr", "img", "wbr", "input", "meta", "link", "area", "base", "col", "embed", "source", "track", "param",
    )

    private val HEADING_TAG = Regex("h[1-6]")

    // Same test as the importer's detectHeaderLevel, at [from].
    private fun isHeading(line: String, from: Int = 0): Boolean =
        line.length >= from + 3 && line[from] == '<' && (line[from + 1] == 'h' || line[from + 1] == 'H') &&
            line[from + 2] in '1'..'6'

    private fun splitLine(line: String, config: Config): List<Piece>? {
        val scan = scan(line) ?: return null
        if (scan.totalVisible <= config.minLineChars) return null
        val cuts = mutableListOf<Cut>()
        var pieceStart = 0
        var from = 0
        while (scan.totalVisible - pieceStart > config.maxChars) {
            val lo = pieceStart + config.targetChars
            val hi = minOf(pieceStart + config.maxChars, scan.totalVisible - config.minTailChars)
            var best = -1
            var i = from
            while (i < scan.cuts.size && scan.cuts[i].visible <= hi) {
                val c = scan.cuts[i]
                // A piece opening with <hN> would become a TOC heading.
                val opensHeading = scan.wrapper.isEmpty() && isHeading(line, c.next)
                if (c.visible >= lo && !opensHeading && (best < 0 || c.kind < scan.cuts[best].kind)) best = i
                i++
            }
            if (best < 0) return null
            cuts += scan.cuts[best]
            pieceStart = scan.cuts[best].visible
            from = best + 1
        }
        if (cuts.isEmpty()) return null
        val openers = scan.wrapper.joinToString("") { it.open }
        val closers = scan.wrapper.asReversed().joinToString("") { "</${it.name}>" }
        val pieces = ArrayList<Piece>(cuts.size + 1)
        var start = 0
        var visible = 0
        for ((k, cut) in cuts.withIndex()) {
            val prefix = if (k == 0) "" else openers
            pieces += Piece(prefix + line.substring(start, cut.end) + closers, visible)
            start = cut.next
            visible = cut.visible
        }
        pieces += Piece(openers + line.substring(start), visible)
        return pieces
    }

    private class Scan(val totalVisible: Int, val wrapper: List<OpenTag>, val cuts: List<Cut>)

    /** Null when the markup is too broken to cut safely (an unterminated tag). */
    private fun scan(line: String): Scan? {
        // Pass 1: the tags open around every visible char form the wrapper.
        var wrapper: List<OpenTag>? = null
        walk(line) { stack ->
            val w = wrapper
            wrapper = if (w == null) stack.toList() else w.take(commonPrefix(w, stack))
        } ?: return null
        val wrap = wrapper.orEmpty()
        // Reopening a footnote or a heading would duplicate the note or turn a piece into a TOC heading.
        if (wrap.any { it.open.contains("footnote") || it.name.matches(HEADING_TAG) }) {
            return Scan(countVisibleChars(line), wrap, emptyList())
        }

        // Pass 2: cut candidates where exactly the wrapper is open.
        val cuts = mutableListOf<Cut>()
        var visible = 0
        var i = 0
        val stack = ArrayList<OpenTag>()
        fun atWrapper() = stack.size == wrap.size && commonPrefix(wrap, stack) == wrap.size
        while (i < line.length) {
            val c = line[i]
            when (c) {
                '<' -> {
                    val gt = line.indexOf('>', i)
                    val tag = line.substring(i, gt + 1)
                    if (tagName(tag) == "br" && !tag.startsWith("</") && atWrapper()) {
                        cuts += Cut(end = i, next = gt + 1, visible = visible, kind = CutKind.BR)
                    }
                    applyTag(stack, tag, i)
                    i = gt + 1
                    continue
                }
                '&' -> {
                    i = entityEnd(line, i)
                    visible++
                }
                else -> {
                    visible++
                    if (c == ' ' && atWrapper()) {
                        val kind = when (line.getOrNull(i - 1)) {
                            ':' -> CutKind.COLON
                            '.' -> CutKind.PERIOD
                            else -> CutKind.SPACE
                        }
                        cuts += Cut(end = i + 1, next = i + 1, visible = visible, kind = kind)
                    }
                }
            }
            i++
        }
        return Scan(visible, wrap, cuts)
    }

    /** Calls [onVisible] per visible char with the open-tag stack; null on an unterminated tag. */
    private inline fun walk(line: String, onVisible: (List<OpenTag>) -> Unit): Unit? {
        val stack = ArrayList<OpenTag>()
        var i = 0
        while (i < line.length) {
            when (line[i]) {
                '<' -> {
                    val gt = line.indexOf('>', i)
                    if (gt < 0) return null
                    applyTag(stack, line.substring(i, gt + 1), i)
                    i = gt
                }
                '&' -> {
                    i = entityEnd(line, i)
                    onVisible(stack)
                }
                else -> onVisible(stack)
            }
            i++
        }
        return Unit
    }

    // Same rule as countVisibleChars: `&...;` within 10 chars is one visible char.
    private fun entityEnd(line: String, i: Int): Int {
        val end = minOf(line.length, i + 10)
        var j = i + 1
        while (j < end) {
            if (line[j] == ';') return j
            j++
        }
        return i
    }

    private fun applyTag(stack: MutableList<OpenTag>, tag: String, seq: Int) {
        if (tag.startsWith("<!") || tag.startsWith("<?")) return
        val name = tagName(tag)
        if (name.isEmpty()) return
        if (tag.startsWith("</")) {
            val at = stack.indexOfLast { it.name == name }
            if (at >= 0) while (stack.size > at) stack.removeAt(stack.size - 1)
            return
        }
        if (name in VOID_TAGS || tag.endsWith("/>")) return
        stack += OpenTag(name, tag, seq)
    }

    private fun tagName(tag: String): String {
        var i = if (tag.startsWith("</")) 2 else 1
        val start = i
        while (i < tag.length && (tag[i].isLetterOrDigit() || tag[i] == '-' || tag[i] == ':')) i++
        return tag.substring(start, i).lowercase()
    }

    private fun commonPrefix(a: List<OpenTag>, b: List<OpenTag>): Int {
        var n = 0
        while (n < a.size && n < b.size && a[n].seq == b[n].seq) n++
        return n
    }
}
