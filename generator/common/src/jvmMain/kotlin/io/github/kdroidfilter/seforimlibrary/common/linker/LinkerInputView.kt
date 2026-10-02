package io.github.kdroidfilter.seforimlibrary.common.linker

/**
 * The text of a stored line as the external linker (LinkerToOtzaria) is handed it.
 *
 * A segment that opens with its structural number, "(א) ", is sent to the linker
 * without that label. Sefaria's NER can tag such a label as a citation, and
 * resolving it changes the resolver's ibid context ahead of the line's first real
 * citation, so a bare "סי' ע"ט ס"ט" stops resolving. When 05252649 printed the
 * label on 61,887 more Sefaria lines, the v30 relink lost 906 LINKER links in them,
 * nearly all on the first citation of a line. The linker's own filter drops only
 * the label's record after resolution, not the state it left behind, and moving it
 * would change the engine fingerprint (a full relink).
 *
 * Which books, per [modeFor]:
 * - Sefaria: every line, plain labels only ([Mode.PLAIN]). A line that only gained
 *   the generated "(א) " is then handed over byte for byte as before it.
 * - Any other source: only a book with at least [MIN_LABELLED_LINES] labelled
 *   lines, and there also a label wrapped in its own tags, "<b>(א)</b> "
 *   ([Mode.PLAIN_AND_TAGGED]). A book with a few stray "(א)" lines is left as is:
 *   changing its input would relink the whole book for almost nothing.
 *
 * The choice is a function of the book's own stored lines and nothing else, so it
 * is the same in every build of the same text. It can change only when labelled
 * lines are added or removed, and such an edit changes the book's snapshot anyway,
 * so the linker re-reads it in that same cycle. Each artifact record's source_hash
 * says which text its offsets index; the importer accepts a record only when that
 * hash matches the stored line or one of its stripped views ([offsetShift]).
 *
 * The plain label is recognised exactly as LinkerToOtzaria's
 * `leading_hebrew_numeral_marker_end` (src/link_books.py) recognises it. One
 * following space goes with it. Only a prefix is ever removed, so an offset into
 * the view plus the removed length is the same position in the stored line.
 */
object LinkerInputView {

    /** Labelled lines from which a non-Sefaria book is handed over without its labels. */
    const val MIN_LABELLED_LINES: Int = 20

    /** Recorded in lines_snapshot_meta, so a snapshot made under another view never compares equal. */
    const val POLICY: String = "leading-numeral-label-stripped-v2;sefaria=plain;other=plain+tagged,min=$MIN_LABELLED_LINES"

    private const val SEFARIA = "Sefaria"

    enum class Mode { NONE, PLAIN, PLAIN_AND_TAGGED }

    private val numeralValues: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצקרשת".toList().zip(
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400),
    ).toMap()

    private const val NUMERAL_PUNCTUATION = "'\"׳״"

    /**
     * Every labelled line contains it, so a reader that only counts labelled lines
     * ([isLabelled]) may skip the lines without it.
     */
    const val LABEL_OPEN: Char = '('

    /** The view of a book from [sourceName] whose stored lines are [contents]. */
    fun modeFor(sourceName: String, contents: Iterable<String>): Mode {
        fixedModeFor(sourceName)?.let { return it }
        var labelled = 0
        for (content in contents) {
            if (isLabelled(content) && ++labelled >= MIN_LABELLED_LINES) break
        }
        return modeForLabelledLines(labelled)
    }

    /**
     * The view of every book from [sourceName] when it does not depend on the
     * book's lines, else null: then it is [modeForLabelledLines] of the number of
     * lines [isLabelled] accepts. A caller can stream such a book's lines once it
     * knows that number, without holding them.
     */
    fun fixedModeFor(sourceName: String): Mode? = if (sourceName == SEFARIA) Mode.PLAIN else null

    /** Whether [content] counts toward [MIN_LABELLED_LINES]. It always contains [LABEL_OPEN]. */
    fun isLabelled(content: String): Boolean = strippedPrefixLength(Mode.PLAIN_AND_TAGGED, content) > 0

    /** The view of a book with no [fixedModeFor] and [labelledLines] labelled lines. */
    fun modeForLabelledLines(labelledLines: Int): Mode =
        if (labelledLines >= MIN_LABELLED_LINES) Mode.PLAIN_AND_TAGGED else Mode.NONE

    /** Number of leading chars of [content] the linker does not see under [mode]. */
    fun strippedPrefixLength(mode: Mode, content: String): Int {
        if (mode == Mode.NONE) return 0
        val plain = leadingNumeralMarkerEnd(content)
        val end = when {
            plain > 0 -> plain
            mode == Mode.PLAIN_AND_TAGGED -> taggedNumeralMarkerEnd(content)
            else -> 0
        }
        if (end == 0) return 0
        return if (end < content.length && content[end] == ' ') end + 1 else end
    }

    /** The line text the linker is given under [mode] (and that its source_hash digests). */
    fun linkerContent(mode: Mode, content: String): String {
        val strip = strippedPrefixLength(mode, content)
        return if (strip == 0) content else content.substring(strip)
    }

    /**
     * How far a record's offsets sit from the stored line [content], or null when
     * [matches] accepts neither the line nor any view of it. The views are tried
     * widest first; a record made on the whole line (before any view) gets 0.
     */
    fun offsetShift(content: String, matches: (String) -> Boolean): Int? {
        val wide = strippedPrefixLength(Mode.PLAIN_AND_TAGGED, content)
        val plain = strippedPrefixLength(Mode.PLAIN, content)
        for (strip in listOf(wide, plain).distinct()) {
            if (strip > 0 && matches(content.substring(strip))) return strip
        }
        return if (matches(content)) 0 else null
    }

    /**
     * End of a parenthesised canonical Hebrew numeral opening [content], or 0.
     * A port of `leading_hebrew_numeral_marker_end`: Python's `\s` and `[א-ת'"׳״]+`,
     * one to three letters in descending value (טו/טז excepted), never "שם".
     */
    internal fun leadingNumeralMarkerEnd(content: String, from: Int = 0): Int {
        var i = from
        while (i < content.length && isPythonSpace(content[i])) i++
        if (i >= content.length || content[i] != LABEL_OPEN) return 0
        val tokenStart = ++i
        while (i < content.length && (content[i] in 'א'..'ת' || content[i] in NUMERAL_PUNCTUATION)) i++
        if (i == tokenStart || i >= content.length || content[i] != ')') return 0
        val letters = content.substring(tokenStart, i).filterNot { it in NUMERAL_PUNCTUATION }
        if (letters.length !in 1..3 || letters == "שם") return 0
        val values = letters.map { numeralValues[it] ?: return 0 }
        if (letters != "טו" && letters != "טז" && values.zipWithNext().any { (left, right) -> left < right }) return 0
        return i + 1
    }

    /**
     * End of a label wrapped alone in its own tags, "<b>(א)</b>" or
     * "<sup style=…>(א)</sup>", closing tags included, or 0. The tags must close
     * in reverse order right after the label, so what is removed is a balanced
     * fragment holding nothing but the label. Headings are never touched.
     */
    internal fun taggedNumeralMarkerEnd(content: String): Int {
        var i = 0
        while (i < content.length && isPythonSpace(content[i])) i++
        val names = ArrayList<String>()
        while (i + 1 < content.length && content[i] == '<' && content[i + 1].isAsciiLetter()) {
            var n = i + 1
            while (n < content.length && (content[n].isAsciiLetter() || content[n].isDigit())) n++
            val name = content.substring(i + 1, n).lowercase()
            if (headingTag.matches(name)) return 0
            val close = content.indexOf('>', n)
            if (close < 0 || content[close - 1] == '/') return 0
            names += name
            i = close + 1
            while (i < content.length && isPythonSpace(content[i])) i++
        }
        if (names.isEmpty()) return 0
        val markerEnd = leadingNumeralMarkerEnd(content, from = i)
        if (markerEnd == 0) return 0
        var j = markerEnd
        for (name in names.asReversed()) {
            val closing = "</$name>"
            if (!content.regionMatches(j, closing, 0, closing.length, ignoreCase = true)) return 0
            j += closing.length
        }
        return j
    }

    private val headingTag = Regex("h[1-6]")

    private fun Char.isAsciiLetter(): Boolean = this in 'a'..'z' || this in 'A'..'Z'

    // str.isspace(): Java's isWhitespace misses the no-break spaces, isSpaceChar misses the controls.
    private fun isPythonSpace(c: Char): Boolean =
        Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085'
}
