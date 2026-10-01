package io.github.kdroidfilter.seforimlibrary.common.linker

/**
 * The text of a stored line as the external linker (LinkerToOtzaria) is handed it.
 *
 * A Sefaria segment that opens with its structural number, "(א) ", whether the
 * generator printed it (SefariaBookPayloadReader's seif prefix) or the source did,
 * is sent to the linker without that label. Sefaria's NER can tag such a label as
 * a citation, and resolving it changes the resolver's ibid context ahead of the
 * line's first real citation, so a bare "סי' ע"ט ס"ט" stops resolving. When
 * 05252649 printed the label on 61,887 more lines, the v30 relink lost 906 LINKER
 * links in them, nearly all on the first citation of a line. The linker's own
 * filter drops only the label's record after resolution, not the state it left
 * behind, and moving it would change the engine fingerprint (a full relink).
 *
 * The label is recognised exactly as LinkerToOtzaria's
 * `leading_hebrew_numeral_marker_end` (src/link_books.py) recognises it, and one
 * following space goes with it, so a line that only gained "(א) " is handed over
 * byte for byte as it was before. Both ends of the contract use this object:
 * DumpLines writes [linkerContent] into lines_snapshot, and GenerateLinkerLinks
 * checks a record's source_hash against the same view and shifts its offsets by
 * [strippedPrefixLength] back into the stored line.
 */
object LinkerInputView {

    /** Recorded in lines_snapshot_meta, so a snapshot made under another view never compares equal. */
    const val POLICY: String = "sefaria-leading-numeral-label-stripped-v1"

    private const val STRIPPED_SOURCE = "Sefaria"

    private val numeralValues: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצקרשת".toList().zip(
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400),
    ).toMap()

    private const val NUMERAL_PUNCTUATION = "'\"׳״"

    /** Number of leading chars of [content] the linker does not see; 0 for every non-Sefaria line. */
    fun strippedPrefixLength(sourceName: String, content: String): Int {
        if (sourceName != STRIPPED_SOURCE) return 0
        val end = leadingNumeralMarkerEnd(content)
        if (end == 0) return 0
        return if (end < content.length && content[end] == ' ') end + 1 else end
    }

    /** The line text the linker is given (and that its source_hash digests). */
    fun linkerContent(sourceName: String, content: String): String {
        val strip = strippedPrefixLength(sourceName, content)
        return if (strip == 0) content else content.substring(strip)
    }

    /**
     * End of a parenthesised canonical Hebrew numeral opening [content], or 0.
     * A port of `leading_hebrew_numeral_marker_end`: Python's `\s` and `[א-ת'"׳״]+`,
     * one to three letters in descending value (טו/טז excepted), never "שם".
     */
    internal fun leadingNumeralMarkerEnd(content: String): Int {
        var i = 0
        while (i < content.length && isPythonSpace(content[i])) i++
        if (i >= content.length || content[i] != '(') return 0
        val tokenStart = ++i
        while (i < content.length && (content[i] in 'א'..'ת' || content[i] in NUMERAL_PUNCTUATION)) i++
        if (i == tokenStart || i >= content.length || content[i] != ')') return 0
        val letters = content.substring(tokenStart, i).filterNot { it in NUMERAL_PUNCTUATION }
        if (letters.length !in 1..3 || letters == "שם") return 0
        val values = letters.map { numeralValues[it] ?: return 0 }
        if (letters != "טו" && letters != "טז" && values.zipWithNext().any { (left, right) -> left < right }) return 0
        return i + 1
    }

    // str.isspace(): Java's isWhitespace misses the no-break spaces, isSpaceChar misses the controls.
    private fun isPythonSpace(c: Char): Boolean =
        Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085'
}
