package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Some Sefaria commentaries (Maggid Mishneh, Lechem Mishneh, Malbim) open a comment
 * that covers several units with a segment holding only the range, e.g. `(א-ג)`.
 * Linking that segment shows the reader an item with no text beside the real
 * comment that follows it, so such lines are excluded from links like headings.
 */
internal object SefariaRangeMarkerLines {

    /** Longest range seen in the corpus is `(לא-לג) `; anything longer is prose. */
    private const val MAX_LENGTH = 24

    private val RANGE_MARKER = Regex("""^\s*\(\s*[א-ת׳״'"]{1,5}\s*[-–—]\s*[א-ת׳״'"]{1,5}\s*\)\s*$""")

    /** Whether [content] is nothing but a parenthesized Hebrew-numeral range. */
    fun isRangeMarker(content: String): Boolean =
        content.length <= MAX_LENGTH && '(' in content && RANGE_MARKER.matches(content)
}
