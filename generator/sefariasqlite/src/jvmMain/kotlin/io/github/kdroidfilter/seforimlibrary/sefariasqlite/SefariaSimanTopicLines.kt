package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * The Shulchan Aruch opens every siman with the author's topic line
 * (`<b>דין השכמת הבוקר. ובו ט סעיפים:</b>`), which Sefaria stores inside the
 * first se'if. Moves it to its own line above the se'if, still bold and not a TOC heading.
 */
internal object SefariaSimanTopicLines {

    val bookHeTitles: Set<String> = setOf(
        "שולחן ערוך, אורח חיים",
        "שולחן ערוך, יורה דעה",
        "שולחן ערוך, אבן העזר",
        "שולחן ערוך, חושן משפט",
    )

    private val LEADING_BOLD = Regex("""^<b>(.*?)</b>\s*""")
    private val INLINE_ITAG = Regex("""<i data-commentator[^>]*></i>""")
    private val TAG = Regex("""<[^>]+>""")
    private val WHITESPACE = Regex("""\s+""")
    private val SEIF_COUNT = Regex("""ובו\s+(?:\S+\s+)?סעי(?:פים|ף)(?:\s+אחד)?[\s:']*$""")

    data class Split(val line: String, val seif: String)

    /** Splits the topic off the siman's first se'if [line], or null when it has none. */
    fun split(bookHeTitle: String, line: String): Split? {
        if (bookHeTitle !in bookHeTitles) return null
        val match = LEADING_BOLD.find(line) ?: return null
        val inner = match.groupValues[1]
        val plain = TAG.replace(inner, "").replace(WHITESPACE, " ").trim()
        // A bare "ובו סעיף אחד" names no topic; a line of its own would only add noise.
        if (plain.startsWith("ובו ") || !SEIF_COUNT.containsMatchIn(plain)) return null
        val rest = line.substring(match.range.last + 1)
        if (rest.isBlank()) return null
        // Commentator markers keep their line: only the se'if line carries a ref to anchor them.
        val markers = INLINE_ITAG.findAll(inner).joinToString("") { it.value }
        return Split(
            line = "<b>${INLINE_ITAG.replace(inner, "")}</b>",
            seif = markers + rest,
        )
    }
}
