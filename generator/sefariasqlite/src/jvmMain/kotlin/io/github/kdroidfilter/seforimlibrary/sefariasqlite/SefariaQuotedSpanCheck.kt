package io.github.kdroidfilter.seforimlibrary.sefariasqlite

private val HTML_TAG = Regex("<[^>]*>")
private val NON_SPACING_MARK = Regex("\\p{Mn}")
private val HEBREW_WORD = Regex("[א-ת]+")
private val MATRES_LECTIONIS = Regex("[וי]")
private const val QUOTE_PREFIX_LETTERS = "והבכלמש"
private const val CONTENT_WORD_LENGTH = 3

/**
 * True when at least half of [span]'s words appear in [quoted] — the text on
 * the other side of a quotation link — and one of the hits is a content word.
 *
 * Sefaria's quotation finder ships some charLevelData offsets that point at
 * unrelated words (often `startChar: 0`), so a matching version title alone
 * does not make the offsets trustworthy.
 */
internal fun spanSharesQuotedWords(span: String, quoted: String): Boolean =
    spanSharesQuotedWords(span, quoteMatchWords(quoted).toHashSet())

internal fun spanSharesQuotedWords(span: String, quotedWords: Set<String>): Boolean {
    val spanWords = quoteMatchWords(span)
    if (spanWords.isEmpty()) return false
    var hits = 0
    var contentHit = false
    for (word in spanWords) {
        val match = if (word in quotedWords) word else quotedWords.firstOrNull { prefixedMatch(word, it) } ?: continue
        hits++
        // "שעל" matching "על" alone must not validate an unrelated span.
        if (match.length >= CONTENT_WORD_LENGTH) contentHit = true
    }
    return contentHit && hits * 2 >= spanWords.size
}

/** Hebrew words without HTML, nikud/teamim, final forms and ו/י spelling variance. */
internal fun quoteMatchWords(text: String): List<String> {
    val plain = NON_SPACING_MARK.replace(HTML_TAG.replace(text, " "), "")
    return HEBREW_WORD.findAll(plain)
        .map { MATRES_LECTIONIS.replace(normalizeFinalLetters(it.value), "").replace("אלק", "אלה") }
        .filter { it.length >= 2 }
        .toList()
}

private fun normalizeFinalLetters(word: String): String = buildString(word.length) {
    for (c in word) {
        append(
            when (c) {
                'ך' -> 'כ'
                'ם' -> 'מ'
                'ן' -> 'נ'
                'ף' -> 'פ'
                'ץ' -> 'צ'
                else -> c
            }
        )
    }
}

/** "ובטב" quotes "טב": the longer word is the shorter one behind 1–2 prefix letters. */
private fun prefixedMatch(a: String, b: String): Boolean {
    val (long, short) = if (a.length > b.length) a to b else b to a
    val prefixLength = long.length - short.length
    return prefixLength in 1..2 &&
        long.endsWith(short) &&
        long.take(prefixLength).all { it in QUOTE_PREFIX_LETTERS }
}
