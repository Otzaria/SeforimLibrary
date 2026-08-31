package io.github.kdroidfilter.seforimlibrary.core.refs

/**
 * Canonical line-reference key — the single normalisation shared by the
 * `line_ref` index built here and by the Otzaria client that queries it.
 *
 * The client hashes the user's typed reference with the very same rules, so
 * any divergence between the two implementations is a silent miss (different
 * hash, no candidate). Both sides are pinned to the same fixture file:
 * `generator/common/src/jvmTest/resources/ref_key_fixtures.json` here and
 * `test/fixtures/ref_key_fixtures.json` in the Otzaria repo.
 */
object RefKey {

    /** Locator words that are not part of the reference values themselves. */
    private val LOCATOR_WORDS = setOf(
        "פרק", "פסוק", "פסקה", "סעיף", "סימן", "הלכה", "משנה", "מאמר", "דף", "עמוד", "אות",
    )

    private val RANGE_SEPARATORS = charArrayOf('-', '–', '־')
    private val POINTS = Regex("[֑-ׇ]")
    private val NON_WORD = Regex("""[^a-zA-Z0-9֐-׿\s]""")
    private val WHITESPACE = Regex("""\s+""")
    private val DAF_SIDE_A = Regex("""(?<![א-ת'"״׳])([א-ת]{1,3})\.(?=[,\s]|$)""")
    private val DAF_SIDE_B = Regex("""(?<![א-ת'"״׳])([א-ת]{1,3}):(?=[,\s]|$)""")

    /**
     * Expands a Talmud page mark into an explicit side token: "ב." -> "ב א",
     * "ב:" -> "ב ב". Also matches when a comma follows ("ברכות ב., א"), the
     * shape `line.heRef` uses, where the side would otherwise be lost.
     */
    private fun expandDafMarks(s: String): String =
        DAF_SIDE_B.replace(DAF_SIDE_A.replace(s) { "${it.groupValues[1]} א" }) { "${it.groupValues[1]} ב" }

    /**
     * Canonical tokens of [ref], in order: range truncation, page-mark
     * expansion, removal of vowels/cantillation/geresh/punctuation, locator
     * removal and ע"א/ע"ב mapping.
     */
    fun tokens(ref: String): List<String> {
        val dash = ref.indexOfFirst { it in RANGE_SEPARATORS }
        val head = if (dash > 0) ref.substring(0, dash) else ref

        var cleaned = expandDafMarks(head)
        // Maqaf and paseq become separators before the points range removes them.
        cleaned = cleaned.replace('־', ' ').replace('׀', ' ').replace('|', ' ')
        cleaned = POINTS.replace(cleaned, "")
        cleaned = cleaned.replace("\"", "").replace("'", "").replace("״", "").replace("׳", "")
        cleaned = NON_WORD.replace(cleaned, " ").lowercase()

        return cleaned.split(WHITESPACE)
            .filter { it.isNotEmpty() && it !in LOCATOR_WORDS }
            .map { if (it == "עא") "א" else if (it == "עב") "ב" else it }
    }

    /** Canonical key of [ref], or `null` when nothing is left of it. */
    fun of(ref: String): String? = tokens(ref).joinToString(" ").ifEmpty { null }

    /**
     * Returns the part of [heRef] after a literal title alias, preferring the
     * longest alias.  This deliberately runs before range parsing: hyphens are
     * valid inside book titles, and treating one as a range boundary first
     * collapses every line of that book to the heading key.
     */
    private fun suffixAfterTitleAlias(heRef: String, titleAliases: Iterable<String>): String? {
        var longestMatch: String? = null
        for (alias in titleAliases) {
            if (alias.isBlank() || alias.length <= (longestMatch?.length ?: 0)) continue
            if (!heRef.startsWith(alias)) continue
            if (heRef.length > alias.length && heRef[alias.length].isLetterOrDigit()) continue
            longestMatch = alias
        }
        return longestMatch?.let { heRef.substring(it.length) }
    }

    /**
     * Canonical key of a line: [heRef] with the leading book-title prefix
     * removed. Returns `null` for a heading line whose heRef is the title
     * itself. When no alias is a prefix the whole heRef is kept — the caller
     * reports that as a mismatch.
     */
    fun ofLine(heRef: String, titleAliases: Iterable<String>): String? {
        val aliases = titleAliases as? List<String> ?: titleAliases.toList()
        suffixAfterTitleAlias(heRef, aliases)?.let { return of(it) }
        return ofLineTokens(
            heRefTokens = tokens(heRef),
            titleAliasTokens = aliases.map(::tokens),
        )
    }

    /**
     * Equivalent to [ofLine], for callers that already tokenised the reference
     * and book-title aliases.
     */
    fun ofLineTokens(
        heRefTokens: List<String>,
        titleAliasTokens: Iterable<List<String>>,
    ): String? {
        if (heRefTokens.isEmpty()) return null

        var longestMatch: List<String>? = null
        for (aliasTokens in titleAliasTokens) {
            if (aliasTokens.isEmpty() || aliasTokens.size > heRefTokens.size) continue
            if (heRefTokens.subList(0, aliasTokens.size) != aliasTokens) continue
            if (aliasTokens.size > (longestMatch?.size ?: 0)) longestMatch = aliasTokens
        }
        val prefix = longestMatch ?: return heRefTokens.joinToString(" ")
        if (prefix.size == heRefTokens.size) return null
        return heRefTokens.subList(prefix.size, heRefTokens.size).joinToString(" ")
    }

    /** FNV-1a 64-bit over the UTF-8 bytes of [refKey] — the stored hash. */
    fun hash(refKey: String): Long {
        var hash = -3750763034362895579L // 14695981039346656037 as signed
        for (byte in refKey.encodeToByteArray()) {
            hash = (hash xor (byte.toInt() and 0xFF).toLong()) * 1099511628211L
        }
        return hash
    }
}
