package io.github.kdroidfilter.seforimlibrary.core.refs

/**
 * Canonical line-reference key — the single normalisation shared by the
 * `line_ref` index built here and by the Otzaria client that queries it.
 *
 * The client hashes the user's typed reference with the very same rules, so
 * any divergence between the two implementations is a silent miss (different
 * hash, no candidate). The companion Otzaria change must copy and pass
 * `generator/common/src/jvmTest/resources/ref_key_fixtures.json` before a
 * database containing this index is released.
 */
object RefKey {

    /** Locator words that are not part of the reference values themselves. */
    private val LOCATOR_WORDS = setOf(
        "פרק", "פסוק", "פסקה", "סעיף", "סימן", "הלכה", "משנה", "מאמר", "דף", "עמוד", "אות",
    )

    private val TRAILING_RANGE = Regex(
        """(?:^|[\s,.:])(?:\d+|[א-ת]{1,3}(?:[\"'״׳][א-ת]{1,2})?)[.:]?\s*([-–־])\s*""" +
            """(?:\d+|[א-ת]{1,3}(?:[\"'״׳][א-ת]{1,2})?)[.:]?\s*$""",
    )
    private val POINTS = Regex("[֑-ׇ]")
    private val NON_WORD = Regex("""[^a-zA-Z0-9֐-׿\s]""")
    private val WHITESPACE = Regex("""\s+""")
    private val DAF_SIDE_A = Regex("""(?<![א-ת'"״׳])([א-ת]{1,3})\.(?=[,\s]|$)""")
    private val DAF_SIDE_B = Regex("""(?<![א-ת'"״׳])([א-ת]{1,3}):(?=[,\s]|$)""")
    private val AMUD_A = Regex("""(?<![א-ת])ע["'״׳]א(?![א-ת])""")
    private val AMUD_B = Regex("""(?<![א-ת])ע["'״׳]ב(?![א-ת])""")

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
        val range = TRAILING_RANGE.find(ref)
        val dash = range?.groups?.get(1)?.range?.first
        val head = if (dash != null && dash > 0) ref.substring(0, dash) else ref

        var cleaned = expandDafMarks(head)
        // Maqaf and paseq become separators before the points range removes them.
        cleaned = cleaned.replace('־', ' ').replace('׀', ' ').replace('|', ' ')
        cleaned = POINTS.replace(cleaned, "")
        // Only the quoted forms mean Talmud page sides. Bare עא/עב are the
        // Hebrew numbers 71/72 and must not collide with א/ב.
        cleaned = AMUD_A.replace(cleaned, "א")
        cleaned = AMUD_B.replace(cleaned, "ב")
        cleaned = cleaned.replace("\"", "").replace("'", "").replace("״", "").replace("׳", "")
        cleaned = NON_WORD.replace(cleaned, " ").lowercase()

        return cleaned.split(WHITESPACE)
            .filter { it.isNotEmpty() && it !in LOCATOR_WORDS }
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

    /** Whether [heRef] begins with one of [titleAliases], literally or after canonical tokenisation. */
    fun hasTitleAliasPrefix(heRef: String, titleAliases: Iterable<String>): Boolean {
        val aliases = titleAliases as? List<String> ?: titleAliases.toList()
        if (suffixAfterTitleAlias(heRef, aliases) != null) return true
        val heRefTokens = tokens(heRef)
        return aliases.asSequence()
            .map(::tokens)
            .filter { it.isNotEmpty() && it.size <= heRefTokens.size }
            .any { heRefTokens.subList(0, it.size) == it }
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

    /**
     * Marks a partial key. Normalised tokens never contain it, so a partial key
     * can't collide with a full one, and clients that don't know it never query it.
     */
    const val PARTIAL_MARKER = "~"

    /** Partial key of a typed reference that may omit the named parts of a line's heRef. */
    fun partialOf(ref: String): String? = of(ref)?.let { "$PARTIAL_MARKER $it" }

    /**
     * Partial keys of a line whose heRef, after the title, opens with named parts
     * ("טור, חושן משפט, שט, ג") — one per omission of the named parts from the
     * outside in ("~ שט ג"), so a reference typed without them still reaches the
     * line. Empty unless at least two numeric components follow the named ones.
     */
    fun partialLineKeys(heRef: String, titleAliases: Iterable<String>): List<String> {
        val suffix = suffixAfterTitleAlias(heRef, titleAliases.toList()) ?: return emptyList()
        val components = suffix.split(',').map { it.trim() }.filter { it.isNotEmpty() }
        val named = components.indexOfFirst(::isNumeralComponent).let {
            if (it < 0) components.size else it
        }
        val numeric = components.subList(named, components.size)
        if (named == 0 || numeric.size < 2 || !numeric.all(::isNumeralComponent)) return emptyList()
        return (1..named).mapNotNull { omitted ->
            partialOf(components.subList(omitted, components.size).joinToString(", "))
        }.distinct()
    }

    private val NUMERAL_VALUES = "אבגדהוזחטיכלמנסעפצקרשת".toList().zip(
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400),
    ).toMap()

    /** A heRef component that is a location number ("שט", "סימן ז", "ב.") rather than a name. */
    internal fun isNumeralComponent(component: String): Boolean {
        val tokens = tokens(component)
        return tokens.isNotEmpty() && tokens.all(::isNumeralToken)
    }

    /**
     * Digits, or Hebrew letters in non-increasing value order.
     * A name that happens to read as a number ("נח") only loses its partial key.
     */
    internal fun isNumeralToken(token: String): Boolean {
        if (token.all { it.isDigit() }) return true
        if (token.length > 6) return false
        val values = token.map { NUMERAL_VALUES[it] ?: return false }
        return values.zipWithNext().all { (a, b) -> a >= b }
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
