package io.github.kdroidfilter.seforimlibrary.core.dh

/**
 * Canonical dibbur-hamatchil normalisation — the single form shared by the
 * `line_dh` index built at generation time and by the Otzaria client that
 * queries it.
 *
 * Unlike `RefKey`, the stored value is the normalised TEXT itself (not a
 * hash), so the client can prefix-match while the user types. The client
 * normalises the typed query with the very same rules; any divergence is a
 * silent miss, so both implementations are pinned to the same fixture file:
 * `generator/common/src/jvmTest/resources/dh_key_fixtures.json` here and
 * `test/fixtures/dh_key_fixtures.json` in the Otzaria repo.
 */
object DhKey {

    /** Hebrew points: vowels, cantillation and other combining marks. */
    private val POINTS = Regex("[֑-ׇ]")
    private val WHITESPACE = Regex("""\s+""")

    /** Punctuation trimmed from both ends of the dibbur (print conventions). */
    private const val EDGE_PUNCTUATION = ".,:;?!()[]"

    /**
     * Canonical form of [raw], or `null` when nothing usable is left.
     *
     * Order matters and is part of the parity contract: maqaf and paseq to
     * spaces (BEFORE points removal — the points range contains them), points
     * removal, quote-mark removal (straight and Hebrew geresh/gershayim),
     * whitespace collapse, then edge-punctuation trim.
     */
    fun normalize(raw: String): String? {
        var s = raw.replace('־', ' ').replace('׀', ' ').replace('|', ' ')
        s = POINTS.replace(s, "")
        s = s.replace("\"", "").replace("'", "").replace("״", "").replace("׳", "")
        s = WHITESPACE.replace(s, " ").trim()
        s = s.trim { it in EDGE_PUNCTUATION || it == ' ' }
        return s.ifEmpty { null }?.takeIf { it.length >= 2 }
    }
}
