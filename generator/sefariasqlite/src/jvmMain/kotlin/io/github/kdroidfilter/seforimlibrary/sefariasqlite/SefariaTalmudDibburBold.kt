package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor

/**
 * Sefaria's Talmud commentaries print the dibbur hamatchil plain, before a
 * spaced dash, where its Tanakh commentaries (and the printed Shas) bold it.
 * Wraps that dibbur in a provenance-marked `<b>` and keeps the dash, so copied plain text reads
 * as before.
 *
 * Applied only to the stored content: line keys, char counts and char-level
 * anchors all come from the unbolded line, and a tag adds no visible chars.
 */
internal object SefariaTalmudDibburBold {

    /** Same book-level share line_dh requires before it trusts a format. */
    private const val MIN_DASH_COVERAGE = 0.4

    /**
     * `true` for a Bavli commentary whose lines mostly open with a dash dibbur.
     * The Gemara itself is excluded: its punctuated text uses the same dashes.
     */
    fun appliesTo(categoriesEn: List<String>, dependence: Dependence?, lines: List<String>): Boolean {
        if (dependence == null || "Bavli" !in categoriesEn) return false
        var contentLines = 0
        var dashLines = 0
        for (line in lines) {
            if (line.isBlank() || DhExtractor.isHeadingLine(line)) continue
            contentLines++
            if (DhExtractor.dashDibburEnd(line) != null) dashLines++
        }
        return contentLines > 0 && dashLines >= MIN_DASH_COVERAGE * contentLines
    }

    /** Returns [line] with its dash dibbur bolded, or unchanged when it has none. */
    fun bold(line: String): String = DhExtractor.boldDashDibbur(line)
}
