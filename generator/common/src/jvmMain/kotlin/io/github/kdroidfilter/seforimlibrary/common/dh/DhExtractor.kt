package io.github.kdroidfilter.seforimlibrary.common.dh

import io.github.kdroidfilter.seforimlibrary.core.dh.DhKey

/**
 * Extracts the dibbur hamatchil from a commentary line's HTML content.
 *
 * Two shapes exist in the corpus, and a book uses one of them consistently:
 *
 *  - [Format.BOLD] — the dibbur is a `<b>…</b>` prefix (Rashi on Tanakh,
 *    Mishnah commentaries, most Otzaria-sourced books):
 *    `<b>בראשית.</b> אמר רבי יצחק…`
 *  - [Format.DASH] — the dibbur is the text before the first spaced dash
 *    (Sefaria's Talmud commentaries): `עד סוף האשמורה הראשונה – שליש הלילה…`
 *
 * Extraction is deliberately conservative: a line yields a dibbur only when
 * it matches the shape exactly, the dibbur is short, actual commentary text
 * follows it, and it is not a bare structural marker (`מתני'`, `גמרא`,
 * `בא"ד`…). Book-level gating (see BuildLineDhIndexCli) is what keeps
 * incidental bold words or mid-sentence dashes in unrelated books out of the
 * index — a book is only indexed in the format that dominates its lines.
 */
object DhExtractor {

    enum class Format { BOLD, DASH }

    /** Longest raw dibbur accepted, in characters (real ones median ~11). */
    private const val MAX_DH_LENGTH = 100

    private val HEADING_LINE = Regex("""^﻿?\s*<h[1-6]""", RegexOption.IGNORE_CASE)

    /**
     * `<b>…</b>` opening the line (wrapper tags like `<big>` allowed before
     * it), with the dibbur containing no nested tags. Whatever follows the
     * closing tag is group 2 — it must carry visible text, otherwise the
     * whole line is bold (a decorated heading, not a dibbur).
     */
    private val BOLD_PREFIX = Regex(
        """^﻿?\s*(?:<(?:big|small|span[^>]*)>\s*)*<b>\s*([^<]{1,$MAX_DH_LENGTH}?)\s*</b>(.*)$""",
        setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL),
    )

    /** A dash that separates dibbur from comment: spaced hyphen/en/em dash. */
    private val SPACED_DASH = Regex("""\s[-–—]\s""")

    /** First sentence break — the shape Sefaria uses for a daf's first comment. */
    private const val SENTENCE_BREAK = ". "

    /** Dash-format dibburim longer than this are re-cut at a sentence break. */
    private const val LONG_DASH_DH = 40

    private val TAG = Regex("<[^>]+>")

    /**
     * Structural markers that open lines in the same position and shape as a
     * dibbur but locate rather than quote (`מתני'`, `גמרא`, `בא"ד`, `שם`…).
     * Compared against the raw dibbur with points stripped and edges trimmed
     * — BEFORE quote-mark removal, so the locator `תוד"ה` is dropped while a
     * genuine dibbur `תודה` survives.
     */
    private val STOP_MARKERS = setOf(
        "מתני'", "מתני", "מתניתין", "גמ'", "גמ", "גמרא", "שם", "בא\"ד", "באד",
        "ד\"ה", "בד\"ה", "תוד\"ה", "תוס'", "תוס", "רש\"י", "רשי'", "רשי", "פירש\"י",
        "הדרן", "סליק", "תשובה", "שאלה", "מכתב", "הקדמה", "הגה", "הג\"ה",
        "פרק", "משנה", "הלכה", "סימן", "סעיף", "ירושלמי", "וכו'", "וכו",
    )

    private val POINTS = Regex("[֑-ׇ]")

    /** Extracts and normalises the dibbur of [line] in [format], or `null`. */
    fun extract(line: String, format: Format): String? = when (format) {
        Format.BOLD -> extractBold(line)
        Format.DASH -> extractDash(line)
    }

    /** `true` when [line] is a `<h1>`–`<h6>` heading (never carries a dibbur). */
    fun isHeadingLine(line: String): Boolean = HEADING_LINE.containsMatchIn(line)

    private fun extractBold(line: String): String? {
        if (isHeadingLine(line)) return null
        val m = BOLD_PREFIX.find(line) ?: return null
        val rest = TAG.replace(m.groupValues[2], "").trim()
        if (rest.isEmpty()) return null // whole-line bold: a heading, not a dibbur
        return accept(m.groupValues[1])
    }

    private fun extractDash(line: String): String? {
        if (isHeadingLine(line)) return null
        val m = SPACED_DASH.find(line) ?: return null
        var dh = line.substring(0, m.range.first)
        if ('<' in dh || dh.length > MAX_DH_LENGTH) return null
        if (line.substring(m.range.last + 1).isBlank()) return null
        // A daf's first comment ends its dibbur with a sentence break instead
        // of a dash; when the dash-cut is implausibly long, re-cut there.
        if (dh.length > LONG_DASH_DH) {
            val cut = dh.indexOf(SENTENCE_BREAK)
            if (cut > 0) dh = dh.substring(0, cut)
        }
        return accept(dh)
    }

    private fun accept(rawDh: String): String? {
        // Match DhKey's edge trimming while deliberately preserving quote
        // marks: תוד"ה is a locator, while תודה is a genuine Hebrew word.
        val marker = POINTS.replace(rawDh, "")
            .replace('״', '"')
            .replace('”', '"')
            .replace('“', '"')
            .replace('׳', '\'')
            .replace('’', '\'')
            .replace('‘', '\'')
            .replace("''", "\"")
            .trim()
            .trim { it in ".,:;?!()[]" || it == ' ' }
        if (marker in STOP_MARKERS) return null
        return DhKey.normalize(rawDh)
    }
}
