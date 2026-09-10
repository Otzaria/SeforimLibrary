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
 *  - [Format.BOLD_LEAD] — only the first word of the dibbur is bold and the
 *    quotation runs on until `כו'` / `וגו'` (Maharsha's Chiddushei Aggadot):
 *    `<b>אין</b> שלום כו'. הכא ניחא…` → `אין שלום`
 *
 * Extraction is deliberately conservative: a line yields a dibbur only when
 * it matches the shape exactly, the dibbur is short, actual commentary text
 * follows it, and it is not a bare structural marker (`מתני'`, `גמרא`,
 * `בא"ד`…). Book-level gating (see BuildLineDhIndexCli) is what keeps
 * incidental bold words or mid-sentence dashes in unrelated books out of the
 * index — a book is only indexed in the format that dominates its lines.
 */
object DhExtractor {

    enum class Format { BOLD, DASH, BOLD_LEAD }

    /**
     * One extracted dibbur: [key] is the [DhKey] form the index is searched
     * by; [display] is the dibbur as printed — points and quote marks kept,
     * whitespace collapsed, edge punctuation trimmed — for showing it as a
     * sub-heading.
     */
    data class Dh(val key: String, val display: String)

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

    /** HTML entities carry no visible commentary text by themselves. */
    private val HTML_ENTITY = Regex("""&(?:#\d+|#x[\da-fA-F]+|[A-Za-z][A-Za-z0-9]+);""")

    /** `כו'` / `וכו'` / `וגו'` closing a quotation, with any apostrophe glyph. */
    private val LEAD_END_MARKER = Regex("""(?:^|\s)(?:ו?כו|וגו)['׳’](?=\s|$|[.,:;)\]])""")

    /** A lead-bold dibbur ends within this many words of the line start. */
    private const val MAX_LEAD_WORDS = 10

    /** A marker after one of these boundaries belongs to commentary, not to the quotation. */
    private val SENTENCE_DELIMITER = Regex("""[.:;?!]\s""")

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
    private val WHITESPACE = Regex("""\s+""")

    /** Same edge trim as DhKey, so key and display agree on where the dibbur ends. */
    private const val EDGE_PUNCTUATION = ".,:;?!()[]"

    /** Extracts the dibbur of [line] in [format], or `null`. */
    fun extract(line: String, format: Format): Dh? = when (format) {
        Format.BOLD -> extractBold(line)
        Format.DASH -> extractDash(line)
        Format.BOLD_LEAD -> extractBoldLead(line)
    }

    /** `true` when [line] is a `<h1>`–`<h6>` heading (never carries a dibbur). */
    fun isHeadingLine(line: String): Boolean = HEADING_LINE.containsMatchIn(line)

    private fun extractBold(line: String): Dh? {
        if (isHeadingLine(line)) return null
        val m = BOLD_PREFIX.find(line) ?: return null
        val rest = TAG.replace(m.groupValues[2], "").trim()
        if (rest.isEmpty()) return null // whole-line bold: a heading, not a dibbur
        return accept(m.groupValues[1])
    }

    private fun extractBoldLead(line: String): Dh? {
        if (isHeadingLine(line)) return null
        val m = BOLD_PREFIX.find(line) ?: return null
        val boldRaw = m.groupValues[1].trim()
        val rest = m.groupValues[2]

        // BOLD_LEAD means that the quotation continues *after* </b>. A marker
        // inside the already-bold dibbur belongs to the ordinary BOLD shape;
        // searching the flattened whole line used to shorten tens of thousands
        // of valid, fully-bold dibburim at their first כו'/וגו'.
        val marker = LEAD_END_MARKER.find(rest) ?: return null

        // Do not cross another HTML segment. In particular, adjacent <b>
        // blocks often hold a second dibbur on the same physical line.
        val firstTag = rest.indexOf('<')
        if (firstTag >= 0 && firstTag < marker.range.first) return null

        val continuation = rest.substring(0, marker.range.first).trim()
        if (continuation.isEmpty()) return null
        val rawDh = "$boldRaw $continuation"
        if (rawDh.length > MAX_DH_LENGTH) return null

        // Terminal punctuation on the bold prefix, or a sentence delimiter in
        // the continuation, marks the boundary between the original dibbur and
        // commentary. A later כו' must not pull that commentary into the key.
        if (boldRaw.lastOrNull()?.let { it in ".,:;?!" } == true) return null
        if (SENTENCE_DELIMITER.containsMatchIn(rawDh)) return null
        if (WHITESPACE.split(rawDh.trim()).size > MAX_LEAD_WORDS) return null

        // Commentary must begin in the same plain-text segment. A later HTML
        // element cannot be used as evidence that the marker ended a quote.
        val commentaryStart = marker.range.last + 1
        val nextTag = rest.indexOf('<', commentaryStart).let { if (it < 0) rest.length else it }
        val commentary = HTML_ENTITY.replace(rest.substring(commentaryStart, nextTag), "").trim()
        if (commentary.none(Char::isLetterOrDigit)) return null

        val boldDh = accept(boldRaw) ?: return null
        val leadDh = accept(rawDh) ?: return null
        return leadDh.takeIf { it.display.length > boldDh.display.length }
    }

    private fun extractDash(line: String): Dh? {
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

    private fun accept(rawDh: String): Dh? {
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
            .trim { it in EDGE_PUNCTUATION || it == ' ' }
        if (marker in STOP_MARKERS) return null
        val key = DhKey.normalize(rawDh) ?: return null
        val display = WHITESPACE.replace(rawDh, " ").trim { it in EDGE_PUNCTUATION || it == ' ' }
        return Dh(key, display)
    }
}
