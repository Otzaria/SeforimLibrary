package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.text.normalizeCategoryPathSegment

internal fun sanitizeFolder(name: String?): String {
    if (name.isNullOrBlank()) return ""
    return normalizeCategoryPathSegment(name)
}

// Legacy Otzar HaChochma style/format markers that Sefaria did not strip when
// ingesting some books (e.g. Maaseh Rokeach, Malbim). Pattern is `@NN<text>}`
// where NN is a two-digit style code. We keep the inner text, drop the marker.
private val OTZAR_MARKUP_REGEX = Regex("""@\d{2}([^}]*)\}""")

private val HTML_LINE_BREAK_REGEX = Regex("""<br\s*/?>""", RegexOption.IGNORE_CASE)

// In almost every Sefaria book an inline `<br>` is real structure (paragraphs,
// a bold heading's own line). These books break mid-sentence instead, into short
// lines that read as broken prose, so their inline breaks collapse to spaces.
private val COLLAPSE_INLINE_BREAK_BOOKS = setOf(
    "Tikkunei Zohar",
)

internal fun collapsesInlineBreaks(bookEnTitle: String): Boolean =
    bookEnTitle in COLLAPSE_INLINE_BREAK_BOOKS

// A `<br>` at the *end* of a line is not an in-paragraph break: Sefaria's MAM
// Tanach emits it after `{פ}` to mark an open parasha (פרשה פתוחה), which the
// reader renders as the break before the next verse. Collapsing it swallowed
// that break, so only breaks with text after them are collapsed.
private val TRAILING_HTML_LINE_BREAK_REGEX =
    Regex("""(?:\s*<br\s*/?>)+\s*$""", RegexOption.IGNORE_CASE)

internal fun cleanSefariaLine(raw: String, collapseInlineBreaks: Boolean = false): String {
    var s = if (raw.contains('\n')) raw.replace("\n", "") else raw
    if (OTZAR_MARKUP_REGEX.containsMatchIn(s)) {
        s = OTZAR_MARKUP_REGEX.replace(s, "$1")
    }
    s = normalizeLineBreaks(s, inlineBreak = if (collapseInlineBreaks) " " else "<br>")
    // Inline any Sefaria textimages as base64 data URIs (no-op if the embedder
    // hasn't been prefetched or the line contains no such URL).
    s = SefariaImageEmbedder.substituteImages(s)
    return s
}

/** [line] with every inline `<br>` as a space — the form a line's id is keyed on. */
internal fun collapseInlineLineBreaks(line: String): String = normalizeLineBreaks(line, inlineBreak = " ")

private fun normalizeLineBreaks(line: String, inlineBreak: String): String {
    if (!HTML_LINE_BREAK_REGEX.containsMatchIn(line)) return line
    val trailing = TRAILING_HTML_LINE_BREAK_REGEX.find(line)
    val body = if (trailing != null) line.substring(0, trailing.range.first) else line
    val s = HTML_LINE_BREAK_REGEX.replace(body, inlineBreak).replace(Regex(" {2,}"), " ").trim()
    return if (trailing != null && s.isNotEmpty()) "$s<br>" else s
}

// Hebrew label Sefaria's aliyah section name maps to. Named so the alt-TOC
// builder can recognise an aliyah level and re-label it by ordinal.
internal const val ALIYAH_SECTION_LABEL = "עליה"

// The seven weekly aliyot are read by their ordinal position (ראשון, שני, …),
// which is how a חומש names them, rather than "עליה" + gematria. Returns the
// ordinal label for a 1-based aliyah index, or null outside the customary range
// so the caller falls back to its generic "<base> <gematria>" label.
private val ALIYAH_ORDINALS = listOf(
    "ראשון", "שני", "שלישי", "רביעי", "חמישי", "שישי", "שביעי"
)

internal fun aliyahOrdinalLabel(index: Int): String? = ALIYAH_ORDINALS.getOrNull(index - 1)

/**
 * Maps common Sefaria English section/address names to their Hebrew equivalents.
 * Used when a schema only exposes `sectionNames` (English) without the
 * matching `heSectionNames`, so the generated TOC labels fall back gracefully
 * instead of showing English strings mid-Hebrew UI.
 *
 * Returns the original string if no mapping applies, or `null` for blank input.
 */
internal fun mapSectionNameToHebrew(base: String?): String? {
    if (base.isNullOrBlank()) return null
    val norm = base.lowercase()
    return when {
        "aliyah" in norm || "aliya" in norm -> ALIYAH_SECTION_LABEL
        "daf" in norm -> "דף"
        "chapter" in norm -> "פרק"
        "perek" in norm -> "פרק"
        "siman" in norm -> "סימן"
        // Sefaria ships this as "Se'if" (with an apostrophe); keep the plain form too.
        "seif" in norm || "se'if" in norm -> "סעיף"
        "section" in norm -> "סימן"
        "klal" in norm -> "כלל"
        "psalm" in norm -> "מזמור"
        "day" in norm -> "יום"
        "tikkun" in norm -> "תיקון"
        "parasha" in norm || "parsha" in norm -> "פרשה"
        "mishna" in norm -> "משנה"
        "halakha" in norm || "halacha" in norm -> "הלכה"
        "volume" in norm -> "כרך"
        "part" in norm -> "חלק"
        "verse" in norm || "pasuk" in norm -> "פסוק"
        "teshuva" in norm || "responsum" in norm || "responsa" in norm -> "תשובה"
        // Section names that Sefaria exposes only in English (the book's
        // heSectionNames is blank/absent), so the TOC/alt-TOC would otherwise
        // show English mid-Hebrew. Each Hebrew value is verified against the
        // book's schema and source text — see SefariaEnglishSectionNamesTest.
        "sheilta" in norm -> "שאילתא"       // שאילתות דרב אחאי גאון, העמק שאלה
        "maayan" in norm -> "מעיין"          // חסד לאברהם
        "nahar" in norm -> "נהר"             // חסד לאברהם
        "shoket" in norm -> "שוקת"           // חסד לאברהם
        "shaar" in norm -> "שער"             // שערי קדושה
        "treatise" in norm -> "שער"          // יסוד מורא ("אסדר שנים עשר שערים")
        "shorash" in norm -> "שורש"          // ביאור על ספר המצוות לרס"ג
        "manuscript" in norm -> "כתב יד"     // ליקוטי מוהר"ן
        "epistle" in norm -> "אגרת"          // נועם אלימלך (אגרת הקודש)
        "comment" in norm -> "פירוש"         // נועם אלימלך
        "question" in norm -> "שאלה"         // אגרת רב שרירא גאון
        "principle" in norm -> "עיקר"        // ספר הבחור ("שלש עשרה עקרים")
        "page" in norm -> "עמוד"             // גנזי מצרים, הלכות ספר תורה
        "word" in norm -> "מילה"             // מחברת מנחם
        "room" in norm -> "חדר"              // רב פנינים על משלי
        norm == "vav" -> "וו"                // ספר יראים (ווים ועמודים)
        norm == "ot" -> "אות"                // מדרש ילמדנו (short — exact match only)
        "paragraph" in norm -> "פסקה"
        "line" in norm -> "שורה"
        "column" in norm -> "טור"
        "folio" in norm -> "דף"
        "segment" in norm -> "קטע"
        else -> base
    }
}

internal fun normalizeTitleKey(value: String?): String? {
    if (value.isNullOrBlank()) return null

    // Normalize various quote styles (ASCII and Hebrew) so titles that differ
    // only by גרש/גרשיים or straight quotes map to the same key.
    val withoutQuotes = value
        .replace("\"", "")
        .replace("'", "")
        .replace("\u05F3", "") // Hebrew geresh
        .replace("\u05F4", "") // Hebrew gershayim

    return withoutQuotes
        .lowercase()
        .replace("\\s+".toRegex(), " ")
        .replace('_', ' ')
        .trim()
}
