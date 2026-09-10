package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import java.util.concurrent.ConcurrentHashMap

/**
 * Sefaria's Talmud commentaries separate the dibbur hamatchil from the comment
 * with a spaced dash (`דיבור – פירוש`). The Tosafot volumes listed here end it
 * with a period instead, so neither the reader nor the `line_dh` index can
 * tell the dibbur from the first sentence. Rewrites the first `. ` of such a
 * line to ` – ` so these volumes read and index like the rest.
 */
internal object SefariaDashlessDibburim {

    /** Sefaria `heTitle`s whose lines end the dibbur with a period (≥ 88% of content lines fit). */
    val bookHeTitles: Set<String> = setOf(
        "תוספות על בבא בתרא",
        "תוספות על מנחות",
        "תוספות על נדה",
        "תוספות על שבועות",
        "תוספות על סוכה",
        "תוספות על ראש השנה",
        "תוספות על מועד קטן",
        "תוספות על ביצה",
        "תוספות על חגיגה",
        "תוספות על תענית",
        "תוספות על מכות",
        "תוספות על הוריות",
    )

    /** Longer first sentences are commentary, not a quoted dibbur. */
    private const val MAX_DIBBUR_WORDS = 12

    private const val SEPARATOR = " – "

    private val SPACED_DASH = Regex("""\s[-–—]\s""")
    private val WHITESPACE = Regex("""\s+""")

    private val separatedByBook = ConcurrentHashMap<String, Int>()

    /** Returns [line] with its dibbur separated by a dash, or unchanged when the book or line does not fit. */
    fun separate(bookHeTitle: String, line: String): String {
        if (bookHeTitle !in bookHeTitles) return line
        if (line.startsWith("<h", ignoreCase = true) || SPACED_DASH.containsMatchIn(line)) return line
        val cut = line.indexOf(". ")
        if (cut <= 0) return line
        val dibbur = line.substring(0, cut)
        if ('<' in dibbur) return line
        val words = WHITESPACE.split(dibbur.trim()).count { it.isNotEmpty() }
        if (words !in 1..MAX_DIBBUR_WORDS) return line
        val comment = line.substring(cut + 2)
        if (comment.isBlank()) return line
        val separated = dibbur + SEPARATOR + comment
        // Keep this source repair aligned with the downstream index. In
        // particular, structural markers such as `מתני'` and `(הג"ה` must not
        // be rewritten merely because they happen to end with a period.
        if (DhExtractor.extract(separated, DhExtractor.Format.DASH) == null) return line
        separatedByBook.merge(bookHeTitle, 1, Int::plus)
        return separated
    }

    /** Starts a fresh per-import summary; this object also serves reusable readers in the same JVM. */
    fun resetSummary() = separatedByBook.clear()

    fun logSummary(logger: Logger) {
        for (title in bookHeTitles) {
            val n = separatedByBook[title] ?: 0
            logger.i { "Dashless dibburim: '$title' — $n lines separated" }
        }
    }
}
