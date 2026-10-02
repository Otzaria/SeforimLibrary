package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import java.sql.Connection

/**
 * Reads book acronyms from the SeforimAcronymizer DB (Books, Acronyms, BookAcronyms).
 *
 * The Acronymizer keys a book by its exact title, tried in a few spellings
 * ([lookupVariants]); the first spelling that has any rows wins.
 */
internal class AcronymizerLookup(private val conn: Connection) {

    /** The raw terms stored under the first variant of [title] that has any; empty when none has. */
    fun rawTerms(title: String): List<String> {
        for (candidate in lookupVariants(title)) {
            val found = mutableListOf<String>()
            conn.prepareStatement(
                """
                SELECT a.acronym
                FROM Books b
                JOIN BookAcronyms ba ON b.id = ba.book_id
                JOIN Acronyms a ON ba.acronym_id = a.id
                WHERE b.title = ?
                ORDER BY a.acronym
                """.trimIndent()
            ).use { ps ->
                ps.setString(1, candidate)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val acronym = rs.getString(1)
                        if (!acronym.isNullOrBlank()) found.add(acronym)
                    }
                }
            }
            if (found.isNotEmpty()) return found
        }
        return emptyList()
    }

    /** The terms a book called [title] gets: sanitized, de-duplicated, without the title itself. */
    fun termsFor(title: String): List<String> = clean(rawTerms(title), title)

    companion object {
        fun lookupVariants(title: String): List<String> = buildList {
            add(title)
            val stripped = title.replace("״", "").replace("\"", "").replace("׳", "").replace("'", "").trim()
            if (stripped.isNotBlank()) add(stripped)
            val noComma = title.replace(",", " ").trim()
            if (noComma.isNotBlank()) add(noComma)
            val noPunct = title.replace("[\\p{Punct}]".toRegex(), " ").replace("\\s+".toRegex(), " ").trim()
            if (noPunct.isNotBlank()) add(noPunct)
            val sanitized = sanitizeTerm(title)
            if (sanitized.isNotBlank()) add(sanitized)
        }.distinct()

        /** Strips diacritics, maqaf and Hebrew geresh/gershayim, and collapses whitespace. */
        fun sanitizeTerm(raw: String): String {
            var s = raw.trim()
            if (s.isEmpty()) return ""
            s = HebrewTextUtils.removeAllDiacritics(s)
            s = HebrewTextUtils.replaceMaqaf(s, " ")
            s = s.replace("״", "") // Hebrew gershayim (״)
            s = s.replace("׳", "") // Hebrew geresh (׳)
            s = s.replace("\\s+".toRegex(), " ").trim()
            return s
        }

        /** Sanitizes [raw], drops blanks and anything equal to [title] after normalization, de-duplicates. */
        fun clean(raw: List<String>, title: String): List<String> {
            if (raw.isEmpty()) return emptyList()
            val titleNormalized = sanitizeTerm(title)
            return raw
                .map { sanitizeTerm(it).trim() }
                .filter { it.isNotEmpty() }
                .filter { !it.equals(title, ignoreCase = true) }
                .filter { !it.equals(titleNormalized, ignoreCase = true) }
                .distinct()
        }
    }
}
