package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.text.normalizeAuthorName

internal const val AUTHOR_CHANGES_FILE = "sefaria_author_changes.csv"

/**
 * `ForDB/sefaria_author_changes.csv` — the human judgement that two differently
 * spelled names are one man.
 *
 * [normalizeAuthorName] removes what is mechanically noise (nikud, bidi marks,
 * stray whitespace). It cannot know that Sefaria's `יעקב בן יעקב משה מליסא` and
 * Dicta's `רבי יעקב בן יעקב משה לורברבוים מליסא` are the same author, and
 * guessing is how a father and his son get merged — `אברהם שמואל בנימין סופר`
 * and `סופר, שמעון בן אברהם שמואל בנימין` look far more alike than these two do.
 * So the pairs are listed by hand, and only the Sefaria side needs them: the
 * Otzaria corpora are spelled directly in otzaria-library's `metadata.json`.
 */
internal class SefariaAuthorCanonicalNames private constructor(
    private val bySefariaName: Map<String, String>,
) {
    val size: Int get() = bySefariaName.size

    /** The canonical name for [raw], or [raw] normalized when no row claims it. */
    fun canonical(raw: String): String {
        val normalized = normalizeAuthorName(raw)
        return bySefariaName[normalized] ?: normalized
    }

    companion object {
        /** No CSV in the pinned archive: every name stays as its corpus spells it. */
        val EMPTY = SefariaAuthorCanonicalNames(emptyMap())

        /**
         * `שם בספריא,שם קנוני` rows, header required.
         *
         * Both sides are normalized on the way in — a row written with nikud
         * would otherwise never match a name that normalization has stripped,
         * and would fail silently.
         */
        fun parse(lines: List<String>): SefariaAuthorCanonicalNames {
            val nonBlank = lines.filter { it.isNotBlank() }
            if (nonBlank.isEmpty()) return EMPTY
            require("שם בספריא" in nonBlank.first()) {
                "$AUTHOR_CHANGES_FILE must start with a שם בספריא header"
            }
            val rows = parseRequiredCsvRows(nonBlank.drop(1), AUTHOR_CHANGES_FILE, minFields = 2)
            val map = LinkedHashMap<String, String>(rows.size)
            rows.forEachIndexed { index, fields ->
                val from = normalizeAuthorName(fields[0])
                val to = normalizeAuthorName(fields[1])
                require(from != to) {
                    "$AUTHOR_CHANGES_FILE row ${index + 2} maps '$from' to itself"
                }
                val previous = map.put(from, to)
                require(previous == null || previous == to) {
                    "$AUTHOR_CHANGES_FILE maps '$from' to both '$previous' and '$to'"
                }
            }
            // A target that is itself a source would make the result depend on
            // row order — one pass, so 'א->ב' plus 'ב->ג' would leave both ב and ג.
            val chained = map.values.filter { it in map.keys }.distinct()
            require(chained.isEmpty()) {
                "$AUTHOR_CHANGES_FILE chains renames through ${chained.joinToString()} — " +
                    "point every row at its final name instead"
            }
            return SefariaAuthorCanonicalNames(map)
        }

        /** Reads the CSV from the ForDB archive; absent is a normal, empty state. */
        fun load(logger: Logger): SefariaAuthorCanonicalNames {
            val lines = downloadOptionalForDbFile(AUTHOR_CHANGES_FILE, logger)
            if (lines == null) {
                logger.i { "No $AUTHOR_CHANGES_FILE in the ForDB archive — Sefaria author names stay as they are" }
                return EMPTY
            }
            return parse(lines).also { logger.i { "$AUTHOR_CHANGES_FILE: ${it.size} author rename(s)" } }
        }
    }
}
