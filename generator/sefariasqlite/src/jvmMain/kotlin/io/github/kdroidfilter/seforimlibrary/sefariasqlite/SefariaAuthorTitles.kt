package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText

/**
 * Sefaria's author-name vocabulary, keyed by topic slug.
 *
 * A book schema's `authors[]` entry carries one bare Hebrew name — "דוד הלוי
 * סגל", "אברהם יצחק הכהן קוק". Sefaria's `topics` collection holds every other
 * form the same person is known by, honorifics included, and the export writes
 * them to `authors.json` (SefariaExport `run_authors_export`). This reads that
 * file and answers two questions: what should we call this author, and what
 * else is he called (which is what the blacklist has to match against).
 *
 * The file is optional. An export produced before the authors step existed has
 * none, and then every name stays exactly as the schema gave it.
 */
internal class SefariaAuthorTitles private constructor(
    private val bySlug: Map<String, List<String>>,
) {
    /**
     * The name to display for [slug], falling back to [schemaHe] when this slug
     * is unknown or has nothing better.
     *
     * "Better" is deliberately narrow: only a form that is the schema name with
     * an honorific attached in front. `דוד הלוי סגל` does not become `ט"ז` —
     * an acronym replaces the name rather than titling it, and it belongs in an
     * alias list, not in the author field.
     */
    fun displayName(slug: String?, schemaHe: String): String {
        val candidates = bySlug[slug?.trim()] ?: return schemaHe
        val base = normalize(schemaHe)
        if (base.isEmpty()) return schemaHe
        // Longest wins, so "הרב לורד יונתן זקס" beats "הרב יונתן זקס"; ties break
        // on the string itself, so the choice never depends on file order.
        return candidates
            .filter { isHonorificOf(normalize(it), base) }
            .maxWithOrNull(compareBy({ it.length }, { it }))
            ?: schemaHe
    }

    /** Every Hebrew name form known for [slug]; empty when it is unknown. */
    fun allNameForms(slug: String?): List<String> = bySlug[slug?.trim()] ?: emptyList()

    /** True when [candidate] is exactly [base] with honorific words in front. */
    private fun isHonorificOf(candidate: String, base: String): Boolean {
        if (!candidate.endsWith(base) || candidate.length <= base.length) return false
        val prefix = candidate.dropLast(base.length)
        // The definite article glued onto an acronym: מלבי"ם -> המלבי"ם. Only an
        // acronym, or "לוי" would become "הלוי" — a different name, not a
        // titled one.
        if (prefix == "ה") return base.any { it in GERSHAYIM }
        // The prefix must end at a word boundary, or "הרבאברהם" would pass as
        // "הרב" + "אברהם".
        if (!prefix.endsWith(' ')) return false
        val words = prefix.trim().split(' ').filter { it.isNotEmpty() }
        return words.isNotEmpty() && words.all { it in HONORIFICS }
    }

    companion object {
        const val FILE_NAME = "authors.json"

        /** No authors.json: every name is used exactly as the schema gives it. */
        val EMPTY = SefariaAuthorTitles(emptyMap())

        // --- normalization (declared before anything that calls it) ---

        private val NIKUD = Regex("[\\u0591-\\u05BD\\u05BF-\\u05C7]")
        private val BIDI = Regex("[\\u200E\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFEFF]")
        // \s misses NBSP and the typographic spaces, which do occur in the data.
        private val WHITESPACE = Regex("[\\s\\u00A0\\u2000-\\u200B\\u202F\\u205F\\u3000]+")

        /** Nikud, bidi marks and quote variants differ between the two sources. */
        private fun normalize(raw: String): String =
            raw.replace(NIKUD, "")
                .replace('״', '"').replace('“', '"').replace('”', '"')
                .replace('׳', '\'').replace('‘', '\'').replace('’', '\'')
                .replace(BIDI, "")
                .replace(WHITESPACE, " ")
                .trim()

        /**
         * Honorific words that may precede a name. An explicit list, not a
         * heuristic: this rewrites author names, and a wrong author name is
         * worse than a missing honorific.
         *
         * Normalized on the way in, because the candidate is normalized before
         * the comparison — an entry written with gershayim would otherwise be
         * unreachable, silently.
         */
        private val HONORIFICS: Set<String> = setOf(
            "רבי", "רבנו", "רבינו", "הרב", "הגאון", "הרה״ג", "הג״מ", "ר׳",
            "מרן", "כ״ק", "אדמו״ר", "חכם", "דון", "מו״ה", "לורד", "הקדוש",
        ).mapTo(HashSet()) { normalize(it) }

        /** Gershayim/geresh, in both the Hebrew and ASCII forms normalize maps to. */
        private val GERSHAYIM = setOf('"', '\'', '״', '׳')

        /**
         * Reads `<exportRoot>/authors.json`.
         *
         * A missing file is normal — an older export simply has none — and
         * yields [EMPTY]. A file that is present but unreadable is not normal,
         * and throws: quietly falling back would strip the honorific from every
         * author in the database because of one malformed record.
         */
        fun load(exportRoot: Path, json: Json, logger: Logger): SefariaAuthorTitles {
            val path = exportRoot.resolve(FILE_NAME)
            if (!Files.isRegularFile(path)) {
                logger.i { "No $FILE_NAME under $exportRoot — author names stay as the schemas give them" }
                return EMPTY
            }
            val records = runCatching { json.parseToJsonElement(path.readText()).jsonArray }
                .getOrElse { error -> throw IllegalStateException("$path is not a JSON array", error) }

            val bySlug = HashMap<String, List<String>>()
            records.forEachIndexed { index, element ->
                val record = element as? JsonObject
                    ?: throw IllegalStateException("$path entry $index is not an object")
                val slug = record["slug"]?.stringOrNull()?.trim().orEmpty()
                if (slug.isEmpty()) return@forEachIndexed
                val hebrew = (record["titles"] as? kotlinx.serialization.json.JsonArray)
                    .orEmpty()
                    .mapNotNull { it as? JsonObject }
                    .filter { it["lang"]?.stringOrNull() == "he" }
                    .mapNotNull { it["text"]?.stringOrNull()?.trim() }
                    .filter { it.isNotEmpty() }
                    .distinct()
                if (hebrew.isEmpty()) return@forEachIndexed
                // Duplicate slugs would silently drop one set of forms.
                require(slug !in bySlug) { "$path lists slug '$slug' more than once" }
                bySlug[slug] = hebrew
            }
            logger.i { "Loaded $FILE_NAME: ${bySlug.size} authors with Hebrew name forms" }
            return SefariaAuthorTitles(bySlug)
        }
    }
}
