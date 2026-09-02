package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText

/**
 * Sefaria's author-name vocabulary, keyed by topic slug.
 *
 * A book schema's `authors[]` entry carries one bare Hebrew name — "דוד הלוי
 * סגל", "משה חיים לוצאטו". Sefaria's `topics` collection holds every other form
 * the same person is known by, honorifics included, and the export writes them
 * to `authors.json` (see SefariaExport `run_authors_export`). This reads that
 * file and answers one question: what should we call this author?
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

    /** True when [candidate] is exactly [base] with honorific words in front. */
    private fun isHonorificOf(candidate: String, base: String): Boolean {
        if (!candidate.endsWith(base) || candidate.length <= base.length) return false
        val prefix = candidate.dropLast(base.length)
        // The definite article glued onto an acronym: מלבי"ם -> המלבי"ם.
        if (prefix == "ה") return true
        val words = prefix.trim().split(' ').filter { it.isNotEmpty() }
        return words.isNotEmpty() && words.all { it in HONORIFICS }
    }

    companion object {
        /** No authors.json: every name is used exactly as the schema gives it. */
        val EMPTY = SefariaAuthorTitles(emptyMap())

        const val FILE_NAME = "authors.json"

        /**
         * Honorific words that may precede a name. Kept as an explicit list —
         * a heuristic here would silently rewrite author names, and a wrong
         * author name is worse than a missing honorific.
         */
        private val HONORIFICS = setOf(
            "רבי", "רבנו", "רבינו", "הרב", "הגאון", "הרה\"ג", "הרה״ג", "הג\"מ", "הג״מ",
            "ר'", "ר׳", "מרן", "כ\"ק", "כ״ק", "אדמו\"ר", "אדמו״ר", "חכם", "דון", "מו\"ה", "מו״ה",
            "הרב", "לורד", "הקדוש",
        )

        /**
         * Reads `<exportRoot>/authors.json`. A missing file is normal and
         * returns [EMPTY]; a present-but-unreadable one is not, and is logged
         * loudly rather than silently degrading every author name.
         */
        fun load(exportRoot: Path, json: Json, logger: Logger): SefariaAuthorTitles {
            val path = exportRoot.resolve(FILE_NAME)
            if (!Files.isRegularFile(path)) {
                logger.i { "No $FILE_NAME under $exportRoot — author names stay as the schemas give them" }
                return EMPTY
            }
            return runCatching {
                val bySlug = HashMap<String, List<String>>()
                json.parseToJsonElement(path.readText()).jsonArray.forEach { element ->
                    val record = element.jsonObject
                    val slug = record["slug"]?.stringOrNull()?.trim().orEmpty()
                    if (slug.isEmpty()) return@forEach
                    val hebrew = record["titles"]?.jsonArray.orEmpty()
                        .mapNotNull { it.jsonObject }
                        .filter { it["lang"]?.stringOrNull() == "he" }
                        .mapNotNull { it["text"]?.stringOrNull()?.trim() }
                        .filter { it.isNotEmpty() }
                        .distinct()
                    if (hebrew.isNotEmpty()) bySlug[slug] = hebrew
                }
                logger.i { "Loaded $FILE_NAME: ${bySlug.size} authors with Hebrew name forms" }
                SefariaAuthorTitles(bySlug)
            }.getOrElse { error ->
                logger.w(error) { "Failed to read $path — author names stay as the schemas give them" }
                EMPTY
            }
        }

        /** Nikud, bidi marks and quote variants differ between the two sources. */
        private fun normalize(raw: String): String =
            raw.replace(NIKUD, "")
                .replace('״', '"').replace('“', '"').replace('”', '"')
                .replace('׳', '\'').replace('‘', '\'').replace('’', '\'')
                .replace(BIDI, "")
                .replace(WHITESPACE, " ")
                .trim()

        private val NIKUD = Regex("[\\u0591-\\u05BD\\u05BF-\\u05C7]")
        private val BIDI = Regex("[\\u200E\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFEFF]")
        private val WHITESPACE = Regex("\\s+")
    }
}
