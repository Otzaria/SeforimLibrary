package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Path

/** One segment of a fingerprinted section, in the book's own order. */
internal data class FingerprintedRef(val ref: String, val hash: String)

/**
 * Text fingerprints of every Sefaria segment a manual link names (`ref_1` / `ref_2`), plus the
 * segment count of each one's section.
 *
 * Refs resolve by string alone, so a Sefaria re-segmentation retargets a link silently (19/11:
 * two segments were prepended to Rashi on Eruvin 17a:4). Comparing the stored fingerprints turns
 * that into a hard failure, while an in-place text edit is only a warning.
 */
internal data class ManualLinksRefFingerprints(
    val sections: Map<String, Int>,
    val refs: Map<String, String>,
) {
    /** Follows a Sefaria title rename exactly as the link records themselves do. */
    fun renamed(old: String, new: String): ManualLinksRefFingerprints =
        ManualLinksRefFingerprints(sections.renameKeys(old, new), refs.renameKeys(old, new))

    /** One entry per line, so a weekly diff stays as small as the change. */
    fun render(): ByteArray = buildString {
        append("{\n\"schema_version\": ").append(SCHEMA_VERSION).append(",\n")
        appendObject("sections", sections) { it.toString() }
        append(",\n")
        appendObject("refs", refs) { ManualLinksJson.mapper.writeValueAsString(it) }
        append("\n}\n")
    }.toByteArray(Charsets.UTF_8)

    private fun <V> StringBuilder.appendObject(name: String, values: Map<String, V>, render: (V) -> String) {
        append('"').append(name).append("\": {")
        values.keys.sorted().forEachIndexed { index, key ->
            append(if (index == 0) "\n" else ",\n")
            append(ManualLinksJson.mapper.writeValueAsString(key)).append(": ").append(render(values.getValue(key)))
        }
        append(if (values.isEmpty()) "}" else "\n}")
    }

    companion object {
        const val FILE_NAME = "manual_links_ref_fingerprints.json"
        private const val SCHEMA_VERSION = 1
        private const val HASH_LENGTH = 16
        private val HASH = Regex("[0-9a-f]{$HASH_LENGTH}")
        private val TAG = Regex("<[^>]*>")
        private val SPACE = Regex("\\s+")

        /** Markup and whitespace are normalised away: they change every week and never move a link. */
        fun hash(content: String): String = ManualLinksJson.sha256(
            content.replace(TAG, " ").replace(SPACE, " ").trim().toByteArray(Charsets.UTF_8),
        ).take(HASH_LENGTH)

        /** The ref's parent node: everything before its last `:` level, else before its last space. */
        fun sectionOf(ref: String): String = when {
            ':' in ref -> ref.substringBeforeLast(':')
            ' ' in ref -> ref.substringBeforeLast(' ')
            else -> ref
        }

        fun read(path: Path): ManualLinksRefFingerprints {
            val root = ManualLinksJson.readStrict(path)
            require(root.isObject && root.fieldNames().asSequence().toSet() == setOf("schema_version", "sections", "refs")) {
                "$FILE_NAME must be an object of {schema_version,sections,refs}"
            }
            require(root.get("schema_version").let { it.isIntegralNumber && it.intValue() == SCHEMA_VERSION }) {
                "unsupported $FILE_NAME schema_version"
            }
            val sections = root.get("sections").also { require(it.isObject) { "$FILE_NAME sections must be an object" } }
            val refs = root.get("refs").also { require(it.isObject) { "$FILE_NAME refs must be an object" } }
            val sectionCounts = sections.fields().asSequence().associate { (key, value) ->
                require(value.isIntegralNumber && value.intValue() > 0) { "$FILE_NAME section '$key' needs a positive count" }
                key to value.intValue()
            }
            val refHashes = refs.fields().asSequence().associate { (key, value) ->
                require(value.isTextual && HASH.matches(value.textValue())) { "$FILE_NAME ref '$key' needs a $HASH_LENGTH-hex hash" }
                require(sectionOf(key) in sectionCounts) { "$FILE_NAME ref '$key' has no section count" }
                key to value.textValue()
            }
            return ManualLinksRefFingerprints(sectionCounts, refHashes)
        }

        private fun <V> Map<String, V>.renameKeys(old: String, new: String): Map<String, V> {
            val result = LinkedHashMap<String, V>(size)
            forEach { (key, value) ->
                val renamed = ManualLinksRefresh.rewriteAtTitleBoundary(key, old, new) ?: key
                require(result.put(renamed, value) == null) { "$FILE_NAME rename '$old' -> '$new' collides on '$renamed'" }
            }
            return result
        }
    }
}
