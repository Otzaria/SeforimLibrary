package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.Path

internal enum class ExpectedState { PRESENT, ABSENT }

internal data class LinksRoot(val path: String, val expectedState: ExpectedState)

internal data class BootstrapFileRename(
    val from: String,
    val to: String,
    val localBookPath: String,
    val expectedDbTitle: String,
)

internal data class BootstrapRecordOverride(
    val path: String,
    val recordSha256: String,
    val postRecordSha256: String,
    val requireHeRef2: String,
    val ref2: String,
    val lineIndex2: Int,
)

internal data class ManualLinksConfig(
    val seforimToolRef: String,
    val linksRoots: List<LinksRoot>,
    val bootstrapAdapters: Map<String, String>,
    val heTitleAliases: Map<String, Map<String, String>>,
    val bootstrapFileRenames: List<BootstrapFileRename>,
    val bootstrapRecordOverrides: List<BootstrapRecordOverride>,
) {
    companion object {
        fun read(path: Path): ManualLinksConfig {
            val root = ManualLinksJson.readStrict(path).requireObject("config")
            require(root.requiredInt("schema_version") == 1) { "Unsupported manual_links_sync schema" }
            val linksRoots = root.requiredArray("links_roots").mapIndexed { index, node ->
                val item = node.requireObject("links_roots[$index]")
                val rawPath = item.requiredText("path")
                LinksRoot(
                    checkedRelativePath(rawPath),
                    when (item.requiredText("expected_state")) {
                        "present" -> ExpectedState.PRESENT
                        "absent" -> ExpectedState.ABSENT
                        else -> error("links_roots[$index].expected_state must be present or absent")
                    },
                )
            }
            require(linksRoots.map { it.path }.distinct().size == linksRoots.size) { "Duplicate links root" }

            val adaptersNode = root.requiredObject("bootstrap_adapters")
            val adapters = adaptersNode.fields().asSequence().associate { (key, value) ->
                checkedRelativePath(key) to value.requireText("bootstrap_adapters.$key")
            }

            // Otzaria titles are quote-stripped; this per-root map is the only bridge to Sefaria heTitles.
            val heTitleAliases = root.requiredObject("he_title_aliases").fields().asSequence().associate { (key, node) ->
                val rootPath = checkedRelativePath(key)
                require(linksRoots.any { it.path == rootPath }) { "he_title_aliases[$key] is not a declared links root" }
                val aliases = node.requireObject("he_title_aliases[$key]").fields().asSequence().associate { (title, value) ->
                    checkedBookTitle(title, "he_title_aliases[$key] key") to
                        checkedBookTitle(value.requireText("he_title_aliases[$key].$title"), "he_title_aliases[$key][$title]")
                }
                require(aliases.isNotEmpty()) { "he_title_aliases[$key] must not be empty" }
                aliases.forEach { (title, heTitle) ->
                    require(title != heTitle) { "he_title_aliases[$key][$title] must differ from the Otzaria title" }
                    require(heTitle !in aliases) { "he_title_aliases[$key][$title] must not chain into another alias" }
                }
                require(aliases.values.distinct().size == aliases.size) { "Duplicate Sefaria heTitle in he_title_aliases[$key]" }
                rootPath to aliases
            }

            val renames = root.requiredArray("bootstrap_file_renames").mapIndexed { index, node ->
                val item = node.requireObject("bootstrap_file_renames[$index]")
                BootstrapFileRename(
                    checkedRelativePath(item.requiredText("from")),
                    checkedRelativePath(item.requiredText("to")),
                    checkedRelativePath(item.requiredText("local_book_path")),
                    item.requiredText("expected_db_title"),
                )
            }

            val overrides = root.requiredArray("bootstrap_record_overrides").mapIndexed { index, node ->
                val item = node.requireObject("bootstrap_record_overrides[$index]")
                BootstrapRecordOverride(
                    path = checkedRelativePath(item.requiredText("path")),
                    recordSha256 = item.requiredSha256("record_sha256"),
                    postRecordSha256 = item.requiredSha256("post_record_sha256"),
                    requireHeRef2 = item.requiredText("require_heRef_2"),
                    ref2 = item.requiredText("ref_2"),
                    lineIndex2 = item.requiredInt("line_index_2"),
                )
            }
            require(overrides.map { it.path to it.recordSha256 }.distinct().size == overrides.size) {
                "Duplicate bootstrap override"
            }
            return ManualLinksConfig(
                seforimToolRef = root.requiredText("seforim_tool_ref"),
                linksRoots = linksRoots,
                bootstrapAdapters = adapters,
                heTitleAliases = heTitleAliases,
                bootstrapFileRenames = renames,
                bootstrapRecordOverrides = overrides,
            )
        }
    }
}

internal fun checkedBookTitle(value: String, location: String): String {
    require(value.isNotBlank() && value.trim() == value) { "$location must be a trimmed non-blank title" }
    require('/' !in value && '\\' !in value && '\u0000' !in value) { "$location must be a bare book title: $value" }
    return value
}

internal fun checkedRelativePath(value: String): String {
    require(value.isNotBlank() && '\u0000' !in value && '\\' !in value) { "Invalid repository-relative path: $value" }
    val path = Path.of(value)
    require(!path.isAbsolute && path.normalize().toString() == value && path.none { it.toString() == ".." }) {
        "Unsafe repository-relative path: $value"
    }
    return value
}

internal fun JsonNode.requireObject(location: String): ObjectNode =
    (this as? ObjectNode) ?: error("$location must be an object")

internal fun ObjectNode.requiredText(name: String): String =
    get(name)?.takeIf { it.isTextual }?.textValue()?.takeIf { it.isNotBlank() }
        ?: error("$name must be non-blank text")

internal fun JsonNode.requireText(location: String): String =
    takeIf { it.isTextual }?.textValue()?.takeIf { it.isNotBlank() } ?: error("$location must be non-blank text")

internal fun ObjectNode.requiredInt(name: String): Int =
    ManualLinksDocument.exactInt(get(name) ?: error("Missing $name"), name, allowZero = true)

internal fun ObjectNode.requiredSha256(name: String): String = requiredText(name).also {
    require(it.matches(Regex("[0-9a-f]{64}"))) { "$name must be lowercase SHA-256" }
}

internal fun ObjectNode.requiredObject(name: String): ObjectNode = get(name)?.requireObject(name) ?: error("Missing $name")

internal fun ObjectNode.requiredArray(name: String): List<JsonNode> =
    get(name)?.takeIf { it.isArray }?.toList() ?: error("$name must be an array")
