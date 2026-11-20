package io.github.kdroidfilter.seforimlibrary.common.fordb

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipInputStream
import kotlin.io.path.exists

private const val FOR_DB_POINTER_URL =
    "https://raw.githubusercontent.com/Otzaria/otzaria-library/main/fordb_latest_pointer.json"
private const val FOR_DB_RELEASE_API_PREFIX =
    "https://api.github.com/repos/Otzaria/otzaria-library/releases/tags/"
private const val FOR_DB_ARCHIVE_NAME = "fordb_latest.zip"
private const val FOR_DB_USER_AGENT = "SeforimLibrary-ForDBFetcher/1.0"
// When set, every ForDB post-process reads this exact local archive instead of
// resolving the repository pointer, so the three separate JVM processes
// (renameCategories/seedGenerations/seedAllMetadata) consume byte-identical
// inputs. The release build pins it; -PforDbSha256 must accompany it.
const val FOR_DB_ARCHIVE_PROPERTY = "forDbArchive"
const val FOR_DB_SHA256_PROPERTY = "forDbSha256"
private val FOR_DB_TEXT_EXTENSIONS = setOf("csv", "json", "txt")

private var cachedForDbFiles: Map<String, List<String>>? = null

/**
 * Every text file directly under `ForDB/` in the ForDB archive, by name: the pinned local
 * archive when -P[FOR_DB_ARCHIVE_PROPERTY] is set, otherwise the immutable release that
 * `fordb_latest_pointer.json` selects. Cached per process; any digest check failure throws.
 */
fun forDbArchiveFiles(logger: Logger): Map<String, List<String>> {
    cachedForDbFiles?.let { return it }
    val pinnedArchive = System.getProperty(FOR_DB_ARCHIVE_PROPERTY)?.takeIf { it.isNotBlank() }
    val files = if (pinnedArchive != null) {
        readPinnedForDbArchive(Paths.get(pinnedArchive), logger)
    } else {
        downloadForDbArchive(logger)
    }
    cachedForDbFiles = files
    return files
}

/** [fileName] from [forDbArchiveFiles]; an absent file fails the build by name. */
fun requiredForDbArchiveFile(fileName: String, logger: Logger): List<String> =
    forDbArchiveFiles(logger)[fileName] ?: error("ForDB release archive is missing required file $fileName")

/**
 * Reads ForDB files from a pinned local archive whose SHA-256 must equal the
 * digest supplied via -P[FOR_DB_SHA256_PROPERTY]. The release build takes this
 * path: the archive is downloaded once and its digest pinned by the dispatcher,
 * so every post-process JVM consumes byte-identical inputs and the build stays a
 * pure function of its inputs. Fails loudly on a missing digest or any mismatch —
 * it never silently falls back to the network.
 */
private fun readPinnedForDbArchive(archive: Path, logger: Logger): Map<String, List<String>> {
    val expectedSha = System.getProperty(FOR_DB_SHA256_PROPERTY)?.takeIf { it.isNotBlank() }?.lowercase()
        ?: throw IllegalStateException(
            "-P$FOR_DB_ARCHIVE_PROPERTY was given without -P$FOR_DB_SHA256_PROPERTY; " +
                "a pinned archive must carry its expected SHA-256"
        )
    check(archive.exists()) { "Pinned ForDB archive not found at $archive" }
    val bytes = archive.toFile().readBytes()
    val actualSha = sha256Hex(bytes)
    check(actualSha == expectedSha) {
        "Pinned ForDB archive SHA-256 mismatch at $archive: expected $expectedSha, got $actualSha"
    }
    logger.i { "Reading pinned ForDB archive $archive (sha256=$actualSha)" }
    return ByteArrayInputStream(bytes).use { readForDbZip(it) }
}

private fun downloadForDbArchive(logger: Logger): Map<String, List<String>> {
    val download = forDbReleaseArchive(logger)
    logger.i { "Downloading immutable ForDB archive ${download.tag} from ${download.url}" }
    return try {
        val bytes = OptimizedHttpClient.downloadStream(
            url = download.url,
            userAgent = FOR_DB_USER_AGENT,
            logger = logger
        ).stream.use { it.readBytes() }
        val actualSha = sha256Hex(bytes)
        check(actualSha == download.sha256) {
            "Immutable ForDB archive SHA-256 mismatch: expected ${download.sha256}, got $actualSha"
        }
        ByteArrayInputStream(bytes).use(::readForDbZip)
    } catch (e: Exception) {
        logger.e(e) { "Failed to download or read required ForDB release archive; aborting" }
        throw IllegalStateException("Failed to load required ForDB release archive", e)
    }
}

private fun sha256Hex(bytes: ByteArray): String {
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
    val sb = StringBuilder(digest.size * 2)
    for (b in digest) {
        val v = b.toInt() and 0xff
        sb.append("0123456789abcdef"[v ushr 4])
        sb.append("0123456789abcdef"[v and 0xf])
    }
    return sb.toString()
}

private data class ForDbDownload(val tag: String, val sha256: String, val url: String)

private fun forDbReleaseArchive(logger: Logger): ForDbDownload {
    val pointerBody = OptimizedHttpClient.fetchJson(FOR_DB_POINTER_URL, FOR_DB_USER_AGENT, logger)
    val pointerElement = Json.parseToJsonElement(pointerBody)
    check(
        pointerBody == Json.encodeToString(
            kotlinx.serialization.json.JsonElement.serializer(),
            pointerElement,
        ) + "\n"
    ) { "ForDB pointer must be canonical JSON with one trailing LF (duplicates are forbidden)" }
    val pointer = pointerElement.jsonObject
    check(
        pointer.keys == setOf(
            "schema_version", "tag", "asset", "sha256", "provenance_asset", "provenance_sha256"
        )
    ) { "ForDB pointer has an unexpected key set" }
    check(pointer.getValue("schema_version").jsonPrimitive.intOrNull == 1) {
        "Unsupported ForDB pointer schema"
    }
    fun requiredString(key: String): String {
        val primitive = pointer.getValue(key).jsonPrimitive
        check(primitive.isString) { "ForDB pointer $key must be a JSON string" }
        return primitive.content
    }
    val tag = requiredString("tag")
    val assetName = requiredString("asset")
    val expectedSha = requiredString("sha256")
    val provenanceAsset = requiredString("provenance_asset")
    val provenanceSha = requiredString("provenance_sha256")
    check(assetName == FOR_DB_ARCHIVE_NAME) { "ForDB pointer names an unexpected archive" }
    check(provenanceAsset == "fordb_provenance.json") { "ForDB pointer names an unexpected provenance asset" }
    check(expectedSha.matches(Regex("[0-9a-f]{64}"))) { "ForDB pointer archive digest is invalid" }
    check(provenanceSha.matches(Regex("[0-9a-f]{64}"))) { "ForDB pointer provenance digest is invalid" }
    check(tag == "fordb-sha256-$expectedSha") { "ForDB pointer tag does not match its archive digest" }

    val releaseBody = OptimizedHttpClient.fetchJson(
        FOR_DB_RELEASE_API_PREFIX + tag,
        FOR_DB_USER_AGENT,
        logger,
    )
    val release = Json.parseToJsonElement(releaseBody).jsonObject
    check(release.getValue("tag_name").jsonPrimitive.content == tag) {
        "GitHub returned a different ForDB release tag"
    }
    val assets = release.getValue("assets").jsonArray
    val matches = assets.filter {
        it.jsonObject["name"]?.jsonPrimitive?.content == FOR_DB_ARCHIVE_NAME
    }
    check(matches.size == 1) { "Immutable ForDB release must contain exactly one $FOR_DB_ARCHIVE_NAME asset" }
    val url = matches.single().jsonObject.getValue("browser_download_url").jsonPrimitive
    check(url.isString && url.content.startsWith("https://github.com/Otzaria/otzaria-library/releases/download/")) {
        "Immutable ForDB release returned an unexpected asset URL"
    }
    return ForDbDownload(tag, expectedSha, url.content)
}

internal fun readForDbZip(stream: InputStream): Map<String, List<String>> {
    val files = mutableMapOf<String, List<String>>()
    ZipInputStream(stream).use { zip ->
        var entry = zip.nextEntry
        while (entry != null) {
            if (!entry.isDirectory) {
                val normalizedName = entry.name.replace('\\', '/')
                if (normalizedName.startsWith("ForDB/")) {
                    val fileName = normalizedName.removePrefix("ForDB/")
                    // Text assets directly under ForDB/: CSV rules, all_metadata.json, title_acronyms.txt.
                    if ('/' !in fileName && fileName.substringAfterLast('.') in FOR_DB_TEXT_EXTENSIONS) {
                        val bytes = ByteArrayOutputStream()
                        zip.copyTo(bytes)
                        files[fileName] = ByteArrayInputStream(bytes.toByteArray())
                            .bufferedReader(StandardCharsets.UTF_8)
                            .readLines()
                    }
                }
            }
            zip.closeEntry()
            entry = zip.nextEntry
        }
    }
    return files
}
