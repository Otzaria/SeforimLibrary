package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Duration
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.isRegularFile
import kotlin.io.path.readBytes

/**
 * Downloads `textimages.sefaria.org` images referenced from book content and
 * keeps an in-memory + on-disk cache of the corresponding `data:image/(mime);base64,...`
 * URIs.
 *
 * Once populated, [cleanSefariaLine] can use [substituteImages] to inline the
 * base64 payloads so the resulting SQLite DB is self-contained (no runtime
 * network dependency on textimages.sefaria.org, whose images render as broken
 * ❌ placeholders in offline use — see issue 392).
 *
 * Workflow:
 *   1. [prefetch] — scan all merged.json paths once, collect unique image URLs,
 *      download anything not yet cached, populate the in-memory map.
 *   2. [substituteImages] — pure function, rewrites `<img src="…">` occurrences
 *      to `<img src="data:…">`; called from [cleanSefariaLine].
 */
object SefariaImageEmbedder {
    private const val URL_PREFIX = "https://textimages.sefaria.org/"
    private const val USER_AGENT = "SeforimLibrary-SefariaImageEmbedder/1.0"
    private const val DOWNLOAD_PARALLELISM = 16
    private const val MAX_IMAGE_BYTES = 5 * 1024 * 1024 // 5 MiB ceiling per image
    private const val DOWNLOAD_ATTEMPTS = 3

    /** `-PimageCacheDir=` / `-DimageCacheDir=`: where downloaded images persist. */
    internal const val CACHE_DIR_PROPERTY = "imageCacheDir"
    internal const val CACHE_DIR_ENV = "SEFORIM_IMAGE_CACHE_DIR"

    /** `-PimageEmbedReport=` / `-DimageEmbedReport=`: where [ImageEmbedReport] lands. */
    internal const val REPORT_PROPERTY = "imageEmbedReport"
    internal const val REPORT_ENV = "SEFORIM_IMAGE_EMBED_REPORT"

    /**
     * Sidecar written beside every cached image: `<key>.url`, holding the URL
     * that produced `<key>` verbatim (UTF-8, no trailing newline). It is what
     * makes a cache entry self-describing — see [cacheFileName].
     */
    internal const val SIDECAR_SUFFIX = ".url"

    private const val HEX = "0123456789abcdef"

    /**
     * Pause before download attempt 2 and 3. A transient timeout on
     * textimages.sefaria.org (10 of 420 images timed out at 30 s in build
     * 34021998271 while every one of them downloaded a day earlier) turns
     * into a remote-URL line, which then differs from the previous build's
     * inline data URI and breaks relink recovery. Tests zero this.
     */
    internal var retryBackoffMillis: LongArray = longArrayOf(2_000, 6_000)

    /** Injectable for tests; production downloads over HTTP. */
    internal var downloader: (String) -> ByteArray = ::downloadBytes

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build()
    }

    // How far a Sefaria image URL runs — ONE definition, shared by the up-front
    // scan ([collectUrls]) and the rewrite ([IMG_TAG_REGEX]). The two must agree:
    // a path may contain spaces (…/Screenshot 2023-05-04 at 3.09.20 PM.png), and
    // a scan that stopped at whitespace keyed the cache on a truncated URL the
    // rewrite could never look up — so that image stayed remote even when it
    // downloaded. Stops at the attribute quote, at merged.json's `\"` escape,
    // and at any line break.
    private const val URL_BODY = """[^"'<>\\\n\r\t]+"""

    // Regex used both to detect URLs up-front and to rewrite them inline.
    // Captures: (1) the full URL, (2) nothing — we replace the URL inside the
    // matched `<img ...>` tag so surrounding attributes are preserved.
    private val IMG_TAG_REGEX = Regex(
        "<img\\s+[^>]*src=[\"'](https://textimages\\.sefaria\\.org/$URL_BODY)[\"'][^>]*/?>",
        RegexOption.IGNORE_CASE
    )

    private val dataUriByUrl = ConcurrentHashMap<String, String>()

    /**
     * Set by the importer at startup. When null, [substituteImages] is a no-op
     * and the original URLs are kept (so tests/debug runs don't require network).
     */
    @Volatile
    var enabled: Boolean = false
        private set

    /**
     * Scan [mergedJsonPaths], extract unique Sefaria image URLs, download any
     * that aren't already in [cacheDir], and populate the in-memory data-URI map.
     * Safe to call multiple times — already-cached entries are reused.
     * Writes [reportPath] on every run: whatever did not download ships as a
     * live network URL, so the build has to be able to see it.
     */
    suspend fun prefetch(
        mergedJsonPaths: Collection<Path>,
        cacheDir: Path = defaultCacheDir(),
        reportPath: Path = defaultReportPath(),
        logger: Logger = Logger.withTag("SefariaImageEmbedder")
    ) = coroutineScope {
        Files.createDirectories(cacheDir)
        logger.i { "Embedder: image cache at ${cacheDir.toAbsolutePath()}" }

        val urls = collectUrls(mergedJsonPaths)
        logger.i { "Embedder: ${urls.size} unique Sefaria image URLs to process" }

        val semaphore = Semaphore(DOWNLOAD_PARALLELISM)
        // Counted from DOWNLOAD_PARALLELISM coroutines: plain `Int++` loses
        // increments (34024655297 printed cached=410 with downloaded=409 on a
        // cold cache), and these numbers now gate the build.
        val newlyDownloaded = AtomicInteger()
        val fromDiskCache = AtomicInteger()
        val failures = ConcurrentHashMap<String, String>()

        urls.map { url ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    when (val fetched = ensureCachedBytes(url, cacheDir, logger)) {
                        is Fetched.Bytes -> {
                            if (fetched.fromNetwork) newlyDownloaded.incrementAndGet()
                            else fromDiskCache.incrementAndGet()
                            dataUriByUrl[url] = toDataUri(url, fetched.bytes)
                        }
                        is Fetched.Failure -> failures[url] = fetched.reason
                    }
                }
            }
        }.awaitAll()

        enabled = true
        val report = ImageEmbedReport(
            attempted = urls.size,
            cached = dataUriByUrl.size,
            downloaded = newlyDownloaded.get(),
            fromDiskCache = fromDiskCache.get(),
            failed = failures.size,
            // Sorted so two runs over the same export produce identical bytes.
            failures = failures.entries.sortedBy { it.key }.map { ImageEmbedFailure(it.key, it.value) },
        )
        writeReport(report, reportPath, logger)
        logger.i {
            "Embedder: cached=${report.cached} (downloaded=${report.downloaded}, " +
                "fromDiskCache=${report.fromDiskCache}, failed=${report.failed}) of ${report.attempted}"
        }
        // One line per failure, no stack trace: the frames are always the same
        // HttpClient chain, and every one of these ships as a remote <img src>.
        report.failures.forEach { logger.w { "Embedder: not embedded (${it.reason}): ${it.url}" } }
    }

    /**
     * Substitute every `<img src="https://textimages.sefaria.org/…">` with a
     * matching `data:` URI, assuming [prefetch] populated the cache.
     * Preserves any other attributes on the tag.
     */
    fun substituteImages(content: String): String {
        if (!enabled || !content.contains(URL_PREFIX)) return content
        return IMG_TAG_REGEX.replace(content) { match ->
            val url = match.groupValues[1]
            val dataUri = dataUriByUrl[url] ?: return@replace match.value
            match.value.replace(url, dataUri)
        }
    }

    /**
     * The on-disk image cache. Configured through the `imageCacheDir` system
     * property (gradle `-PimageCacheDir=`) or `SEFORIM_IMAGE_CACHE_DIR`; the
     * fallback lives under the build dir, so on the CI runner (tmpfs build dir)
     * it is lost with every build. A durable directory makes the embedded
     * images identical across builds of the same export, which the relink
     * recovery comparison depends on.
     */
    internal fun defaultCacheDir(): Path {
        val configured = System.getProperty(CACHE_DIR_PROPERTY)?.takeIf { it.isNotBlank() }
            ?: System.getenv(CACHE_DIR_ENV)?.takeIf { it.isNotBlank() }
        return configured?.let { Paths.get(it) } ?: Paths.get("build", "sefaria", "image-cache")
    }

    /**
     * Where the [ImageEmbedReport] is written. Configured through the
     * `imageEmbedReport` system property (gradle `-PimageEmbedReport=`) or
     * `SEFORIM_IMAGE_EMBED_REPORT`; CI points it beside the DB output and gates
     * the build on it, a local run gets it under the build dir.
     */
    internal fun defaultReportPath(): Path {
        val configured = System.getProperty(REPORT_PROPERTY)?.takeIf { it.isNotBlank() }
            ?: System.getenv(REPORT_ENV)?.takeIf { it.isNotBlank() }
        return configured?.let { Paths.get(it) }
            ?: Paths.get("build", "sefaria", "image-embed-report.json")
    }

    /**
     * Written atomically (temp file + ATOMIC_MOVE) so the gate never reads a
     * half-written report. A write failure is not worth aborting a 40-minute
     * import over — the gate fails the build on the missing file instead.
     */
    private fun writeReport(report: ImageEmbedReport, reportPath: Path, logger: Logger) {
        runCatching {
            val dir = reportPath.toAbsolutePath().parent
            Files.createDirectories(dir)
            val tmp = Files.createTempFile(dir, "image-embed-report", ".json.tmp")
            Files.writeString(tmp, report.toJsonReport())
            Files.move(tmp, reportPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            logger.i { "Embedder: report written to ${reportPath.toAbsolutePath()}" }
        }.onFailure { logger.w(it) { "Failed to write image-embed report to $reportPath" } }
    }

    /**
     * Drop all in-memory cache state (does not touch disk cache). Mostly for tests.
     */
    internal fun resetForTest() {
        dataUriByUrl.clear()
        enabled = false
        downloader = ::downloadBytes
        retryBackoffMillis = longArrayOf(2_000, 6_000)
    }

    /**
     * Seed the in-memory cache directly (tests only — lets us skip the network).
     */
    internal fun seedForTest(url: String, dataUri: String) {
        dataUriByUrl[url] = dataUri
        enabled = true
    }

    private fun collectUrls(mergedJsonPaths: Collection<Path>): Set<String> {
        val out = HashSet<String>()
        val bytePattern = URL_PREFIX.toByteArray(Charsets.UTF_8)
        val textRegex = Regex("https://textimages\\.sefaria\\.org/$URL_BODY")
        for (p in mergedJsonPaths) {
            val bytes = runCatching { Files.readAllBytes(p) }.getOrNull() ?: continue
            if (indexOfSubSequence(bytes, bytePattern) < 0) continue
            val text = bytes.toString(Charsets.UTF_8)
            for (m in textRegex.findAll(text)) {
                out += stripTrailingJsonArtifacts(m.value)
            }
        }
        return out
    }

    private fun stripTrailingJsonArtifacts(url: String): String {
        // Sefaria JSON sometimes embeds URLs ending with a stray backslash or
        // punctuation from the surrounding content. Keep only characters that
        // are legal in a URL path — including the trailing space [URL_BODY] now
        // admits so that spaced paths survive the scan intact.
        return url.trimEnd(' ', '\\', '\'', '"', ',', '.', ';', ')', ']', '}')
    }

    private fun indexOfSubSequence(haystack: ByteArray, needle: ByteArray): Int {
        if (needle.isEmpty() || haystack.size < needle.size) return -1
        outer@ for (i in 0..(haystack.size - needle.size)) {
            for (j in needle.indices) {
                if (haystack[i + j] != needle[j]) continue@outer
            }
            return i
        }
        return -1
    }

    /** What one URL produced: bytes to inline, or why it stays a remote URL. */
    private sealed interface Fetched {
        data class Bytes(
            val bytes: ByteArray,
            val fromNetwork: Boolean,
        ) : Fetched
        data class Failure(val reason: String) : Fetched
    }

    /**
     * A hit requires an entry at `sha256(url)` whose `.url` sidecar holds this
     * exact URL — there is no other way into the cache. In particular a file
     * left over from the pre-sha256 naming scheme (path with `/ : ?` folded to
     * `_`) is NOT adopted: that scheme was not injective (`…/Tikkunei_Zohar/40.png`
     * and `…/Tikkunei_Zohar_40.png` both wrote `Tikkunei_Zohar_40.png`) and the
     * file records no URL, so adopting it would inline one URL's image under
     * another URL's key, durably and reported as a success. Such a file is
     * simply ignored and left where it is — nothing reads it, and deleting
     * files this build did not write in a cache dir that may be shared with a
     * second checkout is the more dangerous option.
     */
    private fun ensureCachedBytes(
        url: String,
        cacheDir: Path,
        logger: Logger,
    ): Fetched {
        val cachePath = cacheDir.resolve(cacheFileName(url))
        val sidecarPath = sidecarOf(cachePath)

        readVerifiedEntry(url, cachePath, sidecarPath, logger)?.let {
            return Fetched.Bytes(it, fromNetwork = false)
        }

        val bytes = runCatching { downloadWithRetry(url, logger) }.getOrElse {
            // Reported once, by the caller, with the URL — see [failureReason].
            return Fetched.Failure(failureReason(it))
        }
        if (bytes.size > MAX_IMAGE_BYTES) {
            return Fetched.Failure("oversized (${bytes.size} bytes)")
        }
        writeEntry(cacheDir, url, cachePath, sidecarPath, bytes)
        return Fetched.Bytes(bytes, fromNetwork = true)
    }

    /** `<key>.url` for `<key>` — see [SIDECAR_SUFFIX]. */
    private fun sidecarOf(path: Path): Path =
        path.resolveSibling(path.fileName.toString() + SIDECAR_SUFFIX)

    private fun readSidecar(sidecarPath: Path): String? =
        runCatching { Files.readString(sidecarPath) }.getOrNull()

    private fun evict(vararg paths: Path) {
        for (path in paths) runCatching { Files.deleteIfExists(path) }
    }

    /**
     * A usable hit at the current name, or null (with the unusable entry
     * evicted). The name is already sha256(url), so the sidecar is not what
     * separates two URLs — it is what makes an entry that did NOT come from this
     * URL (a hand-copied cache dir, a blob dropped in by another tool, a torn
     * write) a miss instead of the wrong image reported as a success. It is also
     * the ONLY way in: no name-shaped heuristic ever adopts bytes.
     */
    private fun readVerifiedEntry(url: String, cachePath: Path, sidecarPath: Path, logger: Logger): ByteArray? {
        if (!cachePath.isRegularFile()) return null
        val owner = readSidecar(sidecarPath)
        if (owner != url) {
            logger.w {
                if (owner == null) "Cached image at $cachePath has no $SIDECAR_SUFFIX sidecar; redownloading: $url"
                else "Cached image at $cachePath was written for another URL; redownloading: $url"
            }
            evict(cachePath, sidecarPath)
            return null
        }
        val bytes = runCatching { cachePath.readBytes() }.getOrElse {
            logger.w(it) { "Corrupted cached image at $cachePath; redownloading" }
            evict(cachePath, sidecarPath)
            return null
        }
        if (bytes.isEmpty()) {
            logger.w { "Empty cached image at $cachePath; redownloading" }
            evict(cachePath, sidecarPath)
            return null
        }
        return bytes
    }

    /**
     * Sidecar first, bytes second: a crash in between leaves an orphan `.url`
     * (harmless — the next run finds no bytes and downloads) instead of bytes no
     * reader is allowed to trust. Both land through [atomicWrite].
     */
    private fun writeEntry(cacheDir: Path, url: String, cachePath: Path, sidecarPath: Path, bytes: ByteArray) {
        atomicWrite(cacheDir, sidecarPath, url.toByteArray(Charsets.UTF_8))
        atomicWrite(cacheDir, cachePath, bytes)
    }

    /**
     * Unique temp file in the cache dir + ATOMIC_MOVE. The temp name is unique
     * (not `<target>.part`) because two builds can share the runner's durable
     * cache, and two writers on one `.part` interleave into a corrupt image that
     * the move then publishes.
     */
    private fun atomicWrite(cacheDir: Path, target: Path, bytes: ByteArray) {
        Files.createDirectories(cacheDir)
        val tmp = Files.createTempFile(cacheDir, target.fileName.toString(), ".part")
        try {
            Files.write(tmp, bytes)
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (error: IOException) {
            // A unique temp name cannot be reused by the next attempt, so a
            // failed write has to take its own file with it (the cache dir has
            // no pruning of any kind).
            evict(tmp)
            throw error
        }
    }

    /** A status the server may answer differently in a moment (408, 429, 5xx). */
    internal class RetriableHttpStatus(val status: Int, url: String) : IOException("HTTP $status for $url")

    /** A definitive answer (404, the content filter's 418, ...): retrying only delays the build. */
    internal class FinalHttpStatus(val status: Int, url: String) : IllegalStateException("HTTP $status for $url")

    internal fun httpFailure(status: Int, url: String): Throwable =
        if (status == 408 || status == 429 || status >= 500) RetriableHttpStatus(status, url)
        else FinalHttpStatus(status, url)

    /** Timeouts (HttpTimeoutException is an IOException), resets, and [RetriableHttpStatus]. */
    internal fun isRetriable(error: Throwable): Boolean = error is IOException

    /**
     * The short reason string [ImageEmbedReport] carries and the workflow turns
     * into one `::warning::` per URL. Kept stable and greppable — a 418 is the
     * self-hosted runner's content filter answering, never Sefaria (see the
     * NetFree pattern), and that reads very differently from a real 404.
     */
    internal fun failureReason(error: Throwable): String = when {
        error is FinalHttpStatus && error.status == 418 ->
            "content-filter (HTTP 418 — NetFree on the self-hosted runner)"
        error is FinalHttpStatus -> "http ${error.status}"
        error is RetriableHttpStatus -> "http ${error.status}"
        error is HttpTimeoutException -> "timeout"
        error is IllegalArgumentException || error is URISyntaxException -> "malformed-url"
        else -> "${error.javaClass.simpleName}: ${error.message}"
    }

    private fun downloadWithRetry(url: String, logger: Logger): ByteArray {
        for (attempt in 1..DOWNLOAD_ATTEMPTS) {
            try {
                return downloader(url)
            } catch (error: Throwable) {
                if (error is InterruptedException) throw error
                if (!isRetriable(error) || attempt == DOWNLOAD_ATTEMPTS) throw error
                val backoff = retryBackoffMillis.getOrElse(attempt - 1) { retryBackoffMillis.last() }
                logger.w {
                    "Image download attempt $attempt/$DOWNLOAD_ATTEMPTS failed " +
                        "(${error.javaClass.simpleName}: ${error.message}); retrying in $backoff ms: $url"
                }
                if (backoff > 0) Thread.sleep(backoff)
            }
        }
        throw IllegalStateException("unreachable: $url")
    }

    /**
     * Characters a [URI] refuses verbatim: a raw space (Sefaria has
     * `…/Screenshot 2023-05-04 at 3.09.20 PM.png`), the RFC 3986 "unwise" set,
     * and anything non-ASCII. `%` is deliberately absent — most Sefaria paths
     * arrive already encoded (`image%20-%200019.png`) and must not be encoded
     * twice.
     */
    private const val REQUEST_UNSAFE_CHARS = " \"<>\\^`{|}"

    /**
     * The wire form of [url]. Encoding happens HERE and nowhere else: the
     * cache map stays keyed on the URL exactly as it appears in the content,
     * which is what [substituteImages] looks up.
     */
    internal fun encodeForRequest(url: String): String {
        val out = StringBuilder(url.length)
        for (byte in url.toByteArray(Charsets.UTF_8)) {
            val code = byte.toInt() and 0xFF
            if (code in 0x21..0x7E && code.toChar() !in REQUEST_UNSAFE_CHARS) out.append(code.toChar())
            else out.append(String.format("%%%02X", code))
        }
        return out.toString()
    }

    private fun downloadBytes(url: String): ByteArray {
        val request = HttpRequest.newBuilder(URI.create(encodeForRequest(url)))
            .timeout(Duration.ofSeconds(30))
            .header("User-Agent", USER_AGENT)
            .header("Accept", "image/*")
            .build()
        val response = client.send(request, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() !in 200..299) {
            throw httpFailure(response.statusCode(), url)
        }
        return response.body()
    }

    /**
     * `sha256(url)`, lowercase hex. The name is a digest of the WHOLE URL, so
     * two URLs can no longer share one file: the pre-sha256 name (the URL suffix
     * with `/ : ?` folded to `_`) mapped both `a/b.png` and `a_b.png` onto
     * `a_b.png`, and a hit is served without a request, so the wrong image was
     * embedded, kept across builds (the cache is durable, has no TTL and stores
     * no provenance) and counted as a success.
     * The fixed 64 chars also retire the >255-byte name a deep path threw on and
     * the `* " < > |` a Windows run choked on. No extension: the data-URI MIME
     * comes from the requested URL ([toDataUri]), never from the cache file, and
     * [SIDECAR_SUFFIX] is what keeps the directory greppable.
     */
    internal fun cacheFileName(url: String): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(url.toByteArray(Charsets.UTF_8))
        val out = StringBuilder(digest.size * 2)
        for (byte in digest) {
            val code = byte.toInt() and 0xFF
            out.append(HEX[code ushr 4]).append(HEX[code and 0x0F])
        }
        return out.toString()
    }

    private fun toDataUri(url: String, bytes: ByteArray): String {
        val mime = when {
            url.endsWith(".png", ignoreCase = true) -> "image/png"
            url.endsWith(".jpg", ignoreCase = true) || url.endsWith(".jpeg", ignoreCase = true) -> "image/jpeg"
            url.endsWith(".gif", ignoreCase = true) -> "image/gif"
            url.endsWith(".svg", ignoreCase = true) -> "image/svg+xml"
            url.endsWith(".webp", ignoreCase = true) -> "image/webp"
            else -> "image/png"
        }
        val b64 = Base64.getEncoder().encodeToString(bytes)
        return "data:$mime;base64,$b64"
    }
}

/** One URL that could not be inlined, and why — see [SefariaImageEmbedder.failureReason]. */
@kotlinx.serialization.Serializable
internal data class ImageEmbedFailure(val url: String, val reason: String)

/**
 * Machine-readable outcome of one [SefariaImageEmbedder.prefetch], written
 * beside the DB output so the build can gate on it (see the "Generate Seforim
 * Database" step in manual-generate-release.yml). Every entry in [failures]
 * ships as a live `https://textimages.sefaria.org/…` `<img src>` — a broken
 * image for an offline client — and an INFO count gated nothing: 10 of 420
 * went out that way in v27.
 *
 * - [attempted]: unique URLs found in the export.
 * - [cached]: URLs with a data URI in hand, i.e. actually embeddable.
 * - [downloaded]/[fromDiskCache]: how those were obtained.
 *
 * Shape is deterministic (declared key order; [failures] sorted by URL).
 */
@kotlinx.serialization.Serializable
internal data class ImageEmbedReport(
    val attempted: Int,
    val cached: Int,
    val downloaded: Int,
    val fromDiskCache: Int,
    val failed: Int,
    val failures: List<ImageEmbedFailure>,
)

private val imageEmbedReportJson = Json { prettyPrint = true }

/** JSON form written beside seforim.db for the workflow gate. */
internal fun ImageEmbedReport.toJsonReport(): String =
    imageEmbedReportJson.encodeToString(ImageEmbedReport.serializer(), this)
