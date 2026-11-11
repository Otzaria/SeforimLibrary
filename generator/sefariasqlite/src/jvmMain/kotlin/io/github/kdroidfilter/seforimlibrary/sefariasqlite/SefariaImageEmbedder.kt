package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Duration
import java.util.Base64
import java.util.HexFormat
import java.util.concurrent.ExecutionException
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.io.path.isRegularFile

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
    // Cheap pre-filter; it also matches the bucket's storage.googleapis.com path.
    private const val URL_MARKER = "textimages.sefaria.org/"
    private const val USER_AGENT = "SeforimLibrary-SefariaImageEmbedder/1.0"
    private const val DOWNLOAD_PARALLELISM = 16
    internal const val MAX_IMAGE_BYTES = 5 * 1024 * 1024
    private val DOWNLOAD_TIMEOUT = Duration.ofSeconds(30)
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

    /** SHA-256 of the 666×367 PNG that NetFree serves in place of the requested image. */
    internal const val FILTER_PLACEHOLDER_SHA256 = "1a00712621da6964deac5b491a5532e15c8854c00de7fba692818e0e042db311"

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

    // Some books (Yahel Ohr) reference the same bucket through its GCS path;
    // without this those images stay remote and render as ❌.
    private const val URL_START = """https://(?:storage\.googleapis\.com/)?textimages\.sefaria\.org/"""

    // Regex used both to detect URLs up-front and to rewrite them inline.
    // Captures: (1) the full URL, (2) nothing — we replace the URL inside the
    // matched `<img ...>` tag so surrounding attributes are preserved.
    private val IMG_TAG_REGEX = Regex(
        "<img\\s+[^>]*src=[\"']($URL_START$URL_BODY)[\"'][^>]*/?>",
        RegexOption.IGNORE_CASE
    )

    @Volatile
    private var dataUriByUrl: Map<String, String>? = null

    /** Whether an image prefetch has completed in this process. */
    val enabled: Boolean get() = dataUriByUrl != null

    /**
     * Collect referenced images for this import and publish the resulting data URIs together.
     * Prefer the export, then the persistent cache, then the network for older or partial exports.
     * Images that fail all three paths stay as remote URLs and are recorded in [reportPath].
     */
    suspend fun prefetch(
        mergedJsonPaths: Collection<Path>,
        cacheDir: Path = defaultCacheDir(),
        reportPath: Path = defaultReportPath(),
        bundledDir: Path? = null,
        logger: Logger = Logger.withTag("SefariaImageEmbedder")
    ) = coroutineScope {
        val fallbackCache = cacheDir.takeUnless {
            bundledDir != null && runCatching { Files.isSameFile(bundledDir, cacheDir) }.getOrDefault(false)
        }
        logger.i { "Embedder: image cache at ${cacheDir.toAbsolutePath()}" }
        if (bundledDir != null) logger.i { "Embedder: images bundled with the export at ${bundledDir.toAbsolutePath()}" }

        val urls = collectUrls(mergedJsonPaths)
        logger.i { "Embedder: ${urls.size} unique Sefaria image URLs to process" }

        val semaphore = Semaphore(DOWNLOAD_PARALLELISM)
        val results = urls.map { url ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    runInterruptible { url to fetchImage(url, fallbackCache, bundledDir, logger) }
                }
            }
        }.awaitAll()

        val dataUris = HashMap<String, String>()
        val failures = ArrayList<ImageEmbedFailure>()
        val sourceCounts = IntArray(Source.entries.size)
        for ((url, result) in results) {
            when (result) {
                is Fetched.Image -> {
                    dataUris[url] = result.dataUri
                    sourceCounts[result.source.ordinal]++
                }
                is Fetched.Failure -> failures += ImageEmbedFailure(url, result.reason)
            }
        }
        dataUriByUrl = dataUris
        val report = ImageEmbedReport(
            attempted = urls.size,
            cached = dataUris.size,
            downloaded = sourceCounts[Source.NETWORK.ordinal],
            fromDiskCache = sourceCounts[Source.CACHE.ordinal],
            failed = failures.size,
            failures = failures.sortedBy { it.url },
            fromBundle = sourceCounts[Source.BUNDLE.ordinal],
        )
        writeReport(report, reportPath, logger)
        logger.i {
            "Embedder: cached=${report.cached} (fromBundle=${report.fromBundle}, " +
                "fromDiskCache=${report.fromDiskCache}, downloaded=${report.downloaded}, " +
                "failed=${report.failed}) of ${report.attempted}"
        }
        report.failures.forEach { logger.w { "Embedder: not embedded (${it.reason}): ${it.url}" } }
    }

    /**
     * Substitute every `<img src="https://textimages.sefaria.org/…">` with a
     * matching `data:` URI, assuming [prefetch] populated the cache.
     * Preserves any other attributes on the tag.
     */
    fun substituteImages(content: String): String {
        val dataUris = dataUriByUrl ?: return content
        if (dataUris.isEmpty() || !content.contains(URL_MARKER)) return content
        return IMG_TAG_REGEX.replace(content) { match ->
            val url = match.groupValues[1]
            val dataUri = dataUris[url] ?: return@replace match.value
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
        dataUriByUrl = null
        downloader = ::downloadBytes
        retryBackoffMillis = longArrayOf(2_000, 6_000)
    }

    /**
     * Seed the in-memory cache directly (tests only — lets us skip the network).
     */
    internal fun seedForTest(url: String, dataUri: String) {
        dataUriByUrl = dataUriByUrl.orEmpty() + (url to dataUri)
    }

    private fun collectUrls(mergedJsonPaths: Collection<Path>): Set<String> {
        val out = HashSet<String>()
        val bytePattern = URL_MARKER.toByteArray(Charsets.UTF_8)
        val textRegex = Regex("$URL_START$URL_BODY")
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

    private enum class Source { BUNDLE, CACHE, NETWORK }

    private sealed interface Fetched {
        data class Image(val dataUri: String, val source: Source) : Fetched
        data class Failure(val reason: String) : Fetched
    }

    private fun fetchImage(
        url: String,
        cacheDir: Path?,
        bundledDir: Path?,
        logger: Logger,
    ): Fetched {
        val key = cacheFileName(url)
        bundledDir?.resolve(key)?.let {
            readVerifiedEntry(url, it, Source.BUNDLE, logger)?.let { image -> return image }
        }
        val cachePath = cacheDir?.resolve(key)
        cachePath?.let {
            readVerifiedEntry(url, it, Source.CACHE, logger)?.let { image -> return image }
        }

        val bytes = try {
            downloadWithRetry(url, logger)
        } catch (error: InterruptedException) {
            throw error
        } catch (error: Exception) {
            return Fetched.Failure(failureReason(error))
        }
        return when (val validation = SefariaImageValidation.validate(bytes)) {
            is SefariaImageValidation.Invalid -> Fetched.Failure(validation.reason)
            is SefariaImageValidation.Valid -> {
                if (cacheDir != null && cachePath != null) {
                    runCatching { writeEntry(cacheDir, url, cachePath, bytes) }
                        .onFailure { logger.w(it) { "Could not cache downloaded image: $url" } }
                }
                Fetched.Image(toDataUri(validation.mime, bytes), Source.NETWORK)
            }
        }
    }

    private fun sidecarOf(path: Path): Path =
        path.resolveSibling(path.fileName.toString() + SIDECAR_SUFFIX)

    private fun evict(vararg paths: Path) {
        for (path in paths) runCatching { Files.deleteIfExists(path) }
    }

    /**
     * Match the exact UTF-8 URL sidecar before reading the payload. Legacy names
     * and orphaned blobs have no verified URL and cannot be hits.
     */
    private fun readVerifiedEntry(url: String, path: Path, source: Source, logger: Logger): Fetched.Image? {
        if (!path.isRegularFile()) return null
        val sidecar = sidecarOf(path)
        fun reject(reason: String): Fetched.Image? {
            logger.w { "Ignoring ${source.name.lowercase()} image at $path ($reason): $url" }
            if (source == Source.CACHE) evict(path, sidecar)
            return null
        }
        if (!sidecar.isRegularFile()) return reject("missing $SIDECAR_SUFFIX sidecar")
        val expected = url.toByteArray(Charsets.UTF_8)
        val owner = try {
            Files.newInputStream(sidecar).use { it.readNBytes(expected.size + 1) }
        } catch (_: IOException) {
            return reject("unreadable $SIDECAR_SUFFIX sidecar")
        }
        if (!owner.contentEquals(expected)) return reject("URL sidecar mismatch")
        val bytes = try {
            Files.newInputStream(path).use { it.readNBytes(MAX_IMAGE_BYTES + 1) }
        } catch (_: IOException) {
            return reject("unreadable payload")
        }
        return when (val validation = SefariaImageValidation.validate(bytes)) {
            is SefariaImageValidation.Invalid -> reject(validation.reason)
            is SefariaImageValidation.Valid -> Fetched.Image(toDataUri(validation.mime, bytes), source)
        }
    }

    private fun writeEntry(cacheDir: Path, url: String, cachePath: Path, bytes: ByteArray) {
        // Publish validated bytes before the sidecar so replacing an entry
        // cannot give the previous payload a new URL.
        atomicWrite(cacheDir, cachePath, bytes)
        atomicWrite(cacheDir, sidecarOf(cachePath), url.toByteArray(Charsets.UTF_8))
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
        error is OversizedImage -> "oversized (> $MAX_IMAGE_BYTES bytes)"
        error is IllegalArgumentException || error is URISyntaxException -> "malformed-url"
        else -> "${error.javaClass.simpleName}: ${error.message}"
    }

    private fun downloadWithRetry(url: String, logger: Logger): ByteArray {
        for (attempt in 1..DOWNLOAD_ATTEMPTS) {
            try {
                return downloader(url)
            } catch (error: IOException) {
                if (attempt == DOWNLOAD_ATTEMPTS) throw error
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

    private fun downloadBytes(url: String): ByteArray = downloadBytes(url, DOWNLOAD_TIMEOUT)

    internal fun downloadBytes(url: String, timeout: Duration): ByteArray {
        val request = HttpRequest.newBuilder(URI.create(encodeForRequest(url)))
            .timeout(timeout)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "image/*")
            .build()
        val pending = client.sendAsync(request) { response ->
            val status = response.statusCode()
            if (status !in 200..299 || status == 206) throw httpFailure(status, url)
            if (response.headers().firstValueAsLong("Content-Length").orElse(0) > MAX_IMAGE_BYTES) {
                throw OversizedImage()
            }
            ImageBodySubscriber()
        }
        // HttpRequest.timeout on JDK 25 covers the headers. Bound the full
        // response as well so a stalled body can be retried.
        return try {
            pending.get(timeout.toNanos(), TimeUnit.NANOSECONDS).body()
        } catch (_: TimeoutException) {
            pending.cancel(true)
            throw HttpTimeoutException("Image response timed out: $url")
        } catch (error: InterruptedException) {
            pending.cancel(true)
            throw error
        } catch (error: ExecutionException) {
            throw error.cause ?: error
        }
    }

    private class OversizedImage : IllegalStateException()

    private class ImageBodySubscriber(
        private val downstream: HttpResponse.BodySubscriber<ByteArray> = HttpResponse.BodySubscribers.ofByteArray(),
    ) : HttpResponse.BodySubscriber<ByteArray> by downstream {
        private lateinit var subscription: Flow.Subscription
        private var bytesReceived = 0L
        private var rejected = false

        override fun onSubscribe(subscription: Flow.Subscription) {
            this.subscription = subscription
            downstream.onSubscribe(subscription)
        }

        override fun onNext(items: List<ByteBuffer>) {
            if (rejected) return
            bytesReceived += items.sumOf { it.remaining().toLong() }
            if (bytesReceived <= MAX_IMAGE_BYTES) downstream.onNext(items) else reject(OversizedImage())
        }

        override fun onError(error: Throwable) {
            if (!rejected) downstream.onError(error)
        }

        override fun onComplete() {
            if (!rejected) downstream.onComplete()
        }

        private fun reject(error: Throwable) {
            rejected = true
            downstream.onError(error)
            subscription.cancel()
        }
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
     * comes from the validated payload, and
     * [SIDECAR_SUFFIX] is what keeps the directory greppable.
     */
    internal fun cacheFileName(url: String): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(url.toByteArray(Charsets.UTF_8))
        return HexFormat.of().formatHex(digest)
    }

    private fun toDataUri(mime: String, bytes: ByteArray): String =
        "data:$mime;base64,${Base64.getEncoder().encodeToString(bytes)}"
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
 * - [fromBundle]/[fromDiskCache]/[downloaded]: how those were obtained.
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
    val fromBundle: Int = 0,
)

private val imageEmbedReportJson = Json {
    prettyPrint = true
    encodeDefaults = true
}

/** JSON form written beside seforim.db for the workflow gate. */
internal fun ImageEmbedReport.toJsonReport(): String =
    imageEmbedReportJson.encodeToString(ImageEmbedReport.serializer(), this)
