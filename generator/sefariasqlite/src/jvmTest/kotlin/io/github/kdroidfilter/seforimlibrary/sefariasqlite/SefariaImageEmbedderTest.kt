package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.net.http.HttpTimeoutException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.writeText
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Unit test for the in-memory replacement path of [SefariaImageEmbedder].
 * Network downloading + disk caching is covered end-to-end by the full
 * importer run (see SefariaDirectImporter.import).
 */
class SefariaImageEmbedderTest {
    @BeforeTest fun reset() = SefariaImageEmbedder.resetForTest()
    @AfterTest fun clean() {
        SefariaImageEmbedder.resetForTest()
        System.clearProperty(SefariaImageEmbedder.CACHE_DIR_PROPERTY)
        System.clearProperty(SefariaImageEmbedder.REPORT_PROPERTY)
    }

    private fun mergedJsonWith(vararg urls: String): Pair<Path, Path> {
        val dir = Files.createTempDirectory("image-cache-test")
        return dir to mergedJsonIn(dir, "merged.json", *urls)
    }

    private fun mergedJsonIn(dir: Path, name: String, vararg urls: String): Path {
        val json = dir.resolve(name)
        val tags = urls.joinToString(" ") { """<img src=\"$it\"> text""" }
        json.writeText("""{"text": ["$tags"]}""")
        return json
    }

    /** The cache file for [url] under the current (sha256) key. */
    private fun cacheFile(dir: Path, url: String): Path =
        dir.resolve(SefariaImageEmbedder.cacheFileName(url))

    /** Its `<key>.url` sidecar. */
    private fun sidecarFile(dir: Path, url: String): Path =
        dir.resolve(SefariaImageEmbedder.cacheFileName(url) + SefariaImageEmbedder.SIDECAR_SUFFIX)

    /** Writes a complete cache entry (bytes + sidecar) the way the embedder does. */
    private fun writeCacheEntry(dir: Path, url: String, bytes: ByteArray, sidecarUrl: String = url) {
        Files.write(cacheFile(dir, url), bytes)
        Files.writeString(sidecarFile(dir, url), sidecarUrl)
    }

    /** Keep every test's report inside its own temp dir. */
    private fun reportPath(dir: Path): Path = dir.resolve("image-embed-report.json")

    private fun reportIn(dir: Path): ImageEmbedReport =
        Json.decodeFromString(ImageEmbedReport.serializer(), Files.readString(reportPath(dir)))

    @Test
    fun transientTimeoutIsRetriedThenCachedOnDisk() = runBlocking {
        val url = "https://textimages.sefaria.org/retry/a.png"
        val (dir, json) = mergedJsonWith(url)
        var calls = 0
        SefariaImageEmbedder.retryBackoffMillis = longArrayOf(0, 0)
        SefariaImageEmbedder.downloader = {
            calls++
            if (calls < 3) throw HttpTimeoutException("request timed out") else byteArrayOf(1, 2, 3)
        }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(3, calls, "two timeouts, then success")
        assertTrue(Files.isRegularFile(cacheFile(dir, url)), "cached on disk")
        assertEquals(url, Files.readString(sidecarFile(dir, url)), "the sidecar names the URL that wrote it")
        assertTrue(cleanSefariaLine("""<img src="$url">""").contains("data:image/png;base64,AQID"))
    }

    @Test
    fun finalHttpStatusIsNotRetried() = runBlocking {
        val url = "https://textimages.sefaria.org/blocked/b.png"
        val (dir, json) = mergedJsonWith(url)
        var calls = 0
        SefariaImageEmbedder.retryBackoffMillis = longArrayOf(0, 0)
        SefariaImageEmbedder.downloader = { calls++; throw SefariaImageEmbedder.httpFailure(418, it) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls, "a content-filter 418 is final")
        assertFalse(Files.exists(cacheFile(dir, url)))
        assertFalse(Files.exists(sidecarFile(dir, url)), "no orphan sidecar when nothing downloaded")
        val input = """<img src="$url">"""
        assertEquals(input, cleanSefariaLine(input), "remote URL kept when the download failed")
    }

    @Test
    fun exhaustedRetriesLeaveTheRemoteUrl() = runBlocking {
        val url = "https://textimages.sefaria.org/flaky/c.png"
        val (dir, json) = mergedJsonWith(url)
        var calls = 0
        SefariaImageEmbedder.retryBackoffMillis = longArrayOf(0, 0)
        SefariaImageEmbedder.downloader = { calls++; throw SefariaImageEmbedder.httpFailure(503, it) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(3, calls, "5xx is retried up to the attempt budget")
        assertFalse(Files.exists(cacheFile(dir, url)))
        assertEquals("http 503", reportIn(dir).failures.single().reason)
    }

    @Test
    fun cachedFileIsReusedWithoutAnyDownload() = runBlocking {
        val url = "https://textimages.sefaria.org/warm/d.png"
        val (dir, json) = mergedJsonWith(url)
        writeCacheEntry(dir, url, byteArrayOf(1, 2, 3))
        SefariaImageEmbedder.downloader = { error("network must not be touched for a cached image") }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertTrue(cleanSefariaLine("""<img src="$url">""").contains("data:image/png;base64,AQID"))
        // Disk hits are a counter, never a log line per file.
        val report = reportIn(dir)
        assertEquals(1, report.fromDiskCache)
        assertEquals(0, report.downloaded)
    }

    // --- cache key: two URLs, two files (audit claim 5) -------------------

    /** `a/b.png` and `a_b.png` — one file under the pre-sha256 key. */
    private val collidingA = "https://textimages.sefaria.org/a/b.png"
    private val collidingB = "https://textimages.sefaria.org/a_b.png"
    private val legacyNameOfBoth = "a_b.png"

    /** The pre-sha256 name, reproduced here because production no longer has it. */
    private fun legacyName(url: String): String =
        url.removePrefix("https://textimages.sefaria.org/")
            .replace('/', '_').replace(':', '_').replace('?', '_')

    @Test
    fun urlsThatCollidedUnderTheLegacyKeyGetTheirOwnFileAndBytes() = runBlocking {
        assertEquals(
            legacyName(collidingA),
            legacyName(collidingB),
            "premise: these two URLs shared one cache file before the sha256 key"
        )
        val (dir, json) = mergedJsonWith(collidingA, collidingB)
        SefariaImageEmbedder.downloader = { url ->
            if (url == collidingA) byteArrayOf(1, 2, 3) else byteArrayOf(4, 5, 6)
        }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(2, reportIn(dir).downloaded, "no hit may be served across the two URLs")
        assertNotEquals(cacheFile(dir, collidingA), cacheFile(dir, collidingB), "distinct cache files")
        assertEquals(collidingA, Files.readString(sidecarFile(dir, collidingA)))
        assertEquals(collidingB, Files.readString(sidecarFile(dir, collidingB)))
        // Each line gets ITS image, not whichever of the two was fetched first.
        assertTrue(cleanSefariaLine("""<img src="$collidingA">""").contains("data:image/png;base64,AQID"))
        assertTrue(cleanSefariaLine("""<img src="$collidingB">""").contains("data:image/png;base64,BAUG"))
    }

    // --- the sidecar is the ONLY way into the cache -----------------------

    @Test
    fun aLoneLegacyNamedBlobIsNeverAdopted() = runBlocking {
        // THE HAZARD, exactly: a past export wrote `a_b.png` — under the
        // pre-sha256 name, which folded `/ : ?` to `_` and so was not injective
        // — from `a_b.png` (U2). This export contains only `a/b.png` (U1), which
        // maps to the SAME legacy name. Nothing on disk says whose bytes those
        // are, so U1 must not inherit them: it downloads its own.
        val (dir, json) = mergedJsonWith(collidingA)
        assertEquals(legacyNameOfBoth, legacyName(collidingA), "premise: A's legacy name")
        assertEquals(legacyNameOfBoth, legacyName(collidingB), "premise: B's legacy name, the same")
        Files.write(dir.resolve(legacyNameOfBoth), byteArrayOf(4, 5, 6)) // U2's image
        var calls = 0
        SefariaImageEmbedder.downloader = { calls++; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls, "a blob with no sidecar naming this URL is a miss")
        val report = reportIn(dir)
        assertEquals(1, report.downloaded)
        assertEquals(0, report.fromDiskCache)
        assertTrue(
            cleanSefariaLine("""<img src="$collidingA">""").contains("data:image/png;base64,AQID"),
            "U1 gets its OWN image, never U2's"
        )
        assertEquals(collidingA, Files.readString(sidecarFile(dir, collidingA)))
        assertEquals(
            byteArrayOf(4, 5, 6).toList(),
            Files.readAllBytes(dir.resolve(legacyNameOfBoth)).toList(),
            "a file this build did not write is ignored where it lies, not moved and not deleted"
        )
    }

    @Test
    fun bothUrlsOfALegacyCollisionDownloadTheirOwnBytes() = runBlocking {
        // Both members of the colliding pair in one export, the legacy file
        // present: neither may claim it, and the two end up with distinct files.
        val (dir, json) = mergedJsonWith(collidingA, collidingB)
        Files.write(dir.resolve(legacyNameOfBoth), byteArrayOf(9, 9, 9))
        val calls = AtomicInteger()
        SefariaImageEmbedder.downloader = { url ->
            calls.incrementAndGet()
            if (url == collidingA) byteArrayOf(1, 2, 3) else byteArrayOf(4, 5, 6)
        }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(2, calls.get())
        assertEquals(0, reportIn(dir).fromDiskCache, "a legacy file is served to nobody")
        assertEquals(
            byteArrayOf(9, 9, 9).toList(),
            Files.readAllBytes(dir.resolve(legacyNameOfBoth)).toList(),
            "left untouched"
        )
        assertNotEquals(cacheFile(dir, collidingA), cacheFile(dir, collidingB))
        assertTrue(cleanSefariaLine("""<img src="$collidingA">""").contains("data:image/png;base64,AQID"))
        assertTrue(cleanSefariaLine("""<img src="$collidingB">""").contains("data:image/png;base64,BAUG"))
    }

    @Test
    fun blobAtTheRightNameWithoutASidecarIsNotAHit() = runBlocking {
        // Bytes sitting at sha256(url) with no `.url` beside them have no
        // provenance either (a hand-copied cache dir, a crash between the two
        // writes): the entry is evicted and refetched.
        val url = "https://textimages.sefaria.org/warm/f.png"
        val (dir, json) = mergedJsonWith(url)
        Files.write(cacheFile(dir, url), byteArrayOf(9, 9, 9))
        var calls = 0
        SefariaImageEmbedder.downloader = { calls++; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls, "no sidecar, no hit")
        assertEquals(0, reportIn(dir).fromDiskCache)
        assertEquals(url, Files.readString(sidecarFile(dir, url)), "the refetch writes the sidecar")
        assertEquals(byteArrayOf(1, 2, 3).toList(), Files.readAllBytes(cacheFile(dir, url)).toList())
        assertTrue(cleanSefariaLine("""<img src="$url">""").contains("data:image/png;base64,AQID"))
    }

    @Test
    fun aMissDownloadsAndWritesBothSidecarAndBlob() = runBlocking {
        val url = "https://textimages.sefaria.org/cold/g.png"
        val (dir, json) = mergedJsonWith(url)
        var calls = 0
        SefariaImageEmbedder.downloader = { calls++; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls)
        assertTrue(Files.isRegularFile(cacheFile(dir, url)), "blob written under the digest")
        assertEquals(url, Files.readString(sidecarFile(dir, url)), "sidecar names the URL byte-for-byte")
        assertEquals(byteArrayOf(1, 2, 3).toList(), Files.readAllBytes(cacheFile(dir, url)).toList())
        // No `.part` temp survives a successful write.
        assertTrue(
            dir.toFile().listFiles().orEmpty().none { it.name.endsWith(".part") },
            "the atomic write leaves no temp behind"
        )

        // Second run over the same durable cache: the sidecar makes it a hit.
        SefariaImageEmbedder.resetForTest()
        SefariaImageEmbedder.downloader = { error("a verified entry must not re-download") }
        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))
        assertEquals(1, reportIn(dir).fromDiskCache)
        assertEquals(0, reportIn(dir).downloaded)
    }

    @Test
    fun entryWhoseSidecarNamesAnotherUrlIsEvictedAndRefetched() = runBlocking {
        val url = "https://textimages.sefaria.org/warm/e.png"
        val (dir, json) = mergedJsonWith(url)
        writeCacheEntry(dir, url, byteArrayOf(9, 9, 9), sidecarUrl = "https://textimages.sefaria.org/other.png")
        var calls = 0
        SefariaImageEmbedder.downloader = { calls++; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls, "a sidecar naming another URL is a miss")
        val report = reportIn(dir)
        assertEquals(1, report.downloaded)
        assertEquals(0, report.fromDiskCache)
        assertEquals(url, Files.readString(sidecarFile(dir, url)), "the entry now names its real owner")
        assertEquals(byteArrayOf(1, 2, 3).toList(), Files.readAllBytes(cacheFile(dir, url)).toList())
        assertTrue(cleanSefariaLine("""<img src="$url">""").contains("data:image/png;base64,AQID"))
    }

    @Test
    fun aSidecarThatIsNotTheUrlByteForByteIsNotAHit() = runBlocking {
        // The hit rule is byte equality, not "looks like the URL": a sidecar
        // written by anything but [writeEntry] — a shell redirect that appends
        // a newline, an editor, a partially flushed write — has not proved the
        // bytes came from this URL, so the entry is evicted and refetched. This
        // also pins the writer: it must never append a terminator of its own.
        val url = "https://textimages.sefaria.org/warm/h.png"
        val (dir, json) = mergedJsonWith(url)
        writeCacheEntry(dir, url, byteArrayOf(9, 9, 9), sidecarUrl = "$url\n")
        var calls = 0
        SefariaImageEmbedder.downloader = { calls++; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(1, calls, "a sidecar that differs by one byte is a miss")
        assertEquals(0, reportIn(dir).fromDiskCache)
        assertEquals(url, Files.readString(sidecarFile(dir, url)), "rewritten with no trailing newline")
        assertEquals(byteArrayOf(1, 2, 3).toList(), Files.readAllBytes(cacheFile(dir, url)).toList())
    }

    @Test
    fun cacheDirComesFromTheSystemPropertyWhenSet() {
        System.setProperty(SefariaImageEmbedder.CACHE_DIR_PROPERTY, "durable/textimages")
        assertEquals(Paths.get("durable", "textimages"), SefariaImageEmbedder.defaultCacheDir())
        System.setProperty(SefariaImageEmbedder.CACHE_DIR_PROPERTY, "  ")
        assertEquals(
            Paths.get("build", "sefaria", "image-cache"),
            SefariaImageEmbedder.defaultCacheDir(),
            "blank property falls back to the build dir"
        )
    }

    @Test
    fun reportPathComesFromTheSystemPropertyWhenSet() {
        System.setProperty(SefariaImageEmbedder.REPORT_PROPERTY, "out/image-embed-report.json")
        assertEquals(Paths.get("out", "image-embed-report.json"), SefariaImageEmbedder.defaultReportPath())
        System.clearProperty(SefariaImageEmbedder.REPORT_PROPERTY)
        assertEquals(
            Paths.get("build", "sefaria", "image-embed-report.json"),
            SefariaImageEmbedder.defaultReportPath(),
            "unset property falls back to the build dir"
        )
    }

    @Test
    fun spacedUrlIsScannedAndSubstitutedWithTheSameKey() = runBlocking {
        // The scan used to stop at whitespace and cache ".../Screenshot", a key
        // substituteImages could never look up — so this image stayed remote in
        // every build even when the bytes arrived.
        val url = "https://textimages.sefaria.org/ShaarHahakdamot/Screenshot 2023-05-04 at 3.09.20 PM.png"
        val (dir, json) = mergedJsonWith(url)
        val requested = mutableListOf<String>()
        SefariaImageEmbedder.downloader = { requested += it; byteArrayOf(1, 2, 3) }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        assertEquals(listOf(url), requested, "the whole spaced URL is the cache key")
        assertEquals(0, reportIn(dir).failed)
        assertTrue(cleanSefariaLine("""<img src="$url">""").contains("data:image/png;base64,AQID"))
    }

    @Test
    fun onlyTheRequestIsPercentEncoded() {
        assertEquals(
            "https://textimages.sefaria.org/a%20b/c.png",
            SefariaImageEmbedder.encodeForRequest("https://textimages.sefaria.org/a b/c.png")
        )
        // Sefaria's own encoded paths must not be encoded a second time.
        val encoded = "https://textimages.sefaria.org/shalach_teshalach/image%20-%200019.png"
        assertEquals(encoded, SefariaImageEmbedder.encodeForRequest(encoded))
        assertEquals(
            "https://textimages.sefaria.org/%D7%90.png",
            SefariaImageEmbedder.encodeForRequest("https://textimages.sefaria.org/א.png")
        )
    }

    @Test
    fun everyFailureIsNamedInTheReport() = runBlocking {
        val blocked = "https://textimages.sefaria.org/ShaarHahakdamot/blocked.png"
        val gone = "https://textimages.sefaria.org/gone/x.png"
        val slow = "https://textimages.sefaria.org/slow/z.png"
        val ok = "https://textimages.sefaria.org/ok/y.png"
        val (dir, json) = mergedJsonWith(blocked, gone, slow, ok)
        SefariaImageEmbedder.retryBackoffMillis = longArrayOf(0, 0)
        SefariaImageEmbedder.downloader = { url ->
            when (url) {
                blocked -> throw SefariaImageEmbedder.httpFailure(418, url)
                gone -> throw SefariaImageEmbedder.httpFailure(404, url)
                slow -> throw HttpTimeoutException("request timed out")
                else -> byteArrayOf(1, 2, 3)
            }
        }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        val report = reportIn(dir)
        assertEquals(4, report.attempted)
        assertEquals(1, report.cached)
        assertEquals(1, report.downloaded)
        assertEquals(3, report.failed)
        assertEquals(listOf(blocked, gone, slow), report.failures.map { it.url }, "sorted by URL")
        assertEquals(
            "content-filter (HTTP 418 — NetFree on the self-hosted runner)",
            report.failures[0].reason
        )
        assertEquals("http 404", report.failures[1].reason)
        assertEquals("timeout", report.failures[2].reason)
    }

    @Test
    fun countersAreExactUnderConcurrentDownloads() = runBlocking {
        // The counters are written from DOWNLOAD_PARALLELISM coroutines and now
        // gate the build, so a lost `Int++` is a wrong gate, not a cosmetic log.
        val urls = (1..120).map { "https://textimages.sefaria.org/concurrent/$it.png" }
        val (dir, json) = mergedJsonWith(*urls.toTypedArray())
        val calls = AtomicInteger()
        SefariaImageEmbedder.downloader = { url ->
            calls.incrementAndGet()
            val n = url.substringAfterLast('/').removeSuffix(".png").toInt()
            if (n % 2 == 0) byteArrayOf(1, 2, 3) else throw SefariaImageEmbedder.httpFailure(404, url)
        }

        SefariaImageEmbedder.prefetch(listOf(json), cacheDir = dir, reportPath = reportPath(dir))

        val report = reportIn(dir)
        assertEquals(120, calls.get(), "a 404 is final, so one call per URL")
        assertEquals(120, report.attempted)
        assertEquals(60, report.downloaded)
        assertEquals(0, report.fromDiskCache)
        assertEquals(60, report.cached)
        assertEquals(60, report.failed)
        assertEquals(60, report.failures.size)
    }

    @Test
    fun substituteImagesReplacesKnownUrlsInImgTags() {
        val url = "https://textimages.sefaria.org/Tikkunei_Zohar/40.png"
        val dataUri = "data:image/png;base64,AAAA"
        SefariaImageEmbedder.seedForTest(url, dataUri)

        val input = """prefix <img src="$url"> middle <img src="$url" class="x"> end"""
        val out = cleanSefariaLine(input)

        assertTrue(out.contains("data:image/png;base64,AAAA"))
        assertFalse(out.contains(url), "original URL should be replaced")
    }

    @Test
    fun unknownUrlIsPreservedVerbatim() {
        // Embedder enabled but URL not in cache — leave it alone so it's
        // obvious (and fixable) instead of silently dropping content.
        SefariaImageEmbedder.seedForTest(
            "https://textimages.sefaria.org/known.png",
            "data:image/png;base64,Z"
        )
        val input = """<img src="https://textimages.sefaria.org/unknown.png">"""
        assertEquals(input, cleanSefariaLine(input))
    }

    @Test
    fun noOpWhenEmbedderDisabled() {
        // Without prefetch, substituteImages is a pass-through
        val input = """<img src="https://textimages.sefaria.org/x.png">"""
        assertEquals(input, cleanSefariaLine(input))
    }

    @Test
    fun compatibleWithOtherCleanSteps() {
        SefariaImageEmbedder.seedForTest(
            "https://textimages.sefaria.org/a.png",
            "data:image/png;base64,X"
        )
        // Otzar markup, <br>, and image embedding all in one line
        val input = """@04פתיחה} <br> <img src="https://textimages.sefaria.org/a.png"> text"""
        val out = cleanSefariaLine(input)
        assertTrue(out.startsWith("פתיחה"))
        assertFalse(out.contains("<br>"))
        assertTrue(out.contains("data:image/png;base64,X"))
    }
}
