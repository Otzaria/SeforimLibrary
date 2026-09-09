package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.io.path.readText
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The Sefaria-stage half of the generation-step audit: a per-type counter block
 * that read as a contradiction, a "done" summary that disagreed with its own
 * warning, and two "N of M" lines that named none of the M − N.
 */
class SefariaGeneratorDiagnosticsTest {

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }

        fun warnings(): List<String> = lines.filter { it.first == Severity.Warn }.map { it.second }
    }

    private fun capturingLogger(capture: Capture) =
        Logger(StaticConfig(Severity.Verbose, listOf(capture)), "test")

    private val previousReportDir: String? = System.getProperty(GeneratorReport.DIR_PROPERTY)

    @AfterTest
    fun restoreReportDir() {
        if (previousReportDir == null) System.clearProperty(GeneratorReport.DIR_PROPERTY)
        else System.setProperty(GeneratorReport.DIR_PROPERTY, previousReportDir)
    }

    private fun reportDir() = Files.createTempDirectory("generator-reports").also {
        System.setProperty(GeneratorReport.DIR_PROPERTY, it.toAbsolutePath().toString())
    }

    // ─── per-type counters ─────────────────────────────────────────────────

    private fun metrics(vararg rows: Pair<String, LinkImportTypeMetrics>) =
        LinkImportMetrics(linkedMapOf(*rows))

    private fun typeMetrics(rowsRead: Long, dropped: Long, resolvedPairs: Long, written: Long) =
        LinkImportTypeMetrics(rowsRead, dropped, resolvedPairs, written)

    @Test
    fun `each counter names the keying it uses so the REFERENCE skew stops reading as a bug`() {
        // The audited numbers. rowsRead/dropped/resolvedPairs are keyed by the
        // CSV's declared type, `written` by the type the row was stored under —
        // so 4,421 resolved pairs against 106,912 written rows is not 24 writes
        // per pair, it is two different row sets sharing a name.
        val text = formatPerTypeCounters(
            metrics(
                "REFERENCE" to typeMetrics(381152, 376731, 4421, 106912),
                "OTHER" to typeMetrics(2035716, 367327, 1668400, 1590315),
            ),
        )

        assertTrue(
            text.startsWith("Sefaria links importer per-type counters "),
            "pipeline-monitor's links_post marker anchors on this prefix: $text",
        )
        assertContains(text, "csv* keyed by the CSV's `Conection Type`")
        assertContains(text, "storedWritten keyed by the type the link was STORED under")
        assertContains(
            text,
            "type=REFERENCE csvRowsRead=381152 csvDropped=376731 csvResolvedPairs=4421 storedWritten=106912",
        )
        assertTrue(
            "type=REFERENCE rowsRead=" !in text && " written=" !in text,
            "no unqualified counter name may survive: $text",
        )
    }

    @Test
    fun `the totals line gives the one comparison that crosses both keyings`() {
        val text = formatPerTypeCounters(
            metrics(
                "REFERENCE" to typeMetrics(381152, 376731, 4421, 106912),
                "OTHER" to typeMetrics(2035716, 367327, 1668400, 1590315),
            ),
        )
        assertContains(
            text,
            "totals csvRowsRead=2416868 csvDropped=744058 csvResolvedPairs=1672821 storedWritten=1697227",
        )
        // Σ written may exceed Σ resolvedPairs in a partial sample; on a full
        // build it is the other way round and the gap is the filtered pairs.
        assertContains(text, "resolvedPairs-written=-24406")
    }

    @Test
    fun `an empty import still renders a well-formed summary`() {
        val text = formatPerTypeCounters(LinkImportMetrics(emptyMap()))
        assertContains(text, "totals csvRowsRead=0 csvDropped=0 csvResolvedPairs=0 storedWritten=0")
        assertContains(text, "resolvedPairs-written=0")
    }

    // ─── generations: the warning and the summary must agree ───────────────

    private fun generationsDb() = DriverManager.getConnection("jdbc:sqlite::memory:").apply {
        createStatement().use { st ->
            st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
            st.execute("CREATE TABLE generation (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
            st.execute(
                "CREATE TABLE book_generation (bookId INTEGER NOT NULL, generationId INTEGER NOT NULL, " +
                    "PRIMARY KEY (bookId, generationId))",
            )
            st.execute("INSERT INTO book VALUES (1, 'ברכות')")
        }
    }

    @Test
    fun `the generations summary reports the same unmatched count as its warning`() {
        generationsDb().use { conn ->
            val capture = Capture()
            val result = applyGenerations(
                conn,
                listOf(
                    "ברכות" to "אמוראים",
                    "אבן הראשה" to "ראשונים",
                    "קול התור" to "אחרונים",
                ),
                capturingLogger(capture),
            )

            assertEquals(1, result.linksCreated)
            // Was hard-coded to 0, so `Generations done: … unmatched=0` flatly
            // contradicted `Generation CSV has 13 unmatched book title(s)`.
            assertEquals(2, result.unmatched)
            val warning = capture.warnings().single()
            assertContains(warning, "Generation CSV has 2 unmatched book title(s)")
            assertEquals(
                result.unmatched,
                Regex("""has (\d+) unmatched""").find(warning)!!.groupValues[1].toInt(),
                "the summary count and the warning count must be the same number",
            )
        }
    }

    @Test
    fun `a fully matched generations CSV reports no unmatched titles and warns about nothing`() {
        generationsDb().use { conn ->
            val capture = Capture()
            val result = applyGenerations(conn, listOf("ברכות" to "אמוראים"), capturingLogger(capture))
            assertEquals(0, result.unmatched)
            assertEquals(emptyList(), capture.warnings())
        }
    }

    // ─── all-metadata: name the records that matched no book ───────────────

    @Test
    fun `unmatched metadata titles are named in the log and listed in full in the report`() {
        val dir = reportDir()
        val capture = Capture()
        val titles = (1..25).map { "ספר $it" }

        reportUnmatchedMetadataTitles(titles, capturingLogger(capture))

        val warning = capture.warnings().single()
        assertContains(warning, "all-metadata: 25 ForDB metadata record(s) matched no book")
        assertContains(warning, "ספר 1")
        assertContains(warning, "… and 5 more")
        assertTrue("ספר 21" !in warning, "the log line stays bounded at 20 names: $warning")

        val report = dir.resolve("sefaria-all-metadata-unmatched.json").readText()
        assertContains(report, "\"unmatched\": 25")
        // The five names the bounded log line had to leave out are all here.
        titles.forEach { assertContains(report, "\"$it\"") }
    }

    @Test
    fun `no unmatched metadata means no warning and no report file`() {
        val dir = reportDir()
        val capture = Capture()
        reportUnmatchedMetadataTitles(emptyList(), capturingLogger(capture))
        assertEquals(emptyList(), capture.warnings())
        assertTrue(Files.list(dir).use { it.findAny().isEmpty }, "an empty finding writes nothing")
    }

    // ─── Sefaria source hashes ─────────────────────────────────────────────

    private fun blacklistResult(skipped: Set<String>) = BlacklistFilterResult(
        payloads = emptyList(),
        skippedTotal = skipped.size,
        skippedByBook = skipped.size,
        skippedByAuthor = 0,
        skippedBookExamples = skipped.toList(),
        skippedAuthorExamples = emptyList(),
        skippedNormalizedPaths = emptySet(),
        skippedHeTitles = skipped,
    )

    @Test
    fun `a blacklisted book explains its missing source hash, anything else says it does not`() {
        val blacklist = blacklistResult(setOf("קדמוניות היהודים"))
        assertEquals(
            "skipped by the book/author blacklist",
            classifyUnimportedSefariaBook(BookKey("Sefaria", "קדמוניות היהודים"), blacklist),
        )
        assertEquals(
            "not imported (reason not tracked)",
            classifyUnimportedSefariaBook(BookKey("Sefaria", "ספר אחר"), blacklist),
        )
    }

    @Test
    fun `unrecorded Sefaria hashes are reported per class, biggest first, with the full list on disk`() {
        val dir = reportDir()
        val capture = Capture()

        reportUnrecordedSefariaSourceHashes(
            computed = 6216,
            byReason = linkedMapOf(
                "not imported (reason not tracked)" to listOf("ב", "א"),
                "skipped by the book/author blacklist" to (1..21).map { "חסום $it" },
            ),
            logger = capturingLogger(capture),
        )

        val warnings = capture.warnings()
        assertEquals(
            "source hashes: 23 of 6216 Sefaria books have no source hash — " +
                "they are fully reprocessed every cycle",
            warnings.first(),
        )
        // Biggest class first, names sorted so two builds' logs are comparable.
        assertTrue(
            warnings[1].startsWith("source hashes: 21 not recorded — skipped by the book/author blacklist ("),
            warnings[1],
        )
        assertContains(warnings[1], "… and 1 more)")
        assertEquals("source hashes: 2 not recorded — not imported (reason not tracked) (א, ב)", warnings[2])

        val report = dir.resolve("sefaria-source-hashes-not-recorded.json").readText()
        assertContains(report, "\"computed\": 6216")
        assertContains(report, "\"notRecorded\": 23")
        assertContains(report, "{ \"reason\": \"skipped by the book/author blacklist\", \"books\": 21 }")
        assertContains(report, "{ \"title\": \"א\", \"reason\": \"not imported (reason not tracked)\" }")
    }

    @Test
    fun `nothing unrecorded means nothing logged`() {
        val capture = Capture()
        reportUnrecordedSefariaSourceHashes(6216, emptyMap(), capturingLogger(capture))
        assertEquals(emptyList(), capture.lines.map { it.second })
    }

    // ─── the blacklist filter feeds the classification ─────────────────────

    private fun payload(heTitle: String) = BookPayload(
        heTitle = heTitle,
        enTitle = heTitle,
        categoriesHe = listOf("תנך"),
        lines = listOf("שורה"),
        refEntries = emptyList(),
        headings = emptyList(),
        authors = emptyList(),
        description = null,
        heShortDesc = null,
        pubDates = emptyList(),
        altStructures = emptyList(),
    )

    @Test
    fun `the blacklist filter records the heTitles it skipped`() {
        val kept = payload("ברכות")
        val dropped = payload("קדמוניות היהודים")
        val result = filterBlacklistedPayloads(
            listOf(kept, dropped),
            SefariaBlacklists(
                authorKeys = emptySet(),
                bookTitleKeys = setOfNotNull(normalizeTitleKey("קדמוניות היהודים")),
                bookPathKeys = emptySet(),
            ),
        )
        assertEquals(listOf(kept), result.payloads)
        // The heTitle IS the natural-key half the source-hash accounting joins
        // on, so this set is what turns "391 unrecorded" into "391 blacklisted".
        assertEquals(setOf("קדמוניות היהודים"), result.skippedHeTitles)
    }
}
