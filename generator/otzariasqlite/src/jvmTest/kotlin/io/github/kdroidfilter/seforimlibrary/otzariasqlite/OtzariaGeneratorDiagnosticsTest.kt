package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.ids.AllocatorStats
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The audited generation step emitted 24,207 lines, 94.8% of them boilerplate,
 * while four real findings — a dead priority list, a fifth of the manual links
 * dropped, 37 heading-map files fed to the link parser, and books with no source
 * hash — were either invisible or drowned in it.
 *
 * These tests drive the real generator over a small library fixture and assert
 * on what it says: each noise cut collapses into one summary line that carries
 * the counts, and the full list lands in a report file.
 */
class OtzariaGeneratorDiagnosticsTest {

    // ─── log capture ───────────────────────────────────────────────────────
    // DatabaseGenerator logs through `Logger.withTag(...)`, which shares the
    // global mutable kermit config, so the capture is installed globally and
    // restored afterwards. Note SeforimRepository's init drops the global
    // severity to Assert — the capture must be installed AFTER the repository.

    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }

        fun matching(prefix: String): List<String> =
            lines.map { it.second }.filter { it.startsWith(prefix) }

        fun warnings(): List<String> = lines.filter { it.first == Severity.Warn }.map { it.second }
    }

    private val previousWriters = Logger.config.logWriterList
    private val previousSeverity = Logger.config.minSeverity
    private val previousReportDir: String? = System.getProperty(GeneratorReport.DIR_PROPERTY)

    @AfterTest
    fun restoreGlobals() {
        Logger.setLogWriters(previousWriters)
        Logger.setMinSeverity(previousSeverity)
        if (previousReportDir == null) System.clearProperty(GeneratorReport.DIR_PROPERTY)
        else System.setProperty(GeneratorReport.DIR_PROPERTY, previousReportDir)
    }

    private fun installCapture(): Capture {
        val capture = Capture()
        Logger.setLogWriters(listOf(capture))
        Logger.setMinSeverity(Severity.Verbose)
        return capture
    }

    private fun reportDir(): Path {
        val dir = Files.createTempDirectory("generator-reports")
        System.setProperty(GeneratorReport.DIR_PROPERTY, dir.toAbsolutePath().toString())
        return dir
    }

    // ─── fixture ───────────────────────────────────────────────────────────

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private fun hash(seed: Char) = seed.toString().repeat(64)

    /**
     * A library with one ordinary book, one book dropped at the library root for
     * having no category, and a manifest entry for a book that is not on disk at
     * all — i.e. one instance of each source-hash class the importer can name.
     */
    private fun writeLibrary(
        linksJson: String? = null,
        headingsJson: String? = null,
    ): Path {
        val root = Files.createTempDirectory("otzaria-diagnostics")
        val musar = Files.createDirectories(root.resolve("אוצריא").resolve("מוסר"))
        Files.writeString(musar.resolve("ספר א.txt"), "<h1>ספר א</h1>\nשורה ראשונה\nשורה שניה")
        Files.writeString(root.resolve("אוצריא").resolve("יתום.txt"), "<h1>יתום</h1>\nשורה")
        Files.writeString(
            root.resolve("files_manifest.json"),
            """
            {
              "אוצריא/מוסר/ספר א.txt": { "hash": "${hash('a')}" },
              "אוצריא/יתום.txt": { "hash": "${hash('b')}" },
              "אוצריא/מוסר/רפאים.txt": { "hash": "${hash('c')}" }
            }
            """.trimIndent(),
        )
        if (linksJson != null || headingsJson != null) {
            val linksDir = Files.createDirectories(root.resolve("links"))
            if (linksJson != null) Files.writeString(linksDir.resolve("ספר א_links.json"), linksJson)
            if (headingsJson != null) Files.writeString(linksDir.resolve("ברכות_headings.json"), headingsJson)
        }
        return root
    }

    /** The two missing target books of the audited build, with their real link counts scaled down. */
    private val kookPath = "אוצריא\\מחשבת ישראל\\כתבי הרב קוק\\עולת ראיה.txt"
    private val kapachPath = "אוצריא\\הלכה\\מלחמות השם יחיא קאפח.txt"

    /** Three manual links at one missing target book and one at another. */
    private fun manualLinks(): String {
        // otzaria-library writes Windows separators in `path_2`, so every
        // backslash is escaped for JSON and arrives single in the parsed value.
        fun entry(i1: Int, target: String) =
            """{"line_index_1": $i1, "heRef_2": "x", "path_2": "${target.replace("\\", "\\\\")}",
               |"line_index_2": 2, "Conection Type": "commentary"}""".trimMargin()
        return "[" + listOf(
            entry(2, kookPath),
            entry(3, kookPath),
            entry(2, kookPath),
            entry(3, kapachPath),
        ).joinToString(",") + "]"
    }

    // ─── 1. `_headings` files never reach the link parser ──────────────────

    @Test
    fun `heading maps are excluded at discovery and never parsed as links`() = runBlocking {
        val root = writeLibrary(
            linksJson = manualLinks(),
            // The real shape: a heading → line-number map, which the link parser
            // rejects with `Expected start of the array '['` and a JSON dump.
            headingsJson = """{"ברכות": 1, "דף ב.": 2, "דף ג.": 14}""",
        )
        val repo = newRepo()
        reportDir()
        val capture = installCapture()

        val generator = DatabaseGenerator(sourceDirectory = root, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        assertTrue(
            capture.lines.none { "Failed to parse links" in it.second },
            "a heading map must never be handed to the link parser: ${capture.warnings()}",
        )
        assertTrue(
            capture.lines.none { "Source book not found for links" in it.second },
            "excluding the file also removes its follow-on warning: ${capture.warnings()}",
        )
        assertTrue(
            capture.lines.none { "\"ברכות\": 1" in it.second },
            "the offending document must not be dumped into the log",
        )
        // Both discovery passes (phase 1's merge planner and phase 2's link
        // importer) report the exclusion once each — the file used to be parsed
        // twice, so silence from either pass would hide half the problem.
        assertEquals(
            listOf(
                "skipped 1 *_headings files (heading maps, not link files)",
                "skipped 1 *_headings files (heading maps, not link files)",
            ),
            capture.matching("skipped 1 *_headings"),
        )
    }

    // ─── 2. dropped manual links ───────────────────────────────────────────

    @Test
    fun `manual links to a missing target are summarised per target, not per link`() = runBlocking {
        val root = writeLibrary(linksJson = manualLinks())
        val repo = newRepo()
        val dir = reportDir()
        val capture = installCapture()

        val generator = DatabaseGenerator(sourceDirectory = root, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        assertEquals(0, repo.countLinks(), "the drop behaviour itself is unchanged")
        assertTrue(
            capture.lines.none { it.second.startsWith("Link ") || it.second.startsWith("Original path:") },
            "the two INFO lines per dropped link are gone: ${capture.lines}",
        )
        assertEquals(
            listOf("manual links: 4 of 4 links dropped — 2 target book(s) not found"),
            capture.matching("manual links: 4 of"),
        )
        // Ordered by link count, so the worst offender is the first thing read.
        assertEquals(
            listOf(
                "manual links: 3 links dropped — target book not found: 'עולת ראיה' ($kookPath)",
                "manual links: 1 links dropped — target book not found: 'מלחמות השם יחיא קאפח' ($kapachPath)",
            ),
            capture.matching("manual links: ").filter { "target book not found" in it },
        )

        val report = dir.resolve("otzaria-manual-links-dropped.json").readText()
        assertContains(report, "\"droppedLinks\": 4")
        assertContains(report, "\"missingTargetBooks\": 2")
        assertContains(report, "\"title\": \"עולת ראיה\", \"links\": 3")
    }

    // ─── 3. priority list drift ────────────────────────────────────────────

    @Test
    fun `a priority list whose entries are all missing warns once and reports the list`() = runBlocking {
        val root = writeLibrary()
        val repo = newRepo()
        val dir = reportDir()
        val capture = installCapture()

        DatabaseGenerator(sourceDirectory = root, repository = repo).generateLinesOnly()

        // None of the shipped priority.txt entries exist under this fixture, so
        // the pass is a total miss — as it very nearly is in production (430/431).
        val summary = capture.matching("priority list: ").single()
        assertTrue(
            Regex("""^priority list: 0/\d+ entries found, \d+ missing \(first: .+\) — list is """)
                .containsMatchIn(summary),
            "unexpected summary: $summary",
        )
        assertContains(summary, "priority.txt")
        assertContains(summary, "operator decision")
        assertTrue(
            capture.warnings().none { it.startsWith("Priority entry ") },
            "the per-entry warnings are demoted to DEBUG, not kept as 430 WARNs",
        )

        val report = dir.resolve("otzaria-priority-list-missing.json").readText()
        assertContains(report, "\"found\": 0")
        assertContains(report, "\"listResource\": \"otzariasqlite/src/commonMain/resources/priority.txt\"")
        // The log line named three of them; the report has to carry every one,
        // which is the whole point of splitting the finding in two.
        val summaryMissing = Regex("""(\d+) missing""").find(summary)!!.groupValues[1].toInt()
        assertContains(report, "\"missing\": $summaryMissing")
        val listed = report.substringAfter("\"missingEntries\": [")
        assertEquals(summaryMissing, Regex("""\.txt"""").findAll(listed).count())
    }

    // ─── 4. books with no source hash ──────────────────────────────────────

    @Test
    fun `books whose source hash cannot be recorded are classified and listed`() = runBlocking {
        val root = writeLibrary()
        val repo = newRepo()
        val dir = reportDir()
        val capture = installCapture()

        DatabaseGenerator(sourceDirectory = root, repository = repo).generateLinesOnly()

        assertEquals(
            listOf("Recorded source hashes for 1 / 3 Otzaria books"),
            capture.matching("Recorded source hashes"),
        )
        assertEquals(
            listOf("source hashes: 2 of 3 Otzaria books have no source hash — they are fully reprocessed every cycle"),
            capture.matching("source hashes: 2 of"),
        )
        val classes = capture.matching("source hashes: 1 not recorded")
        assertEquals(
            setOf(
                "source hashes: 1 not recorded — file has no category (library root) (יתום)",
                "source hashes: 1 not recorded — not imported (reason not tracked) (רפאים)",
            ),
            classes.toSet(),
            "each class names its own books rather than folding into one bucket",
        )

        val report = dir.resolve("otzaria-source-hashes-not-recorded.json").readText()
        assertContains(report, "\"computed\": 3")
        assertContains(report, "\"notRecorded\": 2")
        assertContains(report, "\"title\": \"יתום\", \"reason\": \"file has no category (library root)\"")
        assertContains(report, "\"title\": \"רפאים\", \"reason\": \"not imported (reason not tracked)\"")
    }

    // ─── 5. the insert-path breadcrumb ─────────────────────────────────────

    /** Fails the id allocation for one named book, the way a real insert failure would. */
    private class FailingBookIdAllocator(
        private val delegate: IdAllocator,
        private val failFor: String,
    ) : IdAllocator by delegate {
        override fun bookId(sourceName: String, canonicalHeTitle: String): Long {
            if (canonicalHeTitle == failFor) error("boom in bookId")
            return delegate.bookId(sourceName, canonicalHeTitle)
        }

        override fun stats(): AllocatorStats = delegate.stats()
        override fun peekBookId(sourceName: String, canonicalHeTitle: String): Long? =
            delegate.peekBookId(sourceName, canonicalHeTitle)

        override fun recordSourceHash(key: BookKey, sourceHash: BookSourceHash) =
            delegate.recordSourceHash(key, sourceHash)
    }

    @Test
    fun `a failure on the serial insert path names the book it was on`() = runBlocking {
        val root = writeLibrary()
        val repo = newRepo()
        reportDir()
        val capture = installCapture()

        val generator = DatabaseGenerator(
            sourceDirectory = root,
            repository = repo,
            allocator = FailingBookIdAllocator(InMemoryIdAllocator.load(path = null), failFor = "ספר א"),
        )

        // The exception is rethrown unchanged — the catch exists only to log.
        val failure = assertFailsWith<IllegalStateException> { generator.generateLinesOnly() }
        assertEquals("boom in bookId", failure.message)

        val breadcrumb = capture.lines.map { it.second }.single { it.startsWith("Error during phase 1") }
        assertContains(breadcrumb, "(last book: 'ספר א' file=ספר א.txt categoryId=")
    }

    @Test
    fun `a failure before any book leaves the breadcrumb out rather than lying`() = runBlocking {
        // No אוצריא directory: phase 1 throws before a single book is looked at.
        val root = Files.createTempDirectory("otzaria-empty")
        val repo = newRepo()
        reportDir()
        val capture = installCapture()

        assertFailsWith<IllegalStateException> {
            DatabaseGenerator(sourceDirectory = root, repository = repo).generateLinesOnly()
        }

        val line = capture.lines.map { it.second }.single { it.startsWith("Error during phase 1") }
        assertEquals("Error during phase 1", line)
    }
}
