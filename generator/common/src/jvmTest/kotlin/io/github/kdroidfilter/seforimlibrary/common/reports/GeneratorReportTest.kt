package io.github.kdroidfilter.seforimlibrary.common.reports

import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The report side-channel that lets a noisy finding collapse to one bounded log
 * line without losing the full list behind it.
 *
 * Two properties matter and are pinned here: the emitted JSON is byte-stable
 * (so two builds' reports can be diffed), and a failure to write one can never
 * take a build down — these are diagnostics, and the audited step succeeds or
 * fails on the database, not on its own paperwork.
 */
class GeneratorReportTest {

    private val previousDir: String? = System.getProperty(GeneratorReport.DIR_PROPERTY)

    @AfterTest
    fun restoreDirProperty() {
        if (previousDir == null) System.clearProperty(GeneratorReport.DIR_PROPERTY)
        else System.setProperty(GeneratorReport.DIR_PROPERTY, previousDir)
    }

    /** Collects everything logged, so a test can assert on severity + message. */
    private class Capture : LogWriter() {
        val lines = mutableListOf<Pair<Severity, String>>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += severity to message
        }
    }

    private fun capturingLogger(capture: Capture) =
        Logger(StaticConfig(minSeverity = Severity.Verbose, logWriterList = listOf(capture)), "test")

    private fun withReportDir(dir: Path): Path {
        System.setProperty(GeneratorReport.DIR_PROPERTY, dir.toAbsolutePath().toString())
        return dir
    }

    // ─── builder output ────────────────────────────────────────────────────

    @Test
    fun `builder emits keys in insertion order with stable formatting`() {
        val body = ReportBuilder().apply {
            put("computed", 6216L)
            put("notRecorded", 391)
            putStrings("titles", listOf("אבן הראשה", "קול התור"))
        }.build()

        assertEquals(
            """
            {
              "computed": 6216,
              "notRecorded": 391,
              "titles": [
                "אבן הראשה",
                "קול התור"
              ]
            }
            """.trimIndent() + "\n",
            body,
        )
    }

    @Test
    fun `rows render numbers booleans and nulls unquoted`() {
        val body = ReportBuilder().apply {
            putRows(
                "missingTargets",
                listOf(
                    mapOf<String, Any?>("title" to "עולת ראיה", "links" to 642, "kept" to false),
                    mapOf<String, Any?>("title" to "x", "links" to null, "kept" to true),
                ),
            )
        }.build()

        assertEquals(
            """
            {
              "missingTargets": [
                { "title": "עולת ראיה", "links": 642, "kept": false },
                { "title": "x", "links": null, "kept": true }
              ]
            }
            """.trimIndent() + "\n",
            body,
        )
    }

    @Test
    fun `empty collections render as empty arrays rather than being dropped`() {
        val body = ReportBuilder().apply {
            putStrings("titles", emptyList())
            putRows("rows", emptyList())
        }.build()
        assertEquals("{\n  \"titles\": [],\n  \"rows\": []\n}\n", body)
    }

    @Test
    fun `quoting escapes the characters that would otherwise break the JSON`() {
        val body = ReportBuilder().apply {
            put("path", "אוצריא\\מחשבת ישראל\\\"כתבי\"\tהרב\nקוק.txt")
            put("control", "a" + 1.toChar() + "b")
        }.build()

        assertEquals(
            "{\n" +
                "  \"path\": \"אוצריא\\\\מחשבת ישראל\\\\\\\"כתבי\\\"\\tהרב\\nקוק.txt\",\n" +
                "  \"control\": \"a\\u0001b\"\n" +
                "}\n",
            body,
        )
    }

    // ─── writing ───────────────────────────────────────────────────────────

    @Test
    fun `write puts the named report under the configured directory`() {
        val dir = withReportDir(Files.createTempDirectory("generator-reports"))
        val capture = Capture()

        val written = GeneratorReport.write("otzaria-priority-list-missing", capturingLogger(capture)) {
            put("entries", 431L)
            put("found", 1L)
        }

        assertEquals(dir.resolve("otzaria-priority-list-missing.json"), written)
        assertEquals("{\n  \"entries\": 431,\n  \"found\": 1\n}\n", written!!.readText())
        assertTrue(
            capture.lines.any { it.first == Severity.Info && it.second.startsWith("report written to ") },
            "the log must still say where the full list went: ${capture.lines}",
        )
    }

    @Test
    fun `write leaves no temp file behind`() {
        val dir = withReportDir(Files.createTempDirectory("generator-reports"))
        GeneratorReport.write("a", capturingLogger(Capture())) { put("k", 1L) }
        GeneratorReport.write("a", capturingLogger(Capture())) { put("k", 2L) }

        val names = Files.list(dir).use { s -> s.map { it.fileName.toString() }.sorted().toList() }
        assertEquals(listOf("a.json"), names, "a rewrite must replace, not accumulate temp files")
        assertEquals("{\n  \"k\": 2\n}\n", dir.resolve("a.json").readText())
    }

    @Test
    fun `a report that cannot be written warns and returns null instead of throwing`() {
        // A regular file where the directory should be: createDirectories fails.
        val blocker = Files.createTempFile("generator-reports", ".not-a-dir")
        withReportDir(blocker.resolve("sub"))
        val capture = Capture()

        val written = GeneratorReport.write("doomed", capturingLogger(capture)) { put("k", 1L) }

        assertNull(written, "a failed diagnostics write must not surface as a value")
        val warning = capture.lines.single { it.first == Severity.Warn }.second
        assertContains(warning, "Failed to write doomed report")
    }

    // ─── directory resolution ──────────────────────────────────────────────

    @Test
    fun `directory falls back to the build-relative default when nothing is set`() {
        System.clearProperty(GeneratorReport.DIR_PROPERTY)
        // The env var is not set in the test JVM; if a runner ever sets it, the
        // property-wins case above still pins the precedence that matters.
        if (System.getenv(GeneratorReport.DIR_ENV) == null) {
            // Compared as a Path: the default is written with `/` but renders
            // with the platform separator on Windows runners.
            assertEquals(java.nio.file.Paths.get(GeneratorReport.DEFAULT_DIR), GeneratorReport.directory())
        }
    }

    @Test
    fun `the system property wins over everything else`() {
        val dir = Files.createTempDirectory("explicit-reports")
        System.setProperty(GeneratorReport.DIR_PROPERTY, dir.toString())
        assertEquals(dir.toString(), GeneratorReport.directory().toString())
    }
}
