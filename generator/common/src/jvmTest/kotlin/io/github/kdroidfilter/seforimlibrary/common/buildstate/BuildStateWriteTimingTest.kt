package io.github.kdroidfilter.seforimlibrary.common.buildstate

import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.test.assertContains
import kotlin.test.assertTrue

/**
 * The five build_state snapshots cost 546 s (26%) of the audited generation step
 * and reported no duration at all, so the cost was invisible in the log and only
 * showed up as an unexplained silent gap in the phase splitter.
 *
 * Two things are pinned here: the line now carries its own elapsed seconds, and
 * it still ENDS with `links=<N>)` — pipeline-monitor's `buildstate_write_2`
 * marker anchors on that with a `$`-terminated regex, so appending anything
 * after the counters would silently unhook the phase split.
 */
class BuildStateWriteTimingTest {

    @JvmField
    @Rule
    val tmp = TemporaryFolder()

    private class Capture : LogWriter() {
        val lines = mutableListOf<String>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += message
        }
    }

    private fun writeSnapshotAndCaptureLog(): String {
        val capture = Capture()
        val logger = Logger(StaticConfig(Severity.Verbose, listOf(capture)), "BuildStateWriter")
        val target = tmp.newFolder().toPath().resolve("seforim.db.buildstate")
        BuildStateWriter(logger).write(BuildStateSnapshot.empty(), target)
        return capture.lines.single { it.startsWith("build_state.db snapshot written to ") }
    }

    @Test
    fun `the snapshot line reports its own elapsed seconds`() {
        val line = writeSnapshotAndCaptureLog()
        assertTrue(
            Regex(""" in \d+\.\ds \(""").containsMatchIn(line),
            "expected an ` in <s.d>s (` duration, got: $line",
        )
    }

    @Test
    fun `the snapshot line still ends with the counters the phase splitter anchors on`() {
        val line = writeSnapshotAndCaptureLog()
        assertContains(line, "(books=0, lines=0, tocEntries=0, links=0)")
        assertTrue(
            Regex("""lines=\d+, tocEntries=\d+, links=\d+\)$""").containsMatchIn(line),
            "pipeline-monitor's buildstate_write_2 marker requires this line to END " +
                "with `links=<N>)`; got: $line",
        )
    }

    @Test
    fun `the duration is locale-independent`() {
        val previous = java.util.Locale.getDefault()
        try {
            // A locale whose decimal separator is a comma would turn "113.2" into
            // "113,2" under String.format; the writer uses integer arithmetic.
            java.util.Locale.setDefault(java.util.Locale.of("de", "DE"))
            assertTrue(
                Regex(""" in \d+\.\ds \(""").containsMatchIn(writeSnapshotAndCaptureLog()),
                "the elapsed seconds must not follow the runner's locale",
            )
        } finally {
            java.util.Locale.setDefault(previous)
        }
    }
}
