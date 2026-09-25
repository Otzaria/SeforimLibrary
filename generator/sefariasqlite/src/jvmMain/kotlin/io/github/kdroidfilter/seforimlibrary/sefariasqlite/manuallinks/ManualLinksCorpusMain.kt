package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import kotlin.system.exitProcess

/** Pinned full-corpus gate. Expected counts are explicit workflow inputs. */
fun main() {
    runCatching {
        fun expected(name: String): Int = System.getProperty(name)?.toIntOrNull()
            ?: error("Missing required -P$name")
        val result = ManualLinksRefresh(
            readManualLinksArguments(),
            Logger.withTag("ManualLinksCorpus"),
        ).run()
        val report = ManualLinksJson.readStrict(result.reportPath).requireObject("corpus report")
        val records = report.requiredObject("records")
        val anchors = report.requiredObject("anchors")
        require(records.requiredInt("target_sefaria_relevant") == expected("expectedTargetSefariaRecords"))
        require(records.requiredInt("source_sefaria_relevant") == expected("expectedSourceSefariaRecords"))
        require(anchors.requiredInt("checked") == expected("expectedAnchors"))
        // Drift the tool absorbed never fails it; the pinned corpus must still declare its exact count.
        require(anchors.requiredInt("unrelocatable") == expected("expectedUnrelocatableAnchors")) {
            "anchors.unrelocatable=${anchors.requiredInt("unrelocatable")} " +
                "but expectedUnrelocatableAnchors=${expected("expectedUnrelocatableAnchors")}"
        }
        require(report.requiredInt("packaging_collisions") == 0)
        println("manual-links corpus gate passed: ${result.reportPath}")
    }.onFailure { error ->
        System.err.println("manual-links corpus gate failed: ${error.message}")
        error.printStackTrace(System.err)
        exitProcess(1)
    }
}
