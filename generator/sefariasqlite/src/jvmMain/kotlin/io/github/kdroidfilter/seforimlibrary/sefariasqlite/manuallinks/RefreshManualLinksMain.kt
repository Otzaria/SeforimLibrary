package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import java.nio.file.Path
import kotlin.system.exitProcess

fun main() {
    val arguments = runCatching(::readManualLinksArguments).getOrElse { error ->
        System.err.println("manual-links configuration error: ${error.message}")
        exitProcess(2)
    }
    runCatching { ManualLinksRefresh(arguments, Logger.withTag("ManualLinksRefresh")).run() }
        .onSuccess { result ->
            println("manual-links status=${result.status}")
            println("lineage=${ManualLinksJson.stableHash(result.lineage.toJson())}")
            println("report=${result.reportPath}")
            println("marker=${result.markerPath}")
        }
        .onFailure { error ->
            System.err.println("manual-links failed: ${error.message}")
            error.printStackTrace(System.err)
            exitProcess(1)
        }
}

internal fun readManualLinksArguments(): ManualLinksArguments {
    fun required(name: String): String = System.getProperty(name)?.takeIf { it.isNotBlank() }
        ?: error("Missing required -P$name")
    fun optional(name: String): String? = System.getProperty(name)?.takeIf { it.isNotBlank() }
    fun path(name: String): Path = Path.of(required(name)).toAbsolutePath().normalize()
    fun optionalPath(name: String): Path? = optional(name)?.let { Path.of(it).toAbsolutePath().normalize() }

    val mode = when (required("manualLinksMode")) {
        "bootstrap" -> ManualLinksMode.BOOTSTRAP
        "refresh" -> ManualLinksMode.REFRESH
        "migrate" -> ManualLinksMode.MIGRATE
        else -> error("manualLinksMode must be bootstrap, refresh or migrate")
    }
    val lineage = optionalPath("manualLinksLineage")
    val changelog = optionalPath("sefariaChangelogDir")
    when (mode) {
        ManualLinksMode.BOOTSTRAP -> {
            require(lineage == null) { "bootstrap does not accept manualLinksLineage" }
            require(changelog == null) { "bootstrap does not accept sefariaChangelogDir" }
        }
        ManualLinksMode.REFRESH -> {
            require(lineage != null) { "refresh requires manualLinksLineage" }
            require(changelog != null) { "refresh requires sefariaChangelogDir" }
        }
        ManualLinksMode.MIGRATE -> {
            require(lineage != null) { "migrate requires manualLinksLineage" }
            required("expectedOldConfigSha256")
            required("expectedOldToolCommit")
        }
    }
    return ManualLinksArguments(
        mode = mode,
        repository = path("manualLinksRepo"),
        configPath = path("manualLinksConfig"),
        lineagePath = lineage,
        expectedOldConfigSha256 = optional("expectedOldConfigSha256"),
        expectedOldToolCommit = optional("expectedOldToolCommit"),
        sefariaExport = path("sefariaExport"),
        releaseMetadataPath = path("sefariaReleaseMetadata"),
        releaseMetadataSha256 = required("sefariaReleaseMetadataSha256"),
        changelogDir = changelog,
        seforimToolCommit = required("seforimToolCommit"),
        output = path("manualLinksOutput"),
        anchorUnrelocatableCap = optional("anchorUnrelocatableCap")?.let { value ->
            value.toIntOrNull()?.takeIf { it >= 0 } ?: error("anchorUnrelocatableCap must be a non-negative integer")
        } ?: ManualLinksAnchor.DEFAULT_UNRELOCATABLE_CAP,
    )
}
