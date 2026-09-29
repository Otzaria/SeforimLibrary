package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Writes the schema barrier ("full rebase marker") a release publishes for an
 * anchor whose DB schema no delta can leave (see [requiresFullRebase]):
 *
 *  - `<out>.zst`: a tiny zstd'd patch.db holding only `patch_meta`, with
 *    `schema_version` = [BARRIER_PATCH_FORMAT_VERSION] so every applier refuses it
 *    at preflight;
 *  - `<out>.zst.manifest.json`: an ordinary per-delta manifest with that
 *    `patchFormatVersion`, `"fullRebase": true` and [FULL_REBASE_HASH] as both
 *    content hashes.
 *
 * An old updater sees a valid but unsupported edge ("app update required") and never
 * downloads the new full DB; a new one reads `fullRebase` and downloads the full DB.
 *
 * System properties: `out` (the `patch-v<A>-v<N>.db` path), `fromVersion`,
 * `toVersion`, `fromSchemaVersion`, `toSchemaVersion`.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SchemaBarrierCli")
    fun intProp(key: String): Int =
        System.getProperty(key)?.toIntOrNull() ?: error("-P$key= missing or not an integer")

    val out = Paths.get(System.getProperty("out") ?: error("-Pout= missing"))
    val manifest = writeSchemaBarrier(
        out = out,
        fromVersion = intProp("fromVersion"),
        toVersion = intProp("toVersion"),
        fromSchemaVersion = intProp("fromSchemaVersion"),
        toSchemaVersion = intProp("toSchemaVersion"),
        logger = logger,
    )
    logger.i { "Schema barrier written: $manifest" }
}

/** Never a real patch format: an applier that meets it refuses the patch. */
internal const val BARRIER_PATCH_FORMAT_VERSION: Int = 999

/** Content-hash sentinel of a barrier; no DB can hash to it. */
internal const val FULL_REBASE_HASH: String = "full-rebase"

/** Writes `<out>.zst` and its manifest, removes the raw `<out>`, returns the manifest path. */
internal fun writeSchemaBarrier(
    out: Path,
    fromVersion: Int,
    toVersion: Int,
    fromSchemaVersion: Int,
    toSchemaVersion: Int,
    logger: Logger = Logger.withTag("SchemaBarrier"),
): Path {
    require(fromVersion in 1 until toVersion) { "barrier v$fromVersion -> v$toVersion must move forward" }
    require(requiresFullRebase(fromSchemaVersion, toSchemaVersion)) {
        "schema $fromSchemaVersion -> $toSchemaVersion is patchable; a barrier would hide a real delta"
    }
    val target = out.toAbsolutePath()
    Files.createDirectories(target.parent)
    Files.deleteIfExists(target)
    DriverManager.getConnection("jdbc:sqlite:$target").use { conn ->
        conn.createStatement().use { it.execute(PatchDbSchema.baseStatements.first()) }
        conn.prepareStatement("INSERT INTO patch_meta(key, value) VALUES (?, ?)").use { ps ->
            listOf(
                "schema_version" to BARRIER_PATCH_FORMAT_VERSION.toString(),
                "from_version" to fromVersion.toString(),
                "to_version" to toVersion.toString(),
                "full_rebase" to "true",
            ).forEach { (k, v) -> ps.setString(1, k); ps.setString(2, v); ps.executeUpdate() }
        }
    }
    val compressed = PatchCompressor.compress(target, level = 19, workers = 1)
    val manifest = ReleaseManifestWriter(logger).writeManifest(
        patchFile = target,
        fromVersion = fromVersion,
        toVersion = toVersion,
        fromSchemaVersion = fromSchemaVersion,
        toSchemaVersion = toSchemaVersion,
        fromContentHash = FULL_REBASE_HASH,
        toContentHash = FULL_REBASE_HASH,
        compressed = ReleaseManifestWriter.CompressedPatchSpec(
            file = compressed.compressedFile,
            sha256 = compressed.compressedSha256,
            size = compressed.compressedSize,
            compression = "zstd",
        ),
        patchFormatVersion = BARRIER_PATCH_FORMAT_VERSION,
        fullRebase = true,
    )
    Files.deleteIfExists(target)
    return manifest
}
