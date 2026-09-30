package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Writes the logical content hash of a finished `seforim.db` to `out` (hex + LF):
 * the same value a patch manifest ending at this DB carries as `toContentHash`.
 *
 * Required system properties: `dbPath`, `out`. Optional `dbSchemaVersion`
 * overrides `schema_meta.db_schema_version`, exactly as in [PatchPipelineCli].
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("LogicalContentHashCli")
    val dbPath = Paths.get(System.getProperty("dbPath") ?: error("-PdbPath= missing"))
    val out = Paths.get(System.getProperty("out") ?: error("-Pout= missing"))
    val started = System.nanoTime()
    val hash = writeLogicalContentHash(dbPath, out)
    logger.i { "contentHash $hash of $dbPath in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

/** Hashes [dbPath] with the hasher of its own schema version and writes it to [out]. */
internal fun writeLogicalContentHash(dbPath: Path, out: Path): String {
    val schemaVersion = resolveSchemaVersion(dbPath, "dbSchemaVersion")
    val hash = DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it)
    }
    Files.writeString(out, hash + "\n")
    return hash
}
