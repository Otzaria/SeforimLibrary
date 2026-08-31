package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Stamps `schema_meta.db_version` (and `schema_meta.db_schema_version`) into
 * a freshly-built `seforim.db` so the delta-update client can read its
 * current version off the live DB.
 *
 * Required system properties (forwarded via Gradle `-P` flags):
 *
 *   - `dbPath`         absolute path to the seforim.db to stamp
 *   - `dbVersion`      integer release version (matches release_meta.json
 *                      `latestVersion` and `deltas[].toVersion`)
 *   - `dbSchemaVersion` integer SQLDelight schema version (optional, defaults
 *                      to [PatchDbSchema.CURRENT_VERSION]); matches the
 *                      manifest's `toSchemaVersion`
 *
 * Idempotent: re-running with the same values is a no-op
 * (`INSERT OR REPLACE`).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("StampSchemaVersionCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val dbVersion = System.getProperty("dbVersion")?.toIntOrNull()
        ?: error("-PdbVersion= missing or not an integer")
    val dbSchemaVersion = System.getProperty("dbSchemaVersion")?.toIntOrNull()
        ?: PatchDbSchema.CURRENT_VERSION

    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        stampSchemaVersion(conn, dbVersion, dbSchemaVersion)
    }
    logger.i {
        "Stamped $dbPath: db_version=$dbVersion, db_schema_version=$dbSchemaVersion"
    }
}

/** Writes release/schema metadata only after the DB satisfies that schema's table contract. */
internal fun stampSchemaVersion(conn: Connection, dbVersion: Int, dbSchemaVersion: Int) {
    require(dbVersion >= 1) { "dbVersion=$dbVersion must be positive" }
    require(dbSchemaVersion in 1..PatchDbSchema.CURRENT_VERSION) {
        "dbSchemaVersion=$dbSchemaVersion is outside the supported range 1..${PatchDbSchema.CURRENT_VERSION}"
    }
    check(conn.autoCommit) { "stampSchemaVersion requires an unowned JDBC connection" }

    val requiredTables = when (dbSchemaVersion) {
        4 -> setOf("line_ref", "line_dh")
        else -> emptySet()
    }
    val existingTables = if (requiredTables.isEmpty()) {
        emptySet()
    } else {
        conn.prepareStatement(
            "SELECT name FROM sqlite_master WHERE type='table' " +
                "AND name IN (${requiredTables.joinToString { "?" }})",
        ).use { ps ->
            requiredTables.forEachIndexed { index, table -> ps.setString(index + 1, table) }
            ps.executeQuery().use { rs -> buildSet { while (rs.next()) add(rs.getString(1)) } }
        }
    }
    val missingTables = requiredTables - existingTables
    require(missingTables.isEmpty()) {
        "Cannot stamp DB as schema $dbSchemaVersion; missing required tables: " +
            missingTables.sorted().joinToString()
    }

    conn.autoCommit = false
    try {
        conn.prepareStatement(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES (?, ?)",
        ).use { ps ->
            ps.setString(1, "db_version"); ps.setString(2, dbVersion.toString()); ps.executeUpdate()
            ps.setString(1, "db_schema_version"); ps.setString(2, dbSchemaVersion.toString()); ps.executeUpdate()
        }
        conn.commit()
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}
