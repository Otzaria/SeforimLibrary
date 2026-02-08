package io.github.kdroidfilter.seforimlibrary.dbversion

import kotlinx.serialization.Serializable

/**
 * Report generated after writing version metadata to the database.
 *
 * @property dbPath Path to the SQLite database file
 * @property schemaVersionInt Schema version (PRAGMA user_version)
 * @property contentVersionInt Content version (numeric counter)
 * @property contentVersionStr Content version display string (e.g., "v1-2026-02-05")
 * @property buildId Build identifier (git SHA, CI build number, or timestamp)
 * @property applicationIdHex Application ID in hex format (e.g., "0x5346524D"), or null if not set
 * @property dbUuid Unique database identifier (generated on first write, persisted thereafter)
 * @property integrityCheck Result of PRAGMA integrity_check ("ok" or error message)
 * @property foreignKeyCheckRows Number of foreign key constraint violations found (0 = none)
 * @property status Overall status ("ok" or "failed")
 */
@Serializable
data class VersionReport(
    val dbPath: String,
    val schemaVersionInt: Int,
    val contentVersionInt: Int,
    val contentVersionStr: String,
    val buildId: String,
    val applicationIdHex: String?,
    val dbUuid: String,
    val integrityCheck: String,
    val foreignKeyCheckRows: Int,
    val status: String = "ok"
)
