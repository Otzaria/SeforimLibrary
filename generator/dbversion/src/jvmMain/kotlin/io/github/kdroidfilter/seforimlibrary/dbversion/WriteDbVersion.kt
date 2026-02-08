package io.github.kdroidfilter.seforimlibrary.dbversion

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.system.exitProcess

/**
 * Reads the content version from a backup database file.
 * Returns null if the file doesn't exist or version cannot be read.
 */
fun readContentVersionFromBackup(backupPath: String, logger: Logger): Int? {
    val backupFile = File(backupPath)
    if (!backupFile.exists()) {
        logger.d { "Backup file not found: $backupPath" }
        return null
    }

    var driver: JdbcSqliteDriver? = null
    try {
        logger.d { "Reading content version from backup: $backupPath" }
        driver = JdbcSqliteDriver("jdbc:sqlite:$backupPath")
        
        // Check if db_meta table exists
        val tableExists = driver.executeQuery(
            identifier = null,
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='db_meta'",
            mapper = { cursor: SqlCursor ->
                QueryResult.Value(cursor.next().value)
            },
            parameters = 0
        ).value

        if (!tableExists) {
            logger.d { "db_meta table not found in backup" }
            return null
        }

        // Read content_version_int
        val version = driver.executeQuery(
            identifier = null,
            sql = "SELECT value FROM db_meta WHERE key='content_version_int'",
            mapper = { cursor: SqlCursor ->
                if (cursor.next().value) {
                    QueryResult.Value(cursor.getString(0))
                } else {
                    QueryResult.Value(null)
                }
            },
            parameters = 0
        ).value

        return version?.toIntOrNull()?.also {
            logger.i { "Found content version in backup: $it" }
        }
    } catch (e: Exception) {
        logger.w(e) { "Error reading version from backup: ${e.message}" }
        return null
    } finally {
        driver?.close()
    }
}

/**
 * Reads the existing manifest file.
 * Returns null if the file doesn't exist or cannot be parsed.
 */
fun readManifest(manifestPath: String, logger: Logger): Manifest? {
    val manifestFile = File(manifestPath)
    if (!manifestFile.exists()) {
        logger.d { "Manifest file not found: $manifestPath" }
        return null
    }

    return try {
        val json = Json { ignoreUnknownKeys = true; prettyPrint = true }
        val content = manifestFile.readText()
        json.decodeFromString<Manifest>(content).also {
            logger.d { "Read manifest: schema=${it.latest_schema}, content=${it.latest_content}, patches=${it.patches.size}" }
        }
    } catch (e: Exception) {
        logger.w(e) { "Error reading manifest: ${e.message}" }
        null
    }
}

/**
 * Writes/updates the manifest file with current version information.
 */
fun updateManifest(
    manifestPath: String,
    schemaVersion: Int,
    contentVersion: Int,
    logger: Logger
) {
    try {
        val manifestFile = File(manifestPath)
        manifestFile.parentFile?.mkdirs()

        // Read existing manifest or create new one
        val existingManifest = readManifest(manifestPath, logger)

        val existingPatches = existingManifest?.patches ?: emptyList()
        val existingLatestContent = existingManifest?.latest_content
        val shouldAddPatch = existingLatestContent != null && existingLatestContent != contentVersion
        val nextPatches = if (shouldAddPatch) {
            val alreadyPresent = existingPatches.any { it.from == existingLatestContent && it.to == contentVersion }
            if (alreadyPresent) {
                existingPatches
            } else {
                val fileName = "patch_${existingLatestContent}_to_${contentVersion}.bin"
                existingPatches + PatchEntry(
                    from = existingLatestContent,
                    to = contentVersion,
                    file = fileName,
                    sha256 = "pending"
                )
            }
        } else {
            existingPatches
        }
        
        // Create updated manifest (preserve existing patches)
        val updatedManifest = Manifest(
            latest_schema = schemaVersion,
            latest_content = contentVersion,
            patches = nextPatches
        )
        
        // Write to file
        val json = Json { 
            prettyPrint = true
            encodeDefaults = true  // Ensure empty arrays are written
        }
        val manifestJson = json.encodeToString(updatedManifest)
        manifestFile.writeText(manifestJson)
        
        logger.i { "Updated manifest: $manifestPath" }
        logger.d { "Manifest content: $manifestJson" }
    } catch (e: Exception) {
        logger.e(e) { "Error updating manifest: ${e.message}" }
        throw e
    }
}

private fun resolveManifestPath(dbPath: String): String {
    val override = System.getProperty("manifestPath")?.takeIf { it.isNotBlank() }
        ?: System.getenv("MANIFEST_PATH")?.takeIf { it.isNotBlank() }
    if (override != null) {
        return override
    }

    val dbFile = Paths.get(dbPath)
    val fileName = dbFile.fileName.toString()
    val manifestName = if (fileName.endsWith(".db")) {
        fileName.removeSuffix(".db") + "-manifest.json"
    } else {
        "$fileName-manifest.json"
    }
    val parent = dbFile.parent ?: Paths.get(".")
    return parent.resolve(manifestName).toAbsolutePath().toString()
}

/**
 * Converts content version to date format (YYYYMMDD).
 * If contentVersion is already in date format (>= 20000000), returns it as-is.
 * Otherwise, returns today's date in YYYYMMDD format.
 */
fun contentVersionToDateFormat(contentVersion: Int): Int {
    // If it looks like a date (YYYYMMDD format), use it as-is
    if (contentVersion >= 20000000 && contentVersion <= 99999999) {
        return contentVersion
    }
    
    // Otherwise, convert current date to YYYYMMDD
    return LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInt()
}

/**
 * Entry point for the DB Versioning Agent.
 * Reads configuration from system properties and writes version metadata to the database.
 *
 * Required system properties:
 * - schemaVersion: Schema version (integer >= 1)
 * - contentVersion: Content version (optional - auto-increments from backup if not provided)
 *
 * Optional system properties:
 * - buildId: Build identifier (defaults to git SHA, CI build number, or timestamp)
 * - appId: Application ID in hex format (e.g., "0x5346524D")
 *
 * Command line arguments:
 * - args[0]: Path to SQLite database file (defaults to build/seforim.db)
 *
 * Version logic:
 * - If contentVersion is provided: use it
 * - If backup exists (build/seforim.db.bak): read version from backup and increment by 1
 * - Otherwise: start at version 1
 *
 * Usage:
 *   ./gradlew :dbversion:writeDbVersion -PschemaVersion=1
 *   ./gradlew :dbversion:writeDbVersion -PschemaVersion=1 -PcontentVersion=5 -PbuildId=abc123
 */
fun main(args: Array<String>) {
    // Configure logging
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DbVersion")

    try {
        logger.i { "DB Versioning Agent starting..." }

        // Parse database path
        val dbPath = if (args.isNotEmpty()) {
            args[0]
        } else {
            Paths.get("build", "seforim.db").toAbsolutePath().toString()
        }

        logger.i { "Database path: $dbPath" }

        // Determine backup path
        val backupPath = dbPath.replace(".db", ".db.bak")
        logger.d { "Backup path: $backupPath" }

        // Parse required properties
        val schemaVersionStr = System.getProperty("schemaVersion")
        if (schemaVersionStr == null) {
            logger.e { "Missing required property: -PschemaVersion" }
            throw IllegalArgumentException("Missing required property: -PschemaVersion")
        }

        val schemaVersion = try {
            schemaVersionStr.toInt()
        } catch (e: NumberFormatException) {
            logger.e { "Invalid schemaVersion: $schemaVersionStr (must be an integer)" }
            throw IllegalArgumentException("Invalid schemaVersion: $schemaVersionStr (must be an integer)", e)
        }

        // Validate schema version
        if (schemaVersion < 1) {
            logger.e { "schemaVersion must be >= 1, got: $schemaVersion" }
            throw IllegalArgumentException("schemaVersion must be >= 1, got: $schemaVersion")
        }

        // Determine content version
        val contentVersionStr = System.getProperty("contentVersion")
        val contentVersion = if (contentVersionStr != null) {
            // Use provided version
            try {
                contentVersionStr.toInt().also {
                    logger.i { "Using provided content version: $it" }
                }
            } catch (e: NumberFormatException) {
                logger.e { "Invalid contentVersion: $contentVersionStr (must be an integer)" }
                throw IllegalArgumentException("Invalid contentVersion: $contentVersionStr (must be an integer)", e)
            }
        } else {
            // Auto-increment from backup
            val backupVersion = readContentVersionFromBackup(backupPath, logger)
            if (backupVersion != null) {
                (backupVersion + 1).also {
                    logger.i { "Auto-incrementing from backup version $backupVersion to $it" }
                }
            } else {
                1.also {
                    logger.i { "No backup found, starting at version $it" }
                }
            }
        }

        // Validate content version
        if (contentVersion < 1) {
            logger.e { "contentVersion must be >= 1, got: $contentVersion" }
            throw IllegalArgumentException("contentVersion must be >= 1, got: $contentVersion")
        }

        // Parse optional properties
        val buildId = System.getProperty("buildId") ?: run {
            // Try environment variables
            System.getenv("GITHUB_SHA")
                ?: System.getenv("BUILD_ID")
                ?: run {
                    // Default to timestamp with "manual-" prefix
                    val timestamp = ZonedDateTime.now(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                    "manual-$timestamp"
                }
        }

        val appId = System.getProperty("appId") // null if not provided

        logger.i { "Configuration: schemaVersion=$schemaVersion, contentVersion=$contentVersion, buildId=$buildId, appId=$appId" }

        // Create agent and write version
        val agent = DbVersionAgent(
            dbPath = dbPath,
            schemaVersion = schemaVersion,
            contentVersion = contentVersion,
            buildId = buildId,
            appIdHex = appId
        )

        val report = agent.writeVersion()

        // Output JSON report to stdout
        val json = Json { prettyPrint = true; encodeDefaults = true }
        val reportJson = json.encodeToString(report)
        println(reportJson)

        // Update manifest file
        val manifestPath = resolveManifestPath(dbPath)
        // Use the actual content version that was written to DB (from report)
        val contentVersionForManifest = contentVersionToDateFormat(report.contentVersionInt)
        logger.i { "Updating manifest with content version: $contentVersionForManifest (from DB version ${report.contentVersionInt})" }
        updateManifest(manifestPath, report.schemaVersionInt, contentVersionForManifest, logger)

        // Check status
        if (report.status != "ok" || report.integrityCheck != "ok" || report.foreignKeyCheckRows > 0) {
            logger.e { "❌ DB integrity check failed!" }
            logger.e { "Integrity check result: ${report.integrityCheck}" }
            logger.e { "Foreign key violations: ${report.foreignKeyCheckRows}" }
            exitProcess(1)
        } else {
            logger.i { "✅ DB versioning completed successfully" }
            logger.i { "Schema version: ${report.schemaVersionInt}" }
            logger.i { "Content version: ${report.contentVersionStr}" }
            logger.i { "DB UUID: ${report.dbUuid}" }
            logger.i { "Manifest updated: $manifestPath" }
        }
    } catch (e: IllegalArgumentException) {
        logger.e(e) { "Invalid arguments: ${e.message}" }
        exitProcess(1)
    } catch (e: Exception) {
        logger.e(e) { "Fatal error: ${e.message}" }
        logger.e { "Stack trace:\n${e.stackTraceToString()}" }
        exitProcess(1)
    }
}
