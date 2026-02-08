package io.github.kdroidfilter.seforimlibrary.dbversion

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * DB Versioning Agent - writes version metadata to SQLite database and performs integrity checks.
 *
 * Responsibilities:
 * - Write PRAGMA user_version (schema version)
 * - Write PRAGMA application_id (optional)
 * - Create/update db_meta table with version information
 * - Verify all written values within the transaction
 * - Run integrity checks after commit
 * - Generate JSON report
 *
 * @property dbPath Path to the SQLite database file
 * @property schemaVersion Schema version (must be >= 1)
 * @property contentVersion Content version counter (must be >= 1)
 * @property buildId Build identifier (git SHA, CI build number, or timestamp)
 * @property appIdHex Optional application ID in hex format (e.g., "0x5346524D")
 */
class DbVersionAgent(
    private val dbPath: String,
    private val schemaVersion: Int,
    private val contentVersion: Int,
    private val buildId: String,
    private val appIdHex: String? = null
) {
    private val logger = Logger.withTag("DbVersionAgent")

    init {
        require(schemaVersion >= 1) { "schemaVersion must be >= 1, got: $schemaVersion" }
        require(contentVersion >= 1) { "contentVersion must be >= 1, got: $contentVersion" }
        require(buildId.isNotBlank()) { "buildId cannot be blank" }
    }

    /**
     * Writes version metadata to the database and returns a report.
     * All writes are performed in a single transaction with verification.
     */
    fun writeVersion(): VersionReport = runBlocking {
        logger.i { "Starting DB versioning: schema=$schemaVersion, content=$contentVersion, build=$buildId" }

        // Generate content version string: "v{contentVersion}-{date}"
        val currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
        val contentVersionStr = "v$contentVersion-$currentDate"

        var driver: JdbcSqliteDriver? = null
        var repository: SeforimRepository? = null
        var dbUuid = ""

        try {
            // Open database connection
            logger.d { "Opening database: $dbPath" }
            driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
            repository = SeforimRepository(dbPath, driver)

            // Enable foreign keys
            logger.d { "Enabling foreign key constraints" }
            repository.executeRawQuery("PRAGMA foreign_keys = ON")

            // Write metadata in a transaction
            logger.i { "Writing version metadata in transaction" }
            repository.runInTransaction {
                try {
                    // Write application_id if provided
                    if (appIdHex != null) {
                        logger.d { "Writing PRAGMA application_id = $appIdHex" }
                        repository.executeRawQuery("PRAGMA application_id = $appIdHex")
                    }

                    // Write user_version (schema version)
                    logger.d { "Writing PRAGMA user_version = $schemaVersion" }
                    repository.executeRawQuery("PRAGMA user_version = $schemaVersion")

                    // Create db_meta table (idempotent)
                    logger.d { "Creating db_meta table if not exists" }
                    repository.executeRawQuery("""
                        CREATE TABLE IF NOT EXISTS db_meta (
                            key TEXT PRIMARY KEY,
                            value TEXT NOT NULL
                        )
                    """.trimIndent())

                    // Write mandatory keys
                    logger.d { "Writing mandatory metadata keys" }
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('content_version_int', '$contentVersion')"
                    )
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('content_version_str', '$contentVersionStr')"
                    )
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('build_id', '${buildId.replace("'", "''")}')"
                    )

                    // Write optional keys
                    logger.d { "Writing optional metadata keys" }
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('created_at_utc', datetime('now'))"
                    )
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('generator_agent', 'DB Versioning Agent')"
                    )
                    repository.executeRawQuery(
                        "INSERT OR REPLACE INTO db_meta (key, value) VALUES ('schema_version_int', '$schemaVersion')"
                    )

                    // Create db_uuid only if it doesn't exist
                    logger.d { "Checking for existing db_uuid" }
                    val existingUuid = driver.executeQuery(
                        identifier = null,
                        sql = "SELECT value FROM db_meta WHERE key = 'db_uuid'",
                        mapper = { cursor: SqlCursor ->
                            if (cursor.next().value) {
                                QueryResult.Value(cursor.getString(0))
                            } else {
                                QueryResult.Value(null)
                            }
                        },
                        parameters = 0
                    ).value

                    if (existingUuid == null) {
                        logger.d { "Generating new db_uuid" }
                        repository.executeRawQuery(
                            "INSERT INTO db_meta (key, value) VALUES ('db_uuid', lower(hex(randomblob(16))))"
                        )
                    } else {
                        logger.d { "db_uuid already exists: $existingUuid" }
                    }

                    // Verify written values within transaction
                    logger.d { "Verifying written values" }
                    val verifiedUserVersion = driver.executeQuery(
                        identifier = null,
                        sql = "PRAGMA user_version",
                        mapper = { cursor: SqlCursor ->
                            if (cursor.next().value) {
                                QueryResult.Value(cursor.getLong(0)?.toInt())
                            } else {
                                QueryResult.Value(null)
                            }
                        },
                        parameters = 0
                    ).value

                    if (verifiedUserVersion != schemaVersion) {
                        throw IllegalStateException(
                            "Verification failed: PRAGMA user_version = $verifiedUserVersion, expected $schemaVersion"
                        )
                    }

                    // Verify db_meta values
                    val verifiedContentVersion = driver.executeQuery(
                        identifier = null,
                        sql = "SELECT value FROM db_meta WHERE key = 'content_version_int'",
                        mapper = { cursor: SqlCursor ->
                            if (cursor.next().value) QueryResult.Value(cursor.getString(0))
                            else QueryResult.Value(null)
                        },
                        parameters = 0
                    ).value

                    if (verifiedContentVersion != contentVersion.toString()) {
                        throw IllegalStateException(
                            "Verification failed: content_version_int = $verifiedContentVersion, expected $contentVersion"
                        )
                    }

                    // Read db_uuid for report
                    dbUuid = driver.executeQuery(
                        identifier = null,
                        sql = "SELECT value FROM db_meta WHERE key = 'db_uuid'",
                        mapper = { cursor: SqlCursor ->
                            if (cursor.next().value) QueryResult.Value(cursor.getString(0) ?: "")
                            else QueryResult.Value("")
                        },
                        parameters = 0
                    ).value

                    logger.i { "All metadata written and verified successfully" }
                } catch (e: Exception) {
                    logger.e(e) { "Error writing metadata, transaction will rollback: ${e.message}" }
                    throw e
                }
            }

            // Transaction committed successfully
            logger.i { "Transaction committed. Running integrity checks..." }

            // Run integrity checks AFTER commit
            val integrityResult = checkIntegrity(driver)
            val foreignKeyViolations = checkForeignKeys(driver)

            logger.i { "Integrity check: $integrityResult" }
            logger.i { "Foreign key violations: $foreignKeyViolations" }

            // Build report
            return@runBlocking VersionReport(
                dbPath = dbPath,
                schemaVersionInt = schemaVersion,
                contentVersionInt = contentVersion,
                contentVersionStr = contentVersionStr,
                buildId = buildId,
                applicationIdHex = appIdHex,
                dbUuid = dbUuid,
                integrityCheck = integrityResult,
                foreignKeyCheckRows = foreignKeyViolations,
                status = if (integrityResult == "ok" && foreignKeyViolations == 0) "ok" else "failed"
            )
        } catch (e: Exception) {
            logger.e(e) { "Fatal error during DB versioning: ${e.message}" }
            logger.e { "Stack trace: ${e.stackTraceToString()}" }
            throw e
        } finally {
            repository?.close()
            driver?.close()
            logger.d { "Database connection closed" }
        }
    }

    /**
     * Runs PRAGMA integrity_check and returns "ok" or error message.
     */
    private fun checkIntegrity(driver: JdbcSqliteDriver): String {
        return try {
            val result = driver.executeQuery(
                identifier = null,
                sql = "PRAGMA integrity_check",
                mapper = { cursor: SqlCursor ->
                    if (cursor.next().value) {
                        QueryResult.Value(cursor.getString(0) ?: "unknown")
                    } else {
                        QueryResult.Value("unknown")
                    }
                },
                parameters = 0
            ).value
            result
        } catch (e: Exception) {
            logger.e(e) { "Error running integrity_check: ${e.message}" }
            "error: ${e.message}"
        }
    }

    /**
     * Runs PRAGMA foreign_key_check and returns count of violations.
     */
    private fun checkForeignKeys(driver: JdbcSqliteDriver): Int {
        return try {
            var count = 0
            driver.executeQuery(
                identifier = null,
                sql = "PRAGMA foreign_key_check",
                mapper = { cursor: SqlCursor ->
                    while (cursor.next().value) {
                        count++
                    }
                    QueryResult.Value(count)
                },
                parameters = 0
            ).value
        } catch (e: Exception) {
            logger.e(e) { "Error running foreign_key_check: ${e.message}" }
            -1
        }
    }
}
