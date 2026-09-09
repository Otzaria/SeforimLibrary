package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.db.SEFORIM_DB_PAGE_SIZE_PRAGMA
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * One-step conversion: Sefaria export -> SQLite (direct import, sans Otzaria intermédiaire).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:generateSefariaSqlite -PseforimDb=/path/to.db [-PexportDir=/path/to/database_export]
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SefariaSqlite")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = when {
        System.getProperty("inMemoryDb") != null -> System.getProperty("inMemoryDb") != "false"
        System.getenv("IN_MEMORY_DB") != null -> System.getenv("IN_MEMORY_DB") != "false"
        dbPath == ":memory:" -> true
        else -> true // default to in-memory for perf
    }
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: dbPath

    val exportDirArg = args.getOrNull(1)
        ?: System.getProperty("exportDir")
        ?: System.getenv("SEFARIA_EXPORT_DIR")
    val exportRoot: Path = exportDirArg?.let { Paths.get(it) } ?: SefariaExportFetcher.ensureLocalExport(logger)

    // Prepare DB (optionally in-memory)
    if (!useMemoryDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            val backup = File("$dbPath.bak")
            if (backup.exists()) backup.delete()
            if (dbFile.renameTo(backup)) {
                logger.i { "Existing DB moved to ${backup.absolutePath}" }
            } else {
                logger.w { "Failed to move existing DB; it will be overwritten." }
            }
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // Must run before any table is created so the DB is born with 16 KiB pages.
    // For the in-memory path, VACUUM INTO carries this page size to the on-disk file.
    driver.execute(null, SEFORIM_DB_PAGE_SIZE_PRAGMA, 0)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so importer logs (incl. per-type counters) stay visible.
    Logger.setMinSeverity(Severity.Info)

    // ─── IdAllocator wiring (delta-update support, DELTA_UPDATE_PLAN.md §3.5) ──
    // Load the previous build_state.db so primary keys remain stable across
    // builds. Path defaults to <dbPath>.buildstate; override via -PbuildStatePath
    // or BUILD_STATE_PATH env var.
    val buildStatePath: Path = run {
        val explicit = System.getProperty("buildStatePath")
            ?: System.getenv("BUILD_STATE_PATH")
        if (explicit != null) Paths.get(explicit)
        else Paths.get("$persistDbPath.buildstate")
    }
    val prevBuildState: Path? = buildStatePath.takeIf { java.nio.file.Files.exists(it) }
    val allocator = InMemoryIdAllocator.load(
        path = prevBuildState,
        logger = Logger.withTag("IdAllocator"),
    )
    if (prevBuildState != null) {
        logger.i { "Loaded previous build_state from $prevBuildState" }
    } else {
        logger.i { "No previous build_state at $buildStatePath — starting fresh." }
    }

    // Build version: pulled from -PbuildVersion / BUILD_VERSION env, defaults to current epoch seconds.
    val buildVersion: Int = (System.getProperty("buildVersion")
        ?: System.getenv("BUILD_VERSION"))
        ?.toIntOrNull()
        ?: (System.currentTimeMillis() / 1000).toInt()

    // Path of the persisted DB the metrics report will describe (VACUUM INTO
    // target for the memory path; the live file otherwise).
    val persistedDbPath = if (useMemoryDb) persistDbPath else dbPath

    try {
        // Create the output directory up front so a late failure can't leave a
        // fresh metrics report next to a missing/stale DB.
        File(persistedDbPath).absoluteFile.parentFile?.mkdirs()

        val importer = SefariaDirectImporter(
            exportRoot = exportRoot,
            repository = repository,
            allocator = allocator,
            buildVersion = buildVersion,
            logger = Logger.withTag("SefariaDirect")
        )
        importer.import()

        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
            val outFile = File(persistDbPath)
            outFile.parentFile?.mkdirs()
            if (outFile.exists()) {
                val backup = File(persistDbPath + ".bak")
                if (backup.exists()) backup.delete()
                if (!outFile.renameTo(backup)) {
                    outFile.delete()
                }
                logger.i { "Existing DB moved to ${backup.absolutePath}" }
            }
            val escaped = persistDbPath.replace("'", "''")
            logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO..." }
            repository.executeRawQuery("VACUUM INTO '$escaped'")
            logger.i { "In-memory DB persisted to $persistDbPath" }
        }

        // Machine-checkable per-type link-import metrics, written as a sibling of
        // the shipped DB (QA plan §10.5). Written ONLY after the DB is persisted,
        // and atomically (temp file + ATOMIC_MOVE) so a failed save never leaves a
        // fresh report beside a stale/missing DB. Shape: [LinkImportMetricsReport].
        importer.linkImportMetrics?.let { metrics ->
            val report = LinkImportMetricsReport(
                dbSchemaVersion = repository.getSchemaMeta("db_schema_version"),
                dbVersion = repository.getSchemaMeta("db_version"),
                dbSizeBytes = File(persistedDbPath).length(),
                insertedByType = metrics.insertedByType,
                persistedByType = importer.persistedLinkCountsByType ?: emptyMap(),
            )
            val reportPath = Paths.get("$persistDbPath.link-import-metrics.json")
            val dir = reportPath.toAbsolutePath().parent
            val tmp = Files.createTempFile(dir, "link-import-metrics", ".json.tmp")
            Files.writeString(tmp, report.toJsonReport())
            Files.move(tmp, reportPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            logger.i { "Link-import metrics report written to $reportPath" }
        } ?: logger.i { "No links phase ran — link-import metrics report not written." }

        // Persist build_state.db so the next build re-uses the same primary keys.
        // Already ordered after the VACUUM INTO above, so a failed persist cannot
        // advance the buildstate.
        val buildStateMeta = mapOf(
            "generator" to "sefariasqlite",
            "generated_at" to java.time.Instant.now().toString(),
            "build_version" to buildVersion.toString(),
        )
        runCatching {
            allocator.snapshotTo(target = buildStatePath, extraMeta = buildStateMeta)
        }.onFailure { e ->
            // Fail closed: a build that cannot write its allocator state would
            // publish last week's — and the build after it would re-issue ids
            // this one already handed out.
            logger.e(e) { "Failed to write build_state to $buildStatePath" }
            throw e
        }
        BuildStateVerifier.verifyFreshSnapshot(
            buildStatePath = buildStatePath,
            dbPath = Paths.get(persistedDbPath),
            expectedMeta = buildStateMeta,
            logger = logger,
        )

        logger.i { "Sefaria -> SQLite completed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during Sefaria->SQLite generation" }
        throw e
    } finally {
        repository.close()
    }
}
