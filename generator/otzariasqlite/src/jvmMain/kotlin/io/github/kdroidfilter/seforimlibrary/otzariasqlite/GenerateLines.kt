package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.db.SEFORIM_DB_PAGE_SIZE_PRAGMA
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Phase 1 entry point: generate categories, books, TOCs and lines only.
 *
 * Usage examples:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronym.db]
 *   # To append to an existing DB instead of rotating it:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PappendExistingDb=true
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLines")

    val seforimDbPropOrEnv = System.getProperty("seforimDb") ?: System.getenv("SEFORIM_DB")
    val dbPath = args.getOrNull(0)
        ?: seforimDbPropOrEnv
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = (System.getProperty("inMemoryDb") == "true") || dbPath == ":memory:"
    val sourceDir = args.getOrNull(1)
        ?: System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()
    val acronymDbPath = args.getOrNull(2)
        ?: System.getProperty("acronymDb")
        ?: System.getenv("ACRONYM_DB")
        ?: run {
            // Prefer an already-downloaded DB under build/; otherwise fetch latest
            val defaultPath = Paths.get("build", "acronymizer", "acronymizer.db").toFile()
            if (defaultPath.exists() && defaultPath.isFile) defaultPath.absolutePath
            else AcronymizerFetcher.ensureLocalDb(logger).toAbsolutePath().toString()
        }
    val appendExistingDb = listOf(
        System.getProperty("appendExistingDb"),
        System.getenv("APPEND_EXISTING_DB")
    ).firstOrNull { !it.isNullOrBlank() }
        ?.let { it.equals("true", ignoreCase = true) || it == "1" }
        ?: false
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: if (appendExistingDb) seforimDbPropOrEnv else null
        ?: Paths.get("build", "seforim.db").toString()

    // If writing directly to disk, rotate existing DB; for in-memory we will persist at the end
    if (!useMemoryDb && !appendExistingDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            val backupFile = File("$dbPath.bak")
            if (backupFile.exists()) backupFile.delete()
            dbFile.renameTo(backupFile)
            logger.i { "Existing DB moved to ${backupFile.absolutePath}" }
        }
    } else if (!useMemoryDb && appendExistingDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            logger.i { "Appending to existing DB at ${dbFile.absolutePath}" }
        } else if (DbPublish.allowEmptyBase()) {
            logger.w { "appendExistingDb enabled but no DB found at $dbPath; -PallowEmptyBase is set, a new DB will be created" }
        } else {
            // Same rule as the in-memory seed below: appendExistingDb is an
            // explicit opt-in, so a DB that is not there is a lost input, not a
            // first build. Thrown before the driver opens — and thereby creates —
            // the file, so nothing is left at dbPath.
            throw IllegalStateException(
                DbPublish.missingBaseMessage(
                    "phase 1 (lines) with appendExistingDb",
                    dbFile.absolutePath,
                    "-PseforimDb / SEFORIM_DB",
                )
            )
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // Set 16 KiB pages before any table is created (no-op on an already-populated
    // DB, e.g. the appendExistingDb path). VACUUM INTO carries it to the on-disk file.
    driver.execute(null, SEFORIM_DB_PAGE_SIZE_PRAGMA, 0)
    // Ensure schema exists on a brand-new DB before repository init (idempotent)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)

    if (useMemoryDb && appendExistingDb) {
        val baseDbPath = System.getProperty("baseDb")
            ?: System.getenv("SEFORIM_DB_BASE")
            ?: seforimDbPropOrEnv
            ?: persistDbPath
        if (baseDbPath != null && baseDbPath != ":memory:") {
            val baseFile = File(baseDbPath)
            if (baseFile.exists()) {
                logger.i { "Seeding in-memory DB from base file: ${baseFile.absolutePath}" }
                // Names the table the copy died on; see the fail-closed note below.
                var copyingTable: String? = null
                runCatching {
                    repository.executeRawQuery("PRAGMA foreign_keys=OFF")
                    val escaped = baseFile.absolutePath.replace("'", "''")
                    repository.executeRawQuery("ATTACH DATABASE '$escaped' AS disk")
                    val tables = driver.executeQuery(
                        null,
                        "SELECT name FROM disk.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        { c: SqlCursor ->
                            val list = mutableListOf<String>()
                            while (c.next().value) {
                                c.getString(0)?.let { list.add(it) }
                            }
                            QueryResult.Value(list)
                        },
                        0
                    ).value
                    for (t in tables) {
                        copyingTable = t
                        repository.executeRawQuery("DELETE FROM \"$t\"")
                        repository.executeRawQuery("INSERT INTO \"$t\" SELECT * FROM disk.\"$t\"")
                    }
                    copyingTable = null
                    repository.executeRawQuery("DETACH DATABASE disk")
                    repository.executeRawQuery("PRAGMA foreign_keys=ON")
                    logger.i { "Seeding completed. Imported ${tables.size} tables." }
                }.onFailure { e ->
                    // Fail closed. In the release path baseDb and persistDb are the
                    // SAME file (build.gradle.kts:appendOtzariaLines), so "continuing
                    // with empty DB" meant the VACUUM INTO below would delete the DB
                    // this run produced and replace it with an empty one — reported
                    // as a success. A half-copied seed is just as fatal.
                    val where = copyingTable?.let { " while copying table \"$it\"" } ?: ""
                    logger.e(e) { "Failed to seed in-memory DB from $baseDbPath$where; aborting phase 1." }
                    // This seed runs BEFORE the try/finally below, which is what
                    // would otherwise close the repository on the way out.
                    runCatching { repository.close() }
                    throw e
                }
            } else if (DbPublish.allowEmptyBase()) {
                logger.w { "appendExistingDb enabled but base DB not found at $baseDbPath; -PallowEmptyBase is set, starting from an empty in-memory DB" }
            } else {
                // appendExistingDb is an EXPLICIT opt-in (property/env, default
                // false): a first-ever build simply does not set it and takes
                // the rotate path above. Once it IS set, the base is this run's
                // input, and since baseDb == persistDb in the release path
                // (build.gradle.kts:appendOtzariaLines) continuing meant
                // vacuuming an empty DB over the target and exiting 0.
                runCatching { repository.close() }
                throw IllegalStateException(
                    DbPublish.missingBaseMessage(
                        "phase 1 (lines) with appendExistingDb",
                        baseDbPath,
                        "-PbaseDb / SEFORIM_DB_BASE / -PseforimDb",
                    )
                )
            }
        } else {
            if (!DbPublish.allowEmptyBase()) {
                runCatching { repository.close() }
                throw IllegalStateException(
                    DbPublish.missingBaseMessage(
                        "phase 1 (lines) with appendExistingDb",
                        baseDbPath,
                        "-PbaseDb / SEFORIM_DB_BASE / -PseforimDb",
                    )
                )
            }
            logger.w { "appendExistingDb enabled in-memory but no base DB path provided; -PallowEmptyBase is set, starting from an empty DB" }
        }
    }

    // ─── IdAllocator (delta-update support) ────────────────────────────────────
    val buildStatePath: Path = run {
        val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
        if (explicit != null) Paths.get(explicit) else Paths.get("$persistDbPath.buildstate")
    }
    val prev = buildStatePath.takeIf { Files.exists(it) }
    val allocator = InMemoryIdAllocator.load(prev, Logger.withTag("IdAllocator"))

    // renameCategories runs BEFORE this stage and auto-creates the leaf of every
    // book_moves.csv destination with an implicit rowid — i.e. at max(id)+1, right
    // inside the range this allocator is about to hand out. insertCategoryWithId is
    // INSERT OR IGNORE, so a collision silently drops our folder and dumps its books
    // into that unrelated category. Raise the counter past whatever the DB holds.
    run {
        var maxCategoryId = 0L
        driver.executeQuery(null, "SELECT COALESCE(MAX(id), 0) FROM category",
            { c -> if (c.next().value) maxCategoryId = c.getLong(0) ?: 0L; QueryResult.Value(Unit) }, 0)
        allocator.ensureCounterAtLeast(IdTable.CATEGORY, maxCategoryId + 1)
    }

    try {
        val buildVersion: Int = (System.getProperty("buildVersion")
            ?: System.getenv("BUILD_VERSION"))
            ?.toIntOrNull()
            ?: (System.currentTimeMillis() / 1000).toInt()

        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = acronymDbPath,
            allocator = allocator,
            buildVersion = buildVersion,
        )
        generator.generateLinesOnly()
        if (useMemoryDb) {
            // VACUUM INTO a candidate beside the target, then rename it over the
            // target in one step — never delete-then-write, which left no DB at
            // all if the process died in between (see [DbPublish]).
            runCatching {
                logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO..." }
                DbPublish.publishAtomically(Paths.get(persistDbPath), logger) { candidate ->
                    val escaped = candidate.toString().replace("'", "''")
                    repository.executeRawQuery("VACUUM INTO '$escaped'")
                }
                logger.i { "In-memory DB persisted to $persistDbPath" }
            }.onFailure { e ->
                logger.e(e) { "Failed to persist in-memory DB to $persistDbPath" }
                throw e
            }
        }
        // Persist build_state so subsequent phases/builds reuse the same ids.
        // Written after the persist above on purpose: a failed VACUUM INTO must
        // not leave an advanced buildstate beside a DB that was never written.
        val buildStateMeta = mapOf(
            "generator" to "otzariasqlite/generateLines",
            "generated_at" to java.time.Instant.now().toString(),
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
            dbPath = Paths.get(if (useMemoryDb) persistDbPath else dbPath),
            expectedMeta = buildStateMeta,
            logger = logger,
        )
        logger.i { "Phase 1 completed successfully. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 1 generation" }
        throw e
    } finally {
        repository.close()
    }
}
