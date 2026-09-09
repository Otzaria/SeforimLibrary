package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.db.SEFORIM_DB_PAGE_SIZE_PRAGMA
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Phase 2 entry point: process links only (requires that books/lines already exist).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLinks -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLinks")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = (System.getProperty("inMemoryDb") == "true") || dbPath == ":memory:"
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: Paths.get("build", "seforim.db").toString()
    val sourceDir = args.getOrNull(1)
        ?: System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // This phase does the final VACUUM INTO of the Otzaria chain, so the in-memory DB
    // must use 16 KiB pages before its schema is created (in SeforimRepository.init),
    // otherwise the persisted file would revert to SQLite's 4 KiB default.
    driver.execute(null, SEFORIM_DB_PAGE_SIZE_PRAGMA, 0)
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)

    try {
        // If using in-memory DB, seed it from base DB on disk if provided
        if (useMemoryDb) {
            val baseDb = System.getProperty("baseDb")
                ?: System.getenv("SEFORIM_DB_BASE")
                ?: Paths.get("build", "seforim.db").toString()
            val baseFile = java.io.File(baseDb)
            if (baseFile.exists()) {
                logger.i { "Seeding in-memory DB from base file: $baseDb" }
                // Names the table the copy died on; see the fail-closed note below.
                var copyingTable: String? = null
                runCatching {
                    repository.executeRawQuery("PRAGMA foreign_keys=OFF")
                    val escaped = baseDb.replace("'", "''")
                    repository.executeRawQuery("ATTACH DATABASE '$escaped' AS disk")
                    // Load all table names from attached DB
                    val tables = driver.executeQuery(null,
                        "SELECT name FROM disk.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        { c: SqlCursor ->
                            val list = mutableListOf<String>()
                            while (c.next().value) {
                                c.getString(0)?.let { list.add(it) }
                            }
                            QueryResult.Value(list)
                        }, 0
                    ).value
                    // Copy data for each table into main
                    for (t in tables) {
                        val tn = t
                        copyingTable = tn
                        repository.executeRawQuery("DELETE FROM \"$tn\"")
                        repository.executeRawQuery("INSERT INTO \"$tn\" SELECT * FROM disk.\"$tn\"")
                    }
                    copyingTable = null
                    repository.executeRawQuery("DETACH DATABASE disk")
                    repository.executeRawQuery("PRAGMA foreign_keys=ON")
                    logger.i { "Seeding completed. Imported ${tables.size} tables." }
                }.onFailure { e ->
                    // Fail closed. In the release path baseDb and persistDb are the
                    // SAME file (build.gradle.kts:appendOtzariaLinks), so continuing
                    // here would let the VACUUM INTO below delete the 7 GiB DB this
                    // run produced and replace it with an empty one — reported as a
                    // success. A half-copied seed is just as fatal: the tables
                    // enumerated before the failure are populated, the rest empty.
                    val where = copyingTable?.let { " while copying table \"$it\"" } ?: ""
                    logger.e(e) { "Failed to seed in-memory DB from $baseDb$where; aborting phase 2." }
                    throw e
                }
            } else if (DbPublish.allowEmptyBase()) {
                logger.w { "Base DB not found at $baseDb; -PallowEmptyBase is set, so phase 2 starts from an empty DB" }
            } else {
                // Fail BEFORE anything touches the target. The absence of the
                // base used to be a warning, and since baseDb == persistDb in
                // the release path (build.gradle.kts:appendOtzariaLinks) the
                // run then vacuumed an empty in-memory DB over the target and
                // exited 0 — an empty database published as a success. Only the
                // explicit opt-in above may do that.
                throw IllegalStateException(
                    DbPublish.missingBaseMessage("phase 2 (links)", baseDb, "-PbaseDb / SEFORIM_DB_BASE")
                )
            }
        }

        val buildStatePath: Path = run {
            val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
            if (explicit != null) Paths.get(explicit) else Paths.get("$persistDbPath.buildstate")
        }
        val prev = buildStatePath.takeIf { Files.exists(it) }
        val allocator = InMemoryIdAllocator.load(prev, Logger.withTag("IdAllocator"))

        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = null,
            filterSourcesForLinks = false,
            allocator = allocator,
        )
        generator.generateLinksOnly()
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
        // Persist build_state AFTER the DB itself is on disk (this is also the
        // order GenerateLines uses): written first, a failed VACUUM INTO would
        // leave an advanced buildstate beside a DB that was never written.
        val buildStateMeta = mapOf(
            "generator" to "otzariasqlite/generateLinks",
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
        logger.i { "Phase 2 completed successfully. Links processed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 2 (links)" }
        throw e
    } finally {
        repository.close()
    }
}
