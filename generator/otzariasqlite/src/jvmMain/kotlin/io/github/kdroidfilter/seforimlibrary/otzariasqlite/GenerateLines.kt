package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverAdapter
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverLoader
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths

/**
 * Phase 1 entry point: generate categories, books, TOCs and lines only.
 *
 * Usage examples:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronym.db]
 *   # To append to an existing DB instead of rotating it:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PappendExistingDb=true
 *   # To use ID resolution from a previous DB:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PpreviousDb=/path/to/previous.db
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

    // Load IdResolver from previous DB for stable ID resolution
    // Get project root from system property set by Gradle, or try to find it
    val projectRoot = resolveProjectRoot()
    logger.i { "projectRoot=$projectRoot" }
    val previousDbPath = System.getProperty("previousDb")
        ?: System.getenv("PREVIOUS_DB")
        ?: run {
            // Auto-detect: look for seforim.db.bak in the project's build directory
            // This is created when the existing DB is backed up
            val buildBakFile = File(projectRoot, "build/seforim.db.bak")
            val dbBakFile = File("$dbPath.bak")
            logger.i { "buildBakFile.exists()=${buildBakFile.exists()}, path=${buildBakFile.absolutePath}" }
            logger.i { "dbBakFile.exists()=${dbBakFile.exists()}, path=${dbBakFile.absolutePath}" }
            when {
                buildBakFile.exists() -> buildBakFile.absolutePath
                dbBakFile.exists() -> dbBakFile.absolutePath
                else -> null
            }
        }

    val idResolverAdapter: IdResolverAdapter? = if (previousDbPath != null && File(previousDbPath).exists()) {
        logger.i { "Loading ID mappings from previous DB: $previousDbPath" }
        val resolver = IdResolverLoader.load(previousDbPath)
        val stats = resolver.getStatistics()
        logger.i { "IdResolver loaded: ${stats.loadedBooks} books, ${stats.loadedLines} lines, ${stats.loadedLinks} links" }
        logger.i { "Estimated memory: ${String.format("%.1f", stats.estimatedMemoryUsageMB())} MB" }
        IdResolverAdapter(resolver)
    } else {
        logger.i { "No previous DB found; IDs will be assigned sequentially" }
        null
    }

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
        } else {
            logger.i { "appendExistingDb enabled but no DB found at $dbPath; a new DB will be created" }
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // Ensure schema exists on a brand-new DB before repository init (idempotent)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    logger.i { "useMemoryDb=$useMemoryDb, appendExistingDb=$appendExistingDb" }
    if (useMemoryDb && appendExistingDb) {
        val baseDbPath = System.getProperty("baseDb")
            ?: System.getenv("SEFORIM_DB_BASE")
            ?: seforimDbPropOrEnv
            ?: persistDbPath
        logger.i { "baseDbPath=$baseDbPath" }
        if (baseDbPath != null && baseDbPath != ":memory:") {
            val baseFile = File(baseDbPath)
            logger.i { "baseFile.exists()=${baseFile.exists()}, path=${baseFile.absolutePath}" }
            if (baseFile.exists()) {
                logger.i { "Seeding in-memory DB from base file: ${baseFile.absolutePath}" }
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
                        repository.executeRawQuery("DELETE FROM \"$t\"")
                        repository.executeRawQuery("INSERT INTO \"$t\" SELECT * FROM disk.\"$t\"")
                    }
                    repository.executeRawQuery("DETACH DATABASE disk")
                    repository.executeRawQuery("PRAGMA foreign_keys=ON")
                    logger.i { "Seeding completed. Imported ${tables.size} tables." }
                }.onFailure { e ->
                    logger.e(e) { "Failed to seed in-memory DB from $baseDbPath; continuing with empty DB." }
                }
            } else {
                logger.w { "appendExistingDb enabled but base DB not found at $baseDbPath; starting from empty in-memory DB" }
            }
        } else {
            logger.w { "appendExistingDb enabled in-memory but no base DB path provided; starting from empty DB" }
        }
    }

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = acronymDbPath,
            idResolverProvider = idResolverAdapter
        )
        generator.generateLinesOnly()
        
        // Log IdResolver statistics if used
        idResolverAdapter?.let { adapter ->
            val stats = adapter.getStatistics()
            logger.i { "ID Resolution summary:" }
            logger.i { "  Newly allocated books: ${stats.newlyAllocatedBooks}" }
            logger.i { "  Newly allocated lines: ${stats.newlyAllocatedLines}" }
        }
        
        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
            runCatching {
                val outFile = File(persistDbPath)
                outFile.parentFile?.mkdirs()
                if (outFile.exists()) {
                    outFile.delete()
                    logger.i { "Existing DB removed to allow VACUUM INTO" }
                }
                val escaped = persistDbPath.replace("'", "''")
                logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO..." }
                repository.executeRawQuery("VACUUM INTO '$escaped'")
                logger.i { "In-memory DB persisted to $persistDbPath" }
            }.onFailure { e ->
                logger.e(e) { "Failed to persist in-memory DB to $persistDbPath" }
                throw e
            }
        }
        logger.i { "Phase 1 completed successfully. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 1 generation" }
        throw e
    } finally {
        repository.close()
    }
}

private fun resolveProjectRoot(): String {
    val explicit = System.getProperty("projectRoot") ?: System.getenv("PROJECT_ROOT")
    if (!explicit.isNullOrBlank()) return explicit

    val userDir = File(System.getProperty("user.dir"))
    val detected = findRepoRoot(userDir)
    return detected?.absolutePath ?: userDir.absolutePath
}

private fun findRepoRoot(start: File): File? {
    var current: File? = start
    while (current != null) {
        val hasSettings = File(current, "settings.gradle.kts").exists() || File(current, "settings.gradle").exists()
        val hasGradlew = File(current, "gradlew").exists()
        if (hasSettings || hasGradlew) return current
        current = current.parentFile
    }
    return null
}