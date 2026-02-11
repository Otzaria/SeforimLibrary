package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverAdapter
import io.github.kdroidfilter.seforimlibrary.idresolver.IdResolverLoader
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

/**
 * One-step conversion: Sefaria export -> SQLite (direct import, sans Otzaria intermédiaire).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:generateSefariaSqlite -PseforimDb=/path/to.db [-PexportDir=/path/to/database_export]
 *   # To use ID resolution from a previous DB:
 *   ./gradlew :sefariasqlite:generateSefariaSqlite -PpreviousDb=/path/to/previous.db
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

    // Load IdResolver from previous DB for stable ID resolution
    // Get project root from system property set by Gradle, or try to find it
    val projectRoot = resolveProjectRoot()
    
    val previousDbPath = System.getProperty("previousDb")
        ?: System.getenv("PREVIOUS_DB")
        ?: run {
            // Auto-detect: look for seforim.db.bak
            val candidates = listOf(
                File(projectRoot, "build/seforim.db.bak"),
                File(File(projectRoot), "build/seforim.db.bak"),
                File("build/seforim.db.bak"),
                File(dbPath + ".bak"),
                File(dbPath.removeSuffix(".db") + ".db.bak")
            )
            println("--- IdResolver Debug Diagnostic ---")
            println("Looking for previous DB in candidates:")
            candidates.forEach { println("  Checking: ${it.absolutePath} (exists=${it.exists()})") }
            
            val found = candidates.firstOrNull { it.exists() }
            if (found != null) {
                println(">>> Auto-detected previous DB at: ${found.absolutePath}")
                found.absolutePath
            } else {
                println("!!! Could not find valid backup DB. IDs will be unstable!")
                null
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
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    try {
        val importer = SefariaDirectImporter(
            exportRoot = exportRoot,
            repository = repository,
            logger = Logger.withTag("SefariaDirect"),
            idResolverProvider = idResolverAdapter
        )
        importer.import()

        // Load generation (era) mappings from סדר הדורות CSV and update authors
        val generationCsv = File(projectRoot, "build/סדר הדורות.csv")
            .takeIf { it.exists() }
            ?: File("build/סדר הדורות.csv").takeIf { it.exists() }
            ?: downloadGenerationCsv(File(projectRoot, "build"), logger)

        if (generationCsv != null) {
            logger.i { "Loading generation mappings from ${generationCsv.absolutePath}" }
            GenerationLoader.loadGenerations(generationCsv, repository, idResolverAdapter)
        } else {
            logger.w { "סדר הדורות.csv not found and download failed; skipping generation loading." }
        }

        // Log IdResolver statistics if used
        idResolverAdapter?.let { adapter ->
            val stats = adapter.getStatistics()
            logger.i { "ID Resolution summary:" }
            logger.i { "  Newly allocated books: ${stats.newlyAllocatedBooks}" }
            logger.i { "  Newly allocated lines: ${stats.newlyAllocatedLines}" }
        }

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

        logger.i { "Sefaria -> SQLite completed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during Sefaria->SQLite generation" }
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

private const val GENERATION_CSV_URL =
    "https://raw.githubusercontent.com/Otzaria/otzaria-library/refs/heads/main/" +
    "MoreBooks/%D7%A1%D7%A4%D7%A8%D7%99%D7%9D/%D7%90%D7%95%D7%A6%D7%A8%D7%99%D7%90/" +
    "%D7%90%D7%95%D7%93%D7%95%D7%AA%20%D7%94%D7%AA%D7%95%D7%9B%D7%A0%D7%94/" +
    "%D7%A1%D7%93%D7%A8%20%D7%94%D7%93%D7%95%D7%A8%D7%95%D7%AA.csv"

/**
 * Downloads the סדר הדורות CSV from the Otzaria GitHub repository into [targetDir].
 * Returns the downloaded file, or null if the download failed.
 */
private fun downloadGenerationCsv(targetDir: File, logger: Logger): File? {
    return try {
        targetDir.mkdirs()
        val target = File(targetDir, "סדר הדורות.csv")
        logger.i { "Downloading סדר הדורות.csv from GitHub..." }
        URI(GENERATION_CSV_URL).toURL().openStream().use { input ->
            target.outputStream().use { output ->
                input.copyTo(output)
            }
        }
        logger.i { "Downloaded סדר הדורות.csv to ${target.absolutePath}" }
        target
    } catch (e: Exception) {
        logger.w(e) { "Failed to download סדר הדורות.csv" }
        null
    }
}
