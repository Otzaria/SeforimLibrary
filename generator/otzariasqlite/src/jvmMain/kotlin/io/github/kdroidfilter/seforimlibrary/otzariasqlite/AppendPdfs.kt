package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.Path
import kotlin.io.path.exists

/**
 * Entry point for appending PDF files to an existing database.
 * 
 * This tool is designed to run after generateLines (appendOtzariaLines) to add
 * PDF content to the database as BLOB files.
 * 
 * Usage:
 *   ./gradlew :otzariasqlite:appendOtzariaPdfs
 *   ./gradlew :otzariasqlite:appendOtzariaPdfs -PseforimDb=/path/to/seforim.db
 *   ./gradlew :otzariasqlite:appendOtzariaPdfs -PbooksDir=/custom/path -PmanifestFile=/path/manifest.json
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("AppendPdfs")
    
    // Parse CLI arguments
    val seforimDbPropOrEnv = System.getProperty("seforimDb") ?: System.getenv("SEFORIM_DB")
    val dbPath = args.getOrNull(0)
        ?: seforimDbPropOrEnv
        ?: Paths.get("build", "seforim.db").toString()
    
    // Calculate default paths relative to project root (two directories up from working dir which is generator/otzariasqlite)
    val projectRoot = File(System.getProperty("user.dir")).parentFile?.parentFile?.absolutePath
        ?: System.getProperty("user.dir")
    val defaultBooksDir = File(projectRoot, "generator/otzariasqlite/build/otzaria/source/אוצריא").absolutePath
    val booksDir = args.getOrNull(1)
        ?: System.getProperty("booksDir")
        ?: System.getenv("BOOKS_DIR")
        ?: defaultBooksDir
    
    val manifestFile = args.getOrNull(2)
        ?: System.getProperty("manifestFile")
        ?: System.getenv("MANIFEST_FILE")
        ?: run {
            // Try default location
            val defaultManifest = File(projectRoot, "generator/otzariasqlite/build/otzaria/source/metadata.json")
            if (defaultManifest.exists()) defaultManifest.absolutePath else null
        }
    
    val sourceIdStr = args.getOrNull(3)
        ?: System.getProperty("sourceId")
        ?: System.getenv("SOURCE_ID")
        ?: "2" // Default to Unknown source
    
    val batchSizeStr = args.getOrNull(4)
        ?: System.getProperty("batchSize")
        ?: System.getenv("BATCH_SIZE")
        ?: "300"
    
    // Validate arguments
    val dbFile = File(dbPath)
    if (!dbFile.exists()) {
        logger.e { "Database file not found: $dbPath" }
        logger.e { "Please run generateLines first to create the database" }
        throw IllegalArgumentException("Database file not found: $dbPath")
    }
    
    val booksDirPath = Path(booksDir)
    if (!booksDirPath.exists()) {
        logger.e { "Books directory not found: $booksDir" }
        throw IllegalArgumentException("Books directory not found: $booksDir")
    }
    
    val manifestPath = manifestFile?.let { Path(it) }
    if (manifestFile != null && manifestPath?.exists() != true) {
        logger.w { "Manifest file not found: $manifestFile (continuing without metadata)" }
    }
    
    val sourceId = sourceIdStr.toLongOrNull() ?: run {
        logger.e { "Invalid sourceId: $sourceIdStr" }
        throw IllegalArgumentException("Invalid sourceId: $sourceIdStr")
    }
    
    val batchSize = batchSizeStr.toIntOrNull() ?: run {
        logger.e { "Invalid batchSize: $batchSizeStr" }
        throw IllegalArgumentException("Invalid batchSize: $batchSizeStr")
    }
    
    // Log configuration
    logger.i { "=== PDF Import Configuration ===" }
    logger.i { "Database: $dbPath" }
    logger.i { "Books directory: $booksDir" }
    logger.i { "Manifest file: ${manifestFile ?: "(none)"}" }
    logger.i { "Source ID: $sourceId" }
    logger.i { "Batch size: $batchSize" }
    logger.i { "================================" }
    
    try {
        // Connect to existing database
        logger.i { "Connecting to database..." }
        val jdbcUrl = "jdbc:sqlite:$dbPath"
        val driver = JdbcSqliteDriver(url = jdbcUrl)
        
        // Ensure schema is up to date (will create book_file table if missing)
        logger.i { "Creating schema..." }
        runCatching { SeforimDb.Schema.create(driver) }
        
        logger.i { "Creating database and repository objects..." }
        val database = SeforimDb(driver)
        val repository = SeforimRepository(dbPath, driver)
        
        // Verify source exists
        logger.i { "Verifying source exists..." }
        val sourceExists = try {
            database.sourceQueriesQueries.selectAll()
                .executeAsList()
                .any { it.id == sourceId }
        } catch (e: Exception) {
            logger.e(e) { "Error checking source existence" }
            false
        }
        
        if (!sourceExists) {
            logger.e { "Source ID $sourceId not found in database" }
            logger.i { "Available sources:" }
            database.sourceQueriesQueries.selectAll()
                .executeAsList()
                .forEach { source ->
                    logger.i { "  ${source.id}: ${source.name}" }
                }
            throw IllegalArgumentException("Invalid sourceId: $sourceId")
        }
        
        // Create importer and run
        logger.i { "Creating PdfImporter..." }
        val importer = PdfImporter(
            repository = repository,
            booksDir = booksDirPath,
            manifestFile = if (manifestPath?.exists() == true) manifestPath else null,
            sourceId = sourceId,
            batchSize = batchSize,
            logger = logger
        )
        
        logger.i { "Starting PDF import..." }
        importer.importPdfs()
        
        logger.i { "PDF import completed successfully" }
        
    } catch (e: Exception) {
        logger.e(e) { "Error during PDF import" }
        throw e
    }
}
