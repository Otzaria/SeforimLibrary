package io.github.kdroidfilter.seforimlibrary.externalbooks

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
import java.sql.Connection
import java.sql.DriverManager

/**
 * Entry point — imports HebrewBooks + OtzarHaChochma metadata from books.db into seforim.db.
 *
 * Usage:
 *   ./gradlew :externalbooks:importExternalBooks [-PseforimDb=...] [-PbooksDb=...]
 *
 * If books.db is not found locally it will be downloaded automatically from Google Drive.
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("ImportExternalBooks")

    val seforimDbPath = args.getOrNull(0) ?: "build/seforim.db"
    val booksDbPath = args.getOrNull(1) ?: "generator/externalbooks/build/books.db"

    // Download books.db from Google Drive if it doesn't exist
    val booksDbFile = ensureBooksDb(Paths.get(booksDbPath), logger).toFile()
    if (!booksDbFile.exists()) {
        logger.e { "books.db not found at ${booksDbFile.absolutePath}" }
        return@runBlocking
    }

    val seforimDbFile = File(seforimDbPath)
    if (!seforimDbFile.exists()) {
        logger.e { "seforim.db not found at ${seforimDbFile.absolutePath}" }
        return@runBlocking
    }

    // Load ID resolver from backup DB if available
    val projectRoot = System.getProperty("projectRoot") ?: "."
    val previousDbPath = listOfNotNull(
        System.getProperty("previousDb"),
        File(projectRoot, "build/seforim.db.bak").takeIf { it.exists() }?.absolutePath,
        File("${seforimDbPath}.bak").takeIf { File("${seforimDbPath}.bak").exists() }?.absolutePath
    ).firstOrNull()

    val idResolverAdapter: IdResolverAdapter? = if (previousDbPath != null && File(previousDbPath).exists()) {
        logger.i { "Loading ID mappings from previous DB: $previousDbPath" }
        val resolver = IdResolverLoader.load(previousDbPath)
        val stats = resolver.getStatistics()
        logger.i { "IdResolver loaded: ${stats.loadedBooks} books, ${stats.loadedLines} lines" }
        IdResolverAdapter(resolver)
    } else {
        logger.i { "No previous DB found; IDs will be assigned sequentially" }
        null
    }

    // Open seforim.db
    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$seforimDbPath")
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(seforimDbPath, driver)

    // Open books.db via raw JDBC (read-only)
    val booksConn: Connection = DriverManager.getConnection("jdbc:sqlite:file:${booksDbFile.absolutePath}?mode=ro")
    booksConn.createStatement().execute("PRAGMA cache_size = -32000") // 32MB cache

    try {
        val importer = ExternalBooksImporter(
            repository = repository,
            booksConn = booksConn,
            idResolver = idResolverAdapter,
            logger = logger
        )

        // Performance PRAGMAs
        repository.executeRawQuery("PRAGMA foreign_keys=OFF")
        repository.executeRawQuery("PRAGMA synchronous=OFF")
        repository.executeRawQuery("PRAGMA journal_mode=OFF")

        importer.importAll()

        // Restore PRAGMAs
        repository.executeRawQuery("PRAGMA foreign_keys=ON")
        repository.executeRawQuery("PRAGMA synchronous=NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode=DELETE")

        logger.i { "External books import completed successfully." }
    } catch (e: Exception) {
        logger.e(e) { "Error during external books import" }
        throw e
    } finally {
        booksConn.close()
        repository.close()
    }
}
