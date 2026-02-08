package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.IdResolverProvider
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import kotlin.io.path.extension
import kotlin.io.path.nameWithoutExtension
import kotlin.io.path.readBytes
import kotlin.io.path.readText

/**
 * Importer for PDF files into the SQLite database.
 *
 * This tool scans a directory for PDF files, creates book entries for each,
 * and stores the PDF content as BLOBs in the book_file table.
 *
 * Uses [SeforimRepository.insertBook] for book creation and supports
 * stable ID resolution via [IdResolverProvider].
 */
class PdfImporter(
    private val repository: SeforimRepository,
    private val booksDir: Path,
    private val manifestFile: Path?,
    private val sourceId: Long,
    private val batchSize: Int = 300,
    private val idResolverProvider: IdResolverProvider? = null,
    private val logger: Logger = Logger.withTag("PdfImporter"),
    private val configurePragmas: Boolean = true
) {
    private val manifestData: Map<String, JsonObject>? by lazy {
        manifestFile?.let { loadManifest(it) }
    }

    // Counter for book IDs, initialized from existing DB
    private var nextBookId = 1L
    private var idCountersInitialized = false

    private fun getFileCreatedAtMillis(path: Path): Long {
        return try {
            Files.readAttributes(path, BasicFileAttributes::class.java).creationTime().toMillis()
        } catch (_: Exception) {
            try {
                Files.getLastModifiedTime(path).toMillis()
            } catch (_: Exception) {
                System.currentTimeMillis()
            }
        }
    }

    private suspend fun initializeIdCountersFromExistingDb() {
        if (idCountersInitialized) return
        idCountersInitialized = true
        val maxBookId = try { repository.getMaxBookId() } catch (_: Exception) { 0L }
        nextBookId = (maxBookId + 1).coerceAtLeast(1)
        if (maxBookId > 0) {
            logger.i { "Continuing from existing DB ids: nextBookId=$nextBookId" }
        }
    }

    /**
     * Main import function.
     * Scans for PDFs, creates book entries, and stores file content.
     */
    suspend fun importPdfs() {
        logger.i { "Starting PDF import from: $booksDir" }
        logger.i { "Source ID: $sourceId, Batch size: $batchSize" }

        if (configurePragmas) {
            configurePragmas()
        }

        // Initialize ID counters from existing DB
        initializeIdCountersFromExistingDb()

        // Scan for PDF files
        val pdfFiles = scanPdfFiles()
        logger.i { "Found ${pdfFiles.size} PDF files to process" }
        logger.i { "First few PDFs: ${pdfFiles.take(5).map { it.fileName }}" }

        if (pdfFiles.isEmpty()) {
            logger.w { "No PDF files found in $booksDir" }
            return
        }

        var imported = 0
        var skipped = 0

        // Process in batches
        pdfFiles.chunked(batchSize).forEach { batch ->
            try {
                batch.forEach { pdfFile ->
                    when (processPdfFile(pdfFile)) {
                        ProcessResult.IMPORTED -> imported++
                        ProcessResult.SKIPPED -> skipped++
                    }
                }

                logger.i { "Batch completed. Progress: imported=$imported, skipped=$skipped" }
            } catch (e: Exception) {
                logger.e(e) { "Error processing batch" }
                throw e
            }
        }
    }

    private enum class ProcessResult {
        IMPORTED, SKIPPED
    }

    private suspend fun processPdfFile(pdfFile: Path): ProcessResult {
        try {
            val relPath = booksDir.relativize(pdfFile).toString()
            val baseName = pdfFile.nameWithoutExtension
            val fileType = pdfFile.extension.ifEmpty { "pdf" }

            // Read file content
            val fileBytes = pdfFile.readBytes()
            val sha256 = MessageDigest.getInstance("SHA-256")
                .digest(fileBytes)
                .joinToString("") { "%02x".format(it) }

            // Check for duplicates
            val exists = repository.existsBookFileByKindSha("pdf", sha256)

            if (exists) {
                logger.d { "SKIP (duplicate): $relPath, sha256=$sha256" }
                return ProcessResult.SKIPPED
            }

            // Extract category from path
            val categorySegments = extractCategorySegments(pdfFile)
            val categoryId = getOrCreateCategory(categorySegments)

            // Load metadata from manifest
            val metadata = manifestData?.get(baseName)
            val heShortDesc = metadata?.get("heShortDesc")?.jsonPrimitive?.contentOrNull
            val notesContent = metadata?.get("notesContent")?.jsonPrimitive?.contentOrNull
            val orderIndex = metadata?.get("orderIndex")?.jsonPrimitive?.intOrNull ?: 999

            // --- ID resolution logic (same as createAndProcessBook) ---
            val bookFilePath = relPath.substringBeforeLast('.')
            val resolvedId = idResolverProvider?.resolveBookId(bookFilePath, sourceId, fileType)
            val currentBookId: Long

            if (resolvedId != null) {
                if (repository.getBook(resolvedId) != null) {
                    logger.i { "Book '$baseName' with resolved ID $resolvedId already exists (append mode). Skipping." }
                    return ProcessResult.SKIPPED
                }
                currentBookId = resolvedId
            } else {
                // Ensure nextBookId is free
                while (repository.getBook(nextBookId) != null) {
                    logger.w { "ID collision for nextBookId=$nextBookId. Incrementing..." }
                    nextBookId++
                }
                currentBookId = nextBookId++
            }

            logger.d { "Assigning ID $currentBookId to PDF book '$baseName' (resolved=${resolvedId != null})" }

            // Create Book model and insert via repository
            val book = Book(
                id = currentBookId,
                categoryId = categoryId,
                sourceId = sourceId,
                title = baseName,
                heShortDesc = heShortDesc,
                notesContent = notesContent,
                order = orderIndex.toFloat(),
                totalLines = 0,
                isBaseBook = false,
                filePath = bookFilePath,
                fileType = fileType,
                fileSize = fileBytes.size.toLong()
            )

            val bookId = repository.insertBook(book)

            // Insert PDF file BLOB
            val createdAt = getFileCreatedAtMillis(pdfFile)
            repository.insertBookFile(
                bookId = bookId,
                kind = "pdf",
                data = fileBytes,
                size = fileBytes.size.toLong(),
                sha256 = sha256,
                originalRelPath = relPath,
                createdAt = createdAt
            )

            val categoryPath = categorySegments.joinToString(" / ")
            logger.i { "IMPORT: $relPath -> bookId=$bookId, category='$categoryPath', size=${fileBytes.size}, sha=$sha256" }

            return ProcessResult.IMPORTED
        } catch (e: Exception) {
            logger.e(e) { "Error processing PDF file: ${pdfFile.fileName}" }
            throw e
        }
    }

    private fun extractCategorySegments(pdfFile: Path): List<String> {
        val relPath = booksDir.relativize(pdfFile)
        val segments = mutableListOf<String>()

        // Get all parent directories (excluding the file itself)
        var current = relPath.parent
        while (current != null && current.nameCount > 0) {
            segments.add(0, current.fileName.toString())
            current = current.parent
        }

        return segments
    }

    private suspend fun getOrCreateCategory(pathSegments: List<String>): Long {
        var currentParentId: Long? = null
        var currentLevel = 0

        for (segment in pathSegments) {
            val existing = if (currentParentId == null) {
                repository.getRootCategories().firstOrNull { it.title == segment }
            } else {
                repository.getCategoryChildren(currentParentId).firstOrNull { it.title == segment }
            }

            if (existing != null) {
                currentParentId = existing.id
                currentLevel = existing.level.toInt()
            } else {
                val newId = repository.insertCategory(
                    Category(
                        parentId = currentParentId,
                        title = segment,
                        level = currentLevel,
                        order = 999
                    )
                )
                currentParentId = newId
                currentLevel += 1
            }
        }

        return currentParentId ?: error("Failed to create category hierarchy")
    }

    private fun scanPdfFiles(): List<Path> {
        val pdfFiles = mutableListOf<Path>()

        booksDir.toFile().walkTopDown().forEach { file ->
            if (file.isFile && file.extension.equals("pdf", ignoreCase = true)) {
                pdfFiles.add(file.toPath())
            }
        }

        return pdfFiles.sortedBy { it.toString() }
    }

    private fun loadManifest(manifestPath: Path): Map<String, JsonObject>? {
        return try {
            val jsonText = manifestPath.readText()

            // Handle empty file
            if (jsonText.isBlank()) {
                logger.w { "Manifest file is empty: $manifestPath" }
                return null
            }

            val jsonElement = Json.parseToJsonElement(jsonText)

            when (jsonElement) {
                is JsonObject -> {
                    // Map format: { "bookName": { ... }, "bookName2": { ... } }
                    jsonElement.mapValues { (_, value) ->
                        when (value) {
                            is JsonObject -> value
                            else -> JsonObject(emptyMap())
                        }
                    }
                }
                is JsonArray -> {
                    // Array format: [ { "name": "bookName", ... }, ... ]
                    // Need to identify the key field
                    if (jsonElement.isEmpty()) {
                        logger.w { "Manifest array is empty" }
                        return null
                    }

                    // Look for common key fields
                    val firstEntry = jsonElement.first() as? JsonObject
                    val keyField = firstEntry?.keys?.firstOrNull {
                        it in listOf("name", "title", "id", "baseName", "fileName")
                    }

                    if (keyField == null) {
                        logger.e { "Cannot determine key field in manifest array. First two entries:" }
                        jsonElement.take(2).forEach { logger.e { it.toString() } }
                        error("Manifest array format requires a 'name', 'title', or 'id' field")
                    }

                    logger.i { "Using manifest key field: $keyField" }
                    jsonElement.associate { element ->
                        val obj = element as JsonObject
                        val key = obj[keyField]?.jsonPrimitive?.content
                            ?: error("Missing key field '$keyField' in manifest entry")
                        key to obj
                    }
                }
                else -> {
                    logger.e { "Manifest must be a JSON object or array, got: ${jsonElement::class.simpleName}" }
                    null
                }
            }
        } catch (e: Exception) {
            logger.e(e) { "Failed to load manifest from $manifestPath" }
            null
        }
    }

    private suspend fun configurePragmas() {
        repository.executeRawQuery("PRAGMA foreign_keys=ON")
        repository.executeRawQuery("PRAGMA journal_mode=WAL")
        repository.executeRawQuery("PRAGMA synchronous=NORMAL")
        repository.executeRawQuery("PRAGMA temp_store=MEMORY")

        logger.d { "PRAGMA configured for optimal performance" }
    }
}
