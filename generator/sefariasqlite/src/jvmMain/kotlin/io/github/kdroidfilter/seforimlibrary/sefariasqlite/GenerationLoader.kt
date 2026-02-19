package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.IdResolverProvider
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.io.File

/**
 * Loads the "סדר הדורות" CSV file (book-title → generation mapping) and populates
 * the `generation` table, then updates each author's `generationId` based on the
 * books they wrote.
 *
 * CSV format (with header row):
 * ```
 * שם ספר,קבוצת דור
 * אב בחכמה,אחרונים
 * אבודרהם,ראשונים
 * ```
 *
 * The second column contains the generation/era name (e.g. חז"ל, ראשונים, אחרונים, מחברי זמננו, תורה שבכתב).
 */
object GenerationLoader {

    private val logger = Logger.withTag("GenerationLoader")

    private data class GenerationDetails(
        val startYear: Long?,
        val endYear: Long?,
        val parentGenerationId: Long?
    )

    /**
     * Normalized alias: "חזל" (without quotes) → "חז״ל" canonical form.
     */
    private fun normalizeGenerationName(raw: String): String {
        val trimmed = raw.trim()
        return when (trimmed) {
            "חזל" -> "חז\"ל"
            else -> trimmed
        }
    }

    /**
     * Parse a CSV line respecting quoted fields (RFC 4180-like).
     * Returns a list of field values.
     */
    private fun parseCsvLine(line: String): List<String> {
        val fields = mutableListOf<String>()
        val sb = StringBuilder()
        var inQuotes = false
        var i = 0
        while (i < line.length) {
            val c = line[i]
            when {
                c == '"' && inQuotes -> {
                    // Check for escaped quote ("")
                    if (i + 1 < line.length && line[i + 1] == '"') {
                        sb.append('"')
                        i += 2
                        continue
                    } else {
                        inQuotes = false
                    }
                }
                c == '"' && !inQuotes -> inQuotes = true
                c == ',' && !inQuotes -> {
                    fields.add(sb.toString())
                    sb.clear()
                }
                else -> sb.append(c)
            }
            i++
        }
        fields.add(sb.toString())
        return fields
    }

    /**
     * Reads the CSV file and returns a map of book-title → generation-name.
     */
    fun parseCsv(csvFile: File): Map<String, String> {
        if (!csvFile.exists()) {
            logger.w { "CSV file not found: ${csvFile.absolutePath}" }
            return emptyMap()
        }

        val result = mutableMapOf<String, String>()
        val lines = csvFile.readLines(Charsets.UTF_8)
        // Skip header row
        for (i in 1 until lines.size) {
            val line = lines[i].trim()
            if (line.isEmpty()) continue
            val fields = parseCsvLine(line)
            if (fields.size >= 2) {
                val bookTitle = fields[0].trim()
                val generationName = normalizeGenerationName(fields[1])
                if (bookTitle.isNotEmpty() && generationName.isNotEmpty()) {
                    result[bookTitle] = generationName
                }
            }
        }
        logger.i { "Parsed ${result.size} book→generation mappings from CSV" }
        return result
    }

    /**
     * Reads the generations metadata CSV and returns a map of generation-name → details.
     * Expected headers: Era, Start Year, End Year, parentGenerationId
     */
    private fun parseGenerationDetailsCsv(csvFile: File): Map<String, GenerationDetails> {
        if (!csvFile.exists()) {
            logger.w { "Generation details CSV file not found: ${csvFile.absolutePath}" }
            return emptyMap()
        }

        val result = mutableMapOf<String, GenerationDetails>()
        val lines = csvFile.readLines(Charsets.UTF_8)
        for (i in 1 until lines.size) {
            val line = lines[i].trim()
            if (line.isEmpty()) continue
            val fields = parseCsvLine(line)
            if (fields.isEmpty()) continue

            val eraName = normalizeGenerationName(fields.getOrNull(0).orEmpty())
            if (eraName.isEmpty()) continue

            val startYear = fields.getOrNull(1)?.trim().orEmpty().toLongOrNull()
            val endYear = fields.getOrNull(2)?.trim().orEmpty().toLongOrNull()
            val parentGenerationId = fields.getOrNull(3)?.trim().orEmpty().toLongOrNull()

            if (startYear != null && endYear != null && startYear > endYear) {
                logger.w { "Skipping invalid generation range for '$eraName': startYear=$startYear endYear=$endYear" }
                continue
            }

            result[eraName] = GenerationDetails(
                startYear = startYear,
                endYear = endYear,
                parentGenerationId = parentGenerationId
            )
        }

        logger.i { "Parsed ${result.size} generation detail rows from CSV" }
        return result
    }

    /**
     * Seeds the generation table with all distinct generation names from the CSV,
     * then updates every author's generationId based on the books they wrote.
     *
     * When an [idResolverProvider] is supplied (loaded from a previous DB), generation
     * rows are inserted with their previous stable IDs so that IDs remain consistent
     * across regenerations.
     *
     * Algorithm:
     * 1. Insert all distinct generation names into the `generation` table.
     * 2. For each (bookTitle → generationName) pair in the CSV, look up the book
     *    by title, find its authors, and set their generationId.
     *
     * @param csvFile The סדר הדורות CSV file
     * @param generationDetailsCsv Optional generations details CSV (Era, Start Year, End Year, parentGenerationId)
     * @param repository The SeforimRepository to use for DB operations
     * @param idResolverProvider Optional ID resolver for stable generation IDs
     */
    suspend fun loadGenerations(
        csvFile: File,
        generationDetailsCsv: File? = null,
        repository: SeforimRepository,
        idResolverProvider: IdResolverProvider? = null
    ) {
        val bookToGeneration = parseCsv(csvFile)
        if (bookToGeneration.isEmpty()) {
            logger.w { "No generation mappings found; skipping." }
            return
        }

        val generationDetailsByName = generationDetailsCsv
            ?.takeIf { it.exists() }
            ?.let { parseGenerationDetailsCsv(it) }
            ?: emptyMap()

        // 1. Seed generation table with all distinct values, using stable IDs when available
        val distinctGenerations = bookToGeneration.values.toSet()
        val generationIdMap = mutableMapOf<String, Long>()
        for (genName in distinctGenerations) {
            val previousId = idResolverProvider?.resolveGenerationId(genName)
            val details = generationDetailsByName[genName]
            val genId = repository.insertGeneration(
                name = genName,
                explicitId = previousId,
                startYear = details?.startYear,
                endYear = details?.endYear,
                parentGenerationId = details?.parentGenerationId
            )
            generationIdMap[genName] = genId
            if (previousId != null) {
                logger.d { "Generation '$genName' inserted with stable ID $previousId" }
            }
        }
        logger.i { "Seeded ${generationIdMap.size} generations: ${generationIdMap.keys}" }

        // 2. For each book → generation mapping, update the book's authors
        var updatedCount = 0
        var skippedCount = 0
        for ((bookTitle, genName) in bookToGeneration) {
            val genId = generationIdMap[genName] ?: continue
            val book = repository.getBookByTitle(bookTitle)
            if (book == null) {
                skippedCount++
                continue
            }
            // Update each author of this book
            for (author in book.authors) {
                if (author.generationId == null) {
                    repository.updateAuthorGeneration(author.id, genId)
                    updatedCount++
                }
            }
        }
        logger.i { "Updated generation for $updatedCount author(s), skipped $skippedCount book(s) not found in DB" }
    }
}
