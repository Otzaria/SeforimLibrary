package io.github.kdroidfilter.seforimlibrary.externalbooks

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.IdResolverProvider
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.*
import java.sql.Connection
import java.sql.ResultSet
import java.util.concurrent.atomic.AtomicLong

/**
 * Imports HebrewBooks and OtzarHaChochma metadata from books.db into
 * the main seforim.db, using existing lookup tables (topic, author, pub_place, pub_date)
 * and the book table.
 *
 * books.db schema:
 *   hebrew_books(id_book, title, author, printing_place, printing_year, pub_place,
 *                pub_date, pages, oclc_id, uli_entry, source, external_site,
 *                catalog_info, content, marc_record, tags)
 *   otzar_hahochma(book_id, title, authors, volume, from_year, to_year, places,
 *                  subjects, pages, page_letters, headers, national_library_info__*, hebrew_bibliography_info__*)
 *
 * Only Hebrew-relevant columns are imported (see the mapping in the conversation).
 */
class ExternalBooksImporter(
    private val repository: SeforimRepository,
    private val booksConn: Connection,
    private val idResolver: IdResolverProvider?,
    private val logger: Logger
) {
    private val json = Json { ignoreUnknownKeys = true }

    // Category IDs for external libraries — these are ensured at import time
    private var externalRootCategoryId: Long = 0L
    private var otzarCategoryId: Long = 0L
    private var hebrewBooksCategoryId: Long = 0L

    // Source IDs
    private var hebrewBooksSourceId: Long = 0L
    private var otzarSourceId: Long = 0L

    // ID counter for books (when no idResolver)
    private lateinit var nextBookId: AtomicLong

    suspend fun importAll() {
        initCategories()
        initSources()
        initBookIdCounter()

        val hbCount = countRows("hebrew_books")
        val ohCount = countRows("otzar_hahochma")
        logger.i { "books.db: hebrew_books=$hbCount rows, otzar_hahochma=$ohCount rows" }

        // Note: transaction management is handled by JDBC auto-commit.
        // Performance PRAGMAs (synchronous=OFF, journal_mode=OFF) are set by the caller.
        importHebrewBooks()
        importOtzarHaChochma()
    }

    // ── Initialization ──────────────────────────────────────────────────

    private suspend fun initCategories() {
        // Ensure the three categories exist (they should, but be idempotent)
        externalRootCategoryId = ensureCategory("ספרים מספריות חיצוניות", parentId = null, level = 0, order = 75)
        otzarCategoryId = ensureCategory("אוצר החכמה", parentId = externalRootCategoryId, level = 1, order = 1)
        hebrewBooksCategoryId = ensureCategory("היברו-בוקס", parentId = externalRootCategoryId, level = 1, order = 2)

        logger.i { "Categories: root=$externalRootCategoryId, otzar=$otzarCategoryId, hebrewBooks=$hebrewBooksCategoryId" }
    }

    private suspend fun ensureCategory(title: String, parentId: Long?, level: Int, order: Int): Long {
        // Try to resolve from previous DB
        val resolved = idResolver?.resolveCategoryId(title, parentId, level)
        if (resolved != null) {
            // Ensure it actually exists in the current DB
            val existing = repository.getCategory(resolved)
            if (existing != null) return resolved
        }

        // Check if already exists by title
        val existing = repository.getCategoryByTitle(title)
        if (existing != null) return existing.id

        // Insert
        val cat = Category(
            id = resolved ?: 0L,
            parentId = parentId,
            title = title,
            level = level,
            order = order
        )
        return repository.insertCategory(cat)
    }

    private suspend fun initSources() {
        hebrewBooksSourceId = repository.insertSource("HebrewBooks")
        otzarSourceId = repository.insertSource("OtzarHaChochma")
        logger.i { "Sources: HebrewBooks=$hebrewBooksSourceId, OtzarHaChochma=$otzarSourceId" }
    }

    private suspend fun initBookIdCounter() {
        val maxId = repository.getMaxBookId()
        nextBookId = AtomicLong(maxId + 1)
        logger.i { "Book ID counter starts at: ${nextBookId.get()}" }
    }

    // ── HebrewBooks import ─────────────────────────────────────────────

    private suspend fun importHebrewBooks() {
        logger.i { "Importing HebrewBooks..." }
        var imported = 0
        var skipped = 0

        val stmt = booksConn.createStatement()
        val rs = stmt.executeQuery(
            """
            SELECT id_book, title, author, printing_place, printing_year,
                   pub_date, pages, tags
            FROM hebrew_books
            WHERE title IS NOT NULL AND title != ''
            """
        )

        while (rs.next()) {
            val externalId = "hb:${rs.getLong("id_book")}"
            val title = rs.getString("title")?.trim() ?: continue
            val authorStr = rs.getString("author")?.trim()
            val printingPlace = rs.getString("printing_place")?.trim()
            val printingYear = rs.getString("printing_year")?.trim()
            val pubDateInt = rs.getObject("pub_date") // nullable int
            val pages = rs.getObject("pages") as? Number
            val tagsJson = rs.getString("tags")?.trim()

            // Parse tags JSON array → topics
            val topics = parseJsonStringArray(tagsJson).map { Topic(name = it.trim()) }

            // Parse author — single string, may contain " - " separator
            val authors = if (!authorStr.isNullOrBlank()) {
                // Strip lifecycle dates: "ריקי - רפאל עמנואל חי בן אברהם - 1687-1743"
                // Keep the name part before any pure-number suffix
                val cleaned = authorStr.replace(Regex("\\s*-\\s*\\d{3,4}(-\\d{3,4})?\\s*$"), "").trim()
                if (cleaned.isNotBlank()) listOf(Author(name = cleaned)) else emptyList()
            } else emptyList()

            // Publication places (Hebrew)
            val pubPlaces = if (!printingPlace.isNullOrBlank()) {
                listOf(PubPlace(name = printingPlace))
            } else emptyList()

            // Publication dates (Hebrew year)
            val pubDates = if (!printingYear.isNullOrBlank()) {
                listOf(PubDate(date = printingYear))
            } else emptyList()

            // Resolve or allocate book ID
            val resolvedId = idResolver?.resolveBookId(externalId, hebrewBooksSourceId, "external")
            val bookId = resolvedId ?: nextBookId.getAndIncrement()

            // Insert book via raw query (to populate pages + externalLibraryId)
            repository.executeRawQuery(
                """
                INSERT OR IGNORE INTO book
                    (id, categoryId, sourceId, title, orderIndex, totalLines, isBaseBook,
                     hasTargumConnection, hasReferenceConnection, hasSourceConnection,
                     hasCommentaryConnection, hasOtherConnection, hasAltStructures,
                     hasTeamim, hasNekudot, isContentExternal, externalLibraryId,
                     pages)
                VALUES
                    ($bookId, $hebrewBooksCategoryId, $hebrewBooksSourceId,
                     '${escapeSql(title)}', 999, 0, 0,
                     0, 0, 0, 0, 0, 0, 0, 0, 1, '${escapeSql(externalId)}',
                     ${pages?.toInt() ?: "NULL"})
                """.trimIndent()
            )

            // Insert authors, topics, places, dates via repository (handles dedup)
            for (author in authors) {
                val authorId = repository.insertAuthor(author.name)
                repository.linkAuthorToBook(authorId, bookId)
            }
            for (topic in topics) {
                val topicId = repository.insertTopic(topic.name)
                repository.linkTopicToBook(topicId, bookId)
            }
            for (place in pubPlaces) {
                val pubPlaceId = repository.insertPubPlace(place.name)
                repository.linkPubPlaceToBook(pubPlaceId, bookId)
            }
            for (date in pubDates) {
                val pubDateId = repository.insertPubDate(date.date)
                repository.linkPubDateToBook(pubDateId, bookId)
            }

            imported++
            if (imported % 10000 == 0) {
                logger.i { "  HebrewBooks progress: $imported imported" }
            }
        }

        rs.close()
        stmt.close()
        logger.i { "HebrewBooks: imported=$imported, skipped=$skipped" }
    }

    // ── OtzarHaChochma import ──────────────────────────────────────────

    private suspend fun importOtzarHaChochma() {
        logger.i { "Importing OtzarHaChochma..." }
        var imported = 0
        var skipped = 0

        val stmt = booksConn.createStatement()
        val rs = stmt.executeQuery(
            """
            SELECT book_id, title, authors, volume, from_year, to_year,
                   places, subjects, pages
            FROM otzar_hahochma
            WHERE title IS NOT NULL AND title != ''
            """
        )

        while (rs.next()) {
            val otzarBookId = rs.getLong("book_id")
            val externalId = "oh:$otzarBookId"
            val title = rs.getString("title")?.trim() ?: continue
            val authorsJson = rs.getString("authors")?.trim()
            val volume = rs.getString("volume")?.trim()?.takeIf { it.isNotBlank() }
            val fromYear = rs.getString("from_year")?.trim()
            val places = rs.getString("places")?.trim()
            val subjectsStr = rs.getString("subjects")?.trim()
            val pages = rs.getObject("pages") as? Number

            // Parse authors JSON array
            val authors = parseJsonStringArray(authorsJson).map { Author(name = it.trim()) }

            // Parse subjects — comma-separated Hebrew strings
            val topics = if (!subjectsStr.isNullOrBlank()) {
                subjectsStr.split(",").map { it.trim() }.filter { it.isNotBlank() }.map { Topic(name = it) }
            } else emptyList()

            // Publication places (Hebrew, can be comma-separated)
            val pubPlaces = if (!places.isNullOrBlank()) {
                places.split(",").map { it.trim() }.filter { it.isNotBlank() }.map { PubPlace(name = it) }
            } else emptyList()

            // Publication dates
            val pubDates = if (!fromYear.isNullOrBlank()) {
                listOf(PubDate(date = fromYear))
            } else emptyList()

            // Resolve or allocate book ID
            val resolvedId = idResolver?.resolveBookId(externalId, otzarSourceId, "external")
            val bookId = resolvedId ?: nextBookId.getAndIncrement()

            repository.executeRawQuery(
                """
                INSERT OR IGNORE INTO book
                    (id, categoryId, sourceId, title, orderIndex, totalLines, isBaseBook,
                     hasTargumConnection, hasReferenceConnection, hasSourceConnection,
                     hasCommentaryConnection, hasOtherConnection, hasAltStructures,
                     hasTeamim, hasNekudot, isContentExternal, externalLibraryId,
                     pages, volume)
                VALUES
                    ($bookId, $otzarCategoryId, $otzarSourceId,
                     '${escapeSql(title)}', 999, 0, 0,
                     0, 0, 0, 0, 0, 0, 0, 0, 1, '${escapeSql(externalId)}',
                     ${pages?.toInt() ?: "NULL"},
                     ${if (volume != null) "'${escapeSql(volume)}'" else "NULL"})
                """.trimIndent()
            )

            // Insert authors, topics, places, dates
            for (author in authors) {
                val authorId = repository.insertAuthor(author.name)
                repository.linkAuthorToBook(authorId, bookId)
            }
            for (topic in topics) {
                val topicId = repository.insertTopic(topic.name)
                repository.linkTopicToBook(topicId, bookId)
            }
            for (place in pubPlaces) {
                val pubPlaceId = repository.insertPubPlace(place.name)
                repository.linkPubPlaceToBook(pubPlaceId, bookId)
            }
            for (date in pubDates) {
                val pubDateId = repository.insertPubDate(date.date)
                repository.linkPubDateToBook(pubDateId, bookId)
            }

            imported++
            if (imported % 10000 == 0) {
                logger.i { "  OtzarHaChochma progress: $imported imported" }
            }
        }

        rs.close()
        stmt.close()
        logger.i { "OtzarHaChochma: imported=$imported, skipped=$skipped" }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /**
     * Parses a JSON array of strings, e.g. `["שו\"ת","גאונים"]`.
     * Returns empty list on null/blank/malformed input.
     */
    private fun parseJsonStringArray(jsonStr: String?): List<String> {
        if (jsonStr.isNullOrBlank()) return emptyList()
        return try {
            val arr = json.parseToJsonElement(jsonStr).jsonArray
            arr.mapNotNull { el ->
                val s = el.jsonPrimitive.contentOrNull?.trim()
                s?.takeIf { it.isNotBlank() }
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun countRows(table: String): Long {
        val stmt = booksConn.createStatement()
        val rs = stmt.executeQuery("SELECT count(*) FROM $table")
        rs.next()
        val count = rs.getLong(1)
        rs.close()
        stmt.close()
        return count
    }

    private fun escapeSql(s: String): String = s.replace("'", "''")
}
