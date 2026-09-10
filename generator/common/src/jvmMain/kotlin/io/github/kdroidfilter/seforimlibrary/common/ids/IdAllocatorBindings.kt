package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/**
 * Glue between [IdAllocator] and [SeforimRepository]. Importers should use these
 * helpers rather than calling repo `insertXxx` directly so that ids are driven
 * by the allocator and remain stable across builds.
 *
 * Idempotent contract: calling any helper twice for the same natural key inserts
 * the row once and returns the same id. This relies on repo `*WithId` methods
 * using `ON CONFLICT DO NOTHING` for lookup tables, and on the existing
 * `entity.id > 0` short-circuit for book/line/tocEntry/altToc.
 */
class IdAllocatorBindings(
    val allocator: IdAllocator,
    private val repo: SeforimRepository,
) {
    // Per-process memo of (lookup kind → already-inserted natural keys). The
    // allocator's id_lookup short-circuits the ID resolution in O(1), but the
    // repo's `insertXxxWithId` blindly issued an INSERT OR IGNORE for every
    // call — burning ~0.1 ms of SQLite native step per useless attempt. On
    // ~7.5 M Sefaria links calling upsertConnectionType, that's > 10 min of
    // wasted I/O on a table that holds 5 rows total. JFR profile from
    // 2026-05-13 showed `insertConnectionTypeWithId` second on the hot list.
    //
    // The memo is a per-build cache: it starts empty so the first sighting of
    // any name still triggers the INSERT (covers builds seeded from an empty
    // DB even if the allocator's lookup is pre-populated). Subsequent calls
    // for the same name skip the SQL.
    private val sourcesInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val authorsInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val topicsInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val pubPlacesInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val pubDatesInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val connectionTypesInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val categoriesInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    private val tocTextsInserted = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()

    // ─── Lookup-table helpers ──────────────────────────────────────────────────

    suspend fun upsertSource(name: String): Long {
        val id = allocator.sourceId(name)
        if (sourcesInserted.add(name)) repo.insertSourceWithId(id, name)
        return id
    }

    suspend fun upsertAuthor(name: String): Long {
        val id = allocator.authorId(name)
        if (authorsInserted.add(name)) repo.insertAuthorWithId(id, name)
        return id
    }

    suspend fun upsertTopic(name: String): Long {
        val id = allocator.topicId(name)
        if (topicsInserted.add(name)) repo.insertTopicWithId(id, name)
        return id
    }

    suspend fun upsertPubPlace(name: String): Long {
        val id = allocator.pubPlaceId(name)
        if (pubPlacesInserted.add(name)) repo.insertPubPlaceWithId(id, name)
        return id
    }

    suspend fun upsertPubDate(date: String): Long {
        val id = allocator.pubDateId(date)
        if (pubDatesInserted.add(date)) repo.insertPubDateWithId(id, date)
        return id
    }

    suspend fun upsertConnectionType(name: String): Long {
        val id = allocator.connectionTypeId(name)
        if (connectionTypesInserted.add(name)) repo.insertConnectionTypeWithId(id, name)
        return id
    }

    suspend fun upsertCategory(
        canonicalPath: String,
        parentId: Long?,
        title: String,
        level: Int,
        orderIndex: Int,
        heShortDesc: String? = null,
        heDesc: String? = null,
    ): Long {
        var id = allocator.categoryId(canonicalPath)
        if (categoriesInserted.add(canonicalPath)) {
            repo.insertCategoryWithId(id, parentId, title, level, orderIndex, heShortDesc, heDesc)
            // insertCategoryWithId is INSERT OR IGNORE. A row created outside the
            // allocator (renameCategories auto-creates the leaf of a book_moves.csv
            // destination with an implicit rowid) can already sit on this id — then
            // our category is silently never written and every book we hand this id
            // to lands inside that foreign folder. Verify, and take a fresh id if so.
            var attempts = 0
            while (!storedCategoryMatches(id, parentId, title)) {
                check(++attempts <= MAX_CATEGORY_ID_ATTEMPTS) {
                    "Cannot place category '$canonicalPath': id $id still collides after $attempts attempts"
                }
                id = allocator.reallocateCategoryId(canonicalPath)
                repo.insertCategoryWithId(id, parentId, title, level, orderIndex, heShortDesc, heDesc)
            }
        }
        return id
    }

    private suspend fun storedCategoryMatches(id: Long, parentId: Long?, title: String): Boolean {
        val stored = repo.getCategory(id) ?: return false
        return stored.title == title && stored.parentId == parentId
    }

    suspend fun upsertTocText(text: String): Long {
        val id = allocator.tocTextId(text)
        if (tocTextsInserted.add(text)) repo.insertTocTextWithId(id, text)
        return id
    }

    // ─── Composite-keyed entities ──────────────────────────────────────────────

    /**
     * Assigns a stable id to [book] (mutating-friendly via `copy`) and inserts it.
     * Returns the id that ended up persisted.
     */
    suspend fun insertBookStable(sourceName: String, canonicalHeTitle: String, book: Book): Long {
        val id = allocator.bookId(sourceName, canonicalHeTitle)
        val withId = if (book.id == id) book else book.copy(id = id)
        repo.insertBook(withId)
        return id
    }

    suspend fun insertLineStable(line: Line, occurrenceIdx: Int = 0): Long {
        val id = allocator.lineId(line.bookId, normalisedContentHash(line.content), occurrenceIdx)
        val withId = if (line.id == id) line else line.copy(id = id)
        repo.insertLine(withId)
        return id
    }

    suspend fun insertTocEntryStable(entry: TocEntry, ancestorPath: String): Long {
        val id = allocator.tocEntryId(entry.bookId, ancestorPath)
        val withId = if (entry.id == id) entry else entry.copy(id = id)
        repo.insertTocEntry(withId)
        return id
    }

    // Assign the stable id without inserting; row is flushed later via insertTocEntriesBatch.
    fun assignTocEntryId(entry: TocEntry, ancestorPath: String): TocEntry {
        val id = allocator.tocEntryId(entry.bookId, ancestorPath)
        return if (entry.id == id) entry else entry.copy(id = id)
    }

    suspend fun upsertAltTocStructureStable(structure: AltTocStructure): Long {
        val id = allocator.altTocStructureId(structure.bookId, structure.key)
        val withId = if (structure.id == id) structure else structure.copy(id = id)
        repo.upsertAltTocStructure(withId)
        return id
    }

    suspend fun insertAltTocEntryStable(entry: AltTocEntry, ancestorPath: String): Long {
        val id = allocator.altTocEntryId(entry.structureId, ancestorPath)
        val withId = if (entry.id == id) entry else entry.copy(id = id)
        repo.insertAltTocEntry(withId)
        return id
    }

    suspend fun insertLinkStable(
        sourceBookId: Long,
        targetBookId: Long,
        sourceLineId: Long,
        targetLineId: Long,
        targetLineIndex: Long,
        connectionTypeId: Long,
    ): Long {
        val id = allocator.linkId(sourceLineId, targetLineId, connectionTypeId)
        repo.insertLinkWithId(
            id = id,
            sourceBookId = sourceBookId,
            targetBookId = targetBookId,
            sourceLineId = sourceLineId,
            targetLineId = targetLineId,
            targetLineIndex = targetLineIndex,
            connectionTypeId = connectionTypeId,
        )
        return id
    }

    companion object {
        /**
         * How many times [upsertCategory] may take a fresh id when the one it holds
         * is occupied by a foreign row. One is normally enough (the Otzaria stage
         * raises the allocator's floor past `MAX(category.id)` first); the extra
         * headroom just keeps a forgotten floor from silently misplacing books.
         */
        private const val MAX_CATEGORY_ID_ATTEMPTS = 16

        /**
         * Builds the 20-byte content-hash slot of a line's natural key.
         *
         * Content only: heRef is a rendered label that a formatting pass can
         * rewrite for a whole corpus at once, and keying on it renumbered every
         * line of every simple-schema book in v27 (issue #1211). Lines that
         * repeat inside a book are separated by the occurrence index instead.
         *
         * [content] must be the RAW segment: a generated prefix ("(א) ", a daf
         * label) shifts on a neighbour's insert and would churn ids again.
         */
        fun lineNaturalKeyHash(content: String): ByteArray =
            MessageDigest.getInstance("SHA-1").digest("CT:$content".toByteArray(Charsets.UTF_8))

        /**
         * sha1 of the line content, used as part of the natural key for `line`.
         * Always returns 20 bytes — required by [IdAllocator.lineId].
         *
         * Phase 1 keeps this minimal (raw content); Phase 3 will swap in the
         * Lucene-style normalisation pipeline (HTML strip + nikud strip + ...).
         */
        // Memoize SHA-1 of common line contents. The Otzaria corpus has many
        // duplicate lines (blank, recurring headers, fixed liturgy phrasing)
        // that hash to the same value over and over — JFR showed
        // `normalisedContentHash` as the #1 project hotspot (~3% wall) on the
        // GenerateLines phase. Bounded so adversarial inputs can't blow up
        // the heap; LRU-ish behaviour via simple drop-when-full.
        private const val CONTENT_HASH_CACHE_MAX = 200_000
        private val contentHashCache = ConcurrentHashMap<String, ByteArray>()

        fun normalisedContentHash(content: String): ByteArray {
            contentHashCache[content]?.let { return it }
            val hash = MessageDigest.getInstance("SHA-1").digest(content.toByteArray(Charsets.UTF_8))
            if (contentHashCache.size > CONTENT_HASH_CACHE_MAX) contentHashCache.clear()
            contentHashCache[content] = hash
            return hash
        }
    }
}
