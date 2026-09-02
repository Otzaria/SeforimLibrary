package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.changes.OtzariaSourceHashComputer
import io.github.kdroidfilter.seforimlibrary.common.changes.TouchedBookDetector
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.nameWithoutExtension
import kotlin.io.path.readText

/**
 * DatabaseGenerator is responsible for generating the Otzaria database from source files.
 * It processes directories, books, and links to create a structured database.
 *
 * @property sourceDirectory The path to the source directory containing the data files
 * @property repository The repository used to store the generated data
 */
private const val LINE_FLUSH_THRESHOLD = 1000

class DatabaseGenerator(
    private val sourceDirectory: Path,
    private val repository: SeforimRepository,
    private val acronymDbPath: String? = null,
    private val filterSourcesForLinks: Boolean = true,
    private val allocator: IdAllocator = InMemoryIdAllocator.load(path = null),
    private val buildVersion: Int = 0,
) {

    private val logger = Logger.withTag("DatabaseGenerator")
    private val bindings = IdAllocatorBindings(allocator, repository)


    private val json = Json {
        ignoreUnknownKeys = true
        coerceInputValues = true
    }
    // Per-(bookId, contentHash) occurrence counter so duplicate lines get distinct stable ids.
    private val lineOccurrenceByBook = mutableMapOf<Long, MutableMap<Long, Int>>()
    private fun nextLineOccurrence(bookId: Long, hash: ByteArray): Int {
        val hashKey = hash.fold(0L) { acc, b -> (acc shl 5) - acc + b.toLong() }
        val m = lineOccurrenceByBook.getOrPut(bookId) { mutableMapOf() }
        val v = (m[hashKey] ?: -1) + 1
        m[hashKey] = v
        return v
    }
    private fun stableLineId(bookId: Long, content: String): Long {
        val hash = IdAllocatorBindings.normalisedContentHash(content)
        return allocator.lineId(bookId, hash, nextLineOccurrence(bookId, hash))
    }
    // Per-book stack of (level -> textId) used to derive a stable tocEntry ancestor path.
    private val tocAncestorStackByBook = mutableMapOf<Long, MutableMap<Int, Long>>()
    private suspend fun stableTocEntryId(bookId: Long, level: Int, text: String, lineIndex: Int): Pair<Long, Long> {
        // Use upsertTocText (allocate + insertTocTextWithId) so the id reserved
        // by the allocator matches what's actually in the tocText table.
        // Plain bindings.allocator.tocTextId(text) only reserves an id; the
        // subsequent repo.insertTocText(text) used a non-allocator-aware
        // INSERT OR IGNORE path and ended up with auto-increment ids — which
        // broke the delta producer's stable-id invariant.
        val textId = bindings.upsertTocText(text)
        val stack = tocAncestorStackByBook.getOrPut(bookId) { sortedMapOf() }
        // Drop deeper levels (we're climbing up).
        stack.keys.filter { it >= level }.forEach { stack.remove(it) }
        stack[level] = textId
        val path = stack.entries.sortedBy { it.key }.joinToString("/") { it.value.toString() } +
            "@$lineIndex"
        return allocator.tocEntryId(bookId, path) to textId
    }

    // Optional connection to the Acronymizer DB (opened lazily)
    private var acronymDb: java.sql.Connection? = null

    // Library root used for relative path normalization
    private lateinit var libraryRoot: Path

    // Map from library-relative book key (e.g. "תנ"ך/בראשית.txt") to source name (e.g. "sefariaToOtzaria")
    private val manifestSourcesByRel = mutableMapOf<String, String>()
    // Cache of source name -> id from DB
    private val sourceNameToId = mutableMapOf<String, Long>()

    // Source blacklist loaded from resources (fallback to default)
    private val sourceBlacklist: Set<String> = loadSourceBlacklistFromResources()

    // File name blacklist loaded from resources; entries are plain filenames such as "book.txt"
    private val fileNameBlacklist: Set<String> = loadFileNameBlacklistFromResources()

    // In-memory caches to accelerate link processing using available RAM
    private var booksByTitle: Map<String, Book> = emptyMap()
    private var booksById: Map<Long, Book> = emptyMap()
    // For each bookId, maps lineIndex -> lineId (0 means missing)
    private val lineIdCache = mutableMapOf<Long, LongArray>()
    // Track line IDs that are headings (contain <h1>, <h2>, etc.)
    private val headingLineIds = mutableSetOf<Long>()
    // Track book IDs that come from Sefaria sources (skip Otzaria links for these)
    private val sefariaBookIds = mutableSetOf<Long>()

    // Tracks books processed from the priority list to avoid double insertion
    private val processedPriorityBookKeys = mutableSetOf<String>()

    // Overall progress across books
    private var totalBooksToProcess: Int = 0
    private var processedBooksCount: Int = 0

    // Normalization helpers for categories/titles
    private fun normalizeHebrewLabel(raw: String): String {
        var s = raw.trim()
        // Normalize common quote variants to Hebrew gershayim/geresh
        s = s.replace('\u201C', '"').replace('\u201D', '"')
        s = s.replace('\u2018', '\'').replace('\u2019', '\'')
        s = s.replace("\"", "״")
        s = s.replace("''", "״")
        s = s.replace("׳׳", "״")
        s = s.replace("`", "׳")
        s = s.replace("\u05f3", "׳")
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    private fun comparableLabel(raw: String): String {
        fun stripCorpusSuffix(s: String): String {
            val pattern = "(?i)\\s+על\\s+(התנ\"ך|התורה|התלמוד|המשנה|תנך|תורה|תלמוד|משנה)$".toRegex()
            return s.replace(pattern, "").trim()
        }

        val base = normalizeHebrewLabel(raw)
            .replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .replace("\\s+".toRegex(), " ")
            .trim()
        return stripCorpusSuffix(base)
    }

    private fun normalizeCategorySegments(rawTitle: String): List<String> {
        val cleaned = normalizeHebrewLabel(rawTitle)
        return when (cleaned) {
            "תלמוד בבלי" -> listOf("תלמוד בבלי")
            "תלמוד ירושלמי", "תלמוד ירושלים" -> listOf("תלמוד ירושלמי")
            "תנך", "תנ\"ך", "תנ״ך" -> listOf("תנ״ך")
            "שות", "שו\"ת", "שו״ת" -> listOf("שו״ת")
            else -> listOf(cleaned)
        }
    }

    private data class CategoryPlacement(
        val id: Long,
        val leafLevel: Int,
        val normalizedPath: List<String>,
        val canonicalPath: String
    )

    private suspend fun findExistingCategory(parentId: Long?, title: String): Category? {
        val targetKey = comparableLabel(title)
        val candidates = if (parentId == null) repository.getRootCategories() else repository.getCategoryChildren(parentId)
        return candidates.firstOrNull { comparableLabel(it.title) == targetKey }
    }

    private suspend fun ensureCategoryHierarchy(rawTitle: String, parentId: Long?, startLevel: Int, parentPath: String): CategoryPlacement {
        val normalizedSegments = normalizeCategorySegments(rawTitle)
        var currentParent = parentId
        var currentLevel = startLevel
        var lastId: Long? = null
        // Key by full path from root, not leaf name: same-named leaves under different parents must not collide.
        val pathSoFar = StringBuilder(parentPath)

        for (title in normalizedSegments) {
            if (pathSoFar.isNotEmpty()) pathSoFar.append('/')
            pathSoFar.append(title)
            val existing = findExistingCategory(currentParent, title)
            val categoryId = existing?.id ?: bindings.upsertCategory(
                canonicalPath = pathSoFar.toString(),
                parentId = currentParent,
                title = title,
                level = currentLevel,
                orderIndex = 999,
            )
            lastId = categoryId
            currentParent = categoryId
            currentLevel += 1
        }

        val finalId = lastId ?: throw IllegalStateException("Failed to ensure category hierarchy for $rawTitle")
        return CategoryPlacement(
            id = finalId,
            leafLevel = currentLevel - 1,
            normalizedPath = normalizedSegments,
            canonicalPath = pathSoFar.toString()
        )
    }

    private fun normalizeBookTitle(rawTitle: String): String {
        val base = normalizeHebrewLabel(rawTitle)
        return when (base) {
            "תנך", "תנ\"ך" -> "תנ״ך"
            else -> base
        }
    }

    private fun stripQuotesForLookup(title: String): String {
        return title.replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .trim()
    }

    // Book contents cache: maps library-relative key -> list of lines
    private val bookContentCache = mutableMapOf<String, List<String>>()

    // ===== Companion-notes merge ('הערות על X' → inline footnotes) =====
    // base title → (0-based base line → note lines in reading order)
    private var hearotMergePlans: Map<String, Map<Int, List<String>>> = emptyMap()
    // Companion books consumed by the merge — never imported as standalone books.
    private var mergedHearotTitles: Set<String> = emptySet()
    // base title → sha256(companion file ‖ links json), mixed into the base book's
    // source hash so touched-book detection sees note-only edits.
    private var hearotMergeExtraHash: Map<String, ByteArray> = emptyMap()
    // Planned bases whose merge has not been applied yet — must drain to empty.
    private val pendingHearotMergeBases = mutableSetOf<String>()

    // No-op kept for call-site compatibility. The IdAllocator (seeded from
    // build_state.db) now handles cross-build id continuation — see DELTA_UPDATE_PLAN.md §3.5.
    @Suppress("UnusedPrivateMember", "RedundantSuspendModifier")
    private suspend fun initializeIdCountersFromExistingDb() {
        // Intentionally empty.
    }

    // Source hashes captured by the Phase 2 detector at the start of import.
    // Recorded onto the IdAllocator at the end of import (see [recordSourceHashesAtEndOfImport]).
    private var otzariaSourceHashes: Map<
        BookKey,
        io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash,
    > = emptyMap()

    /** Public hook called by the importer once all Otzaria books are inserted. */
    internal fun recordSourceHashesAtEndOfImport() {
        if (otzariaSourceHashes.isEmpty()) return
        var recorded = 0
        for ((key, hash) in otzariaSourceHashes) {
            if (allocator.peekBookId(key.sourceName, key.canonicalHeTitle) != null) {
                allocator.recordSourceHash(key, hash)
                recorded++
            }
        }
        logger.i { "Recorded source hashes for $recorded / ${otzariaSourceHashes.size} Otzaria books" }
    }


    /**
     * Generates the database by processing metadata, directories, and links.
     * This is the main entry point for the database generation process.
     */
    suspend fun generate(): Unit = coroutineScope {
        logger.i { "Starting database generation..." }
        logger.i { "Source directory: $sourceDirectory" }

        try {
            // Continue IDs from existing DB if present (append/incremental)
            initializeIdCountersFromExistingDb()
            // Disable foreign keys for better performance during bulk insertion
            logger.i { "Disabling foreign keys for better performance..." }
            disableForeignKeys()

            // Lower durability for faster bulk writes (restored afterward)
            logger.i { "Setting PRAGMA synchronous=OFF for bulk generation" }
            repository.setSynchronousOff()
            logger.i { "Setting PRAGMA journal_mode=OFF for bulk generation" }
            repository.setJournalModeOff()

            // Wrap the entire generation in a single transaction for major SQLite speedups
            repository.runInTransaction {
                // Load metadata
                val metadata = loadMetadata()
                logger.i { "Metadata loaded: ${metadata.size} entries" }

                // Load sources from files_manifest.json and upsert source table
                loadSourcesFromManifest()
                precreateSourceEntries()
                backfillAcronymsForExistingBooks()

                // Process hierarchy
                val libraryPath = sourceDirectory.resolve("אוצריא")
                if (!libraryPath.exists()) {
                    throw IllegalStateException("The directory אוצריא does not exist in $sourceDirectory")
                }

                // Save for relative path computations
                libraryRoot = libraryPath

                // Plan companion-notes merges before any book import
                buildHearotMergePlans()

                // Estimate total number of books (txt files) for progress tracking
                totalBooksToProcess = try {
                    Files.walk(libraryRoot).use { s ->
                        s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                            .count().toInt()
                    }
                } catch (_: Exception) { 0 }
                logger.i { "Planned to process approximately $totalBooksToProcess books" }

                // Process priority books first (if any), then process the full library
                runCatching {
                    processPriorityBooks(loadMetadata = { metadata })
                }.onFailure { e ->
                    logger.w(e) { "Failed processing priority list; continuing with full generation" }
                }

                logger.i { "🚀 Starting to process library directory: $libraryPath" }
                // Preload all book .txt contents into RAM for faster processing
                preloadAllBookContents(libraryPath)
                processDirectory(libraryPath, null, 0, metadata)
                assertAllHearotMergesApplied()

                // Process links
                processLinks()

                // Alternate TOC structures (parasha/aliyah-style) from alt_toc/ files
                processAltTocStructures()

                // Build category closure table for fast descendant queries
                logger.i { "Building category_closure (ancestor-descendant) table..." }
                repository.rebuildCategoryClosure()
            }
            // Restore PRAGMAs after commit
            logger.i { "Re-enabling foreign keys..." }
            enableForeignKeys()
            logger.i { "Restoring PRAGMA synchronous=NORMAL" }
            repository.setSynchronousNormal()
            // Restore journal mode after commit
            logger.i { "Restoring PRAGMA journal_mode=WAL" }
            repository.setJournalModeWal()
            logger.i { "Generation completed successfully!" }
        } catch (e: Exception) {
            // Make sure to re-enable foreign keys even if an error occurs
            try {
                enableForeignKeys()
            } catch (innerEx: Exception) {
                logger.w(innerEx) { "Error re-enabling foreign keys after failure" }
            }
            try {
                repository.setSynchronousNormal()
            } catch (_: Exception) {}
            try {
                repository.setJournalModeWal()
            } catch (_: Exception) {}

            logger.e(e) { "Error during generation" }
            throw e
        }
    }

    /**
     * Phase 1: Generate categories, books, TOCs and lines only (no links).
     */
    suspend fun generateLinesOnly(): Unit = coroutineScope {
        logger.i { "Starting phase 1: categories/books/lines generation..." }
        try {
            initializeIdCountersFromExistingDb()

            // Performance PRAGMAs
            disableForeignKeys()
            repository.setSynchronousOff()
            repository.setJournalModeOff()

            repository.runInTransaction {
                val metadata = loadMetadata()
                // Load sources and create entries upfront
                loadSourcesFromManifest()

                val libraryPath = sourceDirectory.resolve("אוצריא")
                if (!libraryPath.exists()) {
                    throw IllegalStateException("The directory אוצריא does not exist in $sourceDirectory")
                }
                libraryRoot = libraryPath

                // Plan companion-notes merges before hash detection and any import:
                // merged notes change the base book's content, so its source hash
                // must reflect the companion file and links json too.
                buildHearotMergePlans()

                // ─── Phase 2: touched-book detection ────────────────────────
                // Runs after manifestSourcesByRel is loaded so the BookKey we
                // emit matches what the importer will record below
                // (via sourceNameForManifestKey + normalizeBookTitle).
                val currentSourceHashes: Map<
                    BookKey,
                    io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash,
                > = runCatching {
                    OtzariaSourceHashComputer(
                        sourceNameResolver = ::sourceNameForManifestKey,
                        titleNormalizer = ::normalizeBookTitle,
                    ).compute(sourceDirectory, buildVersion)
                }.getOrElse {
                    logger.w(it) { "Skipping Otzaria touched-book detection: ${it.message}" }
                    emptyMap()
                }.let(::mixHearotMergeIntoSourceHashes)
                run {
                    val prev = currentSourceHashes.keys
                        .mapNotNull { key -> allocator.previousSourceHash(key)?.let { key to it } }
                        .toMap()
                    val classification = TouchedBookDetector.classify(prev, currentSourceHashes)
                    logger.i { "Source-hash classification (Otzaria): ${classification.summary()}" }
                }
                // Capture for later — recorded onto allocator at end of import.
                otzariaSourceHashes = currentSourceHashes

                precreateSourceEntries()
                backfillAcronymsForExistingBooks()

                totalBooksToProcess = try {
                    Files.walk(libraryRoot).use { s ->
                        s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                            .count().toInt()
                    }
                } catch (_: Exception) { 0 }
                logger.i { "Planned to process approximately $totalBooksToProcess books (phase 1)" }

                runCatching { processPriorityBooks(loadMetadata = { metadata }) }
                    .onFailure { e -> logger.w(e) { "Failed processing priority list; continuing with full generation (phase 1)" } }
                // Preload all book .txt contents into RAM for faster processing
                preloadAllBookContents(libraryPath)
                processDirectory(libraryPath, null, 0, metadata)
                assertAllHearotMergesApplied()

                // Build category closure after categories insertion
                logger.i { "Building category_closure table (phase 1)..." }
                repository.rebuildCategoryClosure()
            }
            recordSourceHashesAtEndOfImport()
        } finally {
            runCatching { enableForeignKeys() }
            runCatching { repository.setSynchronousNormal() }
            runCatching { repository.setJournalModeWal() }
            logger.i { "Phase 1 completed." }
        }
    }

    /**
     * Phase 2: Process links only and update link-related flags.
     */
    suspend fun generateLinksOnly(): Unit = coroutineScope {
        logger.i { "Starting phase 2: links processing..." }
        try {
            disableForeignKeys()
            repository.setSynchronousOff()
            repository.setJournalModeOff()
            repository.runInTransaction {
                processLinks()
                processAltTocStructures()
            }
        } finally {
            runCatching { enableForeignKeys() }
            runCatching { repository.setSynchronousNormal() }
            runCatching { repository.setJournalModeWal() }
            logger.i { "Phase 2 completed." }
        }
    }

    // Prepare caches so that link resolution uses RAM instead of round-trips
    private suspend fun ensureCachesLoaded() {
        if (booksByTitle.isEmpty()) {
            val allBooks = repository.getAllBooks()
            val filtered = if (filterSourcesForLinks) {
                allBooks.filter { book ->
                    val src = runCatching { repository.getSourceById(book.sourceId) }.getOrNull()
                    val name = src?.name ?: "Unknown"
                    val normalized = comparableLabel(name)
                    !sourceBlacklist.any { comparableLabel(it) == normalized }
                }
            } else {
                allBooks
            }
            booksByTitle = filtered.associateBy { it.title }
            booksById = filtered.associateBy { it.id }

            // Build set of Sefaria book IDs to skip Otzaria links for these books
            sefariaBookIds.clear()
            for (book in filtered) {
                val src = runCatching { repository.getSourceById(book.sourceId) }.getOrNull()
                val sourceName = src?.name ?: ""
                if (sourceName.contains("sefaria", ignoreCase = true)) {
                    sefariaBookIds.add(book.id)
                }
            }

            val skipped = allBooks.size - filtered.size
            if (filterSourcesForLinks && skipped > 0) {
                logger.i { "Preloaded ${filtered.size} books into memory for fast link processing (skipped $skipped by source blacklist)" }
            } else {
                logger.i { "Preloaded ${filtered.size} books into memory for fast link processing" }
            }
            logger.i { "Identified ${sefariaBookIds.size} Sefaria books (Otzaria links will be skipped for these)" }
        }
    }

    // Ensure we have lineIndex -> lineId mapping for the given book in memory
    private suspend fun ensureLineIndexCache(bookId: Long) {
        if (lineIdCache.containsKey(bookId)) return
        val totalLines = booksById[bookId]?.totalLines ?: repository.getBook(bookId)?.totalLines ?: 0
        val arr = LongArray(totalLines.coerceAtLeast(0)) { 0L }
        // Use existing repository.getLines to load ids and indices; content is ignored here
        val lines = if (totalLines > 0) repository.getLines(bookId, 0, totalLines - 1) else emptyList()
        for (ln in lines) {
            val idx = ln.lineIndex
            if (idx >= 0 && idx < arr.size) arr[idx] = ln.id
        }
        lineIdCache[bookId] = arr
        logger.d { "Loaded ${lines.size} line id/index pairs for book $bookId into memory" }
    }

    // Lookup helper using the RAM cache
    private suspend fun getLineIdCached(bookId: Long, lineIndex: Int): Long? {
        ensureLineIndexCache(bookId)
        val arr = lineIdCache[bookId] ?: return null
        if (lineIndex < 0 || lineIndex >= arr.size) return null
        val id = arr[lineIndex]
        return if (id == 0L) null else id
    }

    // Preload all book file contents into RAM, keyed by library-relative path
    private suspend fun preloadAllBookContents(libraryPath: Path) {
        // Avoid reloading if already populated
        if (bookContentCache.isNotEmpty()) return
        logger.i { "Preloading book contents into RAM from $libraryPath ..." }
        val files = Files.walk(libraryPath).use { s ->
            s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                .filter { p ->
                    val fileName = p.fileName.toString()
                    // Skip files explicitly blacklisted by name
                    if (fileNameBlacklist.contains(fileName)) {
                        logger.d { "Skipping preload for blacklisted file '$fileName'" }
                        return@filter false
                    }
                    // Skip blacklisted sources when known
                    val rel = runCatching { toLibraryRelativeKey(p) }.getOrElse { p.fileName.toString() }
                    val src = manifestSourcesByRel[rel] ?: "Unknown"
                    if (sourceBlacklist.contains(src)) {
                        logger.d { "Skipping preload for blacklisted source '$src': $rel" }
                        return@filter false
                    }
                    true
                }
                .toList()
        }
        // Parallelize reads
        val loaded = coroutineScope {
            files.map { p ->
                async {
                    val key = toLibraryRelativeKey(p)
                    val content = p.readText(Charsets.UTF_8)
                    key to content.lines()
                }
            }.awaitAll()
        }
        for ((k, v) in loaded) bookContentCache[k] = v
        logger.i { "Preloaded ${bookContentCache.size} books into RAM" }
    }

    /** Title of the book a link's path_2 points at (mirrors processLinksForBook). */
    private fun linkTargetTitleOf(path2: String): String {
        val last = if (path2.contains('\\')) path2.split('\\').last()
        else Paths.get(path2).fileName.toString()
        return last.substringBeforeLast('.', last)
    }

    private val headingLineRegex = Regex("^﻿?<h[1-6]", RegexOption.IGNORE_CASE)

    /**
     * Scans the links dir's `<base>_links.json` files for entries pointing at
     * 'הערות על X' companion files and plans their inline merge into the base
     * book (Havrouta hearot excluded — they feed the Talmud transitive-link
     * pipeline). A pair is merged only when its mapping is complete: every
     * non-blank, non-heading companion line is referenced by a link. Partial
     * pairs stay on the standalone-book mechanism, loudly reported, until their
     * links are completed upstream.
     */
    private suspend fun buildHearotMergePlans() {
        hearotMergePlans = emptyMap()
        mergedHearotTitles = emptySet()
        hearotMergeExtraHash = emptyMap()
        pendingHearotMergeBases.clear()
        val linksDir = sourceDirectory.resolve("links")
        if (!linksDir.exists()) return

        // A title is usable for merging only when it resolves to exactly one file.
        val filesByTitle = HashMap<String, Path?>()
        Files.walk(libraryRoot).use { s ->
            s.filter { Files.isRegularFile(it) && it.extension == "txt" }.forEach { p ->
                val t = normalizeBookTitle(p.fileName.toString().substringBeforeLast('.'))
                filesByTitle[t] = if (t in filesByTitle) null else p
            }
        }

        fun resolveFile(title: String, reasons: MutableList<String>): Path? {
            if (title !in filesByTitle) { reasons += "$title (file not found)"; return null }
            val path = filesByTitle[title] ?: run { reasons += "$title (ambiguous filename)"; return null }
            if (fileNameBlacklist.contains(path.fileName.toString())) { reasons += "$title (blacklisted file)"; return null }
            if (sourceBlacklist.contains(getSourceNameFor(path))) { reasons += "$title (blacklisted source)"; return null }
            return path
        }

        // Every reason for the pair to stay standalone is decided HERE, before any
        // import — a plan must never fail mid-import (the companion's standalone
        // import was already suppressed by then).
        fun unmergeableNoteReason(note: String): String? = when {
            note.isBlank() -> "linked blank note line"
            note.contains("<i>") || note.contains("<i ") || note.contains("</i>") ->
                "note with <i> markup (breaks the app's footnote regex)"
            headingLineRegex.containsMatchIn(note) -> "linked heading note line"
            else -> null
        }

        val plans = HashMap<String, MutableMap<Int, MutableList<String>>>()
        val merged = mutableSetOf<String>()
        val extra = HashMap<String, ByteArray>()
        val standalone = mutableListOf<String>()
        var mergedPairs = 0
        var totalNotes = 0

        val linkFiles = Files.list(linksDir).use { s -> s.filter { it.extension == "json" }.toList() }
            .sortedBy { it.fileName.toString() }
        for (jsonFile in linkFiles) {
            val baseTitle = normalizeBookTitle(jsonFile.nameWithoutExtension.removeSuffix("_links"))
            val jsonBytes = runCatching { Files.readAllBytes(jsonFile) }.getOrNull() ?: continue
            val allEntries = parseLinksFromJson(jsonBytes.toString(Charsets.UTF_8), baseTitle)
            val companionEntries = allEntries.filter {
                HearotCompanionMerge.isMergeableCompanionTitle(
                    normalizeBookTitle(linkTargetTitleOf(it.path_2))
                )
            }
            if (companionEntries.isEmpty()) continue

            val basePath = resolveFile(baseTitle, standalone) ?: continue
            // A base the importer will skip must not consume its companion (M2):
            // Sefaria-priority shadowing and root-level files never import.
            val shadowing = repository.getBookByHeRef(baseTitle)
            if (shadowing != null && repository.getSourceById(shadowing.sourceId)?.name == "Sefaria") {
                standalone += "$baseTitle (base shadowed by a Sefaria book)"
                continue
            }
            if (basePath.parent == libraryRoot) {
                standalone += "$baseTitle (base file at library root is never imported)"
                continue
            }
            val baseLines = basePath.readText(Charsets.UTF_8).lines()

            val byHearot = companionEntries
                .groupBy { normalizeBookTitle(linkTargetTitleOf(it.path_2)) }
                .toSortedMap()
            for ((hearotTitle, rawEntries) in byHearot) {
                val hearotPath = resolveFile(hearotTitle, standalone) ?: continue
                // Exact duplicate rows are packaging noise; drop them deterministically.
                val entries = rawEntries.distinctBy { it.line_index_1 to it.line_index_2 }
                if (entries.any { it.line_index_1_end != null || it.line_index_2_end != null }) {
                    standalone += "$hearotTitle (ranged companion links)"
                    continue
                }
                val hearotBytes = Files.readAllBytes(hearotPath)
                val hearotLines = hearotBytes.toString(Charsets.UTF_8).lines()
                if (entries.any {
                        it.line_index_1.toInt() - 1 !in baseLines.indices ||
                            it.line_index_2.toInt() - 1 !in hearotLines.indices
                    }
                ) {
                    standalone += "$hearotTitle (companion links out of file range)"
                    continue
                }
                val referenced = entries.map { it.line_index_2.toInt() - 1 }.toSet()
                val unlinked = hearotLines.withIndex().count { (j, l) ->
                    j !in referenced && l.isNotBlank() && !headingLineRegex.containsMatchIn(l)
                }
                if (unlinked > 0) {
                    standalone += "$hearotTitle ($unlinked unlinked note lines)"
                    continue
                }
                val badNote = referenced.firstNotNullOfOrNull { j -> unmergeableNoteReason(hearotLines[j]) }
                if (badNote != null) {
                    standalone += "$hearotTitle ($badNote)"
                    continue
                }
                val noteLineIdxs = entries.map { it.line_index_1.toInt() - 1 }.toSet()
                if (noteLineIdxs.any { baseLines[it].contains("class=\"footnote") }) {
                    standalone += "$hearotTitle (base line already carries inline footnotes)"
                    continue
                }
                // Word-anchored links measure raw offsets against the ORIGINAL line;
                // an in-place merge would shift them (see buildLinkAnchor).
                if (allEntries.any { it.start != null && it.line_index_1.toInt() - 1 in noteLineIdxs }) {
                    standalone += "$hearotTitle (word-anchored links on note lines)"
                    continue
                }
                val perLine = plans.getOrPut(baseTitle) { sortedMapOf() }
                for (e in entries.sortedBy { it.line_index_2 }) {
                    perLine.getOrPut(e.line_index_1.toInt() - 1) { mutableListOf() }
                        .add(hearotLines[e.line_index_2.toInt() - 1])
                }
                merged += hearotTitle
                mergedPairs++
                totalNotes += entries.size
                val md = java.security.MessageDigest.getInstance("SHA-256")
                extra[baseTitle]?.let { md.update(it) }
                md.update(hearotBytes)
                md.update(jsonBytes)
                extra[baseTitle] = md.digest()
            }
        }

        hearotMergePlans = plans
        mergedHearotTitles = merged
        hearotMergeExtraHash = extra
        pendingHearotMergeBases.addAll(plans.keys)
        if (mergedPairs > 0 || standalone.isNotEmpty()) {
            logger.i { "📝 Companion-notes merge plan: $mergedPairs pairs, $totalNotes notes → inline footnotes" }
            standalone.forEach { logger.w { "📝 Companion notes stay standalone: $it" } }
        }
    }

    /**
     * Loud invariant: every planned merge was applied. An unapplied plan means a
     * base book was skipped after its companion's import was already suppressed —
     * the notes would silently vanish from the DB.
     */
    private fun assertAllHearotMergesApplied() {
        check(pendingHearotMergeBases.isEmpty()) {
            "companion-notes merge planned but never applied for: $pendingHearotMergeBases"
        }
    }

    /** Folds each merged base's companion+json hash into its source hash. */
    private fun mixHearotMergeIntoSourceHashes(
        hashes: Map<BookKey, BookSourceHash>,
    ): Map<BookKey, BookSourceHash> {
        if (hearotMergeExtraHash.isEmpty() || hashes.isEmpty()) return hashes
        return hashes.mapValues { (key, value) ->
            val extra = hearotMergeExtraHash[key.canonicalHeTitle] ?: return@mapValues value
            val md = java.security.MessageDigest.getInstance("SHA-256")
            md.update(value.hash)
            md.update(extra)
            BookSourceHash(md.digest(), value.lastSeenVersion)
        }
    }

    /**
     * Loads book metadata from the metadata.json file.
     * Attempts to parse the file in different formats (Map or List).
     *
     * @return A map of book titles to their metadata
     */
    private fun loadMetadata(): Map<String, BookMetadata> {
        val metadataFile = sourceDirectory.resolve("metadata.json")
        return if (metadataFile.exists()) {
            val content = metadataFile.readText()
            try {
                // Try to parse as Map first (original format)
                json.decodeFromString<Map<String, BookMetadata>>(content)
            } catch (e: Exception) {
                // If that fails, try to parse as List and convert to Map
                try {
                    val metadataList = json.decodeFromString<List<BookMetadata>>(content)
                    logger.i { "Parsed metadata as List with ${metadataList.size} entries" }
                    // Convert list to map using title as key
                    metadataList.associateBy { it.title }
                } catch (e: Exception) {
                    logger.i(e) { "Failed to parse metadata.json" }
                    emptyMap()
                }
            }
        } else {
            logger.w { "Metadata file metadata.json not found" }
            emptyMap()
        }
    }

    @Serializable
    private data class ManifestEntry(
        val hash: String? = null
    )

    /**
     * Loads `files_manifest.json` and builds a mapping from library-relative path
     * (under אוצריא) to a source name (top-level directory of the manifest entry).
     */
    private fun loadSourcesFromManifest() {
        manifestSourcesByRel.clear()
        // Prefer manifest in the provided source directory; fallback to repo path if present.
        //
        // Otzaria releases may ship both:
        // - files_manifest_new.json: includes the originating source in the prefix (e.g. wiki_jewish_books/…/אוצריא/…)
        // - files_manifest.json: legacy format without per-book source information (e.g. אוצריא/…)
        //
        // For source blacklisting to work, we must prefer the "new" manifest when available.
        val primaryNew = sourceDirectory.resolve("files_manifest_new.json")
        val primaryOld = sourceDirectory.resolve("files_manifest.json")
        val fallbackNew = Paths.get("otzaria-library/files_manifest_new.json")
        val fallbackOld = Paths.get("otzaria-library/files_manifest.json")
        val manifestPath = when {
            primaryNew.exists() -> primaryNew
            primaryOld.exists() -> primaryOld
            fallbackNew.exists() -> fallbackNew
            fallbackOld.exists() -> fallbackOld
            else -> null
        }
        if (manifestPath == null) {
            logger.w { "files_manifest*.json not found; assigning source 'Unknown' to all books" }
            return
        }
        logger.i { "Loading sources from manifest: ${manifestPath.toAbsolutePath()}" }
        runCatching {
            val content = manifestPath.readText()
            val map = json.decodeFromString<Map<String, ManifestEntry>>(content)
            // For every manifest key, if it contains "/אוצריא/", index the subpath after it
            for ((path, _) in map) {
                val parts = path.split('/')
                if (parts.isEmpty()) continue
                val idx = parts.indexOf("אוצריא")
                if (idx < 0 || idx == parts.size - 1) continue
                val rel = parts.drop(idx + 1).joinToString("/")
                val sourceName = if (idx > 0) parts.first() else "Unknown"
                // Keep first assignment, warn on duplicates
                val prev = manifestSourcesByRel.putIfAbsent(rel, sourceName)
                if (prev != null && prev != sourceName) {
                    logger.w { "Duplicate source mapping for '$rel': existing=$prev new=$sourceName; keeping existing" }
                }
            }
            val uniqueSources = manifestSourcesByRel.values.toSet()
            logger.i {
                "Loaded ${manifestSourcesByRel.size} book→source mappings from manifest " +
                    "(unique sources: ${uniqueSources.size})"
            }
        }.onFailure { e ->
            logger.w(e) { "Failed to parse files_manifest.json; sources will be 'Unknown'" }
        }
    }

    /**
     * Ensure all known source names from manifest are present in DB, including 'Unknown'.
     */
    private suspend fun precreateSourceEntries() {
        // Always ensure 'Unknown' exists
        val unknownId = bindings.upsertSource("Unknown")
        sourceNameToId["Unknown"] = unknownId
        // Insert all discovered sources
        val uniqueSources = manifestSourcesByRel.values.toSet()
        for (name in uniqueSources) {
            val id = bindings.upsertSource(name)
            sourceNameToId[name] = id
        }
        logger.i { "Prepared ${sourceNameToId.size} sources in DB" }
    }


    /**
     * Processes a directory recursively, creating categories and books.
     *
     * @param directory The directory to process
     * @param parentCategoryId The ID of the parent category, if any
     * @param level The current level in the directory hierarchy
     * @param metadata The metadata for books
     */
    private suspend fun processDirectory(
        directory: Path,
        parentCategoryId: Long?,
        level: Int,
        metadata: Map<String, BookMetadata>,
        parentPath: String = ""
    ) {
        logger.i { "=== Processing directory: ${directory.fileName} with parentCategoryId: $parentCategoryId (level: $level) ===" }

        Files.list(directory).use { stream ->
            val entries = stream.sorted { a, b ->
                a.fileName.toString().compareTo(b.fileName.toString())
            }.toList()

            logger.d { "Found ${entries.size} entries in directory ${directory.fileName}" }

            for (entry in entries) {
                when {
                    Files.isDirectory(entry) -> {
                        logger.d { "Processing subdirectory: ${entry.fileName} with parentId: $parentCategoryId" }
                        val placement = ensureCategoryHierarchy(entry.fileName.toString(), parentCategoryId, level, parentPath)
                        val normalizedPath = placement.normalizedPath.joinToString(" / ")
                        logger.i { "✅ Category '${entry.fileName}' normalized to '$normalizedPath' with ID: ${placement.id} (parent: $parentCategoryId)" }
                        processDirectory(entry, placement.id, placement.leafLevel + 1, metadata, placement.canonicalPath)
                    }

                    Files.isRegularFile(entry) && entry.extension == "txt" -> {
                        // Skip if already processed from the priority list
                        val key = toLibraryRelativeKey(entry)
                        if (processedPriorityBookKeys.contains(key)) {
                            logger.i { "⏭️ Skipping already-processed priority book: $key" }
                            continue
                        }
                        val fname = entry.fileName.toString()
                        // Skip files explicitly blacklisted by name
                        if (fileNameBlacklist.contains(fname)) {
                            logger.i { "⛔ Skipping blacklisted file '$fname' by name" }
                            continue
                        }
                        if (normalizeBookTitle(fname.substringBeforeLast('.')) in mergedHearotTitles) {
                            logger.i { "📝 Skipping '$fname' — merged into its base book as inline notes" }
                            continue
                        }
                        if (parentCategoryId == null) {
                            logger.w { "❌ Book found without category: $entry" }
                            continue
                        }
                        logger.i { "📚 Processing book ${entry.fileName} with categoryId: $parentCategoryId" }
                        createAndProcessBook(entry, parentCategoryId, metadata)
                    }

                    else -> {
                        logger.d { "Skipping entry: ${entry.fileName} (not a supported file type)" }
                    }
                }
            }
        }
        logger.i { "=== Finished processing directory: ${directory.fileName} ===" }
    }


    /**
     * Creates a book in the database and processes its content.
     *
     * @param path The path to the book file
     * @param categoryId The ID of the category the book belongs to
     * @param metadata The metadata for the book
     */
    private suspend fun createAndProcessBook(
        path: Path,
        categoryId: Long,
        metadata: Map<String, BookMetadata>,
        isBaseBook: Boolean = false
    ) {
        val filename = path.fileName.toString()
        val rawTitle = filename.substringBeforeLast('.')
        val title = normalizeBookTitle(rawTitle)
        val meta = metadata[rawTitle] ?: metadata[title] ?: metadata[stripQuotesForLookup(rawTitle)]

        logger.i { "Processing book: $title with categoryId: $categoryId" }

        // Apply source blacklist
        val srcName = getSourceNameFor(path)
        if (sourceBlacklist.contains(srcName)) {
            logger.i { "⛔ Skipping '$title' from blacklisted source '$srcName'" }
            processedBooksCount += 1
            val pct = if (totalBooksToProcess > 0) (processedBooksCount * 100 / totalBooksToProcess) else 0
            logger.i { "Books progress: $processedBooksCount/$totalBooksToProcess (${pct}%)" }
            return
        }

        // Skip if a book with the same heRef already exists from Sefaria (Sefaria has priority)
        val existingBook = repository.getBookByHeRef(title)
        if (existingBook != null) {
            val existingSource = repository.getSourceById(existingBook.sourceId)
            if (existingSource?.name == "Sefaria") {
                logger.i { "⏭️ Skipping '$title' - already exists from Sefaria (priority source)" }
                processedBooksCount += 1
                val pct = if (totalBooksToProcess > 0) (processedBooksCount * 100 / totalBooksToProcess) else 0
                logger.i { "Books progress: $processedBooksCount/$totalBooksToProcess (${pct}%)" }
                return
            }
        }

        // Assign a stable ID via IdAllocator so cross-build reproducibility holds.
        val currentBookId = allocator.bookId(srcName, title)
        logger.d { "Assigning ID $currentBookId to book '$title' (source=$srcName) with categoryId: $categoryId" }

        // Pre-resolve author / pubPlace / pubDate IDs through the IdAllocator
        // so they remain stable across builds (required for the delta producer's
        // secondary-UNIQUE collision pre-check).
        val authors = (meta?.resolveAuthorNames() ?: emptyList()).map { authorName ->
            Author(id = bindings.upsertAuthor(authorName), name = authorName)
        }
        val pubPlaces = meta?.pubPlace?.let { pubPlaceName ->
            listOf(PubPlace(id = bindings.upsertPubPlace(pubPlaceName), name = pubPlaceName))
        } ?: emptyList()
        val pubDates = meta?.pubDate?.let { pubDateValue ->
            listOf(PubDate(id = bindings.upsertPubDate(pubDateValue), date = pubDateValue))
        } ?: emptyList()

        val sourceId = resolveSourceIdFor(path)
        val book = Book(
            id = currentBookId,
            categoryId = categoryId,
            sourceId = sourceId,
            title = title,
            heRef = title,
            authors = authors,
            pubPlaces = pubPlaces,
            pubDates = pubDates,
            heShortDesc = meta?.heShortDesc,
            order = meta?.order ?: 999f,
            topics = extractTopics(path),
            isBaseBook = isBaseBook
        )

        logger.d { "Inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}" }
        val insertedBookId = repository.insertBook(book)

        // ✅ Important verification: ensure that ID and categoryId are correct
        val insertedBook = repository.getBook(insertedBookId)
        if (insertedBook?.categoryId != categoryId) {
            logger.w { "WARNING: Book inserted with wrong categoryId! Expected: $categoryId, Got: ${insertedBook?.categoryId}" }
            // Correct the categoryId if necessary
            repository.updateBookCategoryId(insertedBookId, categoryId)
        }

        logger.d { "Book '${book.title}' inserted with ID: $insertedBookId and categoryId: $categoryId" }

        // Insert acronyms for this book if an Acronymizer DB is available
        try {
            val terms = fetchAcronymsForTitle(title)
            if (terms.isNotEmpty()) {
                repository.bulkInsertBookAcronyms(insertedBookId, terms)
                logger.i { "Inserted ${terms.size} acronyms for '${title}'" }
            }
        } catch (e: Exception) {
            logger.w(e) { "Failed to insert acronyms for '$title'" }
        }

        // Process content of the book
        processBookContent(path, insertedBookId, title, categoryId)

        // Book-level progress
        processedBooksCount += 1
        val pct = if (totalBooksToProcess > 0) (processedBooksCount * 100 / totalBooksToProcess) else 0
        logger.i { "Books progress: $processedBooksCount/$totalBooksToProcess (${pct}%)" }
    }

    /**
     * Processes the content of a book, extracting lines and TOC entries.
     *
     * @param path The path to the book file
     * @param bookId The ID of the book in the database
     */
    private suspend fun processBookContent(path: Path, bookId: Long, bookTitle: String, categoryId: Long) = coroutineScope {
        logger.d { "Processing content for book ID: $bookId" }
        logger.i { "Processing content of book ID: $bookId (ID generated by the database)" }

        // Prefer preloaded content from RAM if available
        val key = toLibraryRelativeKey(path)
        val rawLines = bookContentCache[key] ?: run {
            val content = path.readText(Charsets.UTF_8)
            content.lines()
        }
        // Inject companion notes as inline footnotes when a merge plan exists
        val plan = hearotMergePlans[bookTitle]
        val lines = if (plan == null) rawLines else {
            val stats = HearotCompanionMerge.MergeStats()
            HearotCompanionMerge.mergeLines(bookTitle, rawLines, plan, stats).also {
                pendingHearotMergeBases.remove(bookTitle)
                logger.i { "📝 '$bookTitle': merged companion notes — ${stats.inPlace} anchored in place, ${stats.appended} appended" }
            }
        }
        logger.i { "Number of lines: ${lines.size}" }

        // Process each line one by one, handling TOC entries as we go
        processLinesWithTocEntries(bookId, bookTitle, categoryId, lines)

        // Update the total number of lines
        repository.updateBookTotalLines(bookId, lines.size)

        logger.i { "Content processed successfully for book ID: $bookId (ID generated by the database)" }
    }


    /**
     * Processes lines of a book, identifying and creating TOC entries.
     *
     * @param bookId The ID of the book in the database
     * @param lines The lines of the book content
     */
    private suspend fun processLinesWithTocEntries(bookId: Long, bookTitle: String, categoryId: Long, lines: List<String>) {
        logger.d { "Processing lines and TOC entries together for book ID: $bookId" }

        // Structure pour stocker toutes les entrées TOC créées
        data class TocEntryData(
            val id: Long,
            val parentId: Long?,
            val level: Int,
            val text: String,
            val lineIndex: Int
        )

        val allTocEntries = mutableListOf<TocEntryData>()
        val parentStack = mutableMapOf<Int, Long>()
        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
        var currentOwningTocEntryId: Long? = null
        val lineTocBuffer = ArrayList<Pair<Long, Long>>(minOf(lines.size, 200_000))
        // Batch line inserts + the FK back-link updates that depend on them.
        // JFR showed each `repository.insertLine` ≈ 1 withContext(Dispatchers.IO)
        // switch + 1 prepareStatement, run 4M times. Buffering collapses the
        // dispatcher churn and lets us flush via insertLinesBatch.
        val lineBuffer = ArrayList<Line>(LINE_FLUSH_THRESHOLD)
        val tocEntryLineIdUpdates = ArrayList<Pair<Long, Long>>()
        val lineTocEntryUpdates = ArrayList<Pair<Long, Long>>()

        suspend fun flushLineBatch() {
            if (lineBuffer.isEmpty()) return
            repository.insertLinesBatch(lineBuffer)
            lineBuffer.clear()
            // Flush back-link updates only after the lines exist in the DB.
            if (tocEntryLineIdUpdates.isNotEmpty()) {
                repository.bulkUpdateTocEntryLineIds(tocEntryLineIdUpdates)
                tocEntryLineIdUpdates.clear()
            }
            if (lineTocEntryUpdates.isNotEmpty()) {
                repository.bulkUpdateLineTocEntryIds(lineTocEntryUpdates)
                lineTocEntryUpdates.clear()
            }
        }

        // PREMIÈRE PASSE : Créer toutes les entrées et lignes
        for ((lineIndex, line) in lines.withIndex()) {
            val level = detectHeaderLevel(line)
            // Merged footnotes on a heading line must not leak into the TOC text
            val plainText = if (level > 0) cleanHtml(HearotCompanionMerge.stripFootnotes(line)) else ""
            val lineCharCount = countVisibleChars(line)

            if (level > 0) {
                if (plainText.isBlank()) {
                    logger.d { "⚠️ Skipping empty header at level $level (line $lineIndex)" }
                    parentStack.remove(level)
                    continue
                }

                val parentId = (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }
                val (currentTocEntryId, tocTextStableId) = stableTocEntryId(bookId, level, plainText, lineIndex)
                val currentLineId = stableLineId(bookId, line)

                // Stocker l'info de cette entrée pour la deuxième passe
                allTocEntries.add(TocEntryData(
                    id = currentTocEntryId,
                    parentId = parentId,
                    level = level,
                    text = plainText,
                    lineIndex = lineIndex
                ))

                // Créer l'entrée TOC avec hasChildren = false par défaut
                val tocEntry = TocEntry(
                    id = currentTocEntryId,
                    bookId = bookId,
                    parentId = parentId,
                    textId = tocTextStableId,
                    text = plainText,
                    level = level,
                    lineId = null,
                    isLastChild = false,
                    hasChildren = false  // Par défaut, sera mis à jour dans la deuxième passe
                )

                val tocEntryId = repository.insertTocEntry(tocEntry)
                parentStack[level] = tocEntryId
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocEntryId)
                currentOwningTocEntryId = tocEntryId

                // Buffer the line — id is allocator-stable so we know it before insert.
                lineBuffer.add(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        charCount = lineCharCount,
                    )
                )
                // Track this as a heading line for link filtering
                headingLineIds.add(currentLineId)
                // Back-links — applied after the next flush, when lines exist.
                tocEntryLineIdUpdates.add(tocEntryId to currentLineId)
                lineTocEntryUpdates.add(currentLineId to tocEntryId)
                lineTocBuffer.add(currentLineId to tocEntryId)
            } else {
                // Regular line
                val currentLineId = stableLineId(bookId, line)
                lineBuffer.add(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        heRef = buildOtzariaRef(bookTitle, lineIndex),
                        charCount = lineCharCount,
                    )
                )
                // Buffer mapping for regular line if there is a current owner
                currentOwningTocEntryId?.let { ownerId ->
                    lineTocBuffer.add(currentLineId to ownerId)
                }
            }

            if (lineBuffer.size >= LINE_FLUSH_THRESHOLD) {
                flushLineBatch()
            }
            if (lineTocBuffer.size >= 200_000) {
                repository.bulkUpsertLineToc(lineTocBuffer)
                lineTocBuffer.clear()
            }

            if (lineIndex % 1000 == 0) {
                val pct = if (lines.isNotEmpty()) (lineIndex * 100 / lines.size) else 0
                logger.i { "Book $bookId '$bookTitle': $lineIndex/${lines.size} lines (${pct}%)" }
            }
        }

        // Flush remaining line + back-link buffers
        flushLineBatch()
        // Flush buffered line→toc mappings in bulk
        repository.bulkUpsertLineToc(lineTocBuffer)

        // DEUXIÈME PASSE : Mettre à jour isLastChild et hasChildren
        logger.d { "Updating isLastChild and hasChildren for book ID: $bookId" }

        // Créer un set des IDs qui ont des enfants
        val parentIds = allTocEntries.mapNotNull { it.parentId }.toSet()

        // Collect hasChildren + isLastChild ids and flush them in a single batch
        // (replaces 2 × per-row update calls; was up to ~2k withContext switches
        // per book on large hierarchies).
        val hasChildrenIds = allTocEntries.asSequence().filter { it.id in parentIds }.map { it.id }.toList()
        val lastChildIds = entriesByParent.values.mapNotNull { it.lastOrNull() }
        repository.bulkUpdateTocEntryFlags(hasChildrenIds, lastChildIds)

        logger.i { "✅ Finished processing lines and TOC entries for book ID: $bookId" }
        logger.i { "   Total TOC entries: ${allTocEntries.size}" }
        logger.i { "   Entries with children: ${parentIds.size}" }
    }

    private fun cleanHtml(html: String): String {
        val cleaned = Jsoup.clean(html, Safelist.none())
            .trim()
            .replace("\\s+".toRegex(), " ")
        val withoutMaqaf = HebrewTextUtils.replaceMaqaf(cleaned, " ")
        return HebrewTextUtils.removeAllDiacritics(withoutMaqaf)
    }

    private fun buildOtzariaRef(bookTitle: String, lineIndex: Int): String {
        val safeTitle = bookTitle.trim()
        val oneBasedIndex = lineIndex + 1
        return "$safeTitle $oneBasedIndex"
    }


    private fun detectHeaderLevel(line: String): Int {
        return when {
            line.startsWith("<h1", ignoreCase = true) -> 1
            line.startsWith("<h2", ignoreCase = true) -> 2
            line.startsWith("<h3", ignoreCase = true) -> 3
            line.startsWith("<h4", ignoreCase = true) -> 4
            line.startsWith("<h5", ignoreCase = true) -> 5
            line.startsWith("<h6", ignoreCase = true) -> 6
            else -> 0
        }
    }

    // ===== Priority handling =====

    /**
     * Reads the priority list from resources and returns normalized relative paths under the library root.
     */
    private fun loadPriorityList(): List<String> {
        return try {
            val stream = this::class.java.classLoader.getResourceAsStream("priority.txt")
                ?: return emptyList()
            stream.bufferedReader(Charsets.UTF_8).use { reader ->
                reader.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .map { raw ->
                        // Normalize separators
                        var s = raw.replace('\\', '/')
                        // Remove BOM if present
                        if (s.isNotEmpty() && s[0].code == 0xFEFF) s = s.substring(1)
                        // Remove leading slash
                        s = s.removePrefix("/")
                        // Try to start from 'אוצריא' if present
                        val idx = s.indexOf("אוצריא")
                        if (idx >= 0) s = s.substring(idx + "אוצריא".length).removePrefix("/")
                        s
                    }
                    .filter { it.endsWith(".txt", ignoreCase = true) }
                    .toList()
            }
        } catch (e: Exception) {
            logger.w(e) { "Unable to read priority.txt from resources" }
            emptyList()
        }
    }

    /**
     * Processes books listed in priority.txt before the normal directory traversal.
     * Ensures categories exist and records processed books to avoid duplicates.
     */
    private suspend fun processPriorityBooks(loadMetadata: () -> Map<String, BookMetadata>) {
        val entries = loadPriorityList()
        if (entries.isEmpty()) {
            logger.i { "No priority entries found" }
            return
        }

        logger.i { "Processing ${entries.size} priority entries first" }
        val metadata = loadMetadata()

        outer@ for ((idx, relative) in entries.withIndex()) {
            // Build the absolute path under the library root
            val parts = relative.split('/').filter { it.isNotEmpty() }
            if (parts.isEmpty()) continue

            // Last part is the book filename, everything before are categories
            val categories = if (parts.size > 1) parts.dropLast(1) else emptyList()
            val bookFileName = parts.last()
            // Skip files explicitly blacklisted by name
            if (fileNameBlacklist.contains(bookFileName)) {
                logger.i { "⛔ Skipping blacklisted file in priority list: $bookFileName" }
                continue@outer
            }
            if (normalizeBookTitle(bookFileName.substringBeforeLast('.')) in mergedHearotTitles) {
                logger.i { "📝 Skipping priority entry '$bookFileName' — merged into its base book" }
                continue@outer
            }

            // Fold into actual filesystem path
            var currentPath = libraryRoot
            for (p in categories) currentPath = currentPath.resolve(p)
            val bookPath = currentPath.resolve(bookFileName)

            if (!Files.isRegularFile(bookPath)) {
                logger.w { "Priority entry ${idx + 1}/${entries.size}: file not found: $bookPath" }
                continue@outer
            }

            // Avoid processing duplicates listed multiple times
            val key = toLibraryRelativeKey(bookPath)
            if (processedPriorityBookKeys.contains(key)) {
                logger.d { "Priority entry ${idx + 1}/${entries.size}: already processed (dup in list): $key" }
                continue@outer
            }

            // Ensure categories exist and get the final parent category id
            var parentId: Long? = null
            var level = 0
            var parentPath = ""
            for (cat in categories) {
                val placement = ensureCategoryHierarchy(cat, parentId, level, parentPath)
                parentId = placement.id
                level = placement.leafLevel + 1
                parentPath = placement.canonicalPath
            }

            if (parentId == null) {
                logger.w { "Priority entry ${idx + 1}/${entries.size}: missing parent category for $bookPath; skipping" }
                continue@outer
            }

            logger.i { "⭐ Priority ${idx + 1}/${entries.size}: processing $bookFileName under categories ${categories.joinToString("/")}" }
            createAndProcessBook(bookPath, parentId, metadata, isBaseBook = true)

            // Mark as processed to avoid double insertion during full traversal
            processedPriorityBookKeys.add(key)
        }
    }

    /**
     * Compute a normalized key for a book file relative to the library root.
     */
    private fun toLibraryRelativeKey(file: Path): String {
        return try {
            val rel = libraryRoot.relativize(file).toString().replace('\\', '/')
            rel
        } catch (_: Exception) {
            // Fallback to filename
            file.fileName.toString()
        }
    }

    // Resolve a source id for a book file using the manifest mapping
    private suspend fun resolveSourceIdFor(file: Path): Long {
        val rel = toLibraryRelativeKey(file)
        val sourceName = manifestSourcesByRel[rel] ?: "Unknown"
        val cached = sourceNameToId[sourceName]
        if (cached != null) return cached
        val id = bindings.upsertSource(sourceName)
        sourceNameToId[sourceName] = id
        return id
    }

    private fun getSourceNameFor(file: Path): String {
        val rel = toLibraryRelativeKey(file)
        return manifestSourcesByRel[rel] ?: "Unknown"
    }

    /**
     * Source name for a **raw manifest key** (e.g. "DictaToOtzaria/…/אוצריא/…/X.txt").
     * Unlike [getSourceNameFor] (which relativizes an imported file Path against
     * [libraryRoot]), this reads the manifest key directly through the SAME
     * [manifestSourcesByRel] map the importer uses, so the BookKey the source-hash
     * detector emits matches exactly what the importer records. Used by
     * [OtzariaSourceHashComputer]; see its docs for why a Path resolver fails here.
     */
    private fun sourceNameForManifestKey(manifestKey: String): String {
        val parts = manifestKey.split('/')
        val idx = parts.indexOf("אוצריא")
        if (idx < 0 || idx == parts.size - 1) return "Unknown"
        val rel = parts.drop(idx + 1).joinToString("/")
        return manifestSourcesByRel[rel] ?: "Unknown"
    }

    private fun loadSourceBlacklistFromResources(): Set<String> {
        val fallback = setOf("wiki_jewish_books")
        return try {
            val resourceNames = listOf("source-blacklist.txt", "/source-blacklist.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name ->
                    cl?.getResourceAsStream(name) ?: DatabaseGenerator::class.java.getResourceAsStream(name)
                }
                .firstOrNull()
            if (stream == null) return fallback
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toSet()
            }.ifEmpty { fallback }
        } catch (e: Exception) {
            Logger.withTag("DatabaseGenerator").w(e) { "Failed to load source-blacklist.txt; using fallback" }
            fallback
        }
    }

    private fun loadFileNameBlacklistFromResources(): Set<String> {
        return try {
            val resourceNames = listOf("files-blacklist.txt", "/files-blacklist.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name ->
                    cl?.getResourceAsStream(name) ?: DatabaseGenerator::class.java.getResourceAsStream(name)
                }
                .firstOrNull()
                ?: return emptySet()
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toSet()
            }
        } catch (e: Exception) {
            Logger.withTag("DatabaseGenerator").w(e) { "Failed to load files-blacklist.txt; ignoring file-name blacklist" }
            emptySet()
        }
    }

    /**
     * Processes all link files in the links directory.
     * Links connect lines between different books.
     */
    private suspend fun processLinks() {
        // Load caches once so most lookups stay in memory
        ensureCachesLoaded()
        // If headingLineIds is empty (e.g., running generateLinksOnly separately),
        // load heading line IDs from the database
        if (headingLineIds.isEmpty()) {
            logger.i { "Loading heading line IDs from database..." }
            val headingIds = repository.getHeadingLineIds()
            headingLineIds.addAll(headingIds)
        }
        logger.i { "Heading lines tracked for filtering: ${headingLineIds.size}" }
        // Deterministic connection-type ids (enum order), matching the Sefaria importer.
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }
        val linksDir = sourceDirectory.resolve("links")
        if (!linksDir.exists()) {
            logger.w { "Links directory not found" }
            return
        }

        // Count links before processing
        val linksBefore = repository.countLinks()
        logger.d { "Links in database before processing: $linksBefore" }

        logger.i { "Loading all link JSON files into RAM..." }
        // Preload all links JSON into memory to minimize IO
        val linkFiles = Files.list(linksDir).use { s -> s.filter { it.extension == "json" }.toList() }
        val linksByBook = coroutineScope {
            linkFiles.map { file ->
                async {
                    val bookTitle = file.nameWithoutExtension.removeSuffix("_links")
                    val content = file.readText()
                    val links = parseLinksFromJson(content, bookTitle)
                    bookTitle to links
                }
            }.mapNotNull { deferred ->
                runCatching { deferred.await() }
                    .onFailure { e -> logger.w(e) { "Failed to preload links from file" } }
                    .getOrNull()
            }.toMap(mutableMapOf())
        }

        logger.i { "Processing links from RAM..." }
        var totalLinks = 0
        for ((bookTitle, links) in linksByBook) {
            val processedLinks = processLinksForBook(bookTitle, links)
            totalLinks += processedLinks
            logger.d { "Processed $processedLinks links for $bookTitle, total so far: $totalLinks" }
        }

        // Count links after processing
        val linksAfter = repository.countLinks()
        logger.d { "Links in database after processing: $linksAfter" }
        logger.d { "Added ${linksAfter - linksBefore} links to the database" }

        logger.i { "Total of $totalLinks links processed" }

        // Update the book_has_links table
        updateBookHasLinksTable()
    }

    /**
     * Processes links that were preloaded in memory for a given source book.
     */
    private suspend fun processLinksForBook(bookTitle: String, links: List<LinkData>): Int {
        val sourceBook = booksByTitle[bookTitle]
        if (sourceBook == null) {
            logger.w { "Source book not found for links: $bookTitle" }
            return 0
        }

        // Sefaria ships its own links between two Sefaria books — those are more
        // accurate, so Otzaria links are skipped per-link when BOTH sides are
        // Sefaria-sourced. A link touching a non-Sefaria side cannot exist in
        // Sefaria at all (e.g. משנה ברורה → שער הציון) and must be kept.
        val sourceIsSefaria = sourceBook.id in sefariaBookIds

        var processed = 0
        val anchorBatch = mutableListOf<LinkAnchor>()
        val rangeBatch = mutableListOf<LinkRange>()
        val coverageBatch = mutableListOf<LinkCoverage>()
        val touchedLinkIds = mutableSetOf<Long>()
        for ((index, linkData) in links.withIndex()) {
            try {
                val path = linkData.path_2
                val targetTitle = if (path.contains('\\')) {
                    val lastComponent = path.split('\\').last()
                    lastComponent.substringBeforeLast('.', lastComponent)
                } else {
                    val targetPath = Paths.get(path)
                    targetPath.fileName.toString().substringBeforeLast('.')
                }

                val targetBook = booksByTitle[targetTitle]
                if (targetBook == null) {
                    // Companion notes merged into their base have no book — expected
                    if (HearotCompanionMerge.isMergeableCompanionTitle(normalizeBookTitle(targetTitle))) {
                        logger.d { "Skipping link into merged companion notes: $targetTitle" }
                    } else {
                        logger.i { "Link ${index + 1}/${links.size} - Target book not found: $targetTitle" }
                        logger.i { "Original path: ${linkData.path_2}" }
                    }
                    continue
                }

                if (sourceIsSefaria && targetBook.id in sefariaBookIds) continue

                val sourceLineIndex = (linkData.line_index_1.toInt() - 1).coerceAtLeast(0)
                val targetLineIndex = (linkData.line_index_2.toInt() - 1).coerceAtLeast(0)

                val sourceLineId = getLineIdCached(sourceBook.id, sourceLineIndex)
                if (sourceLineId == null) continue

                val targetLineId = getLineIdCached(targetBook.id, targetLineIndex)
                if (targetLineId == null) continue

                // Skip links where source or target is a heading line
                if (sourceLineId in headingLineIds || targetLineId in headingLineIds) continue

                // SOURCE is declared from the dependant's file; like the Sefaria importer
                // we flip it to canonical base→dependant and store it as COMMENTARY.
                val declaredType = ConnectionType.fromString(linkData.connectionType)
                // "linker"-typed rows are an old Dicta linker experiment (removed from
                // otzaria-library, may linger in old zips) — never imported. The LINKER
                // layer comes solely from LinkerToOtzaria's Phase-2.
                if (declaredType == ConnectionType.LINKER) continue
                val flip = declaredType == ConnectionType.SOURCE
                val storedType = if (flip) ConnectionType.COMMENTARY else declaredType
                // Stable link id keyed by (sourceLineId, targetLineId, connectionTypeId), like Sefaria.
                val typeId = bindings.upsertConnectionType(storedType.name)
                val linkId = bindings.insertLinkStable(
                    sourceBookId = if (flip) targetBook.id else sourceBook.id,
                    targetBookId = if (flip) sourceBook.id else targetBook.id,
                    sourceLineId = if (flip) targetLineId else sourceLineId,
                    targetLineId = if (flip) sourceLineId else targetLineId,
                    targetLineIndex = (if (flip) sourceLineIndex else targetLineIndex).toLong(),
                    connectionTypeId = typeId,
                )
                touchedLinkIds += linkId
                // Anchor is source-side; skip when flipped (that line is now the stored target).
                if (!flip) {
                    buildLinkAnchor(linkData, linkId, sourceLineId, bookTitle)?.let { anchorBatch += it }
                }
                // Range side 0 = stored source, 1 = stored target; the flip swaps each file's end.
                if (flip) {
                    queueRangeSide(linkData.line_index_2_end, targetBook.id, targetLineIndex, linkId, 0, bookTitle, rangeBatch, coverageBatch)
                    queueRangeSide(linkData.line_index_1_end, sourceBook.id, sourceLineIndex, linkId, 1, bookTitle, rangeBatch, coverageBatch)
                } else {
                    queueRangeSide(linkData.line_index_1_end, sourceBook.id, sourceLineIndex, linkId, 0, bookTitle, rangeBatch, coverageBatch)
                    queueRangeSide(linkData.line_index_2_end, targetBook.id, targetLineIndex, linkId, 1, bookTitle, rangeBatch, coverageBatch)
                }
                processed++
            } catch (_: Exception) {
                // Skip malformed entries but continue
            }
        }
        if (anchorBatch.isNotEmpty()) {
            repository.insertLinkAnchorsBatch(anchorBatch)
            logger.i { "Inserted ${anchorBatch.size} word-level anchors for $bookTitle" }
        }
        // Authoritative range sync: drop stale ranges of every link re-imported in
        // this run, so a shortened or removed line_index_X_end takes effect on rerun.
        touchedLinkIds.chunked(500).forEach { chunk ->
            val ids = chunk.joinToString(",")
            repository.executeRawQuery("DELETE FROM link_range WHERE linkId IN ($ids)")
            repository.executeRawQuery("DELETE FROM link_coverage WHERE linkId IN ($ids)")
        }
        if (rangeBatch.isNotEmpty()) {
            repository.insertLinkRangesBatch(rangeBatch)
            repository.insertLinkCoverageBatch(coverageBatch)
            logger.i { "Inserted ${rangeBatch.size} link ranges (${coverageBatch.size} coverage rows) for $bookTitle" }
        }
        return processed
    }

    /**
     * Queues link_range + link_coverage rows for one ranged side, Sefaria-style:
     * the link stays anchored at the start line, `link_range` records the last
     * line, and coverage rows mark every covered line after the first (headings
     * excluded). Reversed/missing/heading ends are logged and dropped.
     */
    private suspend fun queueRangeSide(
        rawEnd: Double?,
        bookId: Long,
        startLineIndex: Int,
        linkId: Long,
        side: Int,
        bookTitle: String,
        ranges: MutableList<LinkRange>,
        coverage: MutableList<LinkCoverage>,
    ) {
        val endLineIndex = (rawEnd ?: return).toInt() - 1
        // Degenerate range (end == start): a plain single-line link, nothing to record.
        if (endLineIndex == startLineIndex) return
        val endLineId = if (endLineIndex > startLineIndex) getLineIdCached(bookId, endLineIndex) else null
        if (endLineId == null || endLineId in headingLineIds) {
            logger.w { "Range end ${endLineIndex + 1} reversed/missing/heading for $bookTitle (side $side) — range dropped" }
            return
        }
        ranges += LinkRange(linkId = linkId, side = side, endLineId = endLineId, endLineIndex = endLineIndex)
        for (idx in (startLineIndex + 1)..endLineIndex) {
            val coveredId = getLineIdCached(bookId, idx) ?: continue
            if (coveredId in headingLineIds) continue
            coverage += LinkCoverage(lineId = coveredId, linkId = linkId, side = side)
        }
    }

    /**
     * Word-level anchor of a link, when the JSON entry carries one. The JSON
     * `start`/`end` are raw char offsets into the source line's stored content;
     * they are converted here to the visible-char coordinate space the
     * `link_anchor` table mandates (see Database.sq).
     */
    private suspend fun buildLinkAnchor(
        linkData: LinkData,
        linkId: Long,
        sourceLineId: Long,
        bookTitle: String,
    ): LinkAnchor? {
        val rawStart = linkData.start?.toInt() ?: return null
        val content = repository.getLine(sourceLineId)?.content
        if (content == null || rawStart < 0 || rawStart > content.length) {
            logger.w {
                "Anchor offset $rawStart out of bounds (len=${content?.length}) " +
                    "for $bookTitle line ${linkData.line_index_1.toInt()} — anchor dropped"
            }
            return null
        }
        val rawEnd = linkData.end?.toInt()?.takeIf { it in rawStart..content.length }
        return LinkAnchor(
            linkId = linkId,
            side = 0,
            charStart = countVisibleChars(content, rawStart),
            charEnd = rawEnd?.let { countVisibleChars(content, it) },
            label = anchorLabelFromHeRef(linkData.heRef_2),
        )
    }

    /**
     * אות הסימון של עוגן-נקודה, מתוך ה-heRef העשיר של קובץ הקישורים:
     * "שער הציון, סימן א, סעיף א אות ג" → "ג". מחזיר null כשאין רכיב
     * " אות X" תקין (למשל בקישורי טווח של קבצי linker).
     */
    private fun anchorLabelFromHeRef(heRef: String): String? {
        val idx = heRef.lastIndexOf(" אות ")
        if (idx < 0) return null
        val tail = heRef.substring(idx + " אות ".length).trim()
        if (tail.isEmpty() || tail.length > 6) return null
        val valid = tail.all { it in 'א'..'ת' || it == '"' || it == '׳' || it == '״' }
        return if (valid) tail else null
    }

    /**
     * Imports alternate-TOC structures (e.g. parasha/aliyah trees) from
     * `alt_toc/<book>_alt_toc.json` files — the Otzaria-side equivalent of
     * Sefaria's `alts` schemas. Each file holds a list of structures; every
     * node anchors to a 1-based line and children nest recursively.
     */
    private suspend fun processAltTocStructures() {
        ensureCachesLoaded()
        val altTocDir = sourceDirectory.resolve("alt_toc")
        val files = if (altTocDir.exists()) {
            Files.list(altTocDir).use { s -> s.filter { it.extension == "json" }.toList() }
                .sortedBy { it.fileName.toString() }
        } else {
            emptyList()
        }

        // Global sync: an Otzaria book whose alt_toc file disappeared loses its
        // structures. Only non-Sefaria books can carry Otzaria-authored structures,
        // so Sefaria books (whose structures come from their schemas) stay untouched.
        val titlesWithFile = files.map { it.nameWithoutExtension.removeSuffix("_alt_toc") }.toSet()
        for (book in booksById.values) {
            if (book.id in sefariaBookIds || !book.hasAltStructures || book.title in titlesWithFile) continue
            for (structure in repository.getAltTocStructuresForBook(book.id)) {
                deleteAltTocStructure(structure.id)
            }
            repository.updateHasAltStructures(book.id, false)
            logger.i { "Removed alt-toc structures of ${book.title} (no alt_toc file)" }
        }

        var builtBooks = 0
        for (file in files) {
            val bookTitle = file.nameWithoutExtension.removeSuffix("_alt_toc")
            val book = booksByTitle[bookTitle]
            if (book == null) {
                logger.w { "Alt-toc file for unknown book: $bookTitle — skipped" }
                continue
            }
            // Sefaria owns the alt structures of its own books (built from its schemas);
            // an Otzaria file must not overwrite or delete them.
            if (book.id in sefariaBookIds) {
                logger.w { "Alt-toc file targets Sefaria-sourced book $bookTitle — ignored" }
                continue
            }
            val parsed = runCatching { json.decodeFromString<List<AltTocStructureData>>(file.readText()) }
            val structures = parsed.getOrNull()
            if (structures == null) {
                logger.w(parsed.exceptionOrNull()) { "Failed to parse alt-toc JSON for $bookTitle — skipped" }
                continue
            }
            // The file is authoritative for its book: structures whose key is no
            // longer declared are removed.
            val declaredKeys = structures.map { it.key }.toSet()
            for (existing in repository.getAltTocStructuresForBook(book.id)) {
                if (existing.key !in declaredKeys) {
                    deleteAltTocStructure(existing.id)
                    logger.i { "Removed alt-toc structure '${existing.key}' of $bookTitle (no longer declared)" }
                }
            }
            var bookHasStructures = false
            for (structure in structures) {
                if (buildAltTocStructure(book, structure)) bookHasStructures = true
            }
            repository.updateHasAltStructures(book.id, bookHasStructures)
            if (bookHasStructures) builtBooks++
        }
        if (builtBooks > 0) logger.i { "Built alt-toc structures for $builtBooks books" }
    }

    /** Removes a structure with its entries and line mappings (FKs are off in phase 2). */
    private suspend fun deleteAltTocStructure(structureId: Long) {
        repository.executeRawQuery("DELETE FROM line_alt_toc WHERE structureId=$structureId")
        repository.executeRawQuery("DELETE FROM alt_toc_entry WHERE structureId=$structureId")
        repository.executeRawQuery("DELETE FROM alt_toc_structure WHERE id=$structureId")
    }

    /** Builds one alt-toc structure; returns true when at least one entry was inserted. */
    private suspend fun buildAltTocStructure(book: Book, data: AltTocStructureData): Boolean {
        if (data.key.isBlank()) {
            logger.w { "Alt-toc structure with blank key for ${book.title} — skipped" }
            return false
        }
        // Wholesale rebuild — including the structure row itself, since the repo
        // upsert dedups on (bookId, key) without refreshing title/heTitle. The id
        // is allocator-stable per (bookId, key), so the fresh row keeps its id.
        // (Entry ids stay auto-allocated, matching the Sefaria builder's
        // Phase 1.5 deferral.)
        val structureId = allocator.altTocStructureId(book.id, data.key)
        deleteAltTocStructure(structureId)
        if (data.nodes.isEmpty()) {
            logger.w { "Alt-toc structure '${data.key}' of ${book.title} has no nodes — removed" }
            return false
        }
        bindings.upsertAltTocStructureStable(
            AltTocStructure(bookId = book.id, key = data.key, title = data.title, heTitle = data.heTitle)
        )

        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
        val usedLinesByParent = mutableMapOf<Long?, MutableSet<Long>>()
        // lineIndex -> entryId; children insert after their parent, so the deepest
        // entry starting on a shared line wins (same rule as the Sefaria builder).
        val anchorByLineIndex = mutableMapOf<Int, Long>()

        suspend fun addNode(node: AltTocNodeData, level: Int, parentId: Long?): Boolean {
            val lineIndex = node.line.toInt() - 1
            val lineId = getLineIdCached(book.id, lineIndex)
            if (lineId == null) {
                logger.w { "Alt-toc node '${node.heTitle}' line ${node.line.toInt()} out of range in ${book.title} — node dropped" }
                return false
            }
            if (!usedLinesByParent.getOrPut(parentId) { mutableSetOf() }.add(lineId)) {
                logger.w { "Alt-toc node '${node.heTitle}' reuses line ${node.line.toInt()} under the same parent in ${book.title} — node dropped" }
                return false
            }
            val textId = bindings.upsertTocText(node.heTitle)
            val entryId = repository.insertAltTocEntry(
                AltTocEntry(
                    structureId = structureId, parentId = parentId, textId = textId,
                    text = node.heTitle, level = level, lineId = lineId,
                )
            )
            entriesByParent.getOrPut(parentId) { mutableListOf() } += entryId
            anchorByLineIndex[lineIndex] = entryId
            var hasChild = false
            for (child in node.children) if (addNode(child, level + 1, entryId)) hasChild = true
            if (hasChild) repository.updateAltTocEntryHasChildren(entryId, true)
            return true
        }

        var any = false
        for (node in data.nodes) if (addNode(node, 0, null)) any = true
        if (!any) {
            deleteAltTocStructure(structureId)
            logger.w { "All nodes of alt-toc structure '${data.key}' in ${book.title} were dropped — structure removed" }
            return false
        }

        for ((_, children) in entriesByParent) {
            repository.updateAltTocEntryIsLastChild(children.last(), true)
        }
        // Map every line to the nearest preceding anchor.
        val anchors = anchorByLineIndex.entries.sortedBy { it.key }
        var ai = 0
        var current: Long? = null
        for (lineIdx in 0 until book.totalLines) {
            while (ai < anchors.size && anchors[ai].key <= lineIdx) { current = anchors[ai].value; ai++ }
            val entryId = current ?: continue
            val lineId = getLineIdCached(book.id, lineIdx) ?: continue
            repository.upsertLineAltToc(lineId, structureId, entryId)
        }
        return true
    }

    /**
     * Extracts topics from the file path.
     * Topics are derived from the directory structure.
     *
     * @param path The path to the book file
     * @return A list of topics extracted from the path
     */
    private suspend fun extractTopics(path: Path): List<Topic> {
        // Extract topics from the path
        val parts = path.toString().split(File.separator)
        val topicNames = parts.dropLast(1).takeLast(2)

        // Pre-resolve through IdAllocator so topic IDs stay stable across builds.
        return topicNames.map { name ->
            Topic(id = bindings.upsertTopic(name), name = name)
        }
    }

    /**
     * Fetch and sanitize acronym terms for a given book title from the Acronymizer DB.
     * Uses the new relational structure (Books, Acronyms, BookAcronyms).
     */
    private fun fetchAcronymsForTitle(title: String): List<String> {
        val path = acronymDbPath ?: return emptyList()
        try {
            if (acronymDb == null) {
                acronymDb = java.sql.DriverManager.getConnection("jdbc:sqlite:$path")
            }
            val conn = acronymDb ?: return emptyList()

            val lookupTitles = buildList {
                add(title)
                val stripped = stripQuotesForLookup(title)
                if (stripped.isNotBlank()) add(stripped)
                val noComma = title.replace(",", " ").trim()
                if (noComma.isNotBlank()) add(noComma)
                val noPunct = title.replace("[\\p{Punct}]".toRegex(), " ").replace("\\s+".toRegex(), " ").trim()
                if (noPunct.isNotBlank()) add(noPunct)
                val sanitized = sanitizeAcronymTerm(title)
                if (sanitized.isNotBlank()) add(sanitized)
            }.distinct()

            val acronyms = mutableListOf<String>()

            // Query using the new relational structure
            for (candidate in lookupTitles) {
                conn.prepareStatement(
                    """
                    SELECT a.acronym
                    FROM Books b
                    JOIN BookAcronyms ba ON b.id = ba.book_id
                    JOIN Acronyms a ON ba.acronym_id = a.id
                    WHERE b.title = ?
                    ORDER BY a.acronym
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, candidate)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            val acronym = rs.getString(1)
                            if (!acronym.isNullOrBlank()) {
                                acronyms.add(acronym)
                            }
                        }
                    }
                }
                if (acronyms.isNotEmpty()) break
            }

            if (acronyms.isEmpty()) return emptyList()

            // Sanitize and de-duplicate
            val clean = acronyms
                .map { sanitizeAcronymTerm(it) }
                .map { it.trim() }
                .filter { it.isNotEmpty() }

            // De-duplicate and drop items identical to the title after normalization
            val titleNormalized = sanitizeAcronymTerm(title)
            return clean
                .filter { !it.equals(title, ignoreCase = true) }
                .filter { !it.equals(titleNormalized, ignoreCase = true) }
                .distinct()
        } catch (e: Exception) {
            logger.w(e) { "Error reading acronyms for '$title' from $path" }
            return emptyList()
        }
    }

    private suspend fun backfillAcronymsForExistingBooks() {
        val path = acronymDbPath ?: return
        logger.i { "Backfilling missing acronyms from $path for existing books..." }
        val books = runCatching { repository.getAllBooks() }.getOrDefault(emptyList())
        var insertedCount = 0
        var touchedBooks = 0
        for (book in books) {
            val existing = runCatching { repository.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
            if (existing.isNotEmpty()) continue
            val terms = fetchAcronymsForTitle(book.title)
            if (terms.isNotEmpty()) {
                repository.bulkInsertBookAcronyms(book.id, terms)
                insertedCount += terms.size
                touchedBooks += 1
            }
        }
        logger.i { "Backfill complete: added $insertedCount acronyms to $touchedBooks books" }
    }

    // Clean an acronym using HebrewTextUtils and remove gershayim
    private fun sanitizeAcronymTerm(raw: String): String {
        var s = raw.trim()
        if (s.isEmpty()) return ""
        s = HebrewTextUtils.removeAllDiacritics(s)
        s = HebrewTextUtils.replaceMaqaf(s, " ")
        s = s.replace("\u05F4", "") // remove Hebrew gershayim (״)
        s = s.replace("\u05F3", "") // remove Hebrew geresh (׳)
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    /**
     * Disables foreign key constraints in the database to improve performance during bulk insertion.
     * This should be called before starting the data generation process.
     */
    private suspend fun disableForeignKeys() {
        logger.d { "Disabling foreign key constraints" }
        repository.executeRawQuery("PRAGMA foreign_keys = OFF")
    }

    /**
     * Re-enables foreign key constraints in the database after data insertion is complete.
     * This should be called after all data has been inserted to ensure data integrity.
     */
    private suspend fun enableForeignKeys() {
        logger.d { "Re-enabling foreign key constraints" }
        repository.executeRawQuery("PRAGMA foreign_keys = ON")
    }

    // Removed: FTS rebuild is obsolete (Lucene index is committed in this run)

    /**
     * Updates the book_has_links table to indicate which books have source links, target links, or both.
     * This should be called after all links have been processed.
     */
    private suspend fun updateBookHasLinksTable() {
        logger.i { "Updating book_has_links table with separate source and target link flags..." }

        // Ensure all books are present in book_has_links
        runCatching {
            repository.executeRawQuery(
                "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
                        "SELECT id, 0, 0 FROM book"
            )
        }

        // Reset flags, then set from aggregated distinct sets
        runCatching { repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0") }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasSourceLinks=1 " +
                        "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
            )
        }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasTargetLinks=1 " +
                        "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
            )
        }

        // Reset book per-connection-type flags
        runCatching {
            repository.executeRawQuery(
                "UPDATE book SET hasTargumConnection=0, hasReferenceConnection=0, hasSourceConnection=0, " +
                    "hasCommentaryConnection=0, hasOtherConnection=0"
            )
        }

        // Helper to set a type flag in one statement
        suspend fun setConnFlag(
            typeName: String,
            column: String,
            includeTargets: Boolean = true,
            excludeSelfLinks: Boolean = false
        ) {
            val selfFilter = if (excludeSelfLinks) " AND l.sourceBookId != l.targetBookId" else ""
            val sourceSelect =
                "SELECT sourceBookId AS bId FROM link l " +
                    "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE ct.name='$typeName'$selfFilter"
            val targetSelect = if (includeTargets) {
                " UNION SELECT targetBookId AS bId FROM link l " +
                    "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE ct.name='$typeName'$selfFilter"
            } else {
                ""
            }
            val sql = "UPDATE book SET $column=1 WHERE id IN (" +
                    "SELECT DISTINCT bId FROM (" +
                    sourceSelect +
                    targetSelect +
                    ")" +
                    ")"
            repository.executeRawQuery(sql)
        }

        runCatching { setConnFlag("TARGUM", "hasTargumConnection") }
        runCatching { setConnFlag("REFERENCE", "hasReferenceConnection") }
        runCatching { setConnFlag("COMMENTARY", "hasCommentaryConnection") }
        runCatching { setConnFlag("OTHER", "hasOtherConnection") }

        // hasSourceConnection: virtual flag — set when this book is the *target*
        // of an *oriented* dependant link. Lateral types (QUOTATION,
        // MISHNAH_IN_TALMUD, MESORAT_HASHAS, RELATED) are NOT sources.
        // EIN_MISHPAT is included — it is the halakhic-index pointer from a
        // Talmud sugya to the matching halakhah in Mishneh Torah / Tur / SA
        // (the code derives FROM the Talmud). Keep in sync with the mirror
        // SOURCE queries in LinkQueries.sq.
        runCatching {
            val dependantTypes = listOf(
                "COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "MIDRASH",
                "PARSHANUT", "DIBUR_HAMATCHIL", "EIN_MISHPAT", "ELUCIDATION",
            ).joinToString(",") { "'$it'" }
            repository.executeRawQuery(
                "UPDATE book SET hasSourceConnection=1 WHERE id IN (" +
                    "SELECT DISTINCT l.targetBookId FROM link l " +
                    "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE ct.name IN ($dependantTypes) AND l.sourceBookId != l.targetBookId" +
                    ")"
            )
        }

        // Quick summary counts (single queries)
        val totalBooks = repository.getAllBooks().size
        val booksWithSourceLinks = repository.countBooksWithSourceLinks().toInt()
        val booksWithTargetLinks = repository.countBooksWithTargetLinks().toInt()
        val booksWithAnyLinks = repository.countBooksWithAnyLinks().toInt()

        logger.i { "Book_has_links table updated. Found:" }
        logger.i { "- $booksWithSourceLinks books with source links" }
        logger.i { "- $booksWithTargetLinks books with target links" }
        logger.i { "- $booksWithAnyLinks books with any links (source or target)" }
        logger.i { "- $totalBooks total books" }
    }



    /**
     * Parses links from JSON content, handling both Ben-YehudaToOtzaria and DictaToOtzaria formats
     */
    private fun parseLinksFromJson(content: String, bookTitle: String): List<LinkData> {
        return try {
            // First, try to parse as Ben-YehudaToOtzaria format (List<LinkData>)
            json.decodeFromString<List<LinkData>>(content)
        } catch (e: Exception) {
            // If that fails, try to parse as DictaToOtzaria format (Map<String, List<DictaLinkData>>)
            try {
                val dictaLinksMap = json.decodeFromString<Map<String, List<DictaLinkData>>>(content)
                logger.d { "Successfully parsed DictaToOtzaria format for $bookTitle with ${dictaLinksMap.size} line groups" }
                
                // Convert DictaToOtzaria format to LinkData format
                // Note: DictaToOtzaria format doesn't map perfectly to LinkData structure
                // We'll need to handle this conversion carefully or skip for now
                logger.w { "DictaToOtzaria format conversion not yet implemented for $bookTitle" }
                emptyList<LinkData>()
            } catch (e2: Exception) {
                logger.w(e2) { "Failed to parse links from file for $bookTitle in any known format" }
                emptyList<LinkData>()
            }
        }
    }

    // Internal classes

    /**
     * Data class representing a link between two books.
     * Used for deserializing link data from JSON files.
     */
    @Serializable
    private data class LinkData(
        val heRef_2: String,
        val line_index_1: Double,
        val path_2: String,
        val line_index_2: Double,
        @SerialName("Conection Type")
        val connectionType: String = "",
        // Optional word-level anchor: raw char range [start, end) inside the
        // source line's stored content (line_index_1 side). Present e.g. in the
        // "אור הישר" family and in the converted משנה ברורה → שער הציון links.
        val start: Double? = null,
        val end: Double? = null,
        // Optional Sefaria-style ranged link: 1-based inclusive end line per
        // side; the link row itself stays anchored at the start line.
        val line_index_1_end: Double? = null,
        val line_index_2_end: Double? = null
    )

    /**
     * Data class for DictaToOtzaria link format
     */
    @Serializable
    private data class DictaLinkData(
        val start: Int,
        val end: Int,
        val refs: Map<String, String>
    )

    /** One alt-toc node: a heading anchored to a 1-based line, with nested children. */
    @Serializable
    private data class AltTocNodeData(
        val heTitle: String,
        val line: Double,
        val children: List<AltTocNodeData> = emptyList(),
    )

    /** One alternate-TOC structure from an `alt_toc/<book>_alt_toc.json` file. */
    @Serializable
    private data class AltTocStructureData(
        val key: String,
        val title: String? = null,
        val heTitle: String? = null,
        val nodes: List<AltTocNodeData> = emptyList(),
    )

}
