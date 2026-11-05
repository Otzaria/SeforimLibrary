package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.changes.BookIdContinuity
import io.github.kdroidfilter.seforimlibrary.common.changes.OtzariaSourceHashComputer
import io.github.kdroidfilter.seforimlibrary.common.changes.TouchedBookDetector
import io.github.kdroidfilter.seforimlibrary.common.changes.previousOtzariaImportKeys
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.altTocChildPath
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import io.github.kdroidfilter.seforimlibrary.core.text.normalizeAuthorName
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.DecodeSequenceMode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeToSequence
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.LinkedHashMap
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
private const val LINE_ID_PAGE_SIZE = 4096
private const val SEFARIA_SOURCE = "Sefaria"

/**
 * otzaria-library ships `<masechet>_headings.json` inside the very same `links/`
 * directory as the real `<book>_links.json` files, but those are heading → line
 * number maps (`{"בבא בתרא": 1, "דף ב.": 2, …}`), not link files. Nothing in this
 * repository reads them: the link parser failed on all 37 of them twice per
 * build (once from [DatabaseGenerator.buildHearotMergePlans] in phase 1, once
 * from [DatabaseGenerator.processLinks] in phase 2), each failure dragging a
 * JSON dump into the log, and each file then produced a "Source book not found
 * for links" warning — 481 lines that buried any genuine parse failure.
 *
 * They are excluded at discovery instead. This is a strict no-op on database
 * content: a `_headings` file parsed to an empty link list, and an empty list
 * inserts nothing and allocates no id.
 */
private const val HEADINGS_FILE_SUFFIX = "_headings"

/** True for the `links/` entries that really are link files. See [HEADINGS_FILE_SUFFIX]. */
private fun isLinkJsonFile(file: Path): Boolean =
    file.extension == "json" && !file.nameWithoutExtension.endsWith(HEADINGS_FILE_SUFFIX)

/** Longest bounded name list any single summary line prints. */
private const val MAX_NAMES_PER_SUMMARY_LINE = 20

/** Collapses newlines/tabs so a quoted excerpt can never break a log line in two. */
private fun String.oneLine(): String = replace(Regex("\\s+"), " ").trim()

/** Persisted dependant types exposed by the repository's virtual SOURCE view. */
private val ORIENTED_DEPENDANT_TYPES = ConnectionType.SOURCE_VIEW_TYPES

private val DEPENDENCY_TITLE_PREFIXES = listOf(
    "תלמוד בבלי ",
    "תלמוד ירושלמי ",
    "מסכת ",
    "משנה ",
    "ספר ",
)

private data class LegacyAuthoredLinkKey(
    val sourceBookId: Long,
    val targetBookId: Long,
    val sourceLineId: Long,
    val targetLineId: Long,
    val connectionType: ConnectionType,
)

/** A batched SQL write must never be swallowed by malformed-link tolerance. */
private class LinkBatchWriteFailure(cause: Exception) : RuntimeException("Failed writing link batch", cause)

class DatabaseGenerator(
    private val sourceDirectory: Path,
    private val repository: SeforimRepository,
    private val acronymDbPath: String? = null,
    private val filterSourcesForLinks: Boolean = true,
    private val allocator: IdAllocator = InMemoryIdAllocator.load(path = null),
    private val buildVersion: Int = 0,
    // Accept previous books changing id (logged instead of failing the build).
    private val allowBookIdLoss: Boolean = false,
    // Retained inputs and line-batch budget; use the environment/system property
    // documented in BoundedGeneratorInput.kt to tune forked generator runs.
    private val inputBufferBytes: Long = configuredGeneratorInputBytes(),
    private val inputSpillDirectory: Path? = null,
) {

    init { require(inputBufferBytes >= 1024) { "inputBufferBytes must be at least 1024" } }

    private val inputPartBytes = inputBufferBytes / 3

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
    /**
     * [legacyContent]: what builds before a normalisation keyed this line on (only
     * line 0 of a book whose BOM is now stripped). The allocator reuses the seed id
     * filed under it when [content]'s own key is new, so the line keeps its id.
     * Line 0 is a book's first line, so its occurrence index is 0 in either scheme.
     */
    private fun stableLineId(bookId: Long, content: String, legacyContent: String? = null): Long {
        val hash = IdAllocatorBindings.normalisedContentHash(content)
        val occurrence = nextLineOccurrence(bookId, hash)
        val legacy = legacyContent?.let {
            io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey(
                IdAllocatorBindings.normalisedContentHash(it),
                0,
            )
        }
        return allocator.lineId(bookId, hash, occurrence, legacy)
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
    private var acronymLookup: AcronymizerLookup? = null

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
    // Fixed-size, byte-capped LRU pages; a single link must not load an entire
    // book's text, or leave every referenced book's IDs resident for the phase.
    private data class LineIdPageKey(val bookId: Long, val firstIndex: Int)
    private val lineIdCache = LinkedHashMap<LineIdPageKey, LongArray>(16, 0.75f, true)
    private var lineIdCacheBytes = 0L
    private val maxLineIdCacheBytes = minOf(4L * 1024 * 1024, maxOf(32_768L, inputPartBytes))
    // Track line IDs that are headings (contain <h1>, <h2>, etc.)
    private val headingLineIds = mutableSetOf<Long>()
    // Track book IDs that come from Sefaria sources (skip Otzaria links for these)
    private val sefariaBookIds = mutableSetOf<Long>()

    // Tracks books processed from the priority list to avoid double insertion
    private val processedPriorityBookKeys = mutableSetOf<String>()

    // Overall progress across books.
    // The operator-facing signal is `progress` (throttled, INFO); everything
    // per-book is DEBUG and its counts are folded into the progress/summary
    // lines. See GeneratorProgress.kt.
    private var totalBooksToProcess: Int = 0
    private val progress = BookProgressReporter()

    // Counters folded into the end-of-phase summary line, replacing the
    // per-book/per-directory INFO lines they used to be derived from.
    private var directoriesVisited: Int = 0
    private var skippedBooksCount: Int = 0
    private var acronymBooksCount: Int = 0
    private var hearotMergedBooksCount: Int = 0

    // Breadcrumb for the serial insert path. The per-book INFO block used to be
    // the only thing naming the book when allocator.bookId / insertBook /
    // getBook / updateBookCategoryId threw; those failures are otherwise
    // anonymous now that the block is a single DEBUG line emitted only AFTER
    // the insert succeeded. Assigned (never read) inside the loop, so the loop's
    // control flow is unchanged; read by the phase-level catch.
    private var currentBook: String? = null

    // Titles the importer deliberately did NOT insert, kept so the end-of-import
    // source-hash accounting can say WHY a computed hash has no book id instead
    // of just reporting "1545 / 1561".
    private val skippedBlacklistedSourceTitles = mutableSetOf<String>()
    private val skippedSefariaShadowedTitles = mutableSetOf<String>()
    private val skippedBlacklistedFileTitles = mutableSetOf<String>()
    private val skippedUncategorizedTitles = mutableSetOf<String>()

    // Otzaria: the keys [collectCurrentBooks] planned, and the key each book id went to.
    private var plannedBookKeys: Set<BookKey> = emptySet()
    // Otzaria: the keys the previous import brought in (see [previousImportKeys]).
    private var previousImportBookKeys: Set<BookKey> = emptySet()
    private val importedBookKeys = HashMap<Long, BookKey>()
    // Otzaria: the Sefaria books' ids, which no Otzaria book may take.
    private var liveSefariaBookIds: Set<Long> = emptySet()

    // Manual-link drop accounting (finding: 643 of 2,932 links silently dropped,
    // 642 of them for one missing book, logged as two INFO lines per link).
    private class DroppedLinkTarget(var links: Int, val firstPath: String)

    private val droppedLinkTargets = LinkedHashMap<String, DroppedLinkTarget>()
    private var manualLinkRowsSeen: Int = 0

    /**
     * Starts (or restarts) the book-import phase clock and zeroes every counter
     * that feeds the end-of-phase summary, so a second phase on the same
     * instance reports its own numbers rather than cumulative ones.
     */
    private fun startBookImport(total: Int) {
        directoriesVisited = 0
        skippedBooksCount = 0
        acronymBooksCount = 0
        hearotMergedBooksCount = 0
        currentBook = null
        skippedBlacklistedSourceTitles.clear()
        skippedSefariaShadowedTitles.clear()
        skippedBlacklistedFileTitles.clear()
        skippedUncategorizedTitles.clear()
        importedBookKeys.clear()
        progress.start(total)
    }

    /** Counts one finished book (content or skip) and emits the throttled INFO line. */
    private fun noteBookFinished(lines: Int = 0, tocEntries: Int = 0) {
        progress.onBookFinished(lines, tocEntries)?.let { line -> logger.i { line } }
    }

    private fun bookImportSummary(label: String): String = progress.summaryLine(
        label,
        mapOf(
            "directories" to directoriesVisited.toLong(),
            "skipped" to skippedBooksCount.toLong(),
            "acronymBooks" to acronymBooksCount.toLong(),
            "hearotMerged" to hearotMergedBooksCount.toLong(),
        ),
    )

    // Normalization helpers for categories/titles
    // Shared with renameCategories, which must match a book_moves leaf to the Otzaria
    // folder that findExistingCategory would put into it.
    private fun normalizeHebrewLabel(raw: String): String = CategoryLabels.normalize(raw)

    private fun comparableLabel(raw: String): String = CategoryLabels.comparable(raw)

    // Shared with validateForDbInputs, which maps library folders to their categories.
    private fun normalizeCategorySegments(rawTitle: String): List<String> = CategoryLabels.folderSegments(rawTitle)

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
            // A folder that is also a book_moves.csv leaf keeps one id under both of its
            // keys, so it does not move when the book_moves rows into it come or go.
            val categoryId = if (existing != null) {
                bindings.alignWithBookMoveLeaf(pathSoFar.toString(), existing.id)
                existing.id
            } else {
                bindings.upsertOtzariaCategory(
                    canonicalPath = pathSoFar.toString(),
                    parentId = currentParent,
                    title = title,
                    level = currentLevel,
                    orderIndex = 999,
                )
            }
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

    private fun normalizeBookTitle(rawTitle: String): String = CategoryLabels.bookTitle(rawTitle)

    /** Returns true only when a dependant-style title explicitly names [baseTitle]. */
    private fun titleDeclaresDependencyOn(dependantTitle: String, baseTitle: String): Boolean {
        val normalizedDependant = comparableLabel(dependantTitle)
        val declaredBase = normalizedDependant.substringAfterLast(" על ", missingDelimiterValue = "")
        if (declaredBase.isBlank()) return false

        fun dependencyKey(raw: String): String {
            var value = comparableLabel(raw)
            while (true) {
                val prefix = DEPENDENCY_TITLE_PREFIXES.firstOrNull(value::startsWith) ?: break
                value = value.removePrefix(prefix).trim()
            }
            return value
        }

        return dependencyKey(declaredBase) == dependencyKey(baseTitle)
    }

    private fun stripQuotesForLookup(title: String): String {
        return title.replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .trim()
    }

    // Metadata only: same depth-first directory order as processDirectory.
    private var bookContentOrder: List<Path> = emptyList()
    private var bookInputPipeline: OrderedInputPipeline? = null
    private val remainingBookInputs = mutableMapOf<BookKey, Int>()

    // ===== Companion-notes merge ('הערות על X' → inline footnotes) =====
    // base title → (0-based base line → note lines in reading order)
    private var hearotMergePlans: MutableMap<String, Map<Int, List<String>>> = mutableMapOf()
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
        // Otzaria: only books this import made, not skipped ones holding an old id, so the
        // next build can tell which books this one had (see [previousImportKeys]). A book
        // without a hash would look gone to it, and its key free to take.
        val imported = importedBookKeys.values.toHashSet()
        val unhashed = imported - otzariaSourceHashes.keys
        check(unhashed.isEmpty()) {
            "${unhashed.size} imported books have no source hash in files_manifest.json: " +
                unhashed.take(MAX_NAMES_PER_SUMMARY_LINE)
        }
        if (otzariaSourceHashes.isEmpty()) return
        var recorded = 0
        // A hash with no allocated book id is a book that will be fully
        // reprocessed on every future cycle, so the gap is worth naming rather
        // than leaving as a bare "N / M".
        val unrecorded = LinkedHashMap<String, MutableList<String>>()
        for ((key, hash) in otzariaSourceHashes) {
            if (key in imported) {
                allocator.recordSourceHash(key, hash)
                recorded++
            } else {
                unrecorded.getOrPut(classifyUnimportedBook(key.canonicalHeTitle)) { mutableListOf() }
                    .add(key.canonicalHeTitle)
            }
        }
        logger.i { "Recorded source hashes for $recorded / ${otzariaSourceHashes.size} Otzaria books" }
        reportUnrecordedSourceHashes(unrecorded)
    }

    /**
     * Why a book whose source hash was computed never reached the allocator.
     * Every branch mirrors one deliberate `continue`/early-return in the import
     * path; anything left over is a genuinely unexplained gap and says so.
     */
    private fun classifyUnimportedBook(title: String): String = when (title) {
        in mergedHearotTitles -> "merged into a base book as inline notes"
        in skippedSefariaShadowedTitles -> "already exists from Sefaria (priority source)"
        in skippedBlacklistedSourceTitles -> "blacklisted source"
        in skippedBlacklistedFileTitles -> "blacklisted file name"
        in skippedUncategorizedTitles -> "file has no category (library root)"
        else -> "not imported (reason not tracked)"
    }

    /** One WARN per class with counts + bounded names; the full list goes to a report file. */
    private fun reportUnrecordedSourceHashes(byClass: Map<String, List<String>>) {
        if (byClass.isEmpty()) return
        val total = byClass.values.sumOf { it.size }
        logger.w {
            "source hashes: $total of ${otzariaSourceHashes.size} Otzaria books have no source hash — " +
                "they are fully reprocessed every cycle"
        }
        val ordered = byClass.entries.sortedWith(
            compareByDescending<Map.Entry<String, List<String>>> { it.value.size }.thenBy { it.key },
        )
        for ((reason, titles) in ordered) {
            val names = titles.sorted()
            logger.w {
                "source hashes: ${titles.size} not recorded — $reason " +
                    "(${names.take(MAX_NAMES_PER_SUMMARY_LINE).joinToString()}" +
                    (if (names.size > MAX_NAMES_PER_SUMMARY_LINE) ", … and ${names.size - MAX_NAMES_PER_SUMMARY_LINE} more)" else ")")
            }
        }
        GeneratorReport.write("otzaria-source-hashes-not-recorded", logger) {
            put("computed", otzariaSourceHashes.size.toLong())
            put("notRecorded", total.toLong())
            putRows(
                "byReason",
                ordered.map { (reason, titles) ->
                    mapOf<String, Any?>("reason" to reason, "books" to titles.size)
                },
            )
            putRows(
                "books",
                ordered.flatMap { (reason, titles) ->
                    titles.sorted().map { mapOf<String, Any?>("title" to it, "reason" to reason) }
                },
            )
        }
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
                startBookImport(totalBooksToProcess)
                planBookContentOrder(libraryPath)

                // Process priority books first (if any), then process the full library
                runCatching {
                    processPriorityBooks(loadMetadata = { metadata })
                }.onFailure { e ->
                    if (e is CancellationException) throw e
                    logger.w(e) { "Failed processing priority list; continuing with full generation" }
                }

                logger.i { "🚀 Starting to process library directory: $libraryPath" }
                processDirectoryWithBoundedInput(libraryPath, metadata)
                assertAllHearotMergesApplied()
                logger.i { bookImportSummary("Otzaria book import") }
                carryAcronymsAcrossRenames()

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

            logger.e(e) { "Error during generation${bookBreadcrumb()}" }
            throw e
        } finally {
            releaseBookInputState()
            releaseLinkInputState()
        }
    }

    /**
     * ` (last book: 5848 'X' file=X.txt categoryId=612)` — the breadcrumb the
     * deleted per-book INFO block used to provide for failures on the insert
     * path (`allocator.bookId`, `insertBook`, `getBook`,
     * `updateBookCategoryId`), which throw before the per-book DEBUG line is
     * reached. Empty when nothing has been inserted yet.
     */
    private fun bookBreadcrumb(): String = currentBook?.let { " (last book: $it)" } ?: ""

    /**
     * Phase 1: Generate categories, books, TOCs and lines only (no links).
     */
    suspend fun generateLinesOnly(): Unit = coroutineScope {
        logger.i { "Starting phase 1: categories/books/lines generation..." }
        // Captures the previous build's book ids, so it must exist before any allocation.
        val bookIdContinuity = BookIdContinuity(allocator)
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

                // Before any book id is allocated (priority books included).
                registerBookAliases(bookIdContinuity)

                totalBooksToProcess = try {
                    Files.walk(libraryRoot).use { s ->
                        s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                            .count().toInt()
                    }
                } catch (_: Exception) { 0 }
                logger.i { "Planned to process approximately $totalBooksToProcess books (phase 1)" }
                startBookImport(totalBooksToProcess)
                planBookContentOrder(libraryPath)

                runCatching { processPriorityBooks(loadMetadata = { metadata }) }
                    .onFailure { e ->
                        if (e is CancellationException) throw e
                        logger.w(e) { "Failed processing priority list; continuing with full generation (phase 1)" }
                    }
                processDirectoryWithBoundedInput(libraryPath, metadata)
                assertAllHearotMergesApplied()
                checkPlannedBooksImported()
                logger.i { bookImportSummary("Otzaria book import (phase 1)") }
                carryAcronymsAcrossRenames()

                // Build category closure after categories insertion
                logger.i { "Building category_closure table (phase 1)..." }
                repository.rebuildCategoryClosure()
            }
            recordSourceHashesAtEndOfImport()
            checkNoLostBookIds(bookIdContinuity)
        } catch (e: Throwable) {
            // Rethrown unchanged — this exists only to name the book the serial
            // insert loop was on. See [bookBreadcrumb].
            logger.e(e) { "Error during phase 1${bookBreadcrumb()}" }
            throw e
        } finally {
            releaseBookInputState()
            headingLineIds.clear()
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
            releaseLinkInputState()
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

    private suspend fun getLineIdCached(bookId: Long, lineIndex: Int): Long? {
        val totalLines = booksById[bookId]?.totalLines ?: repository.getBook(bookId)?.totalLines ?: 0
        if (lineIndex < 0 || lineIndex >= totalLines) return null
        val firstIndex = lineIndex / LINE_ID_PAGE_SIZE * LINE_ID_PAGE_SIZE
        val key = LineIdPageKey(bookId, firstIndex)
        val page = lineIdCache[key] ?: run {
            val length = minOf(LINE_ID_PAGE_SIZE, totalLines - firstIndex)
            val ids = LongArray(length)
            for ((index, id) in repository.getLineIdIndexPairs(bookId, firstIndex, firstIndex + length - 1)) {
                ids[index - firstIndex] = id
            }
            val bytes = ids.size.toLong() * Long.SIZE_BYTES
            while (lineIdCache.isNotEmpty() && lineIdCacheBytes + bytes > maxLineIdCacheBytes) {
                val oldest = lineIdCache.entries.iterator()
                lineIdCacheBytes -= oldest.next().value.size.toLong() * Long.SIZE_BYTES
                oldest.remove()
            }
            lineIdCache[key] = ids
            lineIdCacheBytes += bytes
            ids
        }
        val id = page[lineIndex - firstIndex]
        return if (id == 0L) null else id
    }

    private fun releaseLinkInputState() {
        lineIdCache.clear()
        lineIdCacheBytes = 0
        booksById = emptyMap()
        booksByTitle = emptyMap()
        sefariaBookIds.clear()
        headingLineIds.clear()
    }

    private fun orderedDirectoryEntries(directory: Path): List<Path> =
        Files.list(directory).use { stream ->
            stream.sorted { a, b -> a.fileName.toString().compareTo(b.fileName.toString()) }.toList()
        }

    /** Metadata/order pass: no book text is kept while the traversal is planned. */
    private fun planBookContentOrder(libraryPath: Path) {
        val paths = ArrayList<Path>()
        fun visit(directory: Path) {
            for (entry in orderedDirectoryEntries(directory)) {
                when {
                    Files.isDirectory(entry) -> visit(entry)
                    Files.isRegularFile(entry) && entry.extension == "txt" && entry.parent != libraryPath -> {
                        val title = normalizeBookTitle(entry.nameWithoutExtension)
                        if (entry.fileName.toString() !in fileNameBlacklist && title !in mergedHearotTitles &&
                            getSourceNameFor(entry) !in sourceBlacklist
                        ) paths.add(entry)
                    }
                }
            }
        }
        visit(libraryPath)
        bookContentOrder = paths
        remainingBookInputs.clear()
        for (path in paths) {
            val key = BookKey(getSourceNameFor(path), normalizeBookTitle(path.nameWithoutExtension))
            remainingBookInputs[key] = (remainingBookInputs[key] ?: 0) + 1
        }
    }

    private suspend fun processDirectoryWithBoundedInput(
        libraryPath: Path,
        metadata: Map<String, BookMetadata>,
    ) = coroutineScope {
        val paths = bookContentOrder.filter { toLibraryRelativeKey(it) !in processedPriorityBookKeys }
        logger.i { "Preparing ${paths.size} books in order (input budget=$inputBufferBytes bytes; oversized inputs spill)" }
        val pipeline = OrderedInputPipeline(this, paths) { prepareBookInput(it, inputPartBytes, inputSpillDirectory) }
        bookInputPipeline = pipeline
        try {
            processDirectory(libraryPath, null, 0, metadata)
        } finally {
            bookInputPipeline = null
            pipeline.close()
        }
    }

    private fun releaseBookInputState() {
        bookContentOrder = emptyList()
        remainingBookInputs.clear()
        lineOccurrenceByBook.clear()
        tocAncestorStackByBook.clear()
        hearotMergePlans.clear()
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
     * book. A pair is merged only when its mapping is complete: every
     * non-blank, non-heading companion line is referenced by a link. Partial
     * pairs stay on the standalone-book mechanism, loudly reported, until their
     * links are completed upstream.
     */
    private suspend fun buildHearotMergePlans() {
        hearotMergePlans = mutableMapOf()
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

        val jsonFiles = Files.list(linksDir).use { s -> s.filter { it.extension == "json" }.toList() }
        val linkFiles = jsonFiles.filter(::isLinkJsonFile).sortedBy { it.fileName.toString() }
        noteSkippedHeadingFiles(jsonFiles.size - linkFiles.size)
        for (jsonFile in linkFiles) {
            val baseTitle = normalizeBookTitle(jsonFile.nameWithoutExtension.removeSuffix("_links"))
            val companionEntries = ArrayList<LinkData>()
            val anchoredBaseLines = HashSet<Int>()
            prepareLinkInput(jsonFile, baseTitle).use { input ->
                input.useLines { records ->
                    for (record in records) {
                        currentCoroutineContext().ensureActive()
                        val entry = json.decodeFromString<LinkData>(record)
                        if (entry.start != null) anchoredBaseLines += entry.line_index_1.toInt() - 1
                        if (HearotCompanionMerge.isMergeableCompanionTitle(
                                normalizeBookTitle(linkTargetTitleOf(entry.path_2))
                            )
                        ) companionEntries.add(entry)
                    }
                }
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
            val baseFootnoteLines = HashSet<Int>()
            val baseLineCount = scanBookInput(basePath) { index, line ->
                if (line.contains("class=\"footnote")) baseFootnoteLines += index
            }

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
                val referenced = entries.map { it.line_index_2.toInt() - 1 }.toSet()
                val hearotLines = HashMap<Int, String>()
                var unlinked = 0
                val hearotLineCount = scanBookInput(hearotPath) { index, line ->
                    if (index in referenced) hearotLines[index] = line
                    else if (line.isNotBlank() && !headingLineRegex.containsMatchIn(line)) unlinked++
                }
                if (entries.any {
                        it.line_index_1.toInt() - 1 !in 0 until baseLineCount ||
                            it.line_index_2.toInt() - 1 !in 0 until hearotLineCount
                    }
                ) {
                    standalone += "$hearotTitle (companion links out of file range)"
                    continue
                }
                if (unlinked > 0) {
                    standalone += "$hearotTitle ($unlinked unlinked note lines)"
                    continue
                }
                val badNote = referenced.firstNotNullOfOrNull { j -> unmergeableNoteReason(hearotLines.getValue(j)) }
                if (badNote != null) {
                    standalone += "$hearotTitle ($badNote)"
                    continue
                }
                val noteLineIdxs = entries.map { it.line_index_1.toInt() - 1 }.toSet()
                if (noteLineIdxs.any { it in baseFootnoteLines }) {
                    standalone += "$hearotTitle (base line already carries inline footnotes)"
                    continue
                }
                // Word-anchored links measure raw offsets against the ORIGINAL line;
                // an in-place merge would shift them (see buildLinkAnchor).
                if (noteLineIdxs.any { it in anchoredBaseLines }) {
                    standalone += "$hearotTitle (word-anchored links on note lines)"
                    continue
                }
                val perLine = plans.getOrPut(baseTitle) { sortedMapOf() }
                for (e in entries.sortedBy { it.line_index_2 }) {
                    perLine.getOrPut(e.line_index_1.toInt() - 1) { mutableListOf() }
                        .add(hearotLines.getValue(e.line_index_2.toInt() - 1))
                }
                merged += hearotTitle
                mergedPairs++
                totalNotes += entries.size
                val md = java.security.MessageDigest.getInstance("SHA-256")
                extra[baseTitle]?.let { md.update(it) }
                updateDigestFromFile(md, hearotPath)
                updateDigestFromFile(md, jsonFile)
                extra[baseTitle] = md.digest()
            }
        }

        hearotMergePlans.putAll(plans)
        mergedHearotTitles = merged
        hearotMergeExtraHash = extra
        pendingHearotMergeBases.addAll(plans.keys)
        if (mergedPairs > 0 || standalone.isNotEmpty()) {
            logger.i { "📝 Companion-notes merge plan: $mergedPairs pairs, $totalNotes notes → inline footnotes" }
            standalone.forEach { logger.w { "📝 Companion notes stay standalone: $it" } }
        }
    }

    private suspend fun scanBookInput(path: Path, consume: (Int, String) -> Unit): Int {
        val context = currentCoroutineContext()
        return Files.newInputStream(path).reader(Charsets.UTF_8).use { reader ->
            var count = 0
            for (raw in reader.exactLineSequence { context.ensureActive() }) {
                check(count < Int.MAX_VALUE) { "Too many lines in $path" }
                consume(count, if (count == 0) Utf8Bom.strip(raw) else raw)
                count++
            }
            count
        }
    }

    private suspend fun updateDigestFromFile(digest: java.security.MessageDigest, path: Path) {
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(8192)
            while (true) {
                currentCoroutineContext().ensureActive()
                val count = input.read(buffer)
                if (count < 0) break
                digest.update(buffer, 0, count)
            }
        }
    }

    /** One INFO line for the `_headings` files skipped at link discovery. See [HEADINGS_FILE_SUFFIX]. */
    private fun noteSkippedHeadingFiles(count: Int) {
        if (count > 0) logger.i { "skipped $count *$HEADINGS_FILE_SUFFIX files (heading maps, not link files)" }
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
            val content = Utf8Bom.strip(metadataFile.readText())
            try {
                // Try to parse as Map first (original format)
                json.decodeFromString<Map<String, BookMetadata>>(content)
            } catch (mapError: Exception) {
                // If that fails, try to parse as List and convert to Map
                try {
                    val metadataList = json.decodeFromString<List<BookMetadata>>(content)
                    logger.i { "Parsed metadata as List with ${metadataList.size} entries" }
                    // Convert list to map using title as key
                    metadataList.associateBy { it.title }
                } catch (listError: Exception) {
                    // Do NOT fall back to an empty map: metadata.json carries the
                    // author, descriptions and order for every Otzaria book, so a
                    // silent empty map builds a whole DB with none of them. A typo
                    // here must stop the build, not degrade it.
                    throw IllegalStateException(
                        "metadata.json is present but parses as neither an object nor an array " +
                            "(${metadataFile}). As an object: ${mapError.message}. " +
                            "As an array: ${listError.message}",
                        listError,
                    )
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
            val content = Utf8Bom.strip(manifestPath.readText())
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
        directoriesVisited += 1
        logger.d { "=== Processing directory: ${directory.fileName} with parentCategoryId: $parentCategoryId (level: $level) ===" }

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
                        logger.d { "✅ Category '${entry.fileName}' normalized to '$normalizedPath' with ID: ${placement.id} (parent: $parentCategoryId)" }
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
                            skippedBlacklistedFileTitles += normalizeBookTitle(fname.substringBeforeLast('.'))
                            logger.i { "⛔ Skipping blacklisted file '$fname' by name" }
                            continue
                        }
                        if (normalizeBookTitle(fname.substringBeforeLast('.')) in mergedHearotTitles) {
                            hearotMergedBooksCount += 1
                            logger.d { "📝 Skipping '$fname' — merged into its base book as inline notes" }
                            continue
                        }
                        if (parentCategoryId == null) {
                            skippedUncategorizedTitles += normalizeBookTitle(fname.substringBeforeLast('.'))
                            logger.w { "❌ Book found without category: $entry" }
                            continue
                        }
                        // No per-book INFO header here: this site and the one inside
                        // createAndProcessBook each printed one per book (1,500 + 1,501
                        // lines / 0.39 MB in one build). Both are gone; createAndProcessBook
                        // now emits a single per-book DEBUG line once the book is done,
                        // and that site also covers the priority-list caller.
                        createAndProcessBook(entry, parentCategoryId, metadata)
                    }

                    else -> {
                        logger.d { "Skipping entry: ${entry.fileName} (not a supported file type)" }
                    }
                }
            }
        }
        logger.d { "=== Finished processing directory: ${directory.fileName} ===" }
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
        // Breadcrumb only — never read by this function. See [bookBreadcrumb].
        currentBook = "'$title' file=$filename categoryId=$categoryId"

        // Apply source blacklist
        val srcName = getSourceNameFor(path)
        if (sourceBlacklist.contains(srcName)) {
            skippedBlacklistedSourceTitles += title
            logger.i { "⛔ Skipping '$title' from blacklisted source '$srcName'" }
            skippedBooksCount += 1
            noteBookFinished()
            return
        }

        // Skip if a book with the same heRef already exists from Sefaria (Sefaria has priority)
        if (existsFromSefaria(title)) {
            skippedSefariaShadowedTitles += title
            logger.i { "⏭️ Skipping '$title' - already exists from Sefaria (priority source)" }
            skippedBooksCount += 1
            noteBookFinished()
            return
        }

        // Assign a stable ID via IdAllocator so cross-build reproducibility holds.
        val currentBookId = allocator.bookId(srcName, title)
        // Otzaria: an alias must never give one id to two books (an old key back beside its new one).
        val bookKey = BookKey(srcName, title)
        importedBookKeys.putIfAbsent(currentBookId, bookKey)?.let { other ->
            check(other == bookKey) { "Book id $currentBookId went to both $other and $bookKey (book alias)" }
        }
        check(currentBookId !in liveSefariaBookIds) { "Book id $currentBookId went to both Sefaria and $bookKey (book alias)" }
        currentBook = "$currentBookId '$title' file=$filename categoryId=$categoryId"
        logger.d { "Assigning ID $currentBookId to book '$title' (source=$srcName) with categoryId: $categoryId" }

        // Pre-resolve author / pubPlace / pubDate IDs through the IdAllocator
        // so they remain stable across builds (required for the delta producer's
        // secondary-UNIQUE collision pre-check).
        // normalizeAuthorName first: metadata.json carries nikud on a few names
        // ("חיים בן עֲטַר") that Sefaria writes plain, and an author row per
        // spelling means a search by author returns only that corpus's half.
        val authors = (meta?.resolveAuthorNames() ?: emptyList())
            .map(::normalizeAuthorName)
            .distinct()
            .map { authorName ->
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
        var acronymCount = 0
        val terms = fetchAcronymsForTitle(title)
        try {
            if (terms.isNotEmpty()) {
                repository.bulkInsertBookAcronyms(insertedBookId, terms)
                acronymCount = terms.size
                acronymBooksCount += 1
                logger.d { "Inserted ${terms.size} acronyms for '${title}'" }
            }
        } catch (e: Exception) {
            logger.w(e) { "Failed to insert acronyms for '$title'" }
        }

        // Process content of the book. One DEBUG line per book replaces the
        // six INFO lines this used to print; on failure the book is named at
        // ERROR level so the breadcrumb survives without the INFO chatter.
        val startedAtMs = monotonicMillis()
        val stats = try {
            processBookContent(path, insertedBookId, title, categoryId)
        } catch (e: Throwable) {
            logger.e(e) { "Failed processing book $insertedBookId '$title' (file=$filename, categoryId=$categoryId)" }
            throw e
        }
        logger.d {
            "book $insertedBookId '$title' file=$filename categoryId=$categoryId " +
                "lines=${stats.lines} toc=${stats.tocEntries} tocWithChildren=${stats.tocEntriesWithChildren} " +
                "acronyms=$acronymCount ${monotonicMillis() - startedAtMs}ms"
        }

        // Book-level progress (throttled: see BookProgressReporter)
        noteBookFinished(lines = stats.lines, tocEntries = stats.tocEntries)
    }

    /**
     * Processes the content of a book, extracting lines and TOC entries.
     *
     * @param path The path to the book file
     * @param bookId The ID of the book in the database
     */
    private suspend fun processBookContent(
        path: Path,
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
    ): BookContentStats = coroutineScope {
        logger.d { "Processing content for book ID: $bookId" }

        val input = bookInputPipeline?.take(path)
            ?: prepareBookInput(path, inputPartBytes, inputSpillDirectory)
        input.use {
            val plan = hearotMergePlans[bookTitle]
            plan?.keys?.sorted()?.forEach { index ->
                check(index in 0 until input.size) {
                    "hearot merge for '$bookTitle': base line $index out of range (${input.size} lines)"
                }
            }
            val mergeStats = HearotCompanionMerge.MergeStats()
            val tocStats = input.useLines { rawLines ->
                val lines = rawLines.mapIndexed { index, raw ->
                    val line = if (index == 0) Utf8Bom.strip(raw) else raw
                    val notes = plan?.get(index)
                    if (notes == null) line else HearotCompanionMerge.mergeLine(bookTitle, line, index, notes, mergeStats)
                }
                processLinesWithTocEntries(bookId, bookTitle, categoryId, lines, input.size, input.firstLineHasBom)
            }
            repository.updateBookTotalLines(bookId, input.size)
            if (plan != null) {
                pendingHearotMergeBases.remove(bookTitle)
                logger.d { "📝 '$bookTitle': merged companion notes — ${mergeStats.inPlace} anchored in place, ${mergeStats.appended} appended" }
            }

            // Duplicate source/title paths retain their occurrence state until the
            // final import, preserving the old allocation order for such inputs.
            val key = BookKey(getSourceNameFor(path), bookTitle)
            val remaining = (remainingBookInputs[key] ?: 1) - 1
            if (remaining <= 0) {
                remainingBookInputs.remove(key)
                lineOccurrenceByBook.remove(bookId)
                tocAncestorStackByBook.remove(bookId)
                if (bookTitle in hearotMergePlans && remainingBookInputs.keys.none { it.canonicalHeTitle == bookTitle }) {
                    hearotMergePlans.remove(bookTitle)
                }
            } else {
                remainingBookInputs[key] = remaining
            }
            BookContentStats(input.size, tocStats.tocEntries, tocStats.tocEntriesWithChildren)
        }
    }

    /** Per-book counters folded into the per-book DEBUG line and the phase summary. */
    private data class BookContentStats(
        val lines: Int,
        val tocEntries: Int,
        val tocEntriesWithChildren: Int,
    )


    /**
     * Processes lines of a book, identifying and creating TOC entries.
     *
     * @param bookId The ID of the book in the database
     * @param lines The lines of the book content
     */
    private suspend fun processLinesWithTocEntries(
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
        lines: Sequence<String>,
        totalLines: Int,
        firstLineHadBom: Boolean = false,
    ): BookContentStats {
        logger.d { "Processing lines and TOC entries together for book ID: $bookId" }

        // Line 0 of a book whose BOM was stripped keeps the id earlier builds gave it.
        fun lineIdFor(lineIndex: Int, line: String): Long =
            if (lineIndex == 0 && firstLineHadBom) {
                stableLineId(bookId, line, legacyContent = Utf8Bom.legacyFirstLineContent(line))
            } else {
                stableLineId(bookId, line)
            }

        var tocEntryCount = 0
        val parentStack = mutableMapOf<Int, Long>()
        val parentIds = linkedSetOf<Long>()
        val lastChildByParent = linkedMapOf<Long?, Long>()
        var currentOwningTocEntryId: Long? = null
        val lineTocBuffer = ArrayList<Pair<Long, Long>>(LINE_FLUSH_THRESHOLD)
        // Batch line inserts + the FK back-link updates that depend on them.
        // JFR showed each `repository.insertLine` ≈ 1 withContext(Dispatchers.IO)
        // switch + 1 prepareStatement, run 4M times. Buffering collapses the
        // dispatcher churn and lets us flush via insertLinesBatch.
        val lineBuffer = ArrayList<Line>(LINE_FLUSH_THRESHOLD)
        var lineBufferBytes = 0L
        val tocEntryLineIdUpdates = ArrayList<Pair<Long, Long>>()
        val lineTocEntryUpdates = ArrayList<Pair<Long, Long>>()
        // Line ticks: silent for small books, ≥10% (or 60 s) apart for large ones.
        val lineTicks = LineTickGate(totalLines = totalLines)

        suspend fun flushLineBatch() {
            if (lineBuffer.isEmpty()) return
            repository.insertLinesBatch(lineBuffer)
            lineBuffer.clear()
            lineBufferBytes = 0L
            // Flush back-link updates only after the lines exist in the DB.
            if (tocEntryLineIdUpdates.isNotEmpty()) {
                repository.bulkUpdateTocEntryLineIds(tocEntryLineIdUpdates)
                tocEntryLineIdUpdates.clear()
            }
            if (lineTocEntryUpdates.isNotEmpty()) {
                repository.bulkUpdateLineTocEntryIds(lineTocEntryUpdates)
                lineTocEntryUpdates.clear()
            }
            if (lineTocBuffer.isNotEmpty()) {
                repository.bulkUpsertLineToc(lineTocBuffer)
                lineTocBuffer.clear()
            }
        }

        // PREMIÈRE PASSE : Créer toutes les entrées et lignes
        for ((lineIndex, line) in lines.withIndex()) {
            currentCoroutineContext().ensureActive()
            val bytes = retainedStringBytes(line) + 128L
            if (lineBuffer.isNotEmpty() && bytes > inputPartBytes - lineBufferBytes) flushLineBatch()
            val level = detectHeaderLevel(line)
            // Merged footnotes on a heading line must not leak into the TOC text
            val plainText = if (level > 0) cleanHtml(HearotCompanionMerge.stripFootnotes(line)) else ""
            val lineCharCount = countVisibleChars(line)

            if (level > 0) {
                // A shallower heading closes every deeper one, or a level skip (h2 → h4) adopts a stale parent.
                parentStack.keys.removeAll { it >= level }
                if (plainText.isBlank()) {
                    logger.d { "⚠️ Skipping empty header at level $level (line $lineIndex)" }
                    continue
                }

                val parentId = (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }
                val (currentTocEntryId, tocTextStableId) = stableTocEntryId(bookId, level, plainText, lineIndex)
                val currentLineId = lineIdFor(lineIndex, line)

                tocEntryCount++
                parentId?.let { parentIds.add(it) }

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
                lastChildByParent[parentId] = tocEntryId
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
                val currentLineId = lineIdFor(lineIndex, line)
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

            lineBufferBytes += bytes
            if (lineBuffer.size >= LINE_FLUSH_THRESHOLD || lineBufferBytes >= inputPartBytes) {
                flushLineBatch()
            }

            if (lineTicks.shouldTick(lineIndex)) {
                logger.i { lineTicks.tickLine(bookId, bookTitle, lineIndex) }
            }
        }

        // Flush remaining line + back-link buffers
        flushLineBatch()

        // DEUXIÈME PASSE : Mettre à jour isLastChild et hasChildren
        logger.d { "Updating isLastChild and hasChildren for book ID: $bookId" }

        // Collect hasChildren + isLastChild ids and flush them in a single batch
        // (replaces 2 × per-row update calls; was up to ~2k withContext switches
        // per book on large hierarchies).
        repository.bulkUpdateTocEntryFlags(parentIds.toList(), lastChildByParent.values.toList())

        return BookContentStats(
            lines = totalLines,
            tocEntries = tocEntryCount,
            tocEntriesWithChildren = parentIds.size,
        )
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
        // The list has drifted almost entirely out of the library (430 of 431
        // entries pointed at files that no longer exist in the audited build).
        // One WARN summary replaces one WARN per entry; the full list goes to a
        // report file so the operator can decide re-pin vs delete. Re-pinning or
        // deleting the list here would change the book insertion ORDER, hence
        // the allocated ids — never a fix this code may make on its own.
        var priorityFilesFound = 0
        val priorityMissing = mutableListOf<String>()

        outer@ for ((idx, relative) in entries.withIndex()) {
            // Build the absolute path under the library root
            val parts = relative.split('/').filter { it.isNotEmpty() }
            if (parts.isEmpty()) continue

            // Last part is the book filename, everything before are categories
            val categories = if (parts.size > 1) parts.dropLast(1) else emptyList()
            val bookFileName = parts.last()
            // Skip files explicitly blacklisted by name
            if (fileNameBlacklist.contains(bookFileName)) {
                skippedBlacklistedFileTitles += normalizeBookTitle(bookFileName.substringBeforeLast('.'))
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
                priorityMissing += relative
                logger.d { "Priority entry ${idx + 1}/${entries.size}: file not found: $bookPath" }
                continue@outer
            }
            priorityFilesFound += 1

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

        reportPriorityListDrift(entries.size, priorityFilesFound, priorityMissing)
    }

    /**
     * One WARN for the whole priority pass instead of one per missing entry
     * (430 WARNs / 0.11 MB in the audited build). The list itself is left
     * exactly as it is: re-pinning or deleting entries reorders the book
     * inserts and therefore the allocated ids, which is an operator decision,
     * not a logging fix.
     */
    private fun reportPriorityListDrift(total: Int, found: Int, missing: List<String>) {
        if (missing.isEmpty()) {
            logger.i { "priority list: $found/$total entries found" }
            return
        }
        logger.w {
            "priority list: $found/$total entries found, ${missing.size} missing " +
                "(first: ${missing.take(3).joinToString()}) — list is " +
                "otzariasqlite/src/commonMain/resources/priority.txt (loaded by " +
                "DatabaseGenerator.loadPriorityList in Generator.kt); needs re-pinning or " +
                "removal (operator decision)"
        }
        GeneratorReport.write("otzaria-priority-list-missing", logger) {
            put("entries", total.toLong())
            put("found", found.toLong())
            put("missing", missing.size.toLong())
            put("listResource", "otzariasqlite/src/commonMain/resources/priority.txt")
            putStrings("missingEntries", missing)
        }
    }

    private suspend fun existsFromSefaria(title: String): Boolean {
        val existing = repository.getBookByHeRef(title) ?: return false
        return repository.getSourceById(existing.sourceId)?.name == SEFARIA_SOURCE
    }

    /**
     * Keys of every book this import will allocate, with their file. Mirrors the
     * skips of [processDirectory] and [createAndProcessBook].
     * Otzaria: our skips (only the companion notes merged into their base, root-level files);
     * [checkPlannedBooksImported] fails the build if the import ends up elsewhere.
     */
    private suspend fun collectCurrentBooks(): Map<BookKey, Path> {
        val files = Files.walk(libraryRoot).use { s ->
            s.filter { Files.isRegularFile(it) && it.extension == "txt" && it.parent != libraryRoot }.sorted().toList()
        }
        val out = HashMap<BookKey, Path>()
        for (path in files) {
            val fileName = path.fileName.toString()
            val rawTitle = fileName.substringBeforeLast('.')
            if (fileName in fileNameBlacklist) continue
            val title = normalizeBookTitle(rawTitle)
            if (title in mergedHearotTitles) continue
            val source = getSourceNameFor(path)
            if (source in sourceBlacklist) continue
            if (existsFromSefaria(title)) continue
            out.putIfAbsent(BookKey(source, title), path)
        }
        return out
    }

    /** Otzaria: the books imported are exactly the ones [collectCurrentBooks] planned. */
    private fun checkPlannedBooksImported() {
        val imported = importedBookKeys.values.toSet()
        val unplanned = imported - plannedBookKeys
        val missing = plannedBookKeys - imported
        check(unplanned.isEmpty() && missing.isEmpty()) {
            "Book-id continuity planned other books than the import made: " +
                "imported but not planned ${unplanned.take(MAX_NAMES_PER_SUMMARY_LINE)}, " +
                "planned but not imported ${missing.take(MAX_NAMES_PER_SUMMARY_LINE)}"
        }
    }

    /**
     * Keeps the previous id of books Otzaria moved to another source or renamed,
     * and of books Sefaria no longer provides (e.g. newly blacklisted) that
     * Otzaria now does.
     */
    private suspend fun registerBookAliases(continuity: BookIdContinuity) {
        val currentBooks = collectCurrentBooks()
        plannedBookKeys = currentBooks.keys
        previousImportBookKeys = previousImportKeys()
        val context = currentCoroutineContext()
        val currentLineHashes = { key: BookKey ->
            // Keep only the hashes the continuity API needs, never a whole book
            // plus its BOM-stripped and companion-merged copies.
            currentBooks[key]?.let { path ->
                Files.newInputStream(path).reader(Charsets.UTF_8).use { reader ->
                    reader.exactLineSequence { context.ensureActive() }.mapIndexed { index, raw ->
                        val line = if (index == 0) Utf8Bom.strip(raw) else raw
                        val notes = hearotMergePlans[key.canonicalHeTitle]?.get(index)
                        val merged = if (notes == null) line else
                            HearotCompanionMerge.mergeLine(key.canonicalHeTitle, line, index, notes)
                        IdAllocatorBindings.normalisedContentHash(merged)
                    }.toList()
                }
            }
        }
        val aliases = continuity.registerAliases(
            currentKeys = currentBooks.keys,
            // Otzaria: only books the previous build had.
            inScope = { it.sourceName != SEFARIA_SOURCE && it in previousImportBookKeys },
            atVersion = buildVersion,
            currentLineHashes = currentLineHashes,
        )

        // Otzaria: then the Sefaria books gone, for the books still new only, so an Otzaria
        // book keeps its own id first. Gone by id, not by DB title (sefariaDisplayTitle), and
        // matched by exact title, as existsFromSefaria tells the books Sefaria has.
        val sefariaSourceId = repository.getSourceByName(SEFARIA_SOURCE)?.id
        liveSefariaBookIds = repository.getAllBooks().filter { it.sourceId == sefariaSourceId }.mapTo(HashSet()) { it.id }
        val stillNew = currentBooks.keys.filterTo(HashSet()) { it !in aliases.values && it !in previousImportBookKeys }
        val stillNewTitles = stillNew.mapTo(HashSet()) { it.canonicalHeTitle }
        val previousIds = allocator.bookIds()
        BookIdContinuity(allocator).registerAliases(
            currentKeys = stillNew,
            inScope = {
                it.sourceName == SEFARIA_SOURCE && it.canonicalHeTitle in stillNewTitles &&
                    previousIds.getValue(it) !in liveSefariaBookIds
            },
            atVersion = buildVersion,
            currentLineHashes = currentLineHashes,
        )

        // Otzaria: an alias to a live Sefaria book: that book keeps the id.
        continuity.dropAliasesLeadingTo(currentBooks.keys) {
            allocator.peekBookId(it.sourceName, it.canonicalHeTitle) in liveSefariaBookIds
        }
    }

    /** Otzaria: the Otzaria keys the previous import brought in (see [previousOtzariaImportKeys]). */
    private fun previousImportKeys(): Set<BookKey> = allocator.previousOtzariaImportKeys(SEFARIA_SOURCE, buildVersion)

    /** Fails the build when a previous book is still there under another id. */
    private suspend fun checkNoLostBookIds(continuity: BookIdContinuity) {
        // Otzaria: books of the previous build only, and a book Sefaria now covers is left to
        // otzaria-library's check (it changes id by design: Sefaria's edition replaces it).
        val (coveredBySefaria, lost) = continuity
            .findLostIds(repository.getAllBooks().associate { it.id to it.title }) { it in previousImportBookKeys }
            .partition { it.key.canonicalHeTitle in skippedSefariaShadowedTitles }
        coveredBySefaria.forEach { logger.w { "Book ${it.key} is now covered by Sefaria and lost id ${it.id}" } }
        if (lost.isEmpty()) return
        lost.forEach { logger.e { "Book ${it.key} lost id ${it.id}; its title now has ids ${it.currentIds}" } }
        val message = "${lost.size} previous books changed id, which orphans user notes. " +
            "Alias them, or rerun with -PallowBookIdLoss=true to accept it."
        if (allowBookIdLoss) logger.w { message } else error(message)
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

        droppedLinkTargets.clear()
        manualLinkRowsSeen = 0
        val jsonFiles = Files.list(linksDir).use { s -> s.filter { it.extension == "json" }.toList() }
        val linkFiles = jsonFiles.filter(::isLinkJsonFile)
        noteSkippedHeadingFiles(jsonFiles.size - linkFiles.size)
        // The old toMap kept the last file for a duplicate title, in the first
        // title's discovery slot. Preserve that exact order and winner so IDs
        // and authored links do not change as a side effect of streaming.
        val filesByTitle = linkedMapOf<String, Path>()
        for (file in linkFiles) filesByTitle[file.nameWithoutExtension.removeSuffix("_links")] = file

        val legacyLinksByKey = repository.getNonBaseToBaseLinksByTypes(ORIENTED_DEPENDANT_TYPES)
            .groupBy(
                keySelector = {
                    LegacyAuthoredLinkKey(
                        sourceBookId = it.sourceBookId,
                        targetBookId = it.targetBookId,
                        sourceLineId = it.sourceLineId,
                        targetLineId = it.targetLineId,
                        connectionType = it.connectionType,
                    )
                },
                valueTransform = { it.id },
            )
        val legacyLinkIdsToDelete = mutableSetOf<Long>()

        logger.i { "Processing link inputs in order (input budget=$inputBufferBytes bytes; oversized inputs spill)" }
        var totalLinks = 0
        coroutineScope {
            val pipeline = OrderedInputPipeline(this, filesByTitle.values.toList()) { file ->
                prepareLinkInput(file, file.nameWithoutExtension.removeSuffix("_links"))
            }
            try {
                for ((bookTitle, file) in filesByTitle) {
                    pipeline.take(file).use { input ->
                        manualLinkRowsSeen += input.size
                        val processedLinks = input.useLines { records ->
                            processLinksForBook(
                                bookTitle = bookTitle,
                                links = records.map { json.decodeFromString<LinkData>(it) },
                                legacyLinksByKey = legacyLinksByKey,
                                legacyLinkIdsToDelete = legacyLinkIdsToDelete,
                            )
                        }
                        totalLinks += processedLinks
                        logger.d { "Processed $processedLinks links for $bookTitle, total so far: $totalLinks" }
                    }
                }
            } finally {
                pipeline.close()
            }
        }
        repository.deleteLinksAndDependants(legacyLinkIdsToDelete)
        if (legacyLinkIdsToDelete.isNotEmpty()) {
            logger.i { "Removed ${legacyLinkIdsToDelete.size} superseded legacy Otzaria links" }
        }
        reportDroppedManualLinks()

        // Count links after processing
        val linksAfter = repository.countLinks()
        logger.d { "Links in database after processing: $linksAfter" }
        logger.d { "Added ${linksAfter - linksBefore} links to the database" }

        logger.i { "Total of $totalLinks links processed" }

        // Update the book_has_links table
        updateBookHasLinksTable()
    }

    /**
     * Manual links whose target book does not exist in the DB are dropped — that
     * is upstream data drift (otzaria-library), not something this importer can
     * repair, and the drop behaviour is deliberately unchanged. What changes is
     * that it is now visible: one WARN per missing target book with its link
     * count and original path, bounded, plus a total. Previously this was two
     * INFO lines per dropped link and no summary at all, so a fifth of the
     * manual-link corpus vanished without a single warning.
     */
    private fun reportDroppedManualLinks() {
        if (droppedLinkTargets.isEmpty()) return
        val ordered = droppedLinkTargets.entries.sortedWith(
            compareByDescending<Map.Entry<String, DroppedLinkTarget>> { it.value.links }.thenBy { it.key },
        )
        val droppedLinks = ordered.sumOf { it.value.links }
        logger.w {
            "manual links: $droppedLinks of $manualLinkRowsSeen links dropped — " +
                "${ordered.size} target book(s) not found"
        }
        for ((title, target) in ordered.take(MAX_NAMES_PER_SUMMARY_LINE)) {
            logger.w {
                "manual links: ${target.links} links dropped — target book not found: " +
                    "'$title' (${target.firstPath})"
            }
        }
        val rest = ordered.drop(MAX_NAMES_PER_SUMMARY_LINE)
        if (rest.isNotEmpty()) {
            logger.w {
                "manual links: … and ${rest.size} more missing target books " +
                    "(${rest.sumOf { it.value.links }} links)"
            }
        }
        GeneratorReport.write("otzaria-manual-links-dropped", logger) {
            put("linkRowsSeen", manualLinkRowsSeen.toLong())
            put("droppedLinks", droppedLinks.toLong())
            put("missingTargetBooks", ordered.size.toLong())
            putRows(
                "missingTargets",
                ordered.map { (title, target) ->
                    mapOf<String, Any?>(
                        "title" to title,
                        "links" to target.links,
                        "originalPath" to target.firstPath,
                    )
                },
            )
        }
    }

    /**
     * Processes one validated input in order, with bounded ancillary-row batches.
     */
    private suspend fun processLinksForBook(
        bookTitle: String,
        links: Sequence<LinkData>,
        legacyLinksByKey: Map<LegacyAuthoredLinkKey, List<Long>>,
        legacyLinkIdsToDelete: MutableSet<Long>,
    ): Int {
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
        val pendingTouchedIds = mutableSetOf<Long>()
        val syncedLinkIds = mutableSetOf<Long>()
        val maxAuxiliaryRows = minOf(1000L, (inputPartBytes / 64L).coerceAtLeast(1L)).toInt()
        var anchorCount = 0
        var rangeCount = 0
        var coverageCount = 0

        suspend fun flushLinkBatch() {
            try {
                // Delete only once per link per file. A repeated link without an end
                // must not erase the earlier row's range, including across batches.
                pendingTouchedIds.chunked(500).forEach { chunk ->
                    val ids = chunk.joinToString(",")
                    repository.executeRawQuery("DELETE FROM link_range WHERE linkId IN ($ids)")
                    repository.executeRawQuery("DELETE FROM link_coverage WHERE linkId IN ($ids)")
                }
                syncedLinkIds.addAll(pendingTouchedIds)
                pendingTouchedIds.clear()
                if (anchorBatch.isNotEmpty()) repository.insertLinkAnchorsBatch(anchorBatch)
                if (rangeBatch.isNotEmpty()) repository.insertLinkRangesBatch(rangeBatch)
                if (coverageBatch.isNotEmpty()) repository.insertLinkCoverageBatch(coverageBatch)
                anchorCount += anchorBatch.size
                rangeCount += rangeBatch.size
                coverageCount += coverageBatch.size
                anchorBatch.clear()
                rangeBatch.clear()
                coverageBatch.clear()
            } catch (error: Exception) {
                if (error is CancellationException) throw error
                throw LinkBatchWriteFailure(error)
            }
        }

        for (linkData in links) {
            currentCoroutineContext().ensureActive()
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
                        // Counted, not printed: two INFO lines per dropped link
                        // was 1,286 lines for 643 drops. The drop itself is
                        // unchanged — see [reportDroppedManualLinks].
                        droppedLinkTargets
                            .getOrPut(targetTitle) { DroppedLinkTarget(0, linkData.path_2) }
                            .links += 1
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
                val pointsFromNonBaseIntoBase = declaredType in ORIENTED_DEPENDANT_TYPES &&
                    targetBook.isBaseBook && !sourceBook.isBaseBook
                val reversedDependant = pointsFromNonBaseIntoBase &&
                    titleDeclaresDependencyOn(sourceBook.title, targetBook.title)
                val flip = declaredType == ConnectionType.SOURCE || reversedDependant
                val storedType = when {
                    declaredType == ConnectionType.SOURCE -> ConnectionType.COMMENTARY
                    pointsFromNonBaseIntoBase && !reversedDependant -> ConnectionType.OTHER
                    else -> declaredType
                }
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
                if (pointsFromNonBaseIntoBase) {
                    val legacyKey = LegacyAuthoredLinkKey(
                        sourceBookId = sourceBook.id,
                        targetBookId = targetBook.id,
                        sourceLineId = sourceLineId,
                        targetLineId = targetLineId,
                        connectionType = declaredType,
                    )
                    legacyLinksByKey[legacyKey]?.let { legacyIds ->
                        check(linkId !in legacyIds) {
                            "Stable link id collision while replacing legacy link $legacyKey"
                        }
                        legacyLinkIdsToDelete += legacyIds
                    }
                }
                if (linkId !in syncedLinkIds) pendingTouchedIds += linkId
                // Anchor is source-side; skip when flipped (that line is now the stored target).
                if (!flip) {
                    buildLinkAnchor(linkData, linkId, sourceLineId, bookTitle)?.let { anchorBatch += it }
                }
                // Range side 0 = stored source, 1 = stored target; the flip swaps each file's end.
                if (flip) {
                    queueRangeSide(linkData.line_index_2_end, targetBook.id, targetLineIndex, linkId, 0, bookTitle, rangeBatch, coverageBatch, maxAuxiliaryRows, ::flushLinkBatch)
                    queueRangeSide(linkData.line_index_1_end, sourceBook.id, sourceLineIndex, linkId, 1, bookTitle, rangeBatch, coverageBatch, maxAuxiliaryRows, ::flushLinkBatch)
                } else {
                    queueRangeSide(linkData.line_index_1_end, sourceBook.id, sourceLineIndex, linkId, 0, bookTitle, rangeBatch, coverageBatch, maxAuxiliaryRows, ::flushLinkBatch)
                    queueRangeSide(linkData.line_index_2_end, targetBook.id, targetLineIndex, linkId, 1, bookTitle, rangeBatch, coverageBatch, maxAuxiliaryRows, ::flushLinkBatch)
                }
                processed++
            } catch (error: Exception) {
                if (error is CancellationException || error is LinkBatchWriteFailure) throw error
                // Skip malformed entries but continue, as before.
            }
            if (pendingTouchedIds.size + anchorBatch.size + rangeBatch.size + coverageBatch.size >= maxAuxiliaryRows) {
                flushLinkBatch()
            }
        }
        flushLinkBatch()
        if (anchorCount > 0) logger.i { "Inserted $anchorCount word-level anchors for $bookTitle" }
        if (rangeCount > 0) logger.i { "Inserted $rangeCount link ranges ($coverageCount coverage rows) for $bookTitle" }
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
        maxBufferedRows: Int,
        flush: suspend () -> Unit,
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
            if (coverage.size >= maxBufferedRows) flush()
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
            val parsed = runCatching { json.decodeFromString<List<AltTocStructureData>>(Utf8Bom.strip(file.readText())) }
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
        // Entry ids are allocator-stable too, keyed on the per-parent ordinal chain.
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
        val nextOrdinalByParent = mutableMapOf<Long?, Int>()
        // lineIndex -> entryId; the last entry inserted on a line wins: a child over
        // its parent (as in the Sefaria builder), a later sibling over earlier ones.
        val anchorByLineIndex = mutableMapOf<Int, Long>()

        suspend fun addNode(node: AltTocNodeData, level: Int, parentId: Long?, parentPath: String): Boolean {
            val lineIndex = node.line.toInt() - 1
            val lineId = getLineIdCached(book.id, lineIndex)
            if (lineId == null) {
                logger.w { "Alt-toc node '${node.heTitle}' line ${node.line.toInt()} out of range in ${book.title} — node dropped" }
                return false
            }
            val textId = bindings.upsertTocText(node.heTitle)
            val ordinal = (nextOrdinalByParent[parentId] ?: 0) + 1
            nextOrdinalByParent[parentId] = ordinal
            val path = altTocChildPath(parentPath, ordinal)
            val entryId = bindings.insertAltTocEntryStable(
                AltTocEntry(
                    structureId = structureId, parentId = parentId, textId = textId,
                    text = node.heTitle, level = level, lineId = lineId,
                ),
                ancestorPath = path,
            )
            entriesByParent.getOrPut(parentId) { mutableListOf() } += entryId
            anchorByLineIndex[lineIndex] = entryId
            var hasChild = false
            for (child in node.children) if (addNode(child, level + 1, entryId, path)) hasChild = true
            if (hasChild) repository.updateAltTocEntryHasChildren(entryId, true)
            return true
        }

        var any = false
        for (node in data.nodes) if (addNode(node, 0, null, "")) any = true
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

    /** The Acronymizer DB, opened on first use; null when this build has none. */
    private fun acronymizer(): AcronymizerLookup? {
        val path = acronymDbPath ?: return null
        acronymLookup?.let { return it }
        val conn = acronymDb ?: java.sql.DriverManager.getConnection("jdbc:sqlite:$path").also { acronymDb = it }
        return AcronymizerLookup(conn).also { acronymLookup = it }
    }

    /** Sanitized acronym terms for a given book title from the Acronymizer DB. */
    private fun fetchAcronymsForTitle(title: String): List<String> = try {
        acronymizer()?.termsFor(title).orEmpty()
    } catch (e: java.sql.SQLException) {
        // Swallowing this would ship a whole DB without acronyms on a schema change.
        throw IllegalStateException("Acronymizer DB at $acronymDbPath is unreadable or has an unexpected schema", e)
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

    /**
     * Gives a renamed book the acronyms the Acronymizer still keeps under its old
     * title, plus the old title itself, so a rename does not take its search terms
     * with it ('אמת ואמונה - מנחם מנדל מקוצק' → 'אמת ואמונה' lost 'קוצקי').
     *
     * Runs once every book is in, and only for a book that has no acronyms and no
     * Acronymizer entry under its current title: an entry for the new title is the
     * curated answer and always wins. Which renames are followed, and which are
     * refused, is [RenamedBookTitles]'s call. A failure here costs these acronyms,
     * not the build: an unreadable Acronymizer has already failed it per book.
     */
    private suspend fun carryAcronymsAcrossRenames() {
        try {
            carryAcronymsAcrossRenames(acronymizer() ?: return)
        } catch (e: Exception) {
            logger.w(e) { "Skipping acronyms of renamed books: cannot read $acronymDbPath" }
        }
    }

    private suspend fun carryAcronymsAcrossRenames(lookup: AcronymizerLookup) {
        val books = repository.getAllBooks()
        val sourceNames = repository.getAllSources().associate { it.id to it.name }
        val withAcronyms = books.filter { repository.getAcronymsForBook(it.id).isNotEmpty() }.mapTo(HashSet()) { it.id }
        val targets = books
            .filter { it.id !in withAcronyms && lookup.rawTerms(it.title).isEmpty() }
            .mapTo(HashSet()) { it.id }
        val library = books.map { LibraryBook(it.id, it.title, sourceNames[it.sourceId].orEmpty()) }
        val formerTitles = RenamedBookTitles().resolve(
            books = library,
            targets = targets,
            knownKeys = allocator.knownBookKeys(),
            lineHashes = allocator::lineContentHashes,
        )
        val titles = books.associate { it.id to it.title }
        var carriedTerms = 0
        val carried = ArrayList<String>()
        for ((bookId, formers) in formerTitles.byBookId) {
            val title = titles.getValue(bookId)
            val terms = AcronymizerLookup.clean(
                formers.flatMap { lookup.rawTerms(it.title) + it.title },
                title,
            )
            if (terms.isEmpty()) continue
            repository.bulkInsertBookAcronyms(bookId, terms)
            carriedTerms += terms.size
            carried += "'${formers.joinToString("', '") { it.title }}' → '$title' (${terms.size}, ${formers.joinToString { it.evidence }})"
        }
        // A refused rename only costs something when the old title had acronyms.
        val lostNames = formerTitles.refused.filter { lookup.rawTerms(it.oldTitle).isNotEmpty() }
        logger.i {
            "Acronyms of renamed books: $carriedTerms terms carried to ${carried.size} books" +
                (if (carried.isEmpty()) "" else ": " + carried.take(MAX_NAMES_PER_SUMMARY_LINE).joinToString("; ")) +
                (if (carried.size > MAX_NAMES_PER_SUMMARY_LINE) "; …" else "")
        }
        if (lostNames.isNotEmpty()) {
            logger.i {
                "Acronyms of renamed books: ${lostNames.size} renames not followed (add the new title " +
                    "to the Acronymizer if it is the same book): " +
                    lostNames.take(MAX_NAMES_PER_SUMMARY_LINE).joinToString("; ") {
                        "'${it.oldTitle}' → '${it.newTitle}' (${it.reason})"
                    }
            }
        }
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

        // hasSourceConnection: target side of a SOURCE_VIEW_TYPES link.
        runCatching { repository.recomputeHasSourceConnection() }

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
     * Validate the entire file before allocating IDs or writing links. A corrupt
     * final JSON record must discard the whole input, as decodeFromString<List>
     * did; consuming a lazy decoder directly in the SQL loop would import a
     * partial file. Large valid inputs spill their normalized records to disk.
     */
    @OptIn(ExperimentalSerializationApi::class)
    private suspend fun prepareLinkInput(path: Path, bookTitle: String): PreparedStrings {
        try {
            return PreparedStrings.Builder(inputPartBytes, inputSpillDirectory).use { builder ->
                Files.newInputStream(path).buffered().withoutUtf8Bom().use { stream ->
                    val records = json.decodeToSequence<LinkData>(stream, DecodeSequenceMode.ARRAY_WRAPPED)
                    for (record in records) {
                        currentCoroutineContext().ensureActive()
                        builder.add(json.encodeToString(record))
                    }
                }
                builder.finish()
            }
        } catch (error: SerializationException) {
            // Dicta maps have never produced link rows (conversion is still
            // unimplemented); malformed arrays also produce zero rows.
            logger.w {
                "Failed to parse links for '$bookTitle' as an authored link array " +
                    "(${error::class.simpleName}: ${error.message?.take(120)?.oneLine()}); " +
                    "DictaToOtzaria object conversion is not implemented"
            }
            return PreparedStrings.Builder(inputPartBytes, inputSpillDirectory).use { it.finish() }
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
