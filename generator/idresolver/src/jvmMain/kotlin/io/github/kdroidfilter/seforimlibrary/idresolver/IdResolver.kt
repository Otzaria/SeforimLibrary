package io.github.kdroidfilter.seforimlibrary.idresolver

import co.touchlab.kermit.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * IdResolver maintains stable IDs across database regenerations.
 * This class is thread-safe for concurrent access from multiple threads.
 * 
 * It loads existing ID mappings from a previous database and provides
 * lookup functions that either return an existing ID or allocate a new one.
 * 
 * Key mappings:
 * - book: filePath|sourceId|fileType → id
 * - line: bookId|lineIndex → id
 * - link: sourceBookId|targetBookId|sourceLineId|targetLineId|connectionTypeId → id
 * - tocEntry: bookId|textId|level|parentId → id
 * - tocText: text → id
 * - category: title|parentId|level → id
 * - author: name → id
 * - topic: name → id
 * - source: name → id
 ction_type: name → id
 * - pub_place: name → id
 * - pub_date: date → id
 * - alt_toc_structure: bookId|key → id
 */
class IdResolver private constructor(
    // Lookup maps from previous DB
    private val bookIdMap: Map<String, Long>,
    private val lineIdMap: Map<String, Long>,
    private val linkIdMap: Map<String, Long>,
    private val tocEntryIdMap: Map<String, Long>,
    private val tocTextIdMap: Map<String, Long>,
    private val categoryIdMap: Map<String, Long>,
    private val authorIdMap: Map<String, Long>,
    private val generationIdMap: Map<String, Long>,
    private val topicIdMap: Map<String, Long>,
    private val sourceIdMap: Map<String, Long>,
    private val connectionTypeIdMap: Map<String, Long>,
    private val pubPlaceIdMap: Map<String, Long>,
    private val pubDateIdMap: Map<String, Long>,
    private val altTocStructureIdMap: Map<String, Long>,
    private val altTocEntryIdMap: Map<String, Long>,
    private val tocEntryIdToTextIdMap: Map<Long, Long>,
    private val tocTextUsageCountMap: Map<Long, Int>,
    // Starting counters for new IDs
    initialMaxBookId: Long,
    initialMaxLineId: Long,
    initialMaxLinkId: Long,
    initialMaxTocEntryId: Long,
    initialMaxTocTextId: Long,
    initialMaxCategoryId: Long,
    initialMaxAuthorId: Long,
    initialMaxGenerationId: Long,
    initialMaxTopicId: Long,
    initialMaxSourceId: Long,
    initialMaxConnectionTypeId: Long,
    initialMaxPubPlaceId: Long,
    initialMaxPubDateId: Long,
    initialMaxAltTocStructureId: Long,
    initialMaxAltTocEntryId: Long
) {
    private val logger = Logger.withTag("IdResolver")

    // Thread-safe counters for allocating new IDs
    private val nextBookId = AtomicLong(initialMaxBookId + 1)
    private val nextLineId = AtomicLong(initialMaxLineId + 1)
    private val nextLinkId = AtomicLong(initialMaxLinkId + 1)
    private val nextTocEntryId = AtomicLong(initialMaxTocEntryId + 1)
    private val nextTocTextId = AtomicLong(initialMaxTocTextId + 1)
    private val nextCategoryId = AtomicLong(initialMaxCategoryId + 1)
    private val nextAuthorId = AtomicLong(initialMaxAuthorId + 1)
    private val nextGenerationId = AtomicLong(initialMaxGenerationId + 1)
    private val nextTopicId = AtomicLong(initialMaxTopicId + 1)
    private val nextSourceId = AtomicLong(initialMaxSourceId + 1)
    private val nextConnectionTypeId = AtomicLong(initialMaxConnectionTypeId + 1)
    private val nextPubPlaceId = AtomicLong(initialMaxPubPlaceId + 1)
    private val nextPubDateId = AtomicLong(initialMaxPubDateId + 1)
    private val nextAltTocStructureId = AtomicLong(initialMaxAltTocStructureId + 1)
    private val nextAltTocEntryId = AtomicLong(initialMaxAltTocEntryId + 1)

    // Thread-safe maps for tracking newly allocated IDs
    private val newlyAllocatedBooks = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedLines = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedLinks = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedTocEntries = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedTocTexts = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedAltTocStructures = ConcurrentHashMap<String, Long>()
    private val newlyAllocatedAltTocEntries = ConcurrentHashMap<String, Long>()

    // ===== Book =====
    
    /**
     * Resolve book ID by filePath + sourceId + fileType.
     * Returns existing ID if found, otherwise allocates a new one.
     * Thread-safe.
     */
    fun resolveBookId(filePath: String?, sourceId: Long, fileType: String = "txt"): Long {
        val key = buildBookKey(filePath, sourceId, fileType)
        
        // First check the loaded map (immutable, thread-safe)
        bookIdMap[key]?.let { return it }
        
        // Thread-safe: computeIfAbsent only calls the lambda once per key
        return newlyAllocatedBooks.computeIfAbsent(key) {
            val newId = nextBookId.getAndIncrement()
            logger.d { "Allocated new book ID $newId for key: $key" }
            newId
        }
    }

    /**
     * Check if a book ID already exists for the given key.
     */
    fun hasExistingBookId(filePath: String?, sourceId: Long, fileType: String = "txt"): Boolean {
        val key = buildBookKey(filePath, sourceId, fileType)
        return bookIdMap.containsKey(key) || newlyAllocatedBooks.containsKey(key)
    }

    // ===== Line =====
    
    /**
     * Resolve line ID by bookId + lineIndex.
     * Returns existing ID if found, otherwise allocates a new one.
     * Thread-safe.
     */
    fun resolveLineId(bookId: Long, lineIndex: Int): Long {
        val key = buildLineKey(bookId, lineIndex)
        
        // First check the loaded map (immutable, thread-safe)
        lineIdMap[key]?.let { return it }
        
        // Thread-safe: computeIfAbsent only calls the lambda once per key
        return newlyAllocatedLines.computeIfAbsent(key) {
            nextLineId.getAndIncrement()
        }
    }

    // ===== Link =====
    
    /**
     * Resolve link ID by source/target book+line IDs and connection type.
     * Returns existing ID if found, otherwise allocates a new one.
     * Thread-safe.
     */
    fun resolveLinkId(
        sourceBookId: Long,
        targetBookId: Long,
        sourceLineId: Long,
        targetLineId: Long,
        connectionTypeId: Long
    ): Long {
        val key = buildLinkKey(sourceBookId, targetBookId, sourceLineId, targetLineId, connectionTypeId)
        
        // First check the loaded map (immutable, thread-safe)
        linkIdMap[key]?.let { return it }
        
        // Thread-safe: computeIfAbsent only calls the lambda once per key
        return newlyAllocatedLinks.computeIfAbsent(key) {
            nextLinkId.getAndIncrement()
        }
    }

    // ===== TocEntry =====
    
    /**
     * Resolve TOC entry ID by bookId + lineId + level + parentId.
     * Use lineId instead of textId to keep ID stable even if text changes.
     * Thread-safe.
     */
    fun resolveTocEntryId(bookId: Long, lineId: Long, level: Int, parentId: Long?): Long {
        val key = buildTocEntryKey(bookId, lineId, level, parentId)
        tocEntryIdMap[key]?.let { return it }
        return newlyAllocatedTocEntries.computeIfAbsent(key) {
            nextTocEntryId.getAndIncrement()
        }
    }

    // ===== TocText =====
    
    /**
     * Resolve TOC text ID by text content. Returns null if not found.
     */
    fun resolveTocTextId(text: String): Long? {
        return tocTextIdMap[text] ?: newlyAllocatedTocTexts[text]
    }

    /**
     * Allocate a new TOC text ID (or return existing if already allocated).
     */
    fun allocateTocTextId(text: String): Long {
        tocTextIdMap[text]?.let { return it }
        return newlyAllocatedTocTexts.computeIfAbsent(text) {
            nextTocTextId.getAndIncrement()
        }
    }

    /**
     * Register a specific ID for a text string (used when reusing old IDs for corrected text).
     */
    fun registerTocTextId(text: String, id: Long) {
        if (!tocTextIdMap.containsKey(text)) {
            newlyAllocatedTocTexts[text] = id
        }
    }

    /**
     * Get the previous TextID associated with a TocEntryID in the old database.
     */
    fun getPreviousTextIdForTocEntry(tocEntryId: Long): Long? {
        return tocEntryIdToTextIdMap[tocEntryId]
    }

    /**
     * Get the usage count of a TextID in the previous database.
     * This helps determine if a TextID is shared by multiple TocEntries.
     */
    fun getTocTextUsageCount(textId: Long): Int {
        return tocTextUsageCountMap[textId] ?: 0
    }

    // ===== Category =====
    
    /**
     * Resolve category ID by title + parentId + level.
     */
    fun resolveCategoryId(title: String, parentId: Long?, level: Int): Long? {
        val key = buildCategoryKey(title, parentId, level)
        return categoryIdMap[key]
    }

    // ===== Author =====
    
    /**
     * Resolve author ID by name.
     */
    fun resolveAuthorId(name: String): Long? {
        return authorIdMap[name.trim()]
    }

    // ===== Generation =====
    
    /**
     * Resolve generation ID by name.
     */
    fun resolveGenerationId(name: String): Long? {
        return generationIdMap[name.trim()]
    }

    // ===== Topic =====
    
    /**
     * Resolve topic ID by name.
     */
    fun resolveTopicId(name: String): Long? {
        return topicIdMap[name.trim()]
    }

    // ===== Source =====
    
    /**
     * Resolve source ID by name.
     */
    fun resolveSourceId(name: String): Long? {
        return sourceIdMap[name.trim()]
    }

    // ===== ConnectionType =====
    
    /**
     * Resolve connection type ID by name.
     */
    fun resolveConnectionTypeId(name: String): Long? {
        return connectionTypeIdMap[name.trim()]
    }

    /**
     * Get all connection type mappings (name → id) from the previous database.
     * This is used to pre-populate connection types with stable IDs.
     */
    fun getAllConnectionTypeMappings(): Map<String, Long> {
        return connectionTypeIdMap
    }

    // ===== PubPlace =====
    
    /**
     * Resolve publication place ID by name.
     */
    fun resolvePubPlaceId(name: String): Long? {
        return pubPlaceIdMap[name.trim()]
    }

    // ===== PubDate =====
    
    /**
     * Resolve publication date ID by date string.
     */
    fun resolvePubDateId(date: String): Long? {
        return pubDateIdMap[date.trim()]
    }

    // ===== AltTocStructure =====
    
    /**
     * Resolve alt TOC structure ID by bookId + key.
     */
    fun resolveAltTocStructureId(bookId: Long, key: String): Long {
        val mapKey = buildAltTocStructureKey(bookId, key)
        altTocStructureIdMap[mapKey]?.let { return it }
        return newlyAllocatedAltTocStructures.computeIfAbsent(mapKey) {
            nextAltTocStructureId.getAndIncrement()
        }
    }

    /**
     * Resolve alt TOC entry ID by structureId + parentId + level + lineId + text.
     * Returns existing ID if found, otherwise allocates a new one from the end of the old table.
     */
    fun resolveAltTocEntryId(
        structureId: Long,
        parentId: Long?,
        level: Int,
        lineId: Long?,
        text: String
    ): Long {
        val mapKey = buildAltTocEntryKey(structureId, parentId, level, lineId, text)
        altTocEntryIdMap[mapKey]?.let { return it }
        return newlyAllocatedAltTocEntries.computeIfAbsent(mapKey) {
            nextAltTocEntryId.getAndIncrement()
        }
    }

    // ===== Key Builders =====

    companion object {
        private const val SEPARATOR = "|"

        fun buildBookKey(filePath: String?, sourceId: Long, fileType: String): String {
            return "${filePath ?: ""}$SEPARATOR$sourceId$SEPARATOR${fileType.ifEmpty { "txt" }}"
        }

        fun buildLineKey(bookId: Long, lineIndex: Int): String {
            return "$bookId$SEPARATOR$lineIndex"
        }

        fun buildLinkKey(
            sourceBookId: Long,
            targetBookId: Long,
            sourceLineId: Long,
            targetLineId: Long,
            connectionTypeId: Long
        ): String {
            return "$sourceBookId$SEPARATOR$targetBookId$SEPARATOR$sourceLineId$SEPARATOR$targetLineId$SEPARATOR$connectionTypeId"
        }

        fun buildTocEntryKey(bookId: Long, lineId: Long, level: Int, parentId: Long?): String {
            return "$bookId$SEPARATOR$lineId$SEPARATOR$level$SEPARATOR${parentId ?: "null"}"
        }

        fun buildCategoryKey(title: String, parentId: Long?, level: Int): String {
            return "${title.trim()}$SEPARATOR${parentId ?: "null"}$SEPARATOR$level"
        }

        fun buildAltTocStructureKey(bookId: Long, key: String): String {
            return "$bookId$SEPARATOR${key.trim()}"
        }

        fun buildAltTocEntryKey(
            structureId: Long,
            parentId: Long?,
            level: Int,
            lineId: Long?,
            text: String
        ): String {
            return "$structureId$SEPARATOR${parentId ?: "null"}$SEPARATOR$level$SEPARATOR${lineId ?: "null"}$SEPARATOR${text.trim()}"
        }

        /**
         * Create an empty IdResolver (no previous DB).
         */
        fun empty(): IdResolver {
            return IdResolver(
                bookIdMap = emptyMap(),
                lineIdMap = emptyMap(),
                linkIdMap = emptyMap(),
                tocEntryIdMap = emptyMap(),
                tocTextIdMap = emptyMap(),
                categoryIdMap = emptyMap(),
                authorIdMap = emptyMap(),
                generationIdMap = emptyMap(),
                topicIdMap = emptyMap(),
                sourceIdMap = emptyMap(),
                connectionTypeIdMap = emptyMap(),
                pubPlaceIdMap = emptyMap(),
                pubDateIdMap = emptyMap(),
                altTocStructureIdMap = emptyMap(),
                altTocEntryIdMap = emptyMap(),
                tocEntryIdToTextIdMap = emptyMap(),
                tocTextUsageCountMap = emptyMap(),
                initialMaxBookId = 0,
                initialMaxLineId = 0,
                initialMaxLinkId = 0,
                initialMaxTocEntryId = 0,
                initialMaxTocTextId = 0,
                initialMaxCategoryId = 0,
                initialMaxAuthorId = 0,
                initialMaxGenerationId = 0,
                initialMaxTopicId = 0,
                initialMaxSourceId = 0,
                initialMaxConnectionTypeId = 0,
                initialMaxPubPlaceId = 0,
                initialMaxPubDateId = 0,
                initialMaxAltTocStructureId = 0,
                initialMaxAltTocEntryId = 0
            )
        }

        /**
         * Create an IdResolver from loaded data.
         */
        fun fromLoadedData(data: IdResolverData): IdResolver {
            return IdResolver(
                bookIdMap = data.bookIdMap,
                lineIdMap = data.lineIdMap,
                linkIdMap = data.linkIdMap,
                tocEntryIdMap = data.tocEntryIdMap,
                tocTextIdMap = data.tocTextIdMap,
                categoryIdMap = data.categoryIdMap,
                authorIdMap = data.authorIdMap,
                generationIdMap = data.generationIdMap,
                topicIdMap = data.topicIdMap,
                sourceIdMap = data.sourceIdMap,
                connectionTypeIdMap = data.connectionTypeIdMap,
                pubPlaceIdMap = data.pubPlaceIdMap,
                pubDateIdMap = data.pubDateIdMap,
                altTocStructureIdMap = data.altTocStructureIdMap,
                altTocEntryIdMap = data.altTocEntryIdMap,
                tocEntryIdToTextIdMap = data.tocEntryIdToTextIdMap,
                tocTextUsageCountMap = data.tocTextUsageCountMap,
                initialMaxBookId = data.maxBookId,
                initialMaxLineId = data.maxLineId,
                initialMaxLinkId = data.maxLinkId,
                initialMaxTocEntryId = data.maxTocEntryId,
                initialMaxTocTextId = data.maxTocTextId,
                initialMaxCategoryId = data.maxCategoryId,
                initialMaxAuthorId = data.maxAuthorId,
                initialMaxGenerationId = data.maxGenerationId,
                initialMaxTopicId = data.maxTopicId,
                initialMaxSourceId = data.maxSourceId,
                initialMaxConnectionTypeId = data.maxConnectionTypeId,
                initialMaxPubPlaceId = data.maxPubPlaceId,
                initialMaxPubDateId = data.maxPubDateId,
                initialMaxAltTocStructureId = data.maxAltTocStructureId,
                initialMaxAltTocEntryId = data.maxAltTocEntryId
            )
        }
    }

    // ===== Statistics =====

    fun getStatistics(): IdResolverStats {
        return IdResolverStats(
            loadedBooks = bookIdMap.size,
            loadedLines = lineIdMap.size,
            loadedLinks = linkIdMap.size,
            loadedTocEntries = tocEntryIdMap.size,
            loadedTocTexts = tocTextIdMap.size,
            loadedCategories = categoryIdMap.size,
            loadedAuthors = authorIdMap.size,
            loadedGenerations = generationIdMap.size,
            loadedTopics = topicIdMap.size,
            loadedSources = sourceIdMap.size,
            loadedConnectionTypes = connectionTypeIdMap.size,
            newlyAllocatedBooks = newlyAllocatedBooks.size,
            newlyAllocatedLines = newlyAllocatedLines.size,
            newlyAllocatedLinks = newlyAllocatedLinks.size,
            newlyAllocatedTocEntries = newlyAllocatedTocEntries.size,
            newlyAllocatedTocTexts = newlyAllocatedTocTexts.size
        )
    }
}

/**
 * Data class holding all loaded ID mappings from a previous database.
 */
data class IdResolverData(
    val bookIdMap: Map<String, Long>,
    val lineIdMap: Map<String, Long>,
    val linkIdMap: Map<String, Long>,
    val tocEntryIdMap: Map<String, Long>,
    val tocTextIdMap: Map<String, Long>,
    val categoryIdMap: Map<String, Long>,
    val authorIdMap: Map<String, Long>,
    val generationIdMap: Map<String, Long>,
    val topicIdMap: Map<String, Long>,
    val sourceIdMap: Map<String, Long>,
    val connectionTypeIdMap: Map<String, Long>,
    val pubPlaceIdMap: Map<String, Long>,
    val pubDateIdMap: Map<String, Long>,
    val altTocStructureIdMap: Map<String, Long>,
    val altTocEntryIdMap: Map<String, Long>,
    val tocEntryIdToTextIdMap: Map<Long, Long>,
    val tocTextUsageCountMap: Map<Long, Int>,
    val maxBookId: Long,
    val maxLineId: Long,
    val maxLinkId: Long,
    val maxTocEntryId: Long,
    val maxTocTextId: Long,
    val maxCategoryId: Long,
    val maxAuthorId: Long,
    val maxGenerationId: Long,
    val maxTopicId: Long,
    val maxSourceId: Long,
    val maxConnectionTypeId: Long,
    val maxPubPlaceId: Long,
    val maxPubDateId: Long,
    val maxAltTocStructureId: Long,
    val maxAltTocEntryId: Long
)

/**
 * Statistics about loaded and newly allocated IDs.
 */
data class IdResolverStats(
    val loadedBooks: Int,
    val loadedLines: Int,
    val loadedLinks: Int,
    val loadedTocEntries: Int,
    val loadedTocTexts: Int,
    val loadedCategories: Int,
    val loadedAuthors: Int,
    val loadedGenerations: Int,
    val loadedTopics: Int,
    val loadedSources: Int,
    val loadedConnectionTypes: Int,
    val newlyAllocatedBooks: Int,
    val newlyAllocatedLines: Int,
    val newlyAllocatedLinks: Int,
    val newlyAllocatedTocEntries: Int,
    val newlyAllocatedTocTexts: Int
) {
    override fun toString(): String {
        return """
            IdResolver Statistics:
            - Loaded books: $loadedBooks
            - Loaded lines: $loadedLines
            - Loaded links: $loadedLinks
            - Loaded tocEntries: $loadedTocEntries
            - Loaded tocTexts: $loadedTocTexts
            - Loaded categories: $loadedCategories
            - Loaded authors: $loadedAuthors
            - Loaded generations: $loadedGenerations
            - Newly allocated books: $newlyAllocatedBooks
            - Newly allocated lines: $newlyAllocatedLines
            - Newly allocated links: $newlyAllocatedLinks
            - Newly allocated tocEntries: $newlyAllocatedTocEntries
            - Newly allocated tocTexts: $newlyAllocatedTocTexts
        """.trimIndent()
    }

    fun estimatedMemoryUsageMB(): Double {
        // Rough estimation: each map entry ~100 bytes average
        val totalEntries = loadedBooks + loadedLines + loadedLinks + 
            loadedTocEntries + loadedTocTexts + loadedCategories + 
            loadedAuthors + loadedTopics + loadedSources + loadedConnectionTypes +
            loadedGenerations
        return (totalEntries * 100.0) / (1024 * 1024)
    }
}
