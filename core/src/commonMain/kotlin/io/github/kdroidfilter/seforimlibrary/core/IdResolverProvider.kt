package io.github.kdroidfilter.seforimlibrary.core

/**
 * Interface for resolving stable IDs across database regenerations.
 * Implementations can provide ID lookup from a previous database.
 */
interface IdResolverProvider {
    fun resolveBookId(filePath: String?, sourceId: Long, fileType: String = "txt"): Long?
    fun resolveLineId(bookId: Long, lineIndex: Int): Long?
    fun resolveLinkId(srcBookId: Long, tgtBookId: Long, srcLineId: Long, tgtLineId: Long, typeId: Long): Long?
    fun resolveTocEntryId(bookId: Long, lineId: Long, level: Int, parentId: Long?): Long?
    fun resolveCategoryId(title: String, parentId: Long?, level: Int): Long?
    fun resolveSourceId(name: String): Long?
    fun resolveAuthorId(name: String): Long?
    fun resolveGenerationId(name: String): Long?
    fun resolveTopicId(name: String): Long?
    fun resolveConnectionTypeId(name: String): Long?
    fun resolvePubPlaceId(name: String): Long?
    fun resolvePubDateId(date: String): Long?
    fun resolveTocTextId(text: String): Long?
    fun allocateTocTextId(text: String): Long
    fun registerTocTextId(text: String, id: Long)
    
    fun getPreviousTextIdForTocEntry(tocEntryId: Long): Long? = null
    fun getTocTextUsageCount(textId: Long): Int = 0

    /**
     * Get all connection type mappings from the previous database.
     * This allows pre-populating connection types with stable IDs.
     * @return Map ction type name to ID, or empty map if not available
     */
    fun getAllConnectionTypeMappings(): Map<String, Long> = emptyMap()
}
