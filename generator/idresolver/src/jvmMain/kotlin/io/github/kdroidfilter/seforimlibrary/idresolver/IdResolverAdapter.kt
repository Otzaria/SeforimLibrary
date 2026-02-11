package io.github.kdroidfilter.seforimlibrary.idresolver

import io.github.kdroidfilter.seforimlibrary.core.IdResolverProvider

/**
 * Adapter that wraps IdResolver and implements IdResolverProvider interface.
 * This allows the Generator to use IdResolver for stable ID resolution.
 */
class IdResolverAdapter(private val resolver: IdResolver) : IdResolverProvider {

    override fun resolveBookId(filePath: String?, sourceId: Long, fileType: String): Long {
        return resolver.resolveBookId(filePath, sourceId, fileType)
    }

    override fun resolveLineId(bookId: Long, lineIndex: Int): Long {
        return resolver.resolveLineId(bookId, lineIndex)
    }

    override fun resolveLinkId(
        srcBookId: Long,
        tgtBookId: Long,
        srcLineId: Long,
        tgtLineId: Long,
        typeId: Long
    ): Long {
        return resolver.resolveLinkId(srcBookId, tgtBookId, srcLineId, tgtLineId, typeId)
    }

    override fun resolveTocEntryId(bookId: Long, lineId: Long, level: Int, parentId: Long?): Long? {
        return resolver.resolveTocEntryId(bookId, lineId, level, parentId)
    }

    override fun resolveCategoryId(title: String, parentId: Long?, level: Int): Long? {
        return resolver.resolveCategoryId(title, parentId, level)
    }

    override fun resolveSourceId(name: String): Long? {
        return resolver.resolveSourceId(name)
    }

    override fun resolveAuthorId(name: String): Long? {
        return resolver.resolveAuthorId(name)
    }

    override fun resolveGenerationId(name: String): Long? {
        return resolver.resolveGenerationId(name)
    }

    override fun resolveTopicId(name: String): Long? {
        return resolver.resolveTopicId(name)
    }

    override fun resolveConnectionTypeId(name: String): Long? {
        return resolver.resolveConnectionTypeId(name)
    }

    override fun resolvePubPlaceId(name: String): Long? {
        return resolver.resolvePubPlaceId(name)
    }

    override fun resolvePubDateId(date: String): Long? {
        return resolver.resolvePubDateId(date)
    }

    override fun resolveTocTextId(text: String): Long? {
        return resolver.resolveTocTextId(text)
    }

    override fun allocateTocTextId(text: String): Long {
        return resolver.allocateTocTextId(text)
    }

    override fun registerTocTextId(text: String, id: Long) {
        resolver.registerTocTextId(text, id)
    }

    override fun getPreviousTextIdForTocEntry(tocEntryId: Long): Long? {
        return resolver.getPreviousTextIdForTocEntry(tocEntryId)
    }

    override fun getTocTextUsageCount(textId: Long): Int {
        return resolver.getTocTextUsageCount(textId)
    }

    override fun getAllConnectionTypeMappings(): Map<String, Long> {
        return resolver.getAllConnectionTypeMappings()
    }

    fun getStatistics(): IdResolverStats = resolver.getStatistics()
}
