package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.core.models.LinkAnchor
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

/**
 * Word-level anchors from mongo's `links.charLevelData` (quotation finder),
 * shipped in the links CSVs as `Char Level Data 1/2` cells.
 *
 * The offsets were computed against a specific version's segment text, so an
 * anchor is imported only when it is provably exact:
 *  - the anchored book was merged from exactly ONE version whose title equals
 *    the cell's `versionTitle` (merged text == version text verbatim), and
 *  - `cleanSefariaLine` did not modify the segment (the stored line is the raw
 *    segment, at most behind a known "(א) " prefix), and
 *  - the offsets are within the raw segment's bounds.
 * Word-based cells (startWord/endWord, Tanakh verse sides) are skipped —
 * Sefaria's word-splitting convention is not reproducible here exactly.
 * Everything skipped is counted and reported, never guessed.
 */
internal class SefariaCharLevelAnchors(
    private val repository: SeforimRepository,
    private val logger: Logger,
) {
    suspend fun generate(
        pending: Collection<PendingCharLevelAnchor>,
        books: List<SefariaInlineAnchors.BookInput>,
    ) {
        if (pending.isEmpty()) return
        val booksByPath = books.associateBy { it.bookPath }

        val skipCounts = linkedMapOf<String, Int>()
        fun skip(reason: String) = skipCounts.merge(reason, 1, Int::plus)

        var inserted = 0L
        val batch = mutableListOf<LinkAnchor>()
        for (entry in pending) {
            if (entry.isWordBased) {
                skip("word-based offsets")
                continue
            }
            if (entry.language.isNotEmpty() && entry.language != "he") {
                skip("non-hebrew offsets")
                continue
            }
            val book = booksByPath[entry.path]
            if (book == null) {
                skip("book not imported")
                continue
            }
            if (book.singleVersionTitle == null) {
                skip("multi-version book")
                continue
            }
            if (book.singleVersionTitle != entry.versionTitle) {
                skip("version mismatch")
                continue
            }
            val shift = book.cleanShiftByLineIndex[entry.lineIndex0] ?: 0
            if (shift == CLEAN_MODIFIED) {
                skip("line modified by cleaning")
                continue
            }
            val content = book.lines.getOrNull(entry.lineIndex0)
            if (content == null) {
                skip("line not found")
                continue
            }
            val rawLength = content.length - shift
            if (entry.startChar < 0 || entry.endChar > rawLength || entry.startChar >= entry.endChar) {
                skip("offsets out of bounds")
                continue
            }
            val linkIds = repository.getLinkIdsBetweenLines(entry.srcLineId, entry.tgtLineId)
            if (linkIds.isEmpty()) {
                skip("no stored link")
                continue
            }
            val charStart = countVisibleChars(content, shift + entry.startChar)
            val charEnd = countVisibleChars(content, shift + entry.endChar)
            for (linkId in linkIds) {
                batch += LinkAnchor(
                    linkId = linkId,
                    side = entry.side,
                    charStart = charStart,
                    charEnd = charEnd,
                    label = null,
                )
            }
            if (batch.size >= SefariaImportTuning.LINK_BATCH_SIZE) {
                inserted += batch.size
                repository.insertLinkAnchorsBatch(batch)
                batch.clear()
            }
        }
        if (batch.isNotEmpty()) {
            inserted += batch.size
            repository.insertLinkAnchorsBatch(batch)
            batch.clear()
        }

        logger.i { "Char-level anchors: $inserted inserted from ${pending.size} CSV cells" }
        if (skipCounts.isNotEmpty()) {
            logger.i {
                "Char-level anchors skipped — " +
                    skipCounts.entries.joinToString("; ") { (r, c) -> "$r: $c" }
            }
        }
    }
}
