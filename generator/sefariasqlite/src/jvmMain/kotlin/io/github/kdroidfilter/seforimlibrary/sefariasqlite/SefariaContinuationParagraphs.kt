package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.LinkCoverage
import io.github.kdroidfilter.seforimlibrary.core.models.LinkRange
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

/**
 * Sefaria often links only the first segment of a multi-paragraph comment
 * ("Abarbanel on Torah, Genesis 1:1:1" is linked, 1:1:2 is not), so the
 * reader's commentary panel showed only that first paragraph (forum topic 1911).
 *
 * This pass extends the commentary-side range of such a link over the
 * unlinked segments that follow it under the SAME parent address, up to the
 * next linked segment. A segment whose address has no numeric parent
 * ("Chatam Sofer on Torah, Bereshit 5") is never extended: its siblings span
 * a whole parasha, not one verse or daf. Only commentary books are walked, and
 * a run stops at a segment that opens a new comment (see [opensNewComment]).
 */
internal class SefariaContinuationParagraphs(
    private val repository: SeforimRepository,
    private val logger: Logger,
) {
    suspend fun generate(
        refsByPath: Map<String, List<RefEntry>>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        headingLineIds: Set<Long>,
        commentaryBookIds: Set<Long>,
        lineIdToBookId: Map<Long, Long>,
    ) {
        val typeNames = COMMENTARY_PANEL_TYPES.map { it.name }
        val covered = repository.selectCoveredTargetLineIds(typeNames)
        val isCovered = { lineId: Long -> java.util.Arrays.binarySearch(covered, lineId) >= 0 }
        val (endLineIds, linkIds) = repository.selectTargetSideEnds(typeNames)
        val ends = TargetSideEnds(endLineIds, linkIds)

        val candidates = ArrayList<ContinuationRun>()
        for ((path, entries) in refsByPath) {
            val firstLineId = entries.firstNotNullOfOrNull { lineKeyToId[path to it.lineIndex - 1] } ?: continue
            if (lineIdToBookId[firstLineId] !in commentaryBookIds) continue
            candidates += findContinuationRuns(path, entries, lineKeyToId, headingLineIds, isCovered, ends::linkIdsEndingAt)
        }
        val contentById = HashMap<Long, String>()
        candidates.flatMap { it.lineIds }.chunked(500).forEach { chunk ->
            repository.getLinesByIds(chunk).forEach { contentById[it.id] = it.content }
        }

        val ranges = ArrayList<LinkRange>()
        val coverage = ArrayList<LinkCoverage>()
        var runs = 0
        var lines = 0
        for (candidate in candidates) {
            val run = candidate.untilNewComment { opensNewComment(contentById[it].orEmpty()) } ?: continue
            runs++
            lines += run.lineIds.size
            for (linkId in run.linkIds) {
                ranges += LinkRange(linkId, side = 1, endLineId = run.lineIds.last(), endLineIndex = run.endLineIndex0)
                run.lineIds.forEach { coverage += LinkCoverage(lineId = it, linkId = linkId, side = 1) }
            }
        }
        ranges.chunked(SefariaImportTuning.LINK_BATCH_SIZE).forEach { repository.insertLinkRangesBatch(it) }
        coverage.chunked(SefariaImportTuning.LINK_BATCH_SIZE).forEach { repository.insertLinkCoverageBatch(it) }
        logger.i {
            "Continuation paragraphs: $lines unlinked segments joined to the preceding comment " +
                "in $runs runs (${ranges.size} range rows, ${coverage.size} coverage rows)"
        }
    }

    companion object {
        val COMMENTARY_PANEL_TYPES = listOf(ConnectionType.COMMENTARY, ConnectionType.SUPER_COMMENTARY)
    }
}

// A segment opening with a locator (ברש״י בד״ה…, בתד״ה…, פר״ח ד״ה…, תוס׳…) or a gloss (נ״ב…).
private val NEW_COMMENT_LOCATOR = Regex(
    """^\s*(?:ב?(?:רש[\"״]י|תוס['׳]|תוספות|ד[\"״]ה|תוד[\"״]ה|תד[\"״]ה|גמ['׳]|גמרא|מתני['׳]|מתניתין)|נ[\"״]ב|\S+\s+ב?ד[\"״]ה)(?=\s|$|[.,:])"""
)

// An unmarked dibbur: up to three words closed by a period ("שני שמות. משום…").
private val SHORT_DIBBUR = Regex("""^\s*(?:\S+\s+){0,2}\S+\.\s""")
private val TAG = Regex("<[^>]+>")

/**
 * True when [content] opens its own dibbur hamatchil (bold or dash shape, per
 * [DhExtractor], or a short period-closed one) or a locator — a new comment.
 */
internal fun opensNewComment(content: String): Boolean {
    if (DhExtractor.extract(content, DhExtractor.Format.BOLD) != null) return true
    if (DhExtractor.extract(content, DhExtractor.Format.DASH) != null) return true
    val plain = TAG.replace(content, "")
    return NEW_COMMENT_LOCATOR.containsMatchIn(plain) || SHORT_DIBBUR.containsMatchIn(plain)
}

/** Unlinked segments [lineIds] that continue the comment of [linkIds]. */
internal data class ContinuationRun(
    val linkIds: List<Long>,
    val lineIds: List<Long>,
    val endLineIndex0: Int,
) {
    /** The run cut before its first segment that [startsNewComment]; null when nothing is left. */
    fun untilNewComment(startsNewComment: (Long) -> Boolean): ContinuationRun? {
        val cut = lineIds.indexOfFirst(startsNewComment)
        return when (cut) {
            -1 -> this
            0 -> null
            else -> copy(lineIds = lineIds.take(cut), endLineIndex0 = endLineIndex0 - (lineIds.size - cut))
        }
    }
}

/**
 * Link ids by the line where their commentary side ends, as two parallel
 * arrays sorted by [endLineIds] (compact: millions of links).
 */
internal class TargetSideEnds(private val endLineIds: LongArray, private val linkIds: LongArray) {
    fun linkIdsEndingAt(lineId: Long): List<Long> {
        var i = java.util.Arrays.binarySearch(endLineIds, lineId)
        if (i < 0) return emptyList()
        while (i > 0 && endLineIds[i - 1] == lineId) i--
        val out = ArrayList<Long>(2)
        while (i < endLineIds.size && endLineIds[i] == lineId) out += linkIds[i++]
        return out
    }
}

/** "title 1:1:2" → "title 1:1"; null when the address has a single component. */
internal fun parentAddress(canonical: String): String? {
    val prefixes = addressPrefixes(canonical)
    return if (prefixes.size < 2) null else prefixes[prefixes.size - 2]
}

internal fun findContinuationRuns(
    path: String,
    entries: List<RefEntry>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    headingLineIds: Set<Long>,
    isCovered: (Long) -> Boolean,
    linksEndingAt: (Long) -> List<Long>,
): List<ContinuationRun> {
    val runs = ArrayList<ContinuationRun>()
    var anchorLinks: List<Long> = emptyList()
    var anchorParent: String? = null
    val run = ArrayList<Long>()
    var runEnd0 = -1
    var prevLineIndex = -1

    fun flush() {
        if (run.isNotEmpty() && anchorLinks.isNotEmpty()) {
            runs += ContinuationRun(anchorLinks, run.toList(), runEnd0)
        }
        run.clear()
    }
    fun reset() {
        flush()
        anchorLinks = emptyList()
        anchorParent = null
    }

    for (entry in entries.sortedBy { it.lineIndex }.distinctBy { it.lineIndex }) {
        val lineIndex0 = entry.lineIndex - 1
        // A line without a ref (e.g. a heading) sits in the gap: never bridge it.
        if (prevLineIndex >= 0 && lineIndex0 != prevLineIndex + 1) reset()
        prevLineIndex = lineIndex0
        val lineId = lineKeyToId[path to lineIndex0]
        if (lineId == null || lineId in headingLineIds) {
            reset()
            continue
        }
        val parent = parentAddress(canonicalCitation(entry.ref))
        if (isCovered(lineId)) {
            flush()
            anchorParent = parent
            anchorLinks = if (parent == null) emptyList() else linksEndingAt(lineId)
            continue
        }
        if (anchorLinks.isNotEmpty() && parent == anchorParent) {
            run += lineId
            runEnd0 = lineIndex0
        } else {
            reset()
        }
    }
    flush()
    return runs
}
