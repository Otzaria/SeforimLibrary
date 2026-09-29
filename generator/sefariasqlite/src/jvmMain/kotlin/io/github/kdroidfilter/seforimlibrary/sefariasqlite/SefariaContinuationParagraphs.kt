package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.LinkCoverage
import io.github.kdroidfilter.seforimlibrary.core.models.LinkRange
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull

/**
 * Extends sparse first-paragraph links only in curated, schema-verified
 * paragraph branches. Sibling comments on a verse or daf are independent:
 * the absence of a visible dibbur is never evidence of continuation.
 * Content and write buffers are bounded; link indexes are loaded per book.
 */
internal class SefariaContinuationParagraphs(
    private val repository: SeforimRepository,
    private val logger: Logger,
) {
    suspend fun generate(
        refsByPath: Map<String, List<RefEntry>>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        headingLineIds: Set<Long>,
        eligibleRefPrefixesByBookId: Map<Long, Set<String>>,
        lineIdToBookId: Map<Long, Long>,
    ) {
        if (eligibleRefPrefixesByBookId.isEmpty()) return
        val typeNames = COMMENTARY_PANEL_TYPES.map { it.name }
        // Only the five curated Torah base books enter this lookup. Incidental
        // same-corpus COMMENTARY citations are barriers, never anchors to extend.
        val baseTitles = eligibleRefPrefixesByBookId.values.flatten().map { it.removePrefix(CURATED_TITLE_PREFIX) }.toSet()
        val baseLineIdsByRef = HashMap<String, Long>()
        for ((path, entries) in refsByPath) {
            val first = entries.firstOrNull()?.ref?.let(::canonicalCitation) ?: continue
            if (baseTitles.none { first.startsWith("$it ") }) continue
            for (entry in entries) {
                val lineId = lineKeyToId[path to entry.lineIndex - 1] ?: continue
                val ref = canonicalCitation(entry.ref)
                val previous = baseLineIdsByRef.putIfAbsent(ref, lineId)
                if (previous != null && previous != lineId) baseLineIdsByRef[ref] = -1L
            }
        }
        val ranges = ArrayList<LinkRange>(SefariaImportTuning.LINK_BATCH_SIZE)
        val coverage = ArrayList<LinkCoverage>(SefariaImportTuning.LINK_BATCH_SIZE)
        var runs = 0L
        var lines = 0L
        var rangeRows = 0L
        var coverageRows = 0L
        suspend fun flush() {
            repository.insertLinkRangesBatch(ranges)
            repository.insertLinkCoverageBatch(coverage)
            ranges.clear()
            coverage.clear()
        }
        for ((path, entries) in refsByPath) {
            val firstLineId = entries.firstNotNullOfOrNull { lineKeyToId[path to it.lineIndex - 1] } ?: continue
            val bookId = lineIdToBookId[firstLineId] ?: continue
            val prefixes = eligibleRefPrefixesByBookId[bookId]?.takeIf { it.isNotEmpty() } ?: continue
            val covered = repository.selectCoveredTargetLineIds(typeNames, bookId)
            val (endLineIds, linkIds, sourceLineIds) = repository.selectTargetSideEndSources(typeNames, bookId)
            val ends = TargetSideEnds(endLineIds, linkIds, sourceLineIds)
            walkContinuationRuns(
                path, entries, lineKeyToId, headingLineIds,
                isCovered = { java.util.Arrays.binarySearch(covered, it) >= 0 },
                linksEndingAt = { lineId, parent ->
                    val expectedSource = baseLineIdsByRef[parent.removePrefix(CURATED_TITLE_PREFIX)]
                    ends.linkIdsEndingAt(lineId) { it == expectedSource && it > 0 }
                },
                isEligibleRef = { canonical -> isEligibleContinuationRef(canonical, prefixes) },
                maxRunSize = CONTENT_BATCH_SIZE,
            ) { candidate ->
                val contentById = repository.getLinesByIds(candidate.lineIds).associateBy { it.id }
                // Missing text is a hard boundary, never an empty continuation.
                val run = candidate.untilNewComment { id ->
                    contentById[id]?.let { opensNewComment(it.content) } ?: true
                }
                if (run != null) {
                    runs++
                    lines += run.lineIds.size
                    for (linkId in run.linkIds) {
                        ranges += LinkRange(linkId, side = 1, endLineId = run.lineIds.last(), endLineIndex = run.endLineIndex0)
                        rangeRows++
                        for (lineId in run.lineIds) {
                            coverage += LinkCoverage(lineId = lineId, linkId = linkId, side = 1)
                            coverageRows++
                            if (coverage.size >= SefariaImportTuning.LINK_BATCH_SIZE) flush()
                        }
                        if (ranges.size >= SefariaImportTuning.LINK_BATCH_SIZE) flush()
                    }
                }
                // A cutoff in a bounded chunk invalidates the anchor for its remaining tail.
                run?.lineIds?.size == candidate.lineIds.size
            }
            flush()
        }
        logger.i {
            "Continuation paragraphs: $lines segments in $runs bounded runs " +
                "($rangeRows range writes, $coverageRows coverage writes)"
        }
    }

    companion object {
        const val CONTENT_BATCH_SIZE = 500
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
 * arrays sorted by [endLineIds], scoped to one eligible book.
 */
internal class TargetSideEnds(
    private val endLineIds: LongArray,
    private val linkIds: LongArray,
    private val sourceLineIds: LongArray,
) {
    fun linkIdsEndingAt(lineId: Long, isValidSource: (Long) -> Boolean): List<Long> {
        var i = java.util.Arrays.binarySearch(endLineIds, lineId)
        if (i < 0) return emptyList()
        while (i > 0 && endLineIds[i - 1] == lineId) i--
        val out = ArrayList<Long>(2)
        while (i < endLineIds.size && endLineIds[i] == lineId) {
            if (isValidSource(sourceLineIds[i])) out += linkIds[i]
            i++
        }
        return out
    }
}

/** "title 1:1:2" → "title 1:1"; null when the address has a single component. */
internal fun parentAddress(canonical: String): String? {
    val colon = canonical.lastIndexOf(':')
    if (colon <= canonical.lastIndexOf(' ')) return null
    return canonical.substring(0, colon)
}

/**
 * A narrowly curated structural contract, not a title-pattern heuristic.
 * The verified Torah export stores paragraphs of a continuous exposition at
 * Chapter/Verse/Paragraph. Other Abarbanel books use Comment, as do independent
 * verse commentaries such as Rashi; none are automatically opted in.
 * Segment dibbur metadata (including inherited regexes) always vetoes opt-in.
 */
internal fun continuationRefPrefixes(title: String, schema: JsonObject): Set<String> {
    if (title != "Abarbanel on Torah") return emptySet()
    val result = linkedSetOf<String>()
    fun visit(node: JsonObject, prefix: String, inheritedDibbur: Boolean) {
        val dibbur = inheritedDibbur ||
            (node["isSegmentLevelDiburHamatchil"] as? JsonPrimitive)?.booleanOrNull == true ||
            ((node["diburHamatchilRegexes"] as? JsonArray)?.isNotEmpty() == true)
        val children = node["nodes"] as? JsonArray
        if (children != null) {
            for (child in children) {
                val obj = child as? JsonObject ?: continue
                val childTitle = (obj["title"] as? JsonPrimitive)?.contentOrNull.orEmpty()
                val isDefault = (obj["key"] as? JsonPrimitive)?.contentOrNull.equals("default", ignoreCase = true)
                visit(obj, if (isDefault || childTitle.isBlank()) prefix else "$prefix, $childTitle", dibbur)
            }
        } else {
            val sections = (node["sectionNames"] as? JsonArray)?.map { (it as? JsonPrimitive)?.contentOrNull }
            val types = (node["addressTypes"] as? JsonArray)?.map { (it as? JsonPrimitive)?.contentOrNull }
            val depth = (node["depth"] as? JsonPrimitive)?.intOrNull
            if (!dibbur && depth == 3 && sections == listOf("Chapter", "Verse", "Paragraph") &&
                types?.size == 3 && types[0] in setOf("Perek", "Integer") &&
                types[1] in setOf("Pasuk", "Integer") && types[2] == "Integer") {
                result += canonicalCitation(prefix)
            }
        }
    }
    visit(schema, title, false)
    return result
}

private const val CURATED_TITLE_PREFIX = "abarbanel on torah "
private val PARAGRAPH_ADDRESS = Regex("[0-9]+:[0-9]+:[0-9]+")

internal fun isEligibleContinuationRef(canonical: String, prefixes: Set<String>): Boolean =
    prefixes.any { prefix ->
        canonical.startsWith("$prefix ") && PARAGRAPH_ADDRESS.matches(canonical.substring(prefix.length + 1))
    }

/** Test adapter for the same ordered, bounded walker used by production. */
internal suspend fun findContinuationRuns(
    path: String,
    entries: List<RefEntry>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    headingLineIds: Set<Long>,
    isCovered: (Long) -> Boolean,
    linksEndingAt: (Long) -> List<Long>,
): List<ContinuationRun> {
    val runs = ArrayList<ContinuationRun>()
    walkContinuationRuns(path, entries, lineKeyToId, headingLineIds, isCovered, { lineId, _ -> linksEndingAt(lineId) },
        isEligibleRef = { true }, maxRunSize = Int.MAX_VALUE) { runs += it; true }
    return runs
}

/** [consume] returns false when a new-comment cutoff invalidates the active anchor. */
internal suspend fun walkContinuationRuns(
    path: String,
    entries: List<RefEntry>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    headingLineIds: Set<Long>,
    isCovered: (Long) -> Boolean,
    linksEndingAt: (Long, String) -> List<Long>,
    isEligibleRef: (String) -> Boolean,
    maxRunSize: Int,
    consume: suspend (ContinuationRun) -> Boolean,
) {
    require(maxRunSize > 0)
    var anchorLinks: List<Long> = emptyList()
    var anchorParent: String? = null
    val run = ArrayList<Long>(minOf(maxRunSize, SefariaContinuationParagraphs.CONTENT_BATCH_SIZE))
    var runEnd0 = -1
    var prevLineIndex = -1

    suspend fun flush() {
        if (run.isNotEmpty() && anchorLinks.isNotEmpty()) {
            if (!consume(ContinuationRun(anchorLinks, run.toList(), runEnd0))) {
                anchorLinks = emptyList()
                anchorParent = null
            }
        }
        run.clear()
    }
    suspend fun reset() {
        flush()
        anchorLinks = emptyList()
        anchorParent = null
    }

    // The payload walker emits refs in line order. Skip aliases on the same line,
    // but fail closed on reordered input instead of allocating a sorted copy.
    for (entry in entries) {
        val lineIndex0 = entry.lineIndex - 1
        require(lineIndex0 >= prevLineIndex) { "Unordered continuation refs in $path" }
        if (lineIndex0 == prevLineIndex) continue
        if (prevLineIndex >= 0 && lineIndex0 != prevLineIndex + 1) reset()
        prevLineIndex = lineIndex0
        val lineId = lineKeyToId[path to lineIndex0]
        if (lineId == null || lineId in headingLineIds) {
            reset()
            continue
        }
        val canonical = canonicalCitation(entry.ref)
        if (!isEligibleRef(canonical)) {
            reset()
            continue
        }
        val parent = parentAddress(canonical)
        if (isCovered(lineId)) {
            flush()
            anchorParent = parent
            anchorLinks = if (parent == null) emptyList() else linksEndingAt(lineId, parent)
            continue
        }
        if (anchorLinks.isNotEmpty() && parent == anchorParent) {
            run += lineId
            runEnd0 = lineIndex0
            if (run.size == maxRunSize) flush()
        } else {
            reset()
        }
    }
    flush()
}
