package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * COMMENTARY links the generator adds where Sefaria ships none, and the rule that made each one.
 * [metricKey] names it in the link-import metrics report.
 */
internal enum class DerivedLinkStage(val metricKey: String) {
    /** Through the intermediate commentary's markers ([anchorUnlinkedCommentaryLines]). */
    INTERMEDIATE_MARKER("intermediate_marker"),

    /** The segment's own address names its base line ([deriveSegmentAddressLinks]). */
    SEGMENT_ADDRESS("segment_address"),
}

/** A COMMENTARY link from a base line to a commentary line that Sefaria does not link. */
internal data class DerivedCommentaryLink(
    val stage: DerivedLinkStage,
    val baseBookId: Long,
    val commentaryBookId: Long,
    val baseLineId: Long,
    val commentaryLineId: Long,
    val commentaryLineIndex: Int,
)

/** A content line with a plain address after its book title: "17:16:2", "96a:3". */
internal data class AddressedLine(val lineId: Long, val lineIndex: Int, val address: String)

/**
 * A commentary with one declared base, and the lines of both that carry a plain address.
 * [mapping] is Sefaria's `base_text_mapping`.
 */
internal data class AddressedCommentary(
    val commentaryBookId: Long,
    val baseBookId: Long,
    val mapping: String?,
    val segments: List<AddressedLine>,
    val baseLines: List<AddressedLine>,
)

private val PLAIN_ADDRESS = Regex("""^[0-9]+[ab]?(?::[0-9]+)*$""")

// The mapping is checked against the links Sefaria does give the book: a book whose own links
// disagree with the rule more often than this gets none derived.
private const val MIN_RULE_AGREEMENT = 0.9
private const val MIN_RULE_SAMPLES = 5

/**
 * Collects commentaries with one declared base and a supported mapping, other than [excludedBookIds].
 * Only lines of the book's main (default) node take part: their ref is the book's English title
 * and a plain address.
 */
internal fun collectAddressedCommentaries(
    refsByPath: Map<String, List<RefEntry>>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    lineIdToBookId: Map<Long, Long>,
    headingLineIds: Set<Long>,
    bookMetaById: Map<Long, BookMeta>,
    excludedBookIds: Set<Long> = emptySet(),
): List<AddressedCommentary> {
    val pathByBookId = HashMap<Long, String>()
    refsByPath.forEach { (path, refs) ->
        val first = refs.firstOrNull() ?: return@forEach
        val lineId = lineKeyToId[path to first.lineIndex - 1] ?: return@forEach
        lineIdToBookId[lineId]?.let { pathByBookId.putIfAbsent(it, path) }
    }

    fun addressedLines(bookId: Long): List<AddressedLine> {
        val path = pathByBookId[bookId] ?: return emptyList()
        val title = bookMetaById[bookId]?.enTitle?.let(::canonicalCitation) ?: return emptyList()
        return refsByPath[path].orEmpty()
            .mapNotNull { entry ->
                val address = canonicalCitation(entry.ref).takeIf { it.startsWith("$title ") }
                    ?.substring(title.length + 1)
                    ?.takeIf { PLAIN_ADDRESS.matches(it) }
                    ?: return@mapNotNull null
                val lineId = lineKeyToId[path to entry.lineIndex - 1] ?: return@mapNotNull null
                if (lineId in headingLineIds) return@mapNotNull null
                AddressedLine(lineId, entry.lineIndex - 1, address)
            }
            .sortedBy { it.lineIndex }
    }

    val baseLinesCache = HashMap<Long, List<AddressedLine>>()
    return bookMetaById.entries
        .sortedBy { it.key }
        .mapNotNull { (bookId, meta) ->
            if (bookId in excludedBookIds || meta.dependence != Dependence.COMMENTARY) return@mapNotNull null
            val mapping = meta.baseTextMapping
            if (mapping != "many_to_one" && mapping != "one_to_one") return@mapNotNull null
            val baseBookId = meta.sefariaDeclaredBaseTextBookIds.singleOrNull() ?: return@mapNotNull null
            if (baseBookId == bookId) return@mapNotNull null
            val segments = addressedLines(bookId).ifEmpty { return@mapNotNull null }
            val baseLines = baseLinesCache.getOrPut(baseBookId) { addressedLines(baseBookId) }
                .ifEmpty { return@mapNotNull null }
            AddressedCommentary(bookId, baseBookId, mapping, segments, baseLines)
        }
}

/**
 * The links stored between each [AddressedCommentary] and its base, keyed by commentary line.
 * [linked] holds every commentary line with any link to its base. Ranges on commentary
 * lines also block derivation, including coverage-free ranges and ranges to other books.
 * Non-exact commentary links are kept separate
 * from point evidence because a range start or a fallback ref cannot confirm the mapping.
 */
internal class CommentaryBaseLinks(commentaries: List<AddressedCommentary>) {
    private val baseByCommentaryBookId = commentaries.associate { it.commentaryBookId to it.baseBookId }
    val linked = HashSet<Long>()
    private val anchors = HashMap<Long, MutableSet<Long>>()
    // CSV workers report ambiguous citations; only the inserter writes linked and anchors.
    private val nonExactSegments = ConcurrentHashMap.newKeySet<Long>()
    private val rangesByBookId = ConcurrentHashMap<Long, ConcurrentLinkedQueue<IntRange>>()

    fun anchorsFor(lineId: Long): Set<Long> = anchors[lineId].orEmpty()

    fun hasNonExactLink(lineId: Long): Boolean = lineId in nonExactSegments

    fun record(link: Link) {
        if (baseByCommentaryBookId[link.targetBookId] == link.sourceBookId) {
            linked += link.targetLineId
            if (link.connectionType == ConnectionType.COMMENTARY) {
                anchors.getOrPut(link.targetLineId) { HashSet() } += link.sourceLineId
            }
        }
        if (baseByCommentaryBookId[link.sourceBookId] == link.targetBookId) linked += link.sourceLineId
    }

    fun recordNonExactCommentary(link: Link) {
        if (baseByCommentaryBookId[link.targetBookId] == link.sourceBookId) nonExactSegments += link.targetLineId
        if (baseByCommentaryBookId[link.sourceBookId] == link.targetBookId) nonExactSegments += link.sourceLineId
    }

    fun recordRange(bookId: Long, firstIndex: Int, lastIndex: Int) {
        if (bookId in baseByCommentaryBookId) {
            rangesByBookId.computeIfAbsent(bookId) { ConcurrentLinkedQueue() } += firstIndex..lastIndex
        }
    }

    fun rangesFor(bookId: Long): List<IntRange> = rangesByBookId[bookId].orEmpty().sortedBy { it.first }
}

/**
 * Stage 1 (Otzaria/otzaria#3854). Sefaria's `base_text_mapping` says which base line a
 * commentary segment's address names: `many_to_one` maps "X on Y 17:16:2" to "Y 17:16",
 * `one_to_one` maps "X on Y 1:10:4" to "Y 1:10:4". A segment without any link to the base gets a
 * link to that line when it exists, unless a sibling on the same address has ambiguous link
 * evidence or is linked only elsewhere. A self-link (Maggid Mishneh on Shabbat 17:16:2) is dropped
 * on import, so it counts as none.
 */
internal fun deriveSegmentAddressLinks(
    commentary: AddressedCommentary,
    links: CommentaryBaseLinks,
): List<DerivedCommentaryLink> {
    val target: (String) -> String? = when (commentary.mapping) {
        "many_to_one" -> { address -> address.substringBeforeLast(':', "").ifEmpty { null } }
        "one_to_one" -> { address -> address }
        else -> return emptyList()
    }
    val baseByAddress = commentary.baseLines.groupBy { it.address }
        .mapNotNull { (address, lines) -> lines.singleOrNull()?.let { address to it } }
        .toMap()
    val contradicted = HashSet<Long>()
    val candidates = mutableListOf<DerivedCommentaryLink>()
    val ranges = links.rangesFor(commentary.commentaryBookId)
    var nextRange = 0
    var rangedThrough = -1
    var anchored = 0
    var agreeing = 0
    for (segment in commentary.segments) {
        while (nextRange < ranges.size && ranges[nextRange].first <= segment.lineIndex) {
            rangedThrough = maxOf(rangedThrough, ranges[nextRange].last)
            nextRange++
        }
        val base = target(segment.address)?.let(baseByAddress::get)
        if (links.hasNonExactLink(segment.lineId)) {
            if (base != null) contradicted += base.lineId
            continue
        }
        val anchors = links.anchorsFor(segment.lineId)
        if (anchors.isNotEmpty()) {
            anchored++
            if (base != null) {
                if (base.lineId in anchors) agreeing++ else contradicted += base.lineId
            }
        } else if (base != null && segment.lineId !in links.linked && segment.lineIndex > rangedThrough) {
            candidates += DerivedCommentaryLink(
                stage = DerivedLinkStage.SEGMENT_ADDRESS,
                baseBookId = commentary.baseBookId,
                commentaryBookId = commentary.commentaryBookId,
                baseLineId = base.lineId,
                commentaryLineId = segment.lineId,
                commentaryLineIndex = segment.lineIndex,
            )
        }
    }
    if (anchored < MIN_RULE_SAMPLES || agreeing < MIN_RULE_AGREEMENT * anchored) return emptyList()
    return candidates.filterNot { it.baseLineId in contradicted }
}
