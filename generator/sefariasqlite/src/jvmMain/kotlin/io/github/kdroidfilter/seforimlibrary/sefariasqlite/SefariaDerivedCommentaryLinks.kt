package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link

/**
 * COMMENTARY links the generator adds where Sefaria ships none, and the rule that made each one.
 * [metricKey] names it in the link-import metrics report.
 */
internal enum class DerivedLinkStage(val metricKey: String) {
    /** Through the intermediate commentary's markers ([anchorUnlinkedCommentaryLines]). */
    INTERMEDIATE_MARKER("intermediate_marker"),

    /** The segment's own address names its base line ([deriveSegmentAddressLinks]). */
    SEGMENT_ADDRESS("segment_address"),

    /** Both neighbours of a daf-only segment comment on the same base line ([deriveTalmudNeighbourLinks]). */
    TALMUD_NEIGHBOURS("talmud_neighbours"),

    /** Only the base lines between a daf-only segment's neighbours are known: a range link. */
    TALMUD_NEIGHBOURS_RANGE("talmud_neighbours_range"),
}

/** The base lines after the first that a derived range link covers. */
internal data class DerivedBaseRange(val endLineId: Long, val endLineIndex: Int, val coveredLineIds: List<Long>)

/** A COMMENTARY link from a base line to a commentary line that Sefaria does not link. */
internal data class DerivedCommentaryLink(
    val stage: DerivedLinkStage,
    val baseBookId: Long,
    val commentaryBookId: Long,
    val baseLineId: Long,
    val commentaryLineId: Long,
    val commentaryLineIndex: Int,
    val range: DerivedBaseRange? = null,
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
private val DAF_SEGMENT_ADDRESS = Regex("""^([0-9]+[ab]):([0-9]+)$""")

// Both rules are checked against the links Sefaria does give the book: a book whose own links
// disagree with the rule more often than this gets none derived.
private const val MIN_RULE_AGREEMENT = 0.9
private const val MIN_RULE_SAMPLES = 5

/** Talmud neighbour ranges longer than this many base lines are not derived. */
internal const val MAX_DERIVED_RANGE_LINES = 12

/**
 * Collects every commentary that declares exactly one base text, other than [excludedBookIds].
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
            val baseBookId = meta.sefariaDeclaredBaseTextBookIds.singleOrNull() ?: return@mapNotNull null
            if (baseBookId == bookId) return@mapNotNull null
            val segments = addressedLines(bookId).ifEmpty { return@mapNotNull null }
            val baseLines = baseLinesCache.getOrPut(baseBookId) { addressedLines(baseBookId) }
                .ifEmpty { return@mapNotNull null }
            AddressedCommentary(bookId, baseBookId, meta.baseTextMapping, segments, baseLines)
        }
}

/**
 * The links stored between each [AddressedCommentary] and its base, keyed by commentary line.
 * [linked] holds every commentary line with any link to its base, or inside a stored range
 * (conservatively, a range to any book); [anchors] the base lines of its COMMENTARY links.
 */
internal class CommentaryBaseLinks(
    commentaries: List<AddressedCommentary>,
    private val lineIdToBookId: Map<Long, Long>,
) {
    private val baseByCommentaryBookId = commentaries.associate { it.commentaryBookId to it.baseBookId }
    val linked = HashSet<Long>()
    val anchors = HashMap<Long, MutableSet<Long>>()

    fun record(link: Link) {
        if (baseByCommentaryBookId[link.targetBookId] == link.sourceBookId) {
            linked += link.targetLineId
            if (link.connectionType == ConnectionType.COMMENTARY) {
                anchors.getOrPut(link.targetLineId) { HashSet() } += link.sourceLineId
            }
        }
        if (baseByCommentaryBookId[link.sourceBookId] == link.targetBookId) linked += link.sourceLineId
    }

    fun recordCoveredLine(lineId: Long) {
        val bookId = lineIdToBookId[lineId] ?: return
        if (bookId in baseByCommentaryBookId) linked += lineId
    }
}

/**
 * Stage 1 (Otzaria/otzaria#3854). Sefaria's `base_text_mapping` says which base line a
 * commentary segment's address names: `many_to_one` maps "X on Y 17:16:2" to "Y 17:16",
 * `one_to_one` maps "X on Y 1:10:4" to "Y 1:10:4". A segment without any link to the base gets a
 * link to that line when it exists, unless a sibling on the same address is linked elsewhere
 * only. A self-link (Maggid Mishneh on Shabbat 17:16:2) is dropped on import, so it counts as none.
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
    val targetOf = commentary.segments.associate { it.lineId to target(it.address)?.let(baseByAddress::get) }

    val anchored = commentary.segments.filter { !links.anchors[it.lineId].isNullOrEmpty() }
    val agreeing = anchored.count { segment ->
        targetOf[segment.lineId]?.lineId?.let { it in links.anchors.getValue(segment.lineId) } == true
    }
    if (anchored.size < MIN_RULE_SAMPLES || agreeing < MIN_RULE_AGREEMENT * anchored.size) return emptyList()

    // A base line some sibling is linked away from (all its anchors elsewhere) is not trusted.
    val contradicted = anchored
        .filter { segment -> targetOf[segment.lineId]?.lineId?.let { it !in links.anchors.getValue(segment.lineId) } ?: false }
        .mapNotNullTo(HashSet()) { targetOf[it.lineId]?.lineId }

    return commentary.segments.mapNotNull { segment ->
        if (segment.lineId in links.linked) return@mapNotNull null
        val base = targetOf[segment.lineId]?.takeIf { it.lineId !in contradicted } ?: return@mapNotNull null
        DerivedCommentaryLink(
            stage = DerivedLinkStage.SEGMENT_ADDRESS,
            baseBookId = commentary.baseBookId,
            commentaryBookId = commentary.commentaryBookId,
            baseLineId = base.lineId,
            commentaryLineId = segment.lineId,
            commentaryLineIndex = segment.lineIndex,
        )
    }
}

/**
 * Stage 2 (Otzaria/otzaria#3264, #3806). A Talmud commentary addressed by daf only ("Meiri on
 * Chullin 96a:3") names no base line. A run of unlinked segments takes its base from the nearest
 * linked segments of the same amud: the same line when both are linked there, otherwise a range
 * between them, or to the amud's edge where the run reaches it. A neighbour linked only outside
 * the amud, a range against the base order or longer than [MAX_DERIVED_RANGE_LINES], and an
 * amud without linked segments derive nothing.
 */
internal fun deriveTalmudNeighbourLinks(
    commentary: AddressedCommentary,
    links: CommentaryBaseLinks,
    maxRangeLines: Int = MAX_DERIVED_RANGE_LINES,
    minOrderedShare: Double = MIN_RULE_AGREEMENT,
): List<DerivedCommentaryLink> {
    fun dafOf(address: String) = DAF_SEGMENT_ADDRESS.matchEntire(address)?.groupValues

    val baseLinesByDaf = commentary.baseLines
        .mapNotNull { line -> dafOf(line.address)?.let { Triple(it[1], it[2].toInt(), line) } }
        .groupBy({ it.first }) { it.second to it.third }
        .mapValues { (_, lines) -> lines.sortedBy { it.first }.map { it.second } }
    if (baseLinesByDaf.isEmpty()) return emptyList()
    val positionByBaseLineId = HashMap<Long, Pair<String, Int>>()
    baseLinesByDaf.forEach { (daf, lines) -> lines.forEachIndexed { i, line -> positionByBaseLineId[line.lineId] = daf to i } }

    val segmentsByDaf = commentary.segments
        .mapNotNull { segment -> dafOf(segment.address)?.let { Triple(it[1], it[2].toInt(), segment) } }
        .groupBy({ it.first }) { it.second to it.third }
        .mapValues { (_, segments) -> segments.sortedBy { it.first }.map { it.second } }
        .filterKeys { it in baseLinesByDaf }

    // In-amud anchor positions of a linked segment; empty = linked, but not within its amud.
    fun inAmud(daf: String, segment: AddressedLine): List<Int> =
        links.anchors[segment.lineId].orEmpty()
            .mapNotNull { positionByBaseLineId[it]?.takeIf { (d, _) -> d == daf }?.second }
            .sorted()

    // Book guard: its linked segments must follow their amud and the base order.
    var anchoredSegments = 0
    var inAmudSegments = 0
    var orderedPairs = 0
    var pairs = 0
    segmentsByDaf.forEach { (daf, segments) ->
        var previous: List<Int>? = null
        segments.filter { it.lineId in links.linked }.forEach { segment ->
            if (links.anchors[segment.lineId].isNullOrEmpty()) return@forEach
            anchoredSegments++
            val positions = inAmud(daf, segment)
            if (positions.isEmpty()) return@forEach
            inAmudSegments++
            previous?.let { prev ->
                pairs++
                if (positions.first() >= prev.first()) orderedPairs++
            }
            previous = positions
        }
    }
    if (anchoredSegments < MIN_RULE_SAMPLES || inAmudSegments < MIN_RULE_AGREEMENT * anchoredSegments) return emptyList()
    if (pairs < MIN_RULE_SAMPLES || orderedPairs < minOrderedShare * pairs) return emptyList()

    val derived = mutableListOf<DerivedCommentaryLink>()
    segmentsByDaf.forEach { (daf, segments) ->
        val baseLines = baseLinesByDaf.getValue(daf)
        var i = 0
        while (i < segments.size) {
            if (segments[i].lineId in links.linked) {
                i++
                continue
            }
            val runStart = i
            while (i < segments.size && segments[i].lineId !in links.linked) i++
            val run = segments.subList(runStart, i)
            val before = segments.getOrNull(runStart - 1)?.let { inAmud(daf, it).ifEmpty { return@let null } }
            val after = segments.getOrNull(i)?.let { inAmud(daf, it).ifEmpty { return@let null } }
            // A neighbour linked only outside the amud leaves its side unknown.
            if (before == null && runStart > 0) continue
            if (after == null && i < segments.size) continue
            if (before == null && after == null) continue
            val from = before?.last() ?: 0
            val to = after?.first() ?: baseLines.lastIndex
            if (from > to || to - from + 1 > maxRangeLines) continue
            val exact = from == to && before != null && after != null
            val range = if (from == to) {
                null
            } else {
                DerivedBaseRange(
                    endLineId = baseLines[to].lineId,
                    endLineIndex = baseLines[to].lineIndex,
                    coveredLineIds = baseLines.subList(from + 1, to + 1).map { it.lineId },
                )
            }
            run.forEach { segment ->
                derived += DerivedCommentaryLink(
                    stage = if (exact) DerivedLinkStage.TALMUD_NEIGHBOURS else DerivedLinkStage.TALMUD_NEIGHBOURS_RANGE,
                    baseBookId = commentary.baseBookId,
                    commentaryBookId = commentary.commentaryBookId,
                    baseLineId = baseLines[from].lineId,
                    commentaryLineId = segment.lineId,
                    commentaryLineIndex = segment.lineIndex,
                    range = range,
                )
            }
        }
    }
    return derived
}
