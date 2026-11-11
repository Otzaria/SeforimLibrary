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
    // Book paths, the key of [LineTextSource].
    val commentaryPath: String = "",
    val basePath: String = "",
)

private val PLAIN_ADDRESS = Regex("""^[0-9]+[ab]?(?::[0-9]+)*$""")
private val DAF_SEGMENT_ADDRESS = Regex("""^([0-9]+[ab]):([0-9]+)$""")

// Both rules are checked against the links Sefaria does give the book: a book whose own links
// disagree with the rule more often than this gets none derived.
private const val MIN_RULE_AGREEMENT = 0.9
private const val MIN_RULE_SAMPLES = 5

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
            AddressedCommentary(
                bookId, baseBookId, meta.baseTextMapping, segments, baseLines,
                commentaryPath = pathByBookId.getValue(bookId), basePath = pathByBookId.getValue(baseBookId),
            )
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

/** The stored text of a book line, by book path and 0-based line index. */
internal fun interface LineTextSource {
    fun text(path: String, lineIndex: Int): String?
}

// Text gate of stage 2: a segment keeps its neighbours' base line only when its own words
// single that line out of the amud, and only in a book where this holds for Sefaria's own links.
private const val TEXT_GATE_MIN_ACCURACY = 0.9
private const val TEXT_GATE_MIN_DECISIVE = 20
private const val TEXT_GATE_MAX_SAMPLES = 400
private const val TEXT_WORDS = 80
private const val TEXT_MIN_MATCHED_FEATURES = 3
private const val TEXT_MIN_MARGIN = 1.5

private val HTML_TAG = Regex("<[^>]*>")
private val NIKUD_AND_TAAMIM = Regex("[֑-ׇ]")
private val QUOTE_MARKS = Regex("[\"'׳״“”‘’]")
private val NOT_HEBREW_LETTER = Regex("[^א-ת]+")
private const val PREFIX_LETTERS = "והבלכמש"

/** The words of [text] without markup, nikud and punctuation, minus one leading prefix letter. */
internal fun textStems(text: String, limit: Int = Int.MAX_VALUE): List<String> =
    text.replace(HTML_TAG, " ").replace('־', ' ').replace(NIKUD_AND_TAAMIM, "")
        .replace(QUOTE_MARKS, "").split(NOT_HEBREW_LETTER)
        .filter { it.length >= 3 }
        .map { if (it.length > 3 && it[0] in PREFIX_LETTERS) it.substring(1) else it }
        .take(limit)

/** The words of [text] and each pair of neighbouring words, the features lines are matched by. */
internal fun textFeatures(text: String, limit: Int = Int.MAX_VALUE): Set<String> {
    val stems = textStems(text, limit)
    return LinkedHashSet(stems).also { features -> stems.zipWithNext { a, b -> features += "$a $b" } }
}

/**
 * The amud base line whose features [segmentText] matches clearly best, or null. Rare features
 * of the amud weigh more (IDF); the best line needs [TEXT_MIN_MATCHED_FEATURES] matched features,
 * one of them a pair of neighbouring words, and [TEXT_MIN_MARGIN] times the weight of the runner-up.
 */
internal fun clearBestLine(segmentText: String, lineFeatures: List<Set<String>>): Int? {
    val words = textFeatures(segmentText, TEXT_WORDS)
    if (words.isEmpty() || lineFeatures.isEmpty()) return null
    val df = HashMap<String, Int>()
    lineFeatures.forEach { features -> features.forEach { df.merge(it, 1, Int::plus) } }
    val weights = DoubleArray(lineFeatures.size)
    val matched = IntArray(lineFeatures.size)
    val matchedPairs = IntArray(lineFeatures.size)
    lineFeatures.forEachIndexed { i, features ->
        words.forEach { w ->
            if (w in features) {
                weights[i] += Math.log(1.0 + lineFeatures.size.toDouble() / df.getValue(w))
                matched[i]++
                if (' ' in w) matchedPairs[i]++
            }
        }
    }
    val best = weights.indices.maxByOrNull { weights[it] } ?: return null
    if (matched[best] < TEXT_MIN_MATCHED_FEATURES || matchedPairs[best] == 0) return null
    val runnerUp = weights.indices.filter { it != best }.maxOfOrNull { weights[it] } ?: 0.0
    return best.takeIf { weights[best] >= TEXT_MIN_MARGIN * runnerUp }
}

/**
 * Stage 2 (Otzaria/otzaria#3264). A Talmud commentary addressed by daf only ("Meiri on Chullin
 * 96a:3") names no base line. An unlinked segment whose nearest linked segments of the same
 * amud, before and after it, are each linked to the same single line of that amud takes that
 * line, when its text agrees: the line is the segment's clear best match in the amud, and the
 * same test, wherever it gives a clear answer, finds the line of at least
 * [TEXT_GATE_MIN_ACCURACY] of the book's own singly linked segments. A segment at the edge of an amud or next to one linked neighbour only, a neighbour
 * linked to several lines or outside the amud, and a book whose links leave the base order or
 * their amud derive nothing.
 */
internal fun deriveTalmudNeighbourLinks(
    commentary: AddressedCommentary,
    links: CommentaryBaseLinks,
    texts: LineTextSource,
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

    // In-amud positions of a linked segment's anchors.
    fun inAmud(daf: String, segment: AddressedLine): List<Int> =
        links.anchors[segment.lineId].orEmpty()
            .mapNotNull { positionByBaseLineId[it]?.takeIf { (d, _) -> d == daf }?.second }
    // The one in-amud base line a segment is linked to; null with several anchors, one outside the amud, or none.
    fun singleAnchor(daf: String, segment: AddressedLine): Int? =
        if (links.anchors[segment.lineId]?.size == 1) inAmud(daf, segment).singleOrNull() else null

    // Book guard: its linked segments must follow their amud and the base order.
    var anchoredSegments = 0
    var inAmudSegments = 0
    var orderedPairs = 0
    var pairs = 0
    segmentsByDaf.forEach { (daf, segments) ->
        var previous: Int? = null
        segments.filter { !links.anchors[it.lineId].isNullOrEmpty() }.forEach { segment ->
            anchoredSegments++
            val position = inAmud(daf, segment).minOrNull() ?: return@forEach
            inAmudSegments++
            previous?.let { prev ->
                pairs++
                if (position >= prev) orderedPairs++
            }
            previous = position
        }
    }
    if (anchoredSegments < MIN_RULE_SAMPLES || inAmudSegments < MIN_RULE_AGREEMENT * anchoredSegments) return emptyList()
    if (pairs < MIN_RULE_SAMPLES || orderedPairs < MIN_RULE_AGREEMENT * pairs) return emptyList()

    // Neighbours' agreement first: a book with nothing to derive never pays for the text gate.
    val candidates = mutableListOf<Triple<String, AddressedLine, Int>>()
    segmentsByDaf.forEach { (daf, segments) ->
        segments.forEachIndexed { i, segment ->
            if (segment.lineId in links.linked) return@forEachIndexed
            val before = segments.subList(0, i).lastOrNull { it.lineId in links.linked } ?: return@forEachIndexed
            val after = segments.subList(i + 1, segments.size).firstOrNull { it.lineId in links.linked } ?: return@forEachIndexed
            val position = singleAnchor(daf, before)
            if (position != null && position == singleAnchor(daf, after)) candidates += Triple(daf, segment, position)
        }
    }
    if (candidates.isEmpty()) return emptyList()

    val lineStemsByDaf = HashMap<String, List<Set<String>>>()
    fun bestLine(daf: String, segment: AddressedLine): Int? {
        val stems = lineStemsByDaf.getOrPut(daf) {
            baseLinesByDaf.getValue(daf).map { textFeatures(texts.text(commentary.basePath, it.lineIndex).orEmpty()) }
        }
        return texts.text(commentary.commentaryPath, segment.lineIndex)?.let { clearBestLine(it, stems) }
    }

    val calibration = segmentsByDaf.flatMap { (daf, segments) ->
        segments.mapNotNull { segment -> singleAnchor(daf, segment)?.let { Triple(daf, segment, it) } }
    }
    val step = (calibration.size + TEXT_GATE_MAX_SAMPLES - 1) / TEXT_GATE_MAX_SAMPLES
    val sampled = calibration.filterIndexed { i, _ -> i % step == 0 }
    val answers = sampled.mapNotNull { (daf, segment, position) -> bestLine(daf, segment)?.let { it == position } }
    if (answers.size < TEXT_GATE_MIN_DECISIVE || answers.count { it } < TEXT_GATE_MIN_ACCURACY * answers.size) return emptyList()

    return candidates.mapNotNull { (daf, segment, position) ->
        if (bestLine(daf, segment) != position) return@mapNotNull null
        DerivedCommentaryLink(
            stage = DerivedLinkStage.TALMUD_NEIGHBOURS,
            baseBookId = commentary.baseBookId,
            commentaryBookId = commentary.commentaryBookId,
            baseLineId = baseLinesByDaf.getValue(daf)[position].lineId,
            commentaryLineId = segment.lineId,
            commentaryLineIndex = segment.lineIndex,
        )
    }
}
