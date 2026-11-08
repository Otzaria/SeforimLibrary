package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.visibleUnits
import io.github.kdroidfilter.seforimlibrary.core.models.LinkAnchor
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

/**
 * A printed commentary marker "(X)" in a base line, in visible-char offsets.
 * [value] is the gematria of X; the range covers the parentheses.
 */
internal data class PrintedMarker(
    val label: String,
    val value: Int,
    val charStart: Int,
    val charEnd: Int,
    val opensLine: Boolean,
)

private val PRINTED_MARKER = Regex("""\(([א-ת"'׳״]{1,4})\)""")

/**
 * Printed "(X)" markers of [content] whose X is a number written canonically ("יט", not
 * "(יבא)", "(הג"א)" or "(שם)"). Markers starting before [skipBefore] are skipped.
 */
internal fun parsePrintedMarkers(content: String, skipBefore: Int = 0): List<PrintedMarker> {
    val visible = buildString {
        for (unit in visibleUnits(content)) append(if (unit.length == 1) unit[0] else '\uFFFC')
    }
    val firstVisible = (skipBefore until visible.length).firstOrNull { !visible[it].isWhitespace() }
    return PRINTED_MARKER.findAll(visible).mapNotNull { match ->
        if (match.range.first < skipBefore) return@mapNotNull null
        val label = match.groupValues[1]
        val value = gematriaValue(label) ?: return@mapNotNull null
        if (toGematria(value) != label.filter { it in 'א'..'ת' }) return@mapNotNull null
        PrintedMarker(label, value, match.range.first, match.range.last + 1, match.range.first == firstVisible)
    }.toList()
}

private val TRAILING_NUMBER = Regex("""(\d+)\s*$""")

/** The segment's own number: the last address component of its ref ("…Commandments 2" → 2). */
internal fun segmentNumber(ref: String): Int? = TRAILING_NUMBER.find(ref)?.groupValues?.get(1)?.toIntOrNull()

/**
 * Word-level anchors for commentaries whose markers Sefaria prints as plain "(X)" text in
 * the base segment instead of itags (Chafetz Chaim → Be'er Mayim Chaim, Otzaria/otzaria#4328).
 *
 * The comments of a base segment are the commentary segments nested under it (the base ref
 * with the commentary title, plus ":n"). A marker is matched to the comment that opens with
 * the same printed "(X)". When no comment of the segment prints a number, a lone marker
 * inside the line goes to the first comment, but only if it carries the segment's own number
 * ("לאוין, ב" … "(ב)"), as the introduction of Chafetz Chaim and Sefer Mitzvot Gadol print it.
 *
 * The anchor is a range over the printed marker, so the existing letter becomes the link
 * instead of a second one being injected beside it. As in [SefariaInlineAnchors], an anchor
 * is written only on an already stored link between the two lines.
 */
internal class SefariaPrintedMarkerAnchors(
    private val repository: SeforimRepository,
    private val logger: Logger,
) {
    private class MarkedLine(
        val base: SefariaInlineAnchors.BookInput,
        val baseRef: String,
        val srcLineId: Long,
        val markers: List<PrintedMarker>,
    )

    suspend fun generate(
        books: List<SefariaInlineAnchors.BookInput>,
        bookMetaById: Map<Long, BookMeta>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
    ) {
        val commentariesByBase = HashMap<Long, MutableSet<SefariaInlineAnchors.BookInput>>()
        for (book in books) {
            bookMetaById[book.bookId]?.sefariaDeclaredBaseTextBookIds?.forEach { baseId ->
                commentariesByBase.getOrPut(baseId) { linkedSetOf() } += book
            }
        }

        val markedLines = mutableListOf<MarkedLine>()
        for (base in books) {
            if (base.bookId !in commentariesByBase) continue
            for ((idx, content) in base.lines.withIndex()) {
                // Itag-carrying lines are anchored by SefariaInlineAnchors.
                if (content.contains("<i data-commentator=")) continue
                // The "(א) " that cleaning prepends is the segment's number, not a marker.
                val prefix = generatedPrefixLength(base.cleanShiftByLineIndex[idx] ?: 0)
                val markers = parsePrintedMarkers(content, skipBefore = countVisibleChars(content, prefix))
                if (markers.isEmpty()) continue
                val baseRef = base.refsByLineIndex[idx]?.ref ?: continue
                if (!baseRef.startsWith(base.enTitle)) continue
                val srcLineId = lineKeyToId[base.bookPath to idx] ?: continue
                markedLines += MarkedLine(base, baseRef, srcLineId, markers)
            }
        }

        // Commentary lines by the canonical ref of the segment they are nested under,
        // kept only for the segments that carry markers.
        val neededParents = HashSet<String>()
        for (line in markedLines) {
            for (commentary in commentariesByBase.getValue(line.base.bookId)) {
                neededParents += canonicalCitation(commentary.enTitle + line.baseRef.removePrefix(line.base.enTitle))
            }
        }
        val commentsByParent = HashMap<String, MutableList<Pair<SefariaInlineAnchors.BookInput, Int>>>()
        val markedBaseIds = markedLines.mapTo(HashSet()) { it.base.bookId }
        for (commentary in markedBaseIds.flatMapTo(LinkedHashSet()) { commentariesByBase.getValue(it) }) {
            for ((lineIdx, entry) in commentary.refsByLineIndex) {
                val colon = entry.ref.lastIndexOf(':')
                if (colon < 0) continue
                val parent = canonicalCitation(entry.ref.substring(0, colon))
                if (parent in neededParents) commentsByParent.getOrPut(parent) { mutableListOf() } += commentary to lineIdx
            }
        }

        var inserted = 0L
        var unmatched = 0
        var ambiguous = 0
        val batch = mutableListOf<LinkAnchor>()
        for (line in markedLines) {
            val markers = line.markers
            // Per marker: the link ids of each commentary comment it matched.
            val matches = markers.associateWith { mutableListOf<List<Long>>() }
            for (commentary in commentariesByBase.getValue(line.base.bookId)) {
                val parent = canonicalCitation(commentary.enTitle + line.baseRef.removePrefix(line.base.enTitle))
                val linked = commentsByParent[parent].orEmpty()
                    .filter { it.first === commentary }
                    .sortedBy { it.second }
                    .mapNotNull { (_, lineIdx) ->
                        val tgtLineId = lineKeyToId[commentary.bookPath to lineIdx] ?: return@mapNotNull null
                        val linkIds = repository.getLinkIdsBetweenLines(line.srcLineId, tgtLineId)
                        if (linkIds.isEmpty()) return@mapNotNull null
                        val text = commentary.lines.getOrNull(lineIdx).orEmpty()
                        val prefix = generatedPrefixLength(commentary.cleanShiftByLineIndex[lineIdx] ?: 0)
                        leadingPrintedLabel(text.substring(prefix.coerceAtMost(text.length))) to linkIds
                    }
                if (linked.isEmpty()) continue
                if (linked.all { it.first == null }) {
                    val marker = markers.singleOrNull()?.takeUnless { it.opensLine }
                    if (marker != null && marker.value == segmentNumber(line.baseRef)) {
                        matches.getValue(marker) += linked.first().second
                    }
                } else {
                    for (marker in markers) {
                        linked.firstOrNull { it.first == marker.value }?.let { matches.getValue(marker) += it.second }
                    }
                }
            }

            for ((marker, claims) in matches) {
                when {
                    claims.isEmpty() -> unmatched++
                    // Two commentaries claiming one marker: not ours to decide.
                    claims.size > 1 -> ambiguous++
                    else -> claims.single().mapTo(batch) { linkId ->
                        LinkAnchor(
                            linkId = linkId,
                            side = 0,
                            charStart = marker.charStart,
                            charEnd = marker.charEnd,
                            label = marker.label,
                        )
                    }
                }
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
        }
        logger.i {
            "Printed-marker anchors: $inserted inserted ($unmatched markers unmatched, $ambiguous ambiguous)"
        }
    }
}
