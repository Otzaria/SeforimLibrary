package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
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
    if ('(' !in content) return emptyList()
    // Preserve the charCount coordinate convention without allocating a String per char.
    val visible = if ('<' !in content && '&' !in content) content else buildString(content.length) {
        var i = 0
        while (i < content.length) {
            when (val ch = content[i]) {
                '<' -> {
                    val close = content.indexOf('>', i)
                    if (close < 0) break
                    i = close
                }
                '&' -> {
                    val end = minOf(content.length, i + 10)
                    var j = i + 1
                    while (j < end && content[j] != ';') j++
                    if (j < end) {
                        append('\uFFFC')
                        i = j
                    } else append(ch)
                }
                else -> append(ch)
            }
            i++
        }
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

// Brit Moshe quotes the base as "שלא (א) ... כו׳", also with an abbreviated first word.
private val BRIT_MOSHE_OPENING_MARKER = Regex("""^\s*(?:[א-ת"'׳״]+\s+)?\(([א-ת"'׳״]{1,4})\)""")
private val PRINTED_MARKER_HTML_TAG = Regex("<[^>]*>")

private fun britMosheOpeningLabel(content: String): Int? {
    val label = BRIT_MOSHE_OPENING_MARKER.find(content.replace(PRINTED_MARKER_HTML_TAG, ""))
        ?.groupValues?.get(1) ?: return null
    val value = gematriaValue(label) ?: return null
    return value.takeIf { toGematria(it) == label.filter { ch -> ch in 'א'..'ת' } }
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
 * the same printed "(X)". Only the two verified book pairs below are eligible: numbered
 * lists in other base texts are not evidence of commentary markers.
 *
 * For Chafetz Chaim, when no comment of the segment prints a number, a lone marker
 * inside the line goes to the first comment, but only if it carries the segment's own number
 * ("לאוין, ב" … "(ב)"), as the introduction of Chafetz Chaim prints it. The target must
 * actually be comment :1; missing links or text never move a marker to a later comment.
 * Brit Moshe also quotes the marker after its first word: the same segment-number rule
 * applies, but the opening quote must explicitly contain that marker as well.
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

    private data class CommentLine(
        val lineIndex: Int,
        val ref: String,
        val label: Int?,
        val openingQuoteLabel: Int?,
    )
    private data class Target(val bookPath: String, val lineIndex: Int)

    suspend fun generate(
        books: List<SefariaInlineAnchors.BookInput>,
        bookMetaById: Map<Long, BookMeta>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
    ) {
        val booksById = books.associateBy { it.bookId }
        // Expand this list only with corpus evidence for the printed-marker convention.
        val supportedPairs = mapOf(
            "Be'er Mayim Chaim" to "Chafetz Chaim",
            "Brit Moshe" to "Sefer Mitzvot Gadol",
        )
        val commentariesByBase = HashMap<Long, MutableList<SefariaInlineAnchors.BookInput>>()
        for (book in books) {
            val baseTitle = supportedPairs[book.enTitle] ?: continue
            bookMetaById[book.bookId]?.sefariaDeclaredBaseTextBookIds?.forEach { baseId ->
                if (booksById[baseId]?.enTitle == baseTitle) {
                    commentariesByBase.getOrPut(baseId) { mutableListOf() } += book
                }
            }
        }

        val markedLines = mutableListOf<MarkedLine>()
        for (baseId in commentariesByBase.keys) {
            val base = booksById.getValue(baseId)
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

        // Use book ids in keys: hashing BookInput would walk all its text and refs.
        val neededParents = HashSet<Pair<Long, String>>()
        for (line in markedLines) {
            for (commentary in commentariesByBase.getValue(line.base.bookId)) {
                val parent = canonicalCitation(commentary.enTitle + line.baseRef.removePrefix(line.base.enTitle))
                neededParents += commentary.bookId to parent
            }
        }
        val commentsByParent = HashMap<Pair<Long, String>, MutableList<CommentLine>>()
        for (commentary in commentariesByBase.values.flatten().distinctBy { it.bookId }) {
            for ((lineIdx, entry) in commentary.refsByLineIndex) {
                val colon = entry.ref.lastIndexOf(':')
                if (colon < 0) continue
                val key = commentary.bookId to canonicalCitation(entry.ref.substring(0, colon))
                if (key !in neededParents) continue
                val text = commentary.lines.getOrNull(lineIdx).orEmpty()
                val prefix = generatedPrefixLength(commentary.cleanShiftByLineIndex[lineIdx] ?: 0)
                val originalText = text.substring(prefix.coerceAtMost(text.length))
                val label = leadingPrintedLabel(originalText)
                val openingQuoteLabel = if (commentary.enTitle == "Brit Moshe") britMosheOpeningLabel(originalText) else null
                commentsByParent.getOrPut(key) { mutableListOf() } += CommentLine(lineIdx, entry.ref, label, openingQuoteLabel)
            }
        }

        var inserted = 0L
        var unmatched = 0
        var ambiguous = 0
        val batch = mutableListOf<LinkAnchor>()
        for (line in markedLines) {
            val markers = line.markers
            val matches = markers.associateWith { mutableListOf<Target>() }
            for (commentary in commentariesByBase.getValue(line.base.bookId)) {
                val parent = canonicalCitation(commentary.enTitle + line.baseRef.removePrefix(line.base.enTitle))
                val comments = commentsByParent[commentary.bookId to parent].orEmpty()
                if (comments.isEmpty()) continue
                // Inspect ALL text before looking for links. A missing link must not
                // hide a printed label, erase ambiguity, or promote comment :2 to :1.
                if (comments.all { it.label == null }) {
                    val marker = markers.singleOrNull()?.takeUnless { it.opensLine }
                    if (marker != null && marker.value == segmentNumber(line.baseRef)) {
                        comments.filter {
                            it.ref.substringAfterLast(':') == "1" &&
                                (line.base.enTitle == "Chafetz Chaim" || it.openingQuoteLabel == marker.value)
                        }.forEach {
                            matches.getValue(marker) += Target(commentary.bookPath, it.lineIndex)
                        }
                    }
                } else {
                    val byLabel = comments.groupBy { it.label }
                    for (marker in markers) {
                        byLabel[marker.value].orEmpty().forEach {
                            matches.getValue(marker) += Target(commentary.bookPath, it.lineIndex)
                        }
                    }
                }
            }

            for ((marker, claims) in matches) {
                if (claims.size > 1) {
                    ambiguous++
                    continue
                }
                val target = claims.singleOrNull()
                val tgtLineId = target?.let { lineKeyToId[it.bookPath to it.lineIndex] }
                val linkIds = if (tgtLineId == null) emptyList() else {
                    repository.getLinkIdsBetweenLines(line.srcLineId, tgtLineId).filter {
                        repository.getLink(it)?.connectionType == ConnectionType.COMMENTARY
                    }
                }
                if (linkIds.isEmpty()) {
                    unmatched++
                    continue
                }
                linkIds.mapTo(batch) { linkId ->
                    LinkAnchor(
                        linkId = linkId,
                        side = 0,
                        charStart = marker.charStart,
                        charEnd = marker.charEnd,
                        label = marker.label,
                    )
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
