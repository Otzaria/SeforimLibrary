package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.LinkAnchor
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

/**
 * Word-level link anchors from Sefaria's inline commentary markers (itags).
 *
 * Sefaria embeds `<i data-commentator="X" data-order="N" data-label="א"></i>`
 * tags inside base texts (Shulchan Arukh, Tur, Rif pages, Mishnah commentaries…)
 * at the exact word a printed commentary marker sits on. The semantics, verified
 * against Sefaria's client (TextRange.jsx) and data:
 *
 *  - `data-commentator` equals the commentary index's `collective_title.en`
 *    (falling back to the index title when no collective title exists, e.g.
 *    Mishnah Berurah).
 *  - `data-order="N"` addresses comment N of that commentary within the shared
 *    top-level section (siman/daf) — i.e. the last component of the commentary
 *    ref — counting Sefaria's empty comment slots.
 *  - When `data-order` is absent, `data-label` carries the printed Hebrew
 *    letter whose gematria value is that same component (Mishnah Berurah).
 *
 * Every anchor is double-checked against the links table: an anchor row is only
 * written when Sefaria's own links CSV already connects the anchored base line
 * to the resolved comment line, so a mis-constructed ref can never invent a
 * connection. Unresolvable tags are counted and reported, never guessed.
 */
internal data class InlineItag(
    val commentator: String,
    val order: Int?,
    val label: String?,
    /** Visible-char offset of the tag in the line content (charCount convention). */
    val charStart: Int,
)

private const val ITAG_MARKER = "<i data-commentator="

// Tolerates Sefaria's handful of malformed tags missing the opening quote
// (`data-commentator=Mishnah Berurah"`): the opening quote is optional, the
// closing quote is required.
private val COMMENTATOR_ATTR = Regex("""data-commentator="?([^"<>]+?)"""")
private val ORDER_ATTR = Regex("""data-order="(\d+)"""")
private val LABEL_ATTR = Regex("""data-label="([^"]*)"""")

/**
 * Extracts the itags of [content] together with their visible-char offsets.
 *
 * The offset walk mirrors [io.github.kdroidfilter.seforimlibrary.common.countVisibleChars]
 * exactly (tags contribute nothing, each `&...;` entity counts as one char) so
 * `charStart` lives in the same coordinate space as `line.charCount`.
 */
internal fun parseInlineItags(content: String): List<InlineItag> {
    if (!content.contains(ITAG_MARKER)) return emptyList()
    val result = mutableListOf<InlineItag>()
    var visible = 0
    var i = 0
    val len = content.length
    while (i < len) {
        val c = content[i]
        when {
            c == '<' -> {
                val close = content.indexOf('>', i)
                if (close < 0) break // unterminated tag: nothing visible follows
                if (content.startsWith(ITAG_MARKER, i)) {
                    val tag = content.substring(i, close + 1)
                    val commentator = COMMENTATOR_ATTR.find(tag)?.groupValues?.get(1)?.trim()
                    if (!commentator.isNullOrEmpty()) {
                        result += InlineItag(
                            commentator = commentator,
                            order = ORDER_ATTR.find(tag)?.groupValues?.get(1)?.toIntOrNull(),
                            label = LABEL_ATTR.find(tag)?.groupValues?.get(1)?.takeIf { it.isNotBlank() },
                            charStart = visible,
                        )
                    }
                }
                i = close + 1
            }
            c == '&' -> {
                val end = minOf(len, i + 10)
                var j = i + 1
                var terminated = false
                while (j < end) {
                    if (content[j] == ';') { terminated = true; break }
                    j++
                }
                visible++
                i = (if (terminated) j else i) + 1
            }
            else -> {
                visible++
                i++
            }
        }
    }
    return result
}

private val GEMATRIA_VALUES = mapOf(
    'א' to 1, 'ב' to 2, 'ג' to 3, 'ד' to 4, 'ה' to 5, 'ו' to 6, 'ז' to 7, 'ח' to 8, 'ט' to 9,
    'י' to 10, 'כ' to 20, 'ל' to 30, 'מ' to 40, 'נ' to 50, 'ס' to 60, 'ע' to 70, 'פ' to 80, 'צ' to 90,
    'ק' to 100, 'ר' to 200, 'ש' to 300, 'ת' to 400,
    'ך' to 20, 'ם' to 40, 'ן' to 50, 'ף' to 80, 'ץ' to 90,
)

private val GEMATRIA_IGNORED = setOf('"', '\'', '׳', '״', ' ')

/**
 * Additive gematria value of a printed marker letter ("א"=1 … "טו"=15 …).
 * Returns null when [label] contains anything but Hebrew letters and
 * geresh/gershayim punctuation — unresolvable labels must not be guessed.
 */
internal fun gematriaValue(label: String): Int? {
    var sum = 0
    var seen = false
    for (ch in label) {
        if (ch in GEMATRIA_IGNORED) continue
        val value = GEMATRIA_VALUES[ch] ?: return null
        sum += value
        seen = true
    }
    return if (seen) sum else null
}

internal class SefariaInlineAnchors(
    private val repository: SeforimRepository,
    private val logger: Logger,
) {
    internal data class BookInput(
        val bookId: Long,
        val enTitle: String,
        val bookPath: String,
        val lines: List<String>,
        /** 0-based lineIndex -> RefEntry (same map the line inserter used). */
        val refsByLineIndex: Map<Int, RefEntry>,
        /** See BookPayload.singleVersionTitle / cleanShiftByLineIndex. */
        val singleVersionTitle: String? = null,
        val cleanShiftByLineIndex: Map<Int, Int> = emptyMap(),
    )

    private val skipCounts = linkedMapOf<String, Int>()
    private val skipSamples = linkedMapOf<String, String>()

    private fun skip(reason: String, sample: () -> String) {
        skipCounts.merge(reason, 1, Int::plus)
        skipSamples.getOrPut(reason) { sample() }
    }

    suspend fun generate(
        books: List<BookInput>,
        bookMetaById: Map<Long, BookMeta>,
        refsByCanonical: Map<String, List<RefEntry>>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
    ) {
        // data-commentator value -> candidate commentary bookIds. The tag value
        // is the commentary's collective_title.en, or its index title when no
        // collective title exists (Sefaria's client falls back the same way).
        val candidatesByName = HashMap<String, MutableList<Long>>()
        for (book in books) {
            val collective = bookMetaById[book.bookId]?.collectiveTitleEn
            collective?.let { candidatesByName.getOrPut(it) { mutableListOf() }.add(book.bookId) }
            if (collective != book.enTitle) {
                candidatesByName.getOrPut(book.enTitle) { mutableListOf() }.add(book.bookId)
            }
        }
        val enTitleByBookId = books.associate { it.bookId to it.enTitle }

        var anchorsInserted = 0L
        var linesWithItags = 0
        val batch = mutableListOf<LinkAnchor>()

        for (book in books) {
            for ((idx, content) in book.lines.withIndex()) {
                val itags = parseInlineItags(content)
                if (itags.isEmpty()) continue
                linesWithItags++
                val baseRef = book.refsByLineIndex[idx]?.ref
                val srcLineId = lineKeyToId[book.bookPath to idx]
                if (baseRef == null || srcLineId == null) {
                    skip("base line without ref") { "${book.enTitle} line $idx" }
                    continue
                }

                for (itag in itags) {
                    val order = itag.order ?: itag.label?.let { gematriaValue(it) }
                    if (order == null) {
                        skip("tag without order/label") { "${itag.commentator} on $baseRef" }
                        continue
                    }
                    val candidates = candidatesByName[itag.commentator]
                        ?.filter { bookMetaById[it]?.sefariaDeclaredBaseTextBookIds?.contains(book.bookId) == true }
                        .orEmpty()
                    val targetBookId = when {
                        candidates.isEmpty() -> {
                            skip("no commentary book for tag") { "${itag.commentator} on $baseRef" }
                            continue
                        }
                        candidates.size > 1 -> {
                            skip("ambiguous commentary for tag") {
                                "${itag.commentator} on $baseRef -> ${candidates.map { enTitleByBookId[it] }}"
                            }
                            continue
                        }
                        else -> candidates.single()
                    }

                    val targetRef = buildCommentRef(
                        baseRef = baseRef,
                        baseEnTitle = book.enTitle,
                        commentaryEnTitle = enTitleByBookId.getValue(targetBookId),
                        order = order,
                    )
                    if (targetRef == null) {
                        skip("base ref without title prefix") { "$baseRef (base ${book.enTitle})" }
                        continue
                    }
                    val entries = refsByCanonical[canonicalCitation(targetRef)]
                    if (entries.isNullOrEmpty()) {
                        skip("comment ref not found") { targetRef }
                        continue
                    }
                    var anchored = false
                    for (entry in entries) {
                        val tgtLineId = lineKeyToId[entry.path to (entry.lineIndex - 1)] ?: continue
                        for (linkId in repository.getLinkIdsBetweenLines(srcLineId, tgtLineId)) {
                            batch += LinkAnchor(
                                linkId = linkId,
                                side = 0,
                                charStart = itag.charStart,
                                charEnd = null,
                                label = itag.label,
                            )
                            anchored = true
                        }
                    }
                    if (!anchored) {
                        skip("no stored link for anchor") { "$baseRef -> $targetRef" }
                        continue
                    }
                    if (batch.size >= SefariaImportTuning.LINK_BATCH_SIZE) {
                        anchorsInserted += batch.size
                        repository.insertLinkAnchorsBatch(batch)
                        batch.clear()
                    }
                }
            }
        }
        if (batch.isNotEmpty()) {
            anchorsInserted += batch.size
            repository.insertLinkAnchorsBatch(batch)
            batch.clear()
        }

        logger.i { "Inline anchors: $anchorsInserted anchors from $linesWithItags itag-carrying lines" }
        if (skipCounts.isNotEmpty()) {
            val details = skipCounts.entries
                .sortedByDescending { it.value }
                .joinToString("; ") { (reason, count) ->
                    "$reason: $count (e.g. ${skipSamples[reason]})"
                }
            logger.w { "Inline anchors skipped tags — $details" }
        }
    }

}

/**
 * Constructs the comment ref addressed by an itag: the base segment's ref
 * with the base title swapped for the commentary title and the last address
 * component replaced by the comment number. Examples:
 *   "Shulchan Arukh, Orach Chayim 1:5"  order=7 -> "Turei Zahav on Shulchan Arukh, Orach Chayim 1:7"
 *   "Rif Berakhot 2a:5"                 order=3 -> "Hagahot HaBach on Rif Berakhot 2a:3"
 *   "Tur, Orach Chayim 1"               order=2 -> "Beit Yosef, Orach Chayim 1:2"
 */
internal fun buildCommentRef(
    baseRef: String,
    baseEnTitle: String,
    commentaryEnTitle: String,
    order: Int,
): String? {
    if (!baseRef.startsWith(baseEnTitle)) return null
    val remainder = baseRef.removePrefix(baseEnTitle)
    val lastColon = remainder.lastIndexOf(':')
    val addressBase = if (lastColon >= 0) remainder.substring(0, lastColon) else remainder
    return "$commentaryEnTitle$addressBase:$order"
}
