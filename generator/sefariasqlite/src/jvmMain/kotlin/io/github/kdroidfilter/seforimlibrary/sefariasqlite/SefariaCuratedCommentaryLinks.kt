package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import java.util.concurrent.ConcurrentHashMap

/**
 * Commentary segments that Sefaria leaves without a link to their base, linked to a reviewed base
 * line (Otzaria/otzaria#3264). [RESOURCE] names both lines by heRef and keeps the words that open
 * the segment: an entry whose ref is missing or ambiguous, or whose segment no longer opens with
 * those words, is skipped, so a renumbered export never links the wrong paragraph. A segment that
 * Sefaria has since linked to the base, or covers with a range, keeps Sefaria's link alone.
 */
internal object SefariaCuratedCommentaryLinks {

    const val RESOURCE = "curated_commentary_links.tsv"

    internal data class Entry(val commentaryHeRef: String, val baseHeRef: String, val opening: String)

    internal data class Resolved(
        val entry: Entry,
        val commentaryBookId: Long,
        val commentaryLineId: Long,
        val commentaryLineIndex: Int,
        val baseBookId: Long,
        val baseLineId: Long,
    )

    private val TAG = Regex("<[^>]+>")
    private val SPACES = Regex("""\s+""")

    fun load(classLoader: ClassLoader?): List<Entry> {
        val stream = classLoader?.getResourceAsStream(RESOURCE)
            ?: throw IllegalStateException("$RESOURCE missing from the sefariasqlite resources")
        return stream.bufferedReader(Charsets.UTF_8).useLines(::parse)
    }

    internal fun parse(lines: Sequence<String>): List<Entry> {
        val entries = lines.filter { it.isNotBlank() && !it.startsWith("#") }.map { row ->
            val cols = row.split('\t')
            require(cols.size == 3 && cols.none { it.isBlank() }) { "$RESOURCE: expected 3 columns in '$row'" }
            Entry(cols[0], cols[1], cols[2])
        }.toList()
        require(entries.map { it.commentaryHeRef }.distinct().size == entries.size) { "$RESOURCE: duplicate segment" }
        return entries
    }

    /** Resolves each entry's lines; entries whose refs do not name exactly one line are logged and dropped. */
    fun resolve(
        entries: List<Entry>,
        refs: List<RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        logger: Logger,
    ): List<Resolved> {
        val wanted = entries.flatMapTo(HashSet()) { listOf(it.commentaryHeRef, it.baseHeRef) }
        val byHeRef = refs.filter { it.heRef in wanted }.groupBy { it.heRef }

        fun line(heRef: String): Triple<Long, Long, Int>? {
            val ref = byHeRef[heRef]?.singleOrNull() ?: return null
            val lineId = lineKeyToId[ref.path to ref.lineIndex - 1] ?: return null
            val bookId = lineIdToBookId[lineId] ?: return null
            return Triple(bookId, lineId, ref.lineIndex - 1)
        }

        return entries.mapNotNull { entry ->
            val commentary = line(entry.commentaryHeRef)
            val base = line(entry.baseHeRef)
            if (commentary == null || base == null) {
                logger.w { "Curated commentary link skipped, ref not found once: ${entry.commentaryHeRef} -> ${entry.baseHeRef}" }
                return@mapNotNull null
            }
            Resolved(entry, commentary.first, commentary.second, commentary.third, base.first, base.second)
        }
    }

    /** True when the visible text of [content] opens with the words of [opening]. */
    internal fun opensWith(content: String, opening: String): Boolean {
        val expected = SPACES.split(opening.trim())
        val visible = SPACES.split(TAG.replace(content, " ").trim())
        return visible.take(expected.size) == expected
    }

    /**
     * The links to write: entries still unlinked to their base whose segment opens with the
     * entry's words. [contentOf] reads a stored line.
     */
    suspend fun linksToWrite(
        resolved: List<Resolved>,
        guard: Guard,
        contentOf: suspend (Long) -> String?,
        logger: Logger,
    ): List<DerivedCommentaryLink> = resolved.mapNotNull { r ->
        if (guard.isLinked(r)) return@mapNotNull null
        if (!opensWith(contentOf(r.commentaryLineId).orEmpty(), r.entry.opening)) {
            logger.w { "Curated commentary link skipped, segment text changed: ${r.entry.commentaryHeRef}" }
            return@mapNotNull null
        }
        DerivedCommentaryLink(
            stage = DerivedLinkStage.CURATED,
            baseBookId = r.baseBookId,
            commentaryBookId = r.commentaryBookId,
            baseLineId = r.baseLineId,
            commentaryLineId = r.commentaryLineId,
            commentaryLineIndex = r.commentaryLineIndex,
        )
    }

    /** Records the links and ranges Sefaria gives the curated segments while the CSVs stream. */
    internal class Guard(resolved: List<Resolved>) {
        private val baseByLineId = resolved.associate { it.commentaryLineId to it.baseBookId }
        private val indexesByBookId = resolved.groupBy({ it.commentaryBookId }, { it.commentaryLineIndex })
        private val linked = ConcurrentHashMap.newKeySet<Long>()
        private val ranged = ConcurrentHashMap.newKeySet<Pair<Long, Int>>()

        fun record(link: Link) {
            if (baseByLineId[link.sourceLineId] == link.targetBookId) linked += link.sourceLineId
            if (baseByLineId[link.targetLineId] == link.sourceBookId) linked += link.targetLineId
        }

        /** Links the generator's own earlier stages wrote, which bypass the CSV callbacks. */
        fun recordDerived(links: List<DerivedCommentaryLink>) {
            links.forEach { if (baseByLineId[it.commentaryLineId] == it.baseBookId) linked += it.commentaryLineId }
        }

        fun recordRange(bookId: Long, firstIndex: Int, lastIndex: Int) {
            indexesByBookId[bookId]?.forEach { if (it in firstIndex..lastIndex) ranged += bookId to it }
        }

        fun isLinked(r: Resolved): Boolean =
            r.commentaryLineId in linked || (r.commentaryBookId to r.commentaryLineIndex) in ranged
    }
}
