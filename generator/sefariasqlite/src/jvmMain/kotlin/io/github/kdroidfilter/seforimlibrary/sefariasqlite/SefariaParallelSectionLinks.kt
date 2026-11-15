package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Two books sharing sections that Sefaria leaves unlinked, such as Torat HaBayit HaKatzar, the
 * Rashba's abridgement of HaAroch (Otzaria/otzaria#4890). Tied as RELATED: neither is a commentary.
 */
internal data class ParallelSectionBooks(
    /** Canonical title of the book stored as the link source. */
    val sourceTitle: String,
    /** Canonical title of the book stored as the link target. */
    val targetTitle: String,
    /**
     * Matches the address after the title and yields the section key, or null for a
     * segment outside the shared sections (an introduction).
     */
    val sectionKey: (address: String) -> String?,
)

// "the seventh house the women's house the first gate 1" → "seventh first".
private val HOUSE_GATE_SEGMENT = Regex("""the (\w+) house(?: .*)? the (\w+) gate \d+""")

internal val PARALLEL_SECTION_BOOKS = listOf(
    ParallelSectionBooks(
        sourceTitle = "torat habayit haaroch",
        targetTitle = "torat habayit hakatzar",
        sectionKey = { address ->
            HOUSE_GATE_SEGMENT.matchEntire(address)?.destructured?.let { (house, gate) -> "$house $gate" }
        },
    ),
)

/** The first and last segment of one section of a book. */
internal data class SectionSpan(val first: RefEntry, val last: RefEntry)

/** One section shared by both books of a [ParallelSectionBooks] pair. */
internal data class ParallelSectionLink(val source: SectionSpan, val target: SectionSpan)

/**
 * Pairs the sections the two books of [books] have in common, in source order. A section
 * found in one book only gets no link.
 */
internal fun parallelSectionLinks(
    books: ParallelSectionBooks,
    refsByPath: Map<String, List<RefEntry>>,
): List<ParallelSectionLink> {
    val sources = sectionSpans(refsByPath, books.sourceTitle, books.sectionKey)
    val targets = sectionSpans(refsByPath, books.targetTitle, books.sectionKey)
    return sources.mapNotNull { (key, source) -> targets[key]?.let { ParallelSectionLink(source, it) } }
}

private fun sectionSpans(
    refsByPath: Map<String, List<RefEntry>>,
    title: String,
    sectionKey: (String) -> String?,
): Map<String, SectionSpan> {
    val prefix = "$title "
    val spans = LinkedHashMap<String, SectionSpan>()
    // A path is one book; spans never mix two paths of the same title.
    val entries = refsByPath.values.firstOrNull { refs ->
        refs.firstOrNull()?.let { canonicalCitation(it.ref).startsWith(prefix) } == true
    } ?: return spans
    for (entry in entries) {
        val canonical = canonicalCitation(entry.ref)
        if (!canonical.startsWith(prefix)) continue
        val key = sectionKey(canonical.removePrefix(prefix)) ?: continue
        val span = spans[key]
        spans[key] = when {
            span == null -> SectionSpan(entry, entry)
            entry.lineIndex < span.first.lineIndex -> span.copy(first = entry)
            entry.lineIndex > span.last.lineIndex -> span.copy(last = entry)
            else -> span
        }
    }
    return spans.entries.sortedBy { it.value.first.lineIndex }.associate { it.key to it.value }
}
