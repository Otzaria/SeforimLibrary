package io.github.kdroidfilter.seforimlibrary.sefariasqlite

internal data class LinkerTarget(val first: RefEntry, val last: RefEntry?)

/**
 * Resolve only the address the producer requested. An exact proper section prefix
 * may select its descendants; a missing segment must never select its parent.
 * Resolve both range endpoints before accepting either one. The legacy Sefaria
 * CSV resolver deliberately has different fallback rules and is left unchanged.
 */
internal fun resolveLinkerTarget(
    citation: String,
    refsByCanonical: Map<String, List<RefEntry>>,
    sections: SectionRefIndex,
    onAmbiguous: () -> Unit = {},
): LinkerTarget? {
    fun unit(canonical: String): LinkerTarget? {
        val exact = refsByCanonical[canonical].orEmpty().distinctBy { it.path to it.lineIndex }
        if (exact.size > 1) onAmbiguous()
        if (exact.isNotEmpty()) return exact.singleOrNull()?.let { LinkerTarget(it, null) }
        val section = sectionTarget(canonical, sections) ?: return null
        return LinkerTarget(section.first, section.second)
    }
    val canonical = canonicalCitation(citation)
    val range = parseCitationRange(canonical)
    if (range == null) return unit(canonical)
    val start = unit(range.startCanonical) ?: return null
    val end = unit(range.endCanonical) ?: return null
    val last = end.last ?: end.first
    if (start.first.path != last.path || last.lineIndex < start.first.lineIndex) return null
    return LinkerTarget(start.first, last.takeIf { it.lineIndex > start.first.lineIndex })
}
