package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Exact resolution of ranged Sefaria citations ("Exodus 1:1-6:1",
 * "Rashi on Genesis 18:10:2-3", "Berakhot 2a:1-2b:4").
 *
 * A range is a dash inside the citation's trailing numeric address run —
 * dashes inside title words never make a range. The end address inherits the
 * start's leading components: "13:11-13" → start 13:11, end 13:13.
 */

// Trailing "<start>-<end>" address run, preceded by a space: group 1 = start
// components, group 2 = end components. Anchored at end of the citation.
private val CITATION_RANGE_REGEX =
    Regex(" ((?:\\d+[ab]?)(?::\\d+[ab]?)*)-((?:\\d+[ab]?)(?::\\d+[ab]?)*)$")

// Trailing address run of a non-ranged canonical ref ("title 13:11:1" → "13:11:1").
private val TRAILING_ADDRESS_REGEX =
    Regex(" ((?:\\d+[ab]?)(?::\\d+[ab]?)*)$")

internal data class CitationRange(
    /** Canonical citation of the range's first address, e.g. "rashi on genesis 13:11". */
    val startCanonical: String,
    /** Canonical citation of the range's last address, e.g. "rashi on genesis 13:13". */
    val endCanonical: String,
)

/**
 * Parses a canonical citation into a [CitationRange], or null when the
 * citation has no trailing address range. The end side may cite fewer
 * components than the start ("18:10:2-3"); missing leading components are
 * inherited from the start. An end with MORE components than the start is
 * malformed and returns null.
 */
internal fun parseCitationRange(canonical: String): CitationRange? {
    val m = CITATION_RANGE_REGEX.find(canonical) ?: return null
    val titlePart = canonical.substring(0, m.range.first)
    if (titlePart.isBlank()) return null
    val startComps = m.groupValues[1].split(':')
    val endComps = m.groupValues[2].split(':')
    if (endComps.size > startComps.size) return null
    val fullEnd = startComps.dropLast(endComps.size) + endComps
    if (fullEnd == startComps) return null
    return CitationRange(
        startCanonical = "$titlePart ${startComps.joinToString(":")}",
        endCanonical = "$titlePart ${fullEnd.joinToString(":")}",
    )
}

/**
 * Per-book-path index: canonical citation prefix → the LAST (max lineIndex)
 * leaf entry under that prefix. Prefix keys are emitted at every address
 * depth ("title 13", "title 13:11", "title 13:11:1"), so a range end cited at
 * any level resolves to the last line it covers. Refs without a trailing
 * address run are keyed by their full canonical form only.
 */
internal class PathRefPrefixIndex private constructor(
    private val lastByPrefix: Map<String, RefEntry>,
) {
    fun lastUnder(canonicalPrefix: String): RefEntry? = lastByPrefix[canonicalPrefix]

    companion object {
        fun build(entries: List<RefEntry>): PathRefPrefixIndex {
            val last = HashMap<String, RefEntry>(entries.size * 2)
            for (entry in entries) {
                val canonical = canonicalCitation(entry.ref)
                for (prefix in addressPrefixes(canonical)) {
                    val current = last[prefix]
                    if (current == null || entry.lineIndex > current.lineIndex) {
                        last[prefix] = entry
                    }
                }
            }
            return PathRefPrefixIndex(last)
        }
    }
}

/** "title 13:11:1" → ["title 13", "title 13:11", "title 13:11:1"]. */
internal fun addressPrefixes(canonical: String): List<String> {
    val m = TRAILING_ADDRESS_REGEX.find(canonical) ?: return listOf(canonical)
    val titlePart = canonical.substring(0, m.range.first)
    if (titlePart.isBlank()) return listOf(canonical)
    val comps = m.groupValues[1].split(':')
    val prefixes = ArrayList<String>(comps.size)
    val sb = StringBuilder(titlePart)
    for (comp in comps) {
        sb.append(if (prefixes.isEmpty()) " " else ":").append(comp)
        prefixes += sb.toString()
    }
    return prefixes
}
