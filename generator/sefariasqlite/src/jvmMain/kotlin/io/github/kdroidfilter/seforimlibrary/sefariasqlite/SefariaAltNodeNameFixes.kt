package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger

/**
 * Hebrew names of Sefaria alt-structure nodes that are wrong upstream, fixed
 * by the node's range rather than by its (wrong) name, so the fix stays right
 * if Sefaria corrects the data and stays put if it does not.
 *
 * Every fix carries the evidence from the book's own text. A fix whose node is
 * no longer found is reported: the structure changed and the fix needs review.
 * The names reach the commentaries too — `inheritChaptersAltToc` copies a
 * base's Chapters names into books that inherit them.
 */
internal object SefariaAltNodeNameFixes {

    data class Fix(
        val bookTitle: String,
        val structureKey: String,
        val wholeRef: String,
        val heTitle: String,
        val evidence: String,
    )

    // Rif Megillah follows the Mishnah order (בני העיר, then הקורא עומד), not
    // the Bavli's; Sefaria named its chapters 3 and 4 in the Bavli order.
    internal val FIXES = listOf(
        Fix(
            bookTitle = "Rif Megillah",
            structureKey = "Chapters",
            wholeRef = "Rif Megillah 7a:4-11b:5",
            heTitle = "בני העיר",
            evidence = "7a:4 opens 'בני העיר שמכרו רחובה של עיר'; 11b:5 ends 'סליקו להו בני העיר'",
        ),
        Fix(
            bookTitle = "Rif Megillah",
            structureKey = "Chapters",
            wholeRef = "Rif Megillah 11b:6-16b:9",
            heTitle = "הקורא עומד",
            evidence = "11b:6 opens 'הקורא את המגילה עומד ויושב' (Bavli 21a)",
        ),
    )

    private val FIXES_BY_BOOK = FIXES.groupBy { it.bookTitle }

    fun apply(
        bookTitle: String,
        structures: List<AltStructurePayload>,
        logger: Logger? = null,
        fixes: List<Fix> = FIXES_BY_BOOK[bookTitle].orEmpty(),
    ): List<AltStructurePayload> {
        if (fixes.isEmpty()) return structures
        val applied = HashSet<Fix>()
        val fixed = structures.map { structure ->
            val forStructure = fixes.filter { it.structureKey == structure.key }
            if (forStructure.isEmpty()) structure
            else structure.copy(nodes = structure.nodes.map { fixNode(it, forStructure, applied, logger, bookTitle) })
        }
        for (fix in fixes - applied) {
            logger?.w {
                "Alt-structure name fix not applied: no '${fix.structureKey}' node " +
                    "'${fix.wholeRef}' in $bookTitle — the structure changed, review the fix"
            }
        }
        return fixed
    }

    private fun fixNode(
        node: AltNodePayload,
        fixes: List<Fix>,
        applied: MutableSet<Fix>,
        logger: Logger?,
        bookTitle: String,
    ): AltNodePayload {
        val children = node.children.map { fixNode(it, fixes, applied, logger, bookTitle) }
        val fix = fixes.firstOrNull { it.wholeRef == node.wholeRef }
        if (fix == null) return if (children == node.children) node else node.copy(children = children)
        applied += fix
        if (node.heTitle == fix.heTitle) {
            logger?.i { "Alt-structure name fix for $bookTitle '${fix.wholeRef}' already upstream — can be removed" }
        } else {
            logger?.i { "Alt-structure name fix: $bookTitle '${fix.wholeRef}' '${node.heTitle}' → '${fix.heTitle}'" }
        }
        return node.copy(heTitle = fix.heTitle, children = children)
    }
}
