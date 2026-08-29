package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * The refs Sefaria refuses to surface on the cited side: whole Talmud/Mishnah/
 * Tosefta **perakim** and whole Torah **parashiyot**.
 *
 * Sefaria stores such a link with a range ref on the cited side ("Migdal Oz on
 * Mishneh Torah, Hilchot Gezeilah v'Aveidah 5:1" ↔ "Bava Batra 28a:1-60b:22")
 * and expands it into `expandedRefs`, so the connections query does return it
 * for every segment in the perek. `get_links()` then drops it, because the
 * anchor-side ref is in one of two sets (`sefaria/client/wrapper.py`).
 *
 * We rebuild both sets and use them for exactly one decision: a ranged citation
 * whose whole canonical form is a member gets its `link_range` row (so the panel
 * still shows the cited extent) but **no** `link_coverage` rows, so it stops
 * appearing on every segment of the perek/parasha. Membership is an exact
 * canonical-string match on the full citation, like Sefaria's `in` test — never
 * a width threshold or a span comparison.
 *
 * How faithfully each set is reproduced:
 *
 *  * **perek** — Sefaria's `get_talmud_perek_ref_set()` walks
 *    `index.get_referenceable_alone_nodes()` over four category paths. We walk
 *    the alt-struct nodes of the same paths and apply the same predicate
 *    ([AltNodePayload.referenceableAlone] = a `match_templates` entry scoped
 *    "any"/"alone"). Sefaria also walks the primary schema tree; as of the
 *    2026-08 export no primary node in those categories carries a `wholeRef`,
 *    so it contributes nothing a ranged citation could match. Mishnah and
 *    Tosefta likewise contribute nothing: their perakim are section-level refs
 *    ("Mishnah Berakhot 1"), which are not ranged citations at all. In practice
 *    the set is Bavli + Yerushalmi.
 *  * **parasha** — Sefaria matches an alt-struct leaf's `titles`/`sharedTitle`
 *    against the `Parasha` TermSet. We do **not** export that TermSet, so this
 *    is a **proxy**: nodes under a Torah alt-struct whose key starts "Parasha".
 *    Both pick out the same 54 refs in the current export. The proxy would
 *    diverge if aliyot ever gained a `wholeRef`, so [build] fails loudly if a
 *    parasha node with a `wholeRef` has a descendant carrying one.
 */
internal object SefariaWholeUnitRefs {

    /** Sefaria's category paths for the perek set, matched as a prefix. */
    private val PEREK_CATEGORY_PATHS: Map<String, List<String>> = linkedMapOf(
        "Talmud/Bavli" to listOf("Talmud", "Bavli"),
        "Talmud/Yerushalmi" to listOf("Talmud", "Yerushalmi"),
        "Tosefta" to listOf("Tosefta"),
        "Mishnah" to listOf("Mishnah"),
    )

    private const val PARASHA_ALT_KEY_PREFIX = "parasha"

    private val TORAH_CATEGORY_PATH = listOf("Tanakh", "Torah")

    /**
     * Canonical citations of every whole perek / whole parasha, split by where
     * they came from so callers can verify each source independently — a single
     * flat set cannot tell "the parasha structure moved" from "all is well".
     */
    data class Result(
        /** Perek refs per [PEREK_CATEGORY_PATHS] key. Families with no ranged perek refs are absent. */
        val perekByFamily: Map<String, Set<String>>,
        val parasha: Set<String>,
    ) {
        val all: Set<String> = HashSet<String>().apply {
            perekByFamily.values.forEach { addAll(it) }
            addAll(parasha)
        }
    }

    fun build(payloads: Collection<BookPayload>): Result {
        val perek = LinkedHashMap<String, MutableSet<String>>()
        val parasha = HashSet<String>()
        for (payload in payloads) {
            val categories = payload.categoriesEn
            val family = PEREK_CATEGORY_PATHS.entries
                .firstOrNull { categories.startsWith(it.value) }?.key
            val isTorah = categories.startsWith(TORAH_CATEGORY_PATH)
            if (family == null && !isTorah) continue
            for (structure in payload.altStructures) {
                if (isTorah && structure.key.lowercase().startsWith(PARASHA_ALT_KEY_PREFIX)) {
                    collectParashiyot(structure, payload.enTitle, parasha)
                } else if (family != null) {
                    forEachNode(structure.nodes) { node ->
                        val wholeRef = node.wholeRef ?: return@forEachNode
                        if (node.referenceableAlone) {
                            perek.getOrPut(family) { HashSet() } += canonicalCitation(wholeRef)
                        }
                    }
                }
            }
        }
        return Result(perekByFamily = perek, parasha = parasha)
    }

    /**
     * Sefaria takes alt-struct **leaves** whose title is a Parasha term, so the
     * key-based proxy only holds while every parasha ref sits on a top-level
     * leaf. Any `wholeRef` deeper than that — an aliyah gaining one — or a
     * `wholeRef` on a node that has children fails the build rather than
     * silently suppressing a citation Sefaria would show.
     */
    private fun collectParashiyot(
        structure: AltStructurePayload,
        bookTitle: String,
        into: MutableSet<String>,
    ) {
        for (node in structure.nodes) {
            val nested = node.children.firstNotNullOfOrNull { firstWholeRefIn(it) }
            check(nested == null) {
                "Parasha structure of $bookTitle carries a nested wholeRef ('$nested') — the " +
                    "Parasha-key proxy for Sefaria's term matching no longer holds; match node " +
                    "titles against the Parasha terms instead"
            }
            val wholeRef = node.wholeRef ?: continue
            check(node.children.isEmpty()) {
                "Parasha node '$wholeRef' in $bookTitle has children, so it is not a leaf and " +
                    "Sefaria would not treat it as a parasha"
            }
            into += canonicalCitation(wholeRef)
        }
    }

    private fun <T> List<T>.startsWith(prefix: List<T>): Boolean =
        size >= prefix.size && subList(0, prefix.size) == prefix

    private fun forEachNode(nodes: List<AltNodePayload>, action: (AltNodePayload) -> Unit) {
        for (node in nodes) {
            action(node)
            forEachNode(node.children, action)
        }
    }

    private fun firstWholeRefIn(node: AltNodePayload): String? =
        node.wholeRef ?: node.children.firstNotNullOfOrNull { firstWholeRefIn(it) }
}
