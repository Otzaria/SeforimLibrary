package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.altTocChildPath
import java.sql.Connection

/**
 * Alt-TOC by the Gemara daf a commentary cites inline — `(דף ח:)` or `(דף יז ע"א)` in the
 * middle of a Rif paragraph — for the books listed in ForDB [GEMARA_DAF_ALT_TOC_FILE].
 *
 * Entries follow the Gemara's order, one per amud; an amud the text reaches from more
 * than one main-TOC heading (Rif page) gets a child per heading, in reading order.
 * Each line belongs to the entry of the last marker at or before it.
 * Like [INLINE_MARKER_ALT_TOC_FILE], the stage owns the rule's structure key for the
 * listed books, and ids come from the build-state allocator keyed on ancestor paths.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:buildGemaraDafAltToc -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) =
    runMarkerAltTocStage(args, "GemaraDafAltToc", GEMARA_DAF_ALT_TOC_FILE, ::parseGemaraDafRules) { conn, rules, ids ->
        writeGemaraDafAltTocs(conn, rules, ids)
    }

internal const val GEMARA_DAF_ALT_TOC_FILE = "gemara_daf_alt_toc.csv"
private const val GEMARA_DAF_HEADER = "bookName,structureKey,structureHeTitle"

internal data class GemaraDafRule(val bookTitle: String, val structureKey: String, val structureHeTitle: String)

/** Fails on the first malformed row: the build must not silently drop a rule. */
internal fun parseGemaraDafRules(lines: List<String>): List<GemaraDafRule> {
    val rows = forDbText(lines).lines().map { it.trimEnd('\r') }.filter { it.isNotBlank() }
    if (rows.isEmpty()) return emptyList()
    require(rows.first().trim() == GEMARA_DAF_HEADER) {
        "$GEMARA_DAF_ALT_TOC_FILE: header must be '$GEMARA_DAF_HEADER'"
    }
    val rules = rows.drop(1).mapIndexed { i, row ->
        val cells = row.split(',').map { it.trim() }
        val where = "$GEMARA_DAF_ALT_TOC_FILE row ${i + 2}"
        require(cells.size == 3) { "$where: expected 3 cells, got ${cells.size}" }
        val (book, key, heTitle) = cells
        require(book.isNotEmpty() && heTitle.isNotEmpty()) { "$where: bookName and structureHeTitle are required" }
        require(STRUCTURE_KEY_REGEX.matches(key) && key !in RESERVED_STRUCTURE_KEYS) {
            "$where: structureKey '$key' must be an English identifier outside $RESERVED_STRUCTURE_KEYS"
        }
        GemaraDafRule(book, key, heTitle)
    }
    val duplicate = rules.groupBy { it.bookTitle }.filterValues { it.size > 1 }.keys
    require(duplicate.isEmpty()) { "$GEMARA_DAF_ALT_TOC_FILE: more than one rule for $duplicate" }
    return rules
}

/** One amud; [daf] keeps the letters as written, [value] orders and identifies it. */
internal data class GemaraAmud(val daf: String, val value: Int, val side: Char) {
    val text get() = "דף $daf$side"
    val key get() = value * 2 + if (side == '.') 0 else 1
}

// Nothing but the daf and amud inside the brackets: `(דף ח: ע"ש)`, `(דף לעיל מח:)` are references.
private val GEMARA_DAF_MARKER = Regex("""([(\[])דף ([א-ת"'׳״]{1,5})(?:([.:])|\s+ע["״]([אב]))([)\]])""")

/** The Gemara-daf markers of [content], in order. */
internal fun findGemaraDafMarkers(content: String): List<GemaraAmud> =
    GEMARA_DAF_MARKER.findAll(content).mapNotNull { match ->
        val (open, daf, side, amud, close) = match.destructured
        if ((open == "(") != (close == ")")) return@mapNotNull null
        val value = gematriaValue(daf) ?: return@mapNotNull null
        GemaraAmud(daf, value, side.firstOrNull() ?: if (amud == "א") '.' else ':')
    }.toList()

internal data class GemaraDafPlan(val entries: List<PlannedMarkerEntry>, val ownerPathByLineIndex: Map<Int, String>)

internal fun planGemaraDafEntries(lines: List<MarkerLineRow>, toc: List<MarkerTocRow>): GemaraDafPlan {
    val tocById = toc.associateBy { it.id }
    val soleRoot = toc.filter { it.parentId == null }.singleOrNull()?.id
    // Several markers of one amud on one line are one occurrence.
    val occurrences = LinkedHashMap<Int, MutableList<Pair<GemaraAmud, MarkerLineRow>>>()
    val lastKeyByLineIndex = HashMap<Int, Int>()
    for (line in lines) {
        val markers = findGemaraDafMarkers(line.content)
        for (amud in markers) {
            val list = occurrences.getOrPut(amud.key) { mutableListOf() }
            if (list.lastOrNull()?.second?.index != line.index) list += amud to line
        }
        markers.lastOrNull()?.let { lastKeyByLineIndex[line.index] = it.key }
    }

    val entries = mutableListOf<PlannedMarkerEntry>()
    val pathByOccurrence = HashMap<Pair<Int, Int>, String>()
    occurrences.entries.sortedBy { it.key }.forEachIndexed { i, (key, list) ->
        val path = altTocChildPath("", i + 1)
        val amud = list.first().first
        // One child per Rif page; lines without a page heading stay separate.
        val byPage = list.groupBy { (_, line) -> line.tocEntryId?.takeIf { it != soleRoot && it in tocById } ?: -1L - line.index }
            .values.map { group -> group.map { it.second } }
        val first = byPage.first().first()
        entries += PlannedMarkerEntry(path, null, 0, amud.text, first.id, first.index, isMarker = byPage.size == 1)
        if (byPage.size == 1) {
            for (line in byPage.single()) pathByOccurrence[key to line.index] = path
            return@forEachIndexed
        }
        byPage.forEachIndexed { j, group ->
            val line = group.first()
            val heading = line.tocEntryId?.takeIf { it != soleRoot }?.let(tocById::get)?.text?.trimHeadingStart()?.trim()
            val childPath = altTocChildPath(path, j + 1)
            val text = "${amud.text} – ${heading?.let { "רי\"ף $it" } ?: "${j + 1}"}"
            entries += PlannedMarkerEntry(childPath, path, 1, text, line.id, line.index, isMarker = true)
            for (occurrence in group) pathByOccurrence[key to occurrence.index] = childPath
        }
    }
    val owners = lastKeyByLineIndex.mapValues { (index, key) -> pathByOccurrence.getValue(key to index) }
    return GemaraDafPlan(entries, owners)
}

/** Atomically replaces each rule's structure; returns entry counts by book title. */
internal fun writeGemaraDafAltTocs(
    conn: Connection,
    rules: List<GemaraDafRule>,
    stableIds: AttachedBuildStateIds? = null,
): Map<String, Int> = writeEachBookAtomically(conn, GEMARA_DAF_ALT_TOC_FILE, rules.map { it.bookTitle to it }) { bookId, rule ->
    val lines = readMarkerLines(conn, bookId)
    val plan = planGemaraDafEntries(lines, readMarkerToc(conn, bookId))
    check(plan.entries.isNotEmpty()) { "$GEMARA_DAF_ALT_TOC_FILE: no '(דף …)' marker in '${rule.bookTitle}'" }
    writeMarkerStructure(
        conn, bookId, rule.structureKey, rule.structureHeTitle, plan.entries, lines, stableIds, plan.ownerPathByLineIndex,
    )
}
