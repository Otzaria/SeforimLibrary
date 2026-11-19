package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
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
    /** The app finds the entry's line by searching for `(text)` or its ע"א/ע"ב form. */
    val text get() = "דף $daf$side"
    val key get() = value * 2 + if (side == '.') 0 else 1
}

// Nothing but the daf and amud inside round brackets: `(דף ח: ע"ש)` and `(דף לעיל מח:)`
// are references. A stray extra `(` is common in the text, so `((דף` counts too.
private val GEMARA_DAF_MARKER = Regex("""(?<!\()(\(?)\(דף ([א-ת"'׳״]{1,5})(?:([.:])| ע"([אב]))\)""")
private val GEMARA_DAF_HEADING = Regex("""דף ([א-ת"'׳״]{1,5})[.:]""")
private val HTML_TAG = Regex("<[^>]*>")
private val WHITESPACE = Regex("""\s+""")
private val REFERENCE_WORD = Regex("""ו?(?:ד|כד)?(?:לקמן|לעיל|בפרק|בפירקא)""")

/** `לקמן ((דף יט ע"ב)`: one of the three words before a `((` marker makes it a reference. */
private fun followsReferenceWord(textBefore: String): Boolean =
    textBefore.replace(HTML_TAG, " ").split(WHITESPACE).filter { it.isNotEmpty() }.takeLast(3)
        .any { REFERENCE_WORD.matches(it.trim('(', ')', '\'', '"', ',', '.', ':')) }

/** The Gemara-daf markers of [content], in order. */
internal fun findGemaraDafMarkers(content: String): List<GemaraAmud> =
    GEMARA_DAF_MARKER.findAll(content).mapNotNull { match ->
        val (extraParen, daf, side, amud) = match.destructured
        if (extraParen.isNotEmpty() && followsReferenceWord(content.substring(0, match.range.first))) return@mapNotNull null
        val value = gematriaValue(daf) ?: return@mapNotNull null
        GemaraAmud(daf, value, side.firstOrNull() ?: if (amud == "א") '.' else ':')
    }.toList()

internal data class GemaraDafPlan(
    val entries: List<PlannedMarkerEntry>,
    val ownerPathByLineIndex: Map<Int, String>,
    /** Markers past [planGemaraDafEntries]'s lastDaf, with their line index. */
    val dropped: List<Pair<GemaraAmud, Int>> = emptyList(),
)

/**
 * Plans the structure; markers whose daf is past [lastDaf] (the tractate's end) cite
 * another tractate and are dropped.
 */
internal fun planGemaraDafEntries(lines: List<MarkerLineRow>, toc: List<MarkerTocRow>, lastDaf: Int? = null): GemaraDafPlan {
    val tocById = toc.associateBy { it.id }
    val soleRoot = toc.filter { it.parentId == null }.singleOrNull()?.id
    // Several markers of one amud on one line are one occurrence.
    val occurrences = LinkedHashMap<Int, MutableList<Pair<GemaraAmud, MarkerLineRow>>>()
    val lastKeyByLineIndex = HashMap<Int, Int>()
    val dropped = mutableListOf<Pair<GemaraAmud, Int>>()
    for (line in lines) {
        val (markers, beyond) = findGemaraDafMarkers(line.content).partition { lastDaf == null || it.value <= lastDaf }
        beyond.mapTo(dropped) { it to line.index }
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
        // One child per Rif page; lines without a page heading stay separate.
        val byPage = list.groupBy { (_, line) -> line.tocEntryId?.takeIf { it != soleRoot && it in tocById } ?: -1L - line.index }
            .values.toList()
        val (amud, first) = byPage.first().first()
        entries += PlannedMarkerEntry(path, null, 0, amud.text, first.id, first.index, isMarker = byPage.size == 1)
        if (byPage.size == 1) {
            for ((_, line) in byPage.single()) pathByOccurrence[key to line.index] = path
            return@forEachIndexed
        }
        byPage.forEachIndexed { j, group ->
            val (own, line) = group.first()
            val heading = line.tocEntryId?.takeIf { it != soleRoot }?.let(tocById::get)?.text?.trimHeadingStart()?.trim()
            val childPath = altTocChildPath(path, j + 1)
            val text = "${own.text} – ${heading?.let { "רי\"ף $it" } ?: "${j + 1}"}"
            entries += PlannedMarkerEntry(childPath, path, 1, text, line.id, line.index, isMarker = true)
            for ((_, occurrence) in group) pathByOccurrence[key to occurrence.index] = childPath
        }
    }
    val owners = lastKeyByLineIndex.mapValues { (index, key) -> pathByOccurrence.getValue(key to index) }
    return GemaraDafPlan(entries, owners, dropped)
}

/**
 * Last daf of the Bavli tractate a `רי"ף X` book follows: the highest `דף` heading of
 * the one book titled X that has them; null when there is no single such book.
 */
internal fun bavliLastDaf(conn: Connection, rifTitle: String): Int? {
    val tractate = rifTitle.removePrefix(RIF_TITLE_PREFIX).takeIf { it != rifTitle } ?: return null
    val lastByBook = conn.prepareStatement(
        """
        SELECT t.bookId, tt.text FROM tocEntry t JOIN tocText tt ON tt.id = t.textId
        WHERE t.bookId IN (SELECT id FROM book WHERE title = ?)
        """.trimIndent(),
    ).use { st ->
        st.setString(1, tractate)
        st.executeQuery().use { rs ->
            val last = HashMap<Long, Int>()
            while (rs.next()) {
                val daf = GEMARA_DAF_HEADING.matchEntire(rs.getString(2).trimHeadingStart().trim())?.groupValues?.get(1)
                val value = daf?.let(::gematriaValue) ?: continue
                last.merge(rs.getLong(1), value, ::maxOf)
            }
            last
        }
    }
    return lastByBook.values.singleOrNull()
}

private const val RIF_TITLE_PREFIX = "רי\"ף "

/** Atomically replaces each rule's structure; returns entry counts by book title. */
internal fun writeGemaraDafAltTocs(
    conn: Connection,
    rules: List<GemaraDafRule>,
    stableIds: AttachedBuildStateIds? = null,
): Map<String, Int> = writeEachBookAtomically(conn, GEMARA_DAF_ALT_TOC_FILE, rules.map { it.bookTitle to it }) { bookId, rule ->
    val logger = Logger.withTag("GemaraDafAltToc")
    val lastDaf = bavliLastDaf(conn, rule.bookTitle)
    if (lastDaf == null) logger.w { "${rule.bookTitle}: no single Bavli tractate found; markers are not limited to its dapim" }
    val lines = readMarkerLines(conn, bookId)
    val plan = planGemaraDafEntries(lines, readMarkerToc(conn, bookId), lastDaf)
    if (plan.dropped.isNotEmpty()) {
        logger.i {
            "${rule.bookTitle}: dropped ${plan.dropped.size} markers past the tractate's end: " +
                plan.dropped.joinToString { (amud, index) -> "${amud.text} (line $index)" }
        }
    }
    check(plan.entries.isNotEmpty()) { "$GEMARA_DAF_ALT_TOC_FILE: no '(דף …)' marker in '${rule.bookTitle}'" }
    writeMarkerStructure(
        conn, bookId, rule.structureKey, rule.structureHeTitle, plan.entries, lines, stableIds, plan.ownerPathByLineIndex,
    )
}
