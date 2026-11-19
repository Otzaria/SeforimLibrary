package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.altTocChildPath
import java.sql.Connection

/**
 * Alt-TOC by the Gemara daf a commentary cites inline — `(דף ח:)` or `(דף יז ע"א)` in the
 * middle of a Rif paragraph — for the books listed in ForDB [GEMARA_DAF_ALT_TOC_FILE].
 *
 * Entries follow the Gemara's order, one per amud; an amud the text reaches from more
 * than one main-TOC heading (Rif page) gets a child per heading, in reading order.
 * A marker needs a link to the corresponding page of the declared base tractate.
 * Without a marker, the line can keep the preceding entry if its links confirm
 * that it still discusses that page.
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

private val GEMARA_DAF_MARKER = Regex("""(?<!\()(?:\()?\(דף ([א-ת"'׳״]{1,5})(?:([.:])| ע["״]([אב]))?( ע["״]ש)?\)""")
private val HTML_TAG = Regex("<[^>]*>")
private val WHITESPACE = Regex("""\s+""")
private val HEBREW_MARK = Regex("""\p{M}+""")
private val HEBREW_WORD = Regex("[א-ת]+")
private const val INLINE_QUOTE_WORDS = 8
private val OPENING_PREFIX = Regex("""(?:(?:גמ(?:רא)?|מתני(?:תין|תהין)?)["'״׳]?)?""")
private val REFERENCE_WORD = Regex("""ו?(?:ד|כד)?(?:לקמן|לעיל|בפרק|בפירקא)|במסכת""")

internal data class GemaraDafEvidence(
    val baseTextByAmud: Map<Int, List<String>> = emptyMap(),
    val foreignAmudim: Set<Int> = emptySet(),
)

private data class PrintedDaf(
    val letters: String,
    val number: Int,
    val side: Char?,
    val qualified: Boolean,
    val range: IntRange,
) {
    fun possibleAmudim(): List<GemaraAmud> =
        (side?.let(::listOf) ?: listOf('.', ':')).map { GemaraAmud(letters, number, it) }
}

private fun hebrewWords(text: String): Sequence<String> =
    HEBREW_WORD.findAll(HEBREW_MARK.replace(text, "")).map { it.value }

/**
 * An inline citation can name the same daf number as a base link while quoting
 * a different tractate. Require a contiguous quotation from immediately after
 * an inline marker in a linked segment of that page. A marker at the start
 * already marks the start of the paragraph, including `גמ' (דף עג: ע"ש)`.
 */
internal fun findGemaraDafMarkers(
    content: String,
    evidence: GemaraDafEvidence,
): List<GemaraAmud> {
    if (evidence.baseTextByAmud.isEmpty()) return emptyList()
    val plain = WHITESPACE.replace(HTML_TAG.replace(content, " "), " ").trim()
    val candidates = GEMARA_DAF_MARKER.findAll(plain).mapNotNull { match ->
        val (daf, punctuation, amud, qualification) = match.destructured
        val value = gematriaValue(daf) ?: return@mapNotNull null
        val side = punctuation.firstOrNull() ?: when (amud) {
            "א" -> '.'
            "ב" -> ':'
            else -> null
        }
        PrintedDaf(daf, value, side, qualification.isNotEmpty(), match.range)
    }.toList()
    val candidateCounts = candidates.flatMap { it.possibleAmudim() }.groupingBy { it.key }.eachCount()
    return candidates.mapNotNull { printed ->
        val amud = printed.possibleAmudim()
            .singleOrNull { it.key in evidence.baseTextByAmud } ?: return@mapNotNull null
        if (amud.key in evidence.foreignAmudim || candidateCounts[amud.key] != 1) return@mapNotNull null
        val prefix = plain.substring(0, printed.range.first)
        if (prefix.lastIndexOf('[') > prefix.lastIndexOf(']')) return@mapNotNull null
        // The sidebar also searches square brackets and would scroll to an earlier occurrence.
        if ("[${amud.text}]" in prefix || "[${amud.text} " in prefix) return@mapNotNull null
        val terminalWords = prefix.trimEnd().split(' ').takeLast(3)
        if (terminalWords.any { REFERENCE_WORD.matches(it.trim('(', ')', '\'', '"', ',', '.', ':')) }) return@mapNotNull null
        val beginsParagraph = OPENING_PREFIX.matches(prefix.trim())
        if (beginsParagraph && printed.side != null) return@mapNotNull amud
        // The app cannot scroll to these variants inside a paragraph.
        if (!beginsParagraph && (printed.qualified || printed.side == null)) return@mapNotNull null
        val followingWords = hebrewWords(plain.substring(printed.range.last + 1)).take(INLINE_QUOTE_WORDS).toList()
        if (followingWords.size != INLINE_QUOTE_WORDS) return@mapNotNull null
        val matchesLinkedText = evidence.baseTextByAmud.getValue(amud.key).any { linkedText ->
            hebrewWords(HTML_TAG.replace(linkedText, " ")).windowed(INLINE_QUOTE_WORDS).any { it == followingWords }
        }
        amud.takeIf { matchesLinkedText }
    }
}

internal data class GemaraDafPlan(
    val entries: List<PlannedMarkerEntry>,
    val ownerPathByLineIndex: Map<Int, String>,
)

internal fun planGemaraDafEntries(
    lines: List<MarkerLineRow>,
    toc: List<MarkerTocRow>,
    evidenceByLineId: Map<Long, GemaraDafEvidence>,
): GemaraDafPlan {
    val tocById = toc.associateBy { it.id }
    val soleRoot = toc.filter { it.parentId == null }.singleOrNull()?.id
    val occurrences = LinkedHashMap<Int, MutableList<Pair<GemaraAmud, MarkerLineRow>>>()
    val lastKeyByLineIndex = HashMap<Int, Int>()
    for (line in lines) {
        val evidence = evidenceByLineId[line.id] ?: continue
        val markers = findGemaraDafMarkers(line.content, evidence)
        for (amud in markers) {
            occurrences.getOrPut(amud.key) { mutableListOf() } += amud to line
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
    val owners = buildMap {
        var lastKey: Int? = null
        var path: String? = null
        for (line in lines) {
            val markerKey = lastKeyByLineIndex[line.index]
            if (markerKey != null) {
                lastKey = markerKey
                path = pathByOccurrence.getValue(markerKey to line.index)
            }
            val linkedPages = evidenceByLineId[line.id]?.baseTextByAmud?.keys.orEmpty()
            val owner = path ?: continue
            if (markerKey != null || (linkedPages.size == 1 && lastKey in linkedPages)) put(line.index, owner)
        }
    }
    return GemaraDafPlan(entries, owners)
}

/** Resolve the declared base, even when a different book has the same title. */
private fun readGemaraBaseBookId(conn: Connection, bookId: Long): Long =
    conn.prepareStatement("SELECT baseBookId FROM book_base_text WHERE bookId = ?").use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            check(rs.next()) { "Rif book $bookId has no declared base tractate" }
            val baseId = rs.getLong(1)
            check(!rs.next()) { "Rif book $bookId has multiple declared base tractates" }
            baseId
        }
    }

private fun readGemaraEvidence(
    conn: Connection,
    bookId: Long,
    baseId: Long,
): Map<Long, GemaraDafEvidence> {
    val baseByLine = mutableMapOf<Long, MutableMap<Int, MutableList<String>>>()
    val foreignByLine = mutableMapOf<Long, MutableSet<Int>>()
    conn.prepareStatement(
        """
        WITH rif_links(rifLineId, otherLineId, otherBookId, typeId) AS (
            SELECT targetLineId, sourceLineId, sourceBookId, connectionTypeId
            FROM link INDEXED BY idx_link_target_book WHERE targetBookId = ?
            UNION ALL
            SELECT sourceLineId, targetLineId, targetBookId, connectionTypeId
            FROM link INDEXED BY idx_link_source_book WHERE sourceBookId = ?
        )
        SELECT r.rifLineId, b.id, l.heRef, l.content
        FROM rif_links r
        JOIN book b ON b.id = r.otherBookId
        JOIN line l ON l.id = r.otherLineId
        JOIN connection_type t ON t.id = r.typeId
        WHERE (b.id = ? AND t.name = 'COMMENTARY')
           OR (b.id <> ? AND b.isBaseBook = 1 AND t.name IN ('COMMENTARY', 'OTHER', 'REFERENCE', 'RELATED', 'QUOTATION')
               AND EXISTS (
                   SELECT 1 FROM category_closure cc JOIN category c ON c.id = cc.ancestorId
                   WHERE cc.descendantId = b.categoryId AND c.title = 'תלמוד בבלי'
               ))
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.setLong(2, bookId)
        st.setLong(3, baseId)
        st.setLong(4, baseId)
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val lineId = rs.getLong(1)
                val amud = parseHeRefAmud(rs.getString(3)) ?: continue
                if (rs.getLong(2) == baseId) {
                    baseByLine.getOrPut(lineId) { mutableMapOf() }
                        .getOrPut(amud) { mutableListOf() } += rs.getString(4).orEmpty()
                } else {
                    foreignByLine.getOrPut(lineId) { mutableSetOf() } += amud
                }
            }
        }
    }
    return baseByLine.mapValues { (lineId, base) -> GemaraDafEvidence(base, foreignByLine[lineId].orEmpty()) }
}

/** Atomically replaces each rule's structure; returns entry counts by book title. */
internal fun writeGemaraDafAltTocs(
    conn: Connection,
    rules: List<GemaraDafRule>,
    stableIds: AttachedBuildStateIds? = null,
): Map<String, Int> = writeEachBookAtomically(conn, GEMARA_DAF_ALT_TOC_FILE, rules.map { it.bookTitle to it }) { bookId, rule ->
    val baseId = readGemaraBaseBookId(conn, bookId)
    val lines = readMarkerLines(conn, bookId)
    val plan = planGemaraDafEntries(lines, readMarkerToc(conn, bookId), readGemaraEvidence(conn, bookId, baseId))
    check(plan.entries.isNotEmpty()) { "$GEMARA_DAF_ALT_TOC_FILE: no verified Gemara daf marker in '${rule.bookTitle}'" }
    writeMarkerStructure(
        conn, bookId, rule.structureKey, rule.structureHeTitle, plan.entries, lines, stableIds, plan.ownerPathByLineIndex, carryOwner = false,
    )
}
