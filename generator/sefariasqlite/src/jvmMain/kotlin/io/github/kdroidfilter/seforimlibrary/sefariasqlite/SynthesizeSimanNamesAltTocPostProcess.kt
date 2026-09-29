package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.ids.altTocChildPath
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Puts siman NAMES into the alt-TOC sidebar ("כותרות"): `[סימן כה] דיני תפילין`.
 *
 * The name is read from the siman's first content line — the leading bold
 * segment, e.g. `(א) <b>דין השכמת הבוקר. ובו ט סעיפים:</b>`. The trailing
 * "ובו … סעיפים" is the signature: a bold opening without it is not a name.
 * A main-TOC sibling group of simanim uses its names only when at least
 * [SIMAN_NAME_MIN_HIT_RATE] of them yield one, which excludes incidental bold.
 *
 * The app injects every `Topic` entry into the book text unless the label is
 * already visible at its line ([isInlineHeadingVisible]), so a Topic is only
 * touched where the name comes from that very line — a book naming its own
 * simanim that already has a Topic:
 * - NEST — flat Topic ("הלכות …"): one child per siman under the entry whose
 *   range holds it, anchored at the siman's first content line;
 * - RELABEL — the Topic already holds siman entries (ערוך השולחן).
 * A label the app would inject falls back to the bare "סימן X", or is skipped.
 *
 * Every other book gets a separate [SIMAN_NAMES_STRUCTURE_KEY] structure, which
 * the app never injects, anchored at the siman heading lines:
 * - a commentary borrows names by siman number from the SA part it comments
 *   on — at least [BORROW_MIN_LINK_SHARE] of its COMMENTARY links come from
 *   that part, they reach [BORROW_MIN_SIMAN_COVERAGE] of its simanim, and
 *   [BORROW_MIN_MATCH_SHARE] of its siman numbers (and Topic titles, if it has
 *   a Topic) are the part's; its roots are the part's Topic entries;
 * - a book naming its own simanim without a Topic: roots are the named groups'
 *   main-TOC parents, or none (the simanim are the roots).
 * This stage owns that key: each run drops those structures and rebuilds them.
 *
 * Ids come from the build-state allocator keyed on ancestor paths, so existing
 * ids never move and a rerun reproduces every row.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:synthesizeSimanNamesAltToc -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SynthesizeSimanNamesAltToc")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    logger.i { "Adding siman names to alt-TOCs in $dbPath" }

    try {
        val buildStatePath = resolveSeifimBuildStatePath(dbPath)
        check(Files.exists(buildStatePath)) {
            "build_state not found at $buildStatePath; run the DB generation pipeline first"
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            // AttachedBuildStateIds reads the attached state under this alias.
            conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
                st.setString(1, buildStatePath.toAbsolutePath().toString())
                st.execute()
            }
            val books = readSimanNamesSnapshots(conn)
            val evidence = readBorrowEvidence(conn, books)
            for (book in books) {
                val e = evidence[book.bookId] ?: continue
                logger.i {
                    "  evidence ${book.bookId}: source=${e.sourceId} share=${"%.3f".format(e.linkShare)} " +
                        "coverage=${"%.3f".format(e.simanCoverage)}"
                }
            }
            val plans = planSimanNames(books, evidence)
            for (plan in plans) {
                logger.i {
                    "  ${plan.book.bookId}: ${plan.mode} source=${plan.nameSource} " +
                        "named=${plan.namesByTocId.size}/${plan.book.simanim.size}"
                }
            }
            val result = writeSimanNames(conn, plans, AttachedBuildStateIds(conn))
            logger.i { "Siman names done: $result" }
        }
        BuildStateVerifier.verifyFreshSnapshot(buildStatePath, dbPath, emptyMap())
    } catch (e: Exception) {
        logger.e(e) { "Failed to add siman names to alt-TOCs; aborting" }
        exitProcess(1)
    }
}

/** The one place the label format lives; the app strips the bracketed prefix (Kitzur SA's Topic uses it too). */
internal fun simanLabel(heading: String, name: String?): String =
    if (name == null) heading else "[$heading] $name"

/** The siman heading a label was built from — the inverse of [simanLabel]. */
internal fun simanLabelHeading(label: String): String {
    val trimmed = label.trimHeadingStart().trim()
    if (!trimmed.startsWith("[")) return trimmed
    val close = trimmed.indexOf(']')
    return if (close > 0) trimmed.substring(1, close).trim() else trimmed
}

internal const val TOPIC_STRUCTURE_KEY = "Topic"
internal const val SIMAN_NAMES_STRUCTURE_KEY = "SimanNames"
internal const val SIMAN_NAMES_STRUCTURE_TITLE_EN = "Simanim"
/** "סימנים" alone is also the heTitle of the unrelated Simanim key (paragraph letters), so halachot roots say so. */
internal const val SIMAN_NAMES_STRUCTURE_TITLE_HE_HALACHOT = "הלכות וסימנים"
internal const val SIMAN_NAMES_STRUCTURE_TITLE_HE = "סימנים"
internal const val SIMAN_NAME_MIN_HIT_RATE = 0.5
internal const val BORROW_MIN_LINK_SHARE = 0.9
internal const val BORROW_MIN_SIMAN_COVERAGE = 0.5
internal const val BORROW_MIN_MATCH_SHARE = 0.9

private val LEADING_SEIF_MARKER = Regex("""^\s*\((?:[א-ת]{1,4}["'׳״]?|\d{1,3})\)\s*""")
private val LEADING_EMPTY_TAGS = Regex("""^(?:\s*<i\b[^>]*>\s*</i>)+\s*""")
private val LEADING_BOLD = Regex("""^<b>(.*?)</b>""", RegexOption.DOT_MATCHES_ALL)
private val EMPTY_ITALIC = Regex("""<i\b[^>]*>\s*</i>""")
private val SUP = Regex("""<sup\b[^>]*>.*?</sup>""", RegexOption.DOT_MATCHES_ALL)

// An editorial note inside the name ("<small>[… נשמטו בשו"ע …]</small>"), not part of it.
private val SMALL = Regex("""<small\b[^>]*>.*?</small>""", RegexOption.DOT_MATCHES_ALL)
private val ANY_TAG = Regex("""<[^>]+>""")
private val FOOTNOTE_NUMBER = Regex("""\[\d+]""")
private val WHITESPACE = Regex("""[\s ]+""")
private val TRAILING_PUNCTUATION = Regex("""[\s.,:;\]'׳]+$""")

// "ובו ט סעיפים", "ובו כ"א סעי'", "ובו סעיף אחד" — anchored at the segment's end.
private val SEIFIM_COUNT_TAIL = Regex("""(?:^|[\s.,;:])ובו(?:\s+\S{1,5})?\s+סעי(?:פים|ף|['׳])?(?:\s+אחד)?$""")
private val TRAILING_STRAY_UVO = Regex("""(?:^|\s)ובו$""")

/**
 * The siman name at the start of its first content line, or null when the
 * line does not open with a bold segment ending in "ובו … סעיפים".
 */
internal fun extractSimanName(content: String): String? {
    var rest = content.replaceFirst(LEADING_SEIF_MARKER, "")
    rest = rest.replaceFirst(LEADING_EMPTY_TAGS, "")
    val bold = LEADING_BOLD.find(rest)?.groupValues?.get(1) ?: return null
    var text = bold
        .replace(EMPTY_ITALIC, "")
        .replace(SUP, "")
        .replace(SMALL, "")
        .replace(ANY_TAG, "")
        .replace(FOOTNOTE_NUMBER, "")
        .replace(WHITESPACE, " ")
        .trim()
        .replace(TRAILING_PUNCTUATION, "")
    val tail = SEIFIM_COUNT_TAIL.find(text) ?: return null
    text = text.substring(0, tail.range.first)
    text = text.replace(TRAILING_STRAY_UVO, "")
    text = text.replace(TRAILING_PUNCTUATION, "").trimStart('[', ' ').trim()
    return text.takeIf { name -> name.count { it in 'א'..'ת' } >= 2 }
}

// Port of the app's inline_section_markers.dart (cleanSectionHeadingLabel,
// isSectionHeadingVisible): a Topic label failing it is injected into the text.
private val APP_BRACKET_PREFIX = Regex("""^\s*\[[^\]]*]\s*""")
private val APP_HTML_TAG = Regex("""<[^>]+>""")
private val APP_NIKUD_AND_MATRES = Regex("[֑-ׇוי]")
private val APP_NON_LETTERS = Regex("[^א-ת0-9 ]")
private val APP_SPACES = Regex("""\s+""")

private fun normalizeForInlineMatch(text: String): String = text
    .replace(APP_HTML_TAG, " ")
    .replace(APP_NIKUD_AND_MATRES, "")
    .replace(APP_NON_LETTERS, " ")
    .replace(APP_SPACES, " ")
    .trim()

/** True when the app would NOT inject [label] anchored at window[0] (window = that line and the two above). */
internal fun isInlineHeadingVisible(label: String, window: List<String?>): Boolean {
    val cleaned = label.replace("﻿", "").replaceFirst(APP_BRACKET_PREFIX, "").trim()
    if (cleaned.isEmpty()) return true
    val key = normalizeForInlineMatch(cleaned).split(' ').take(3).joinToString(" ")
    if (key.isEmpty()) return true
    return window.any { line ->
        val index = line?.let { normalizeForInlineMatch(it).indexOf(key) } ?: -1
        index in 0..8
    }
}

/** "סימן כ\"א" → "כא": the siman number, comparable across books. */
internal fun simanNumberKey(heading: String): String =
    heading.trimHeadingStart().removePrefix("סימן").filter { it in 'א'..'ת' }

internal data class SimanHeadingRow(
    val tocId: Long,
    val parentTocId: Long?,
    val text: String,
    val headingLineIndex: Long,
    /** First content line after the heading; Topic entries anchor here. */
    val anchorLineId: Long,
    val anchorLineIndex: Long,
    /** Exclusive end: the next main-TOC heading that is not inside this siman. */
    val endLineIndex: Long,
    val firstContent: String?,
    /** The main-TOC parent heading, for a structure built from the book's own groups. */
    val parentText: String? = null,
    val parentLevel: Int? = null,
    val headingLineId: Long = anchorLineId,
    /** The two lines above the anchor line, nearest first — the rest of the app's visibility window. */
    val linesAboveAnchor: List<String?> = emptyList(),
) {
    val anchorWindow: List<String?> get() = listOf(firstContent) + linesAboveAnchor

    /** The same siman anchored at its heading line, as [SIMAN_NAMES_STRUCTURE_KEY] entries are. */
    fun atHeading(): SimanHeadingRow = copy(anchorLineId = headingLineId, anchorLineIndex = headingLineIndex)
}

internal data class TopicEntryRow(
    val id: Long,
    val parentId: Long?,
    val level: Int,
    val text: String,
    val lineId: Long?,
    val lineIndex: Long?,
    val hasChildren: Boolean,
)

internal data class SimanNamesBookSnapshot(
    val bookId: Long,
    val title: String,
    /** The book's Topic structure, or null when it has none. */
    val structureId: Long?,
    val simanim: List<SimanHeadingRow>,
    val entries: List<TopicEntryRow>,
)

internal enum class SimanNamesMode { NEST, RELABEL, SEPARATE }

/** A level-0 entry of a [SIMAN_NAMES_STRUCTURE_KEY] structure. */
internal data class CreatedRoot(val text: String, val lineId: Long, val lineIndex: Long)

internal data class SimanNamesPlan(
    val book: SimanNamesBookSnapshot,
    val mode: SimanNamesMode,
    /** Book id the names came from: the book itself, or the SA part it borrows from. */
    val nameSource: Long,
    val namesByTocId: Map<Long, String>,
    /** SEPARATE: the roots to nest under; empty = the [flatSimanTocIds] are the roots. */
    val createdRoots: List<CreatedRoot> = emptyList(),
    val flatSimanTocIds: List<Long> = emptyList(),
)

/** Why a book may borrow: its dominant COMMENTARY source and how strongly it is tied to it. */
internal data class BorrowEvidence(val sourceId: Long, val linkShare: Double, val simanCoverage: Double)

internal data class SimanNamesResult(
    val books: Int,
    /** Topic children (NEST). */
    val children: Int,
    val named: Int,
    val relabeled: Int,
    /** Siman entries that already carried their label (a rerun). */
    val relabelUnchanged: Int = 0,
    val separateStructures: Int = 0,
    val separateEntries: Int = 0,
    /** Topic names dropped because the app would inject them. */
    val invisibleNames: Int = 0,
)

/** Names per siman, only for main-TOC sibling groups that pass [SIMAN_NAME_MIN_HIT_RATE]. */
internal fun ownSimanNames(simanim: List<SimanHeadingRow>): Map<Long, String> {
    val names = HashMap<Long, String>()
    for ((_, group) in simanim.groupBy { it.parentTocId }) {
        val found = group.mapNotNull { siman -> siman.firstContent?.let(::extractSimanName)?.let { siman.tocId to it } }
        if (found.isNotEmpty() && found.size >= group.size * SIMAN_NAME_MIN_HIT_RATE) names.putAll(found)
    }
    return names
}

/**
 * RELABEL when the Topic structure already holds siman entries, NEST when it
 * is flat (every entry level 0 with a line, ignoring this stage's own
 * children), null when there is none or it has another shape.
 */
internal fun classifyTopicStructure(book: SimanNamesBookSnapshot): SimanNamesMode? {
    if (book.structureId == null) return null
    val own = ownChildIds(book)
    val others = book.entries.filter { it.id !in own }
    if (others.isEmpty()) return null
    if (others.any { isSimanHeading(simanLabelHeading(it.text)) }) return SimanNamesMode.RELABEL
    if (others.all { it.level == 0 && it.parentId == null && it.lineIndex != null }) return SimanNamesMode.NEST
    return null
}

/** Children an earlier run of this stage nested under a flat Topic structure. */
private fun ownChildIds(book: SimanNamesBookSnapshot): Set<Long> {
    val rootIds = book.entries.filter { it.parentId == null && it.level == 0 }.map { it.id }.toHashSet()
    val anchors = book.simanim.map { it.anchorLineIndex }.toHashSet()
    return book.entries.filter {
        it.parentId in rootIds && it.level == 1 && it.lineIndex in anchors &&
            isSimanHeading(simanLabelHeading(it.text))
    }.map { it.id }.toHashSet()
}

/** Decides, per book, which names it gets — its own, or a source's by siman number — and where they go. */
internal fun planSimanNames(
    books: List<SimanNamesBookSnapshot>,
    evidence: Map<Long, BorrowEvidence>,
): List<SimanNamesPlan> {
    val ownByBook = books.associate { it.bookId to ownSimanNames(it.simanim) }
    val booksById = books.associateBy { it.bookId }
    val plans = mutableListOf<SimanNamesPlan>()
    for (book in books.sortedBy { it.bookId }) {
        val own = ownByBook.getValue(book.bookId)
        if (own.isNotEmpty()) {
            val topicMode = classifyTopicStructure(book)
            plans += if (topicMode != null) SimanNamesPlan(book, topicMode, book.bookId, own) else separateFromOwnGroups(book, own)
            continue
        }
        borrowPlan(book, evidence[book.bookId] ?: continue, booksById, ownByBook)?.let(plans::add)
    }
    return plans
}

private fun borrowPlan(
    book: SimanNamesBookSnapshot,
    e: BorrowEvidence,
    booksById: Map<Long, SimanNamesBookSnapshot>,
    ownByBook: Map<Long, Map<Long, String>>,
): SimanNamesPlan? {
    if (e.linkShare < BORROW_MIN_LINK_SHARE || e.simanCoverage < BORROW_MIN_SIMAN_COVERAGE) return null
    val source = booksById[e.sourceId] ?: return null
    val sourceNames = ownByBook.getValue(source.bookId)
    // The roots are the source's Topic entries, so the source needs a flat Topic.
    if (sourceNames.isEmpty() || classifyTopicStructure(source) != SimanNamesMode.NEST) return null
    // Only a source with one siman per number can be read by number.
    val sourceKeys = source.simanim.groupBy { simanNumberKey(it.text) }
    if (sourceKeys.values.any { it.size > 1 }) return null
    val keyShare = book.simanim.count { simanNumberKey(it.text) in sourceKeys }.toDouble() / book.simanim.size
    if (keyShare < BORROW_MIN_MATCH_SHARE) return null
    if (book.structureId != null && topicTitleShare(book, source) < BORROW_MIN_MATCH_SHARE) return null
    val byKey = sourceKeys.mapNotNull { (key, rows) -> sourceNames[rows.single().tocId]?.let { key to it } }.toMap()
    val borrowed = book.simanim.mapNotNull { s -> byKey[simanNumberKey(s.text)]?.let { s.tocId to it } }.toMap()
    if (borrowed.isEmpty()) return null
    val roots = borrowedRoots(source, book)
    if (roots.isEmpty()) return null
    return SimanNamesPlan(book, SimanNamesMode.SEPARATE, source.bookId, borrowed, createdRoots = roots)
}

private fun topicTitleShare(book: SimanNamesBookSnapshot, source: SimanNamesBookSnapshot): Double {
    fun roots(b: SimanNamesBookSnapshot) = b.entries.filter { it.parentId == null }.map { it.text.trim() }
    val titles = roots(book)
    if (titles.isEmpty()) return 0.0
    val sourceTitles = roots(source).toHashSet()
    return titles.count { it in sourceTitles }.toDouble() / titles.size
}

/**
 * The named groups' main-TOC parents become the roots, anchored at their first
 * siman's heading; when a group hangs off the book's top heading instead, the
 * named groups' simanim themselves are the level-0 entries.
 */
private fun separateFromOwnGroups(book: SimanNamesBookSnapshot, own: Map<Long, String>): SimanNamesPlan {
    val groups = book.simanim.groupBy { it.parentTocId }.values.filter { group -> group.any { it.tocId in own } }
    val underSubheadings = groups.all { group -> group.first().parentLevel.let { it != null && it >= 1 } }
    if (!underSubheadings) {
        val flat = groups.flatten().sortedBy { it.headingLineIndex }.map { it.tocId }
        return SimanNamesPlan(book, SimanNamesMode.SEPARATE, book.bookId, own, flatSimanTocIds = flat)
    }
    val roots = groups.map { group ->
        val first = group.minBy { it.headingLineIndex }
        CreatedRoot(first.parentText!!.trimHeadingStart().trim(), first.headingLineId, first.headingLineIndex)
    }.sortedBy { it.lineIndex }
    return SimanNamesPlan(book, SimanNamesMode.SEPARATE, book.bookId, own, createdRoots = roots)
}

/**
 * The source's Topic roots, each anchored at the heading of the first siman of
 * [book] whose number sits under that root in the source; a root holding none is dropped.
 */
internal fun borrowedRoots(source: SimanNamesBookSnapshot, book: SimanNamesBookSnapshot): List<CreatedRoot> {
    val sourceRoots = sortedRoots(source.entries.filter { it.parentId == null && it.level == 0 })
    val rootByKey = source.simanim.mapNotNull { s -> rootOf(sourceRoots, s)?.let { simanNumberKey(s.text) to it } }.toMap()
    val seen = HashSet<Long>()
    val roots = mutableListOf<CreatedRoot>()
    for (siman in book.simanim.sortedBy { it.headingLineIndex }) {
        val root = rootByKey[simanNumberKey(siman.text)] ?: continue
        if (seen.add(root.id)) roots += CreatedRoot(root.text, siman.headingLineId, siman.headingLineIndex)
    }
    return roots
}

private fun sortedRoots(roots: List<TopicEntryRow>): List<TopicEntryRow> =
    roots.filter { it.lineIndex != null }.sortedWith(compareBy({ it.lineIndex }, { it.id }))

/** The last of [sortedRoots] that starts at or before the siman's anchor line. */
private fun rootOf(sortedRoots: List<TopicEntryRow>, siman: SimanHeadingRow): TopicEntryRow? =
    sortedRoots.lastOrNull { it.lineIndex!! <= siman.anchorLineIndex }

/** One child to nest under a root, in document order. */
internal data class PlannedSimanChild(
    val parentId: Long,
    val text: String,
    val lineId: Long,
    val lineIndex: Long,
    val named: Boolean,
    /** Exclusive main-TOC boundary; the next heading belongs to another section. */
    val endLineIndex: Long,
)

/**
 * Each siman goes under the last root that starts at or before its anchor
 * line; simanim before the first root get no child. With [inlineVisibleOnly]
 * (a Topic), a label the app would inject falls back to the bare heading, and
 * a siman whose bare heading would be injected too gets no child.
 */
internal fun planNestedChildren(
    roots: List<TopicEntryRow>,
    simanim: List<SimanHeadingRow>,
    namesByTocId: Map<Long, String>,
    inlineVisibleOnly: Boolean = false,
): List<PlannedSimanChild> {
    val sorted = sortedRoots(roots)
    val children = mutableListOf<PlannedSimanChild>()
    for (siman in simanim.sortedBy { it.anchorLineIndex }) {
        val parent = rootOf(sorted, siman) ?: continue
        val heading = siman.text.trimHeadingStart().trim()
        var name = namesByTocId[siman.tocId]
        if (inlineVisibleOnly) {
            if (name != null && !isInlineHeadingVisible(simanLabel(heading, name), siman.anchorWindow)) name = null
            if (name == null && !isInlineHeadingVisible(heading, siman.anchorWindow)) continue
        }
        children += PlannedSimanChild(
            parent.id, simanLabel(heading, name), siman.anchorLineId, siman.anchorLineIndex, name != null, siman.endLineIndex,
        )
    }
    return children
}

/**
 * line_alt_toc owner per line: the nearest entry anchored at or before it,
 * the deeper one on a shared line — the rule the Sefaria builder applies.
 * Bounded entries expire at their exclusive [endByEntryId] boundary, falling
 * back to the latest preceding entry still in range (an original Topic root),
 * or leaving the line unowned. [anchors] holds (lineIndex, level, entryId).
 * [lines] must be in lineIndex order. After sorting anchors, each is
 * pushed/popped once: O(lines + anchors).
 */
internal fun nearestPrecedingOwners(
    lines: List<Pair<Long, Long>>,
    anchors: List<Triple<Long, Int, Long>>,
    endByEntryId: Map<Long, Long> = emptyMap(),
): Map<Long, Long> {
    val sorted = anchors.sortedWith(compareBy({ it.first }, { it.second }))
    val owners = HashMap<Long, Long>(lines.size)
    var next = 0
    val active = ArrayDeque<Long>()
    for ((lineId, lineIndex) in lines) {
        while (next < sorted.size && sorted[next].first <= lineIndex) active.addLast(sorted[next++].third)
        while (active.isNotEmpty() && (endByEntryId[active.last()] ?: Long.MAX_VALUE) <= lineIndex) {
            active.removeLast()
        }
        active.lastOrNull()?.let { owners[lineId] = it }
    }
    return owners
}

/** Share of [simanim] that hold at least one of [linkedLines]. */
internal fun linkedSimanShare(simanim: List<SimanHeadingRow>, linkedLines: Collection<Long>): Double {
    if (simanim.isEmpty()) return 0.0
    val sorted = linkedLines.sorted()
    val hit = simanim.count { s ->
        val i = sorted.binarySearch(s.headingLineIndex).let { if (it >= 0) it else -it - 1 }
        i < sorted.size && sorted[i] < s.endLineIndex
    }
    return hit.toDouble() / simanim.size
}

/**
 * Every book that can take siman names: those with a Topic structure, those
 * whose own text names its simanim, and those receiving COMMENTARY links from
 * a book that names them.
 */
internal fun readSimanNamesSnapshots(conn: Connection): List<SimanNamesBookSnapshot> {
    val topicStructures = LinkedHashMap<Long, Long>()
    conn.prepareStatement("SELECT bookId, id FROM alt_toc_structure WHERE key = ? ORDER BY bookId").use { st ->
        st.setString(1, TOPIC_STRUCTURE_KEY)
        st.executeQuery().use { rs -> while (rs.next()) topicStructures[rs.getLong(1)] = rs.getLong(2) }
    }
    val snapshots = LinkedHashMap<Long, SimanNamesBookSnapshot>()
    fun add(bookId: Long) {
        if (bookId in snapshots) return
        readSnapshot(conn, bookId, topicStructures[bookId])?.let { snapshots[bookId] = it }
    }
    topicStructures.keys.forEach(::add)
    readOwnNameCandidates(conn).forEach(::add)
    val namedSources = snapshots.values.filter { ownSimanNames(it.simanim).isNotEmpty() }.map { it.bookId }
    readCommentaryTargets(conn, namedSources).forEach(::add)
    return snapshots.values.sortedBy { it.bookId }
}

private fun readSnapshot(conn: Connection, bookId: Long, structureId: Long?): SimanNamesBookSnapshot? {
    val title = conn.prepareStatement("SELECT title FROM book WHERE id = ?").use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
    } ?: return null
    val simanim = readSimanHeadings(conn, bookId)
    if (simanim.isEmpty()) return null
    val entries = structureId?.let { readTopicEntries(conn, it) } ?: emptyList()
    return SimanNamesBookSnapshot(bookId, title, structureId, simanim, entries)
}

/** Books whose first line after a siman heading opens with the bold "ובו … סעי…" signature. */
private fun readOwnNameCandidates(conn: Connection): List<Long> =
    conn.prepareStatement(
        """
        SELECT DISTINCT e.bookId
        FROM tocEntry e JOIN tocText t ON t.id = e.textId JOIN line l ON l.id = e.lineId
        JOIN line x ON x.bookId = e.bookId AND x.lineIndex = l.lineIndex + 1
        WHERE t.text LIKE '%סימן%' AND x.content LIKE '%<b>%ובו%סעי%'
        ORDER BY e.bookId
        """.trimIndent(),
    ).use { st -> st.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } } }

private fun readCommentaryTargets(conn: Connection, sources: List<Long>): List<Long> {
    val targets = sortedSetOf<Long>()
    conn.prepareStatement(
        """
        SELECT DISTINCT k.targetBookId
        FROM link k INDEXED BY idx_link_source_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        WHERE k.sourceBookId = ? AND c.name = 'COMMENTARY'
        """.trimIndent(),
    ).use { st ->
        for (source in sources) {
            st.setLong(1, source)
            st.executeQuery().use { rs -> while (rs.next()) targets += rs.getLong(1) }
        }
    }
    return targets.toList()
}

private fun readBookLines(conn: Connection, bookId: Long): List<Pair<Long, Long>> {
    val lines = mutableListOf<Pair<Long, Long>>()
    conn.prepareStatement("SELECT id, lineIndex FROM line WHERE bookId = ? ORDER BY lineIndex").use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs -> while (rs.next()) lines += rs.getLong(1) to rs.getLong(2) }
    }
    return lines
}

private fun readSimanHeadings(conn: Connection, bookId: Long): List<SimanHeadingRow> {
    data class Node(val id: Long, val parentId: Long?, val level: Int, val text: String, val lineIndex: Long)
    val nodes = mutableListOf<Node>()
    conn.prepareStatement(
        """
        SELECT e.id, e.parentId, e.level, t.text, l.lineIndex
        FROM tocEntry e JOIN tocText t ON t.id = e.textId JOIN line l ON l.id = e.lineId
        WHERE e.bookId = ? ORDER BY l.lineIndex, e.level, e.id
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val parentId = rs.getLong(2).let { if (rs.wasNull()) null else it }
                nodes += Node(rs.getLong(1), parentId, rs.getInt(3), rs.getString(4), rs.getLong(5))
            }
        }
    }
    if (nodes.none { isSimanHeading(it.text) }) return emptyList()
    val nodeById = nodes.associateBy { it.id }
    fun isInside(nodeId: Long, ancestorId: Long): Boolean {
        var current = nodeById[nodeId]?.parentId
        while (current != null) {
            if (current == ancestorId) return true
            current = nodeById[current]?.parentId
        }
        return false
    }
    val headingLines = nodes.map { it.lineIndex }.toHashSet()
    val lines = readBookLines(conn, bookId)
    val lineIdByIndex = lines.associate { (id, index) -> index to id }
    val lastLine = lines.lastOrNull()?.second ?: return emptyList()

    val simanim = mutableListOf<Pair<Node, Pair<Long, Long>>>() // node -> (anchorLineIndex, end)
    for ((i, node) in nodes.withIndex()) {
        if (!isSimanHeading(node.text)) continue
        var end = lastLine + 1
        for (j in i + 1 until nodes.size) {
            val next = nodes[j]
            if (next.lineIndex > node.lineIndex && !isInside(next.id, node.id)) {
                end = next.lineIndex
                break
            }
        }
        var anchor = node.lineIndex + 1
        while (anchor < end && anchor in headingLines) anchor++
        if (anchor >= end || anchor !in lineIdByIndex || node.lineIndex !in lineIdByIndex) continue
        simanim += node to (anchor to end)
    }
    val contentByLine = HashMap<Long, String?>()
    conn.prepareStatement("SELECT content FROM line WHERE bookId = ? AND lineIndex = ?").use { st ->
        for ((_, span) in simanim) {
            for (index in span.first - 2..span.first) {
                if (index in contentByLine) continue
                st.setLong(1, bookId)
                st.setLong(2, index)
                st.executeQuery().use { rs -> contentByLine[index] = if (rs.next()) rs.getString(1) else null }
            }
        }
    }
    return simanim.map { (node, span) ->
        val parent = node.parentId?.let(nodeById::get)
        SimanHeadingRow(
            tocId = node.id,
            parentTocId = node.parentId,
            text = node.text,
            headingLineIndex = node.lineIndex,
            anchorLineId = lineIdByIndex.getValue(span.first),
            anchorLineIndex = span.first,
            endLineIndex = span.second,
            firstContent = contentByLine[span.first],
            parentText = parent?.text,
            parentLevel = parent?.level,
            headingLineId = lineIdByIndex.getValue(node.lineIndex),
            linesAboveAnchor = listOf(contentByLine[span.first - 1], contentByLine[span.first - 2]),
        )
    }
}

private fun readTopicEntries(conn: Connection, structureId: Long): List<TopicEntryRow> {
    val entries = mutableListOf<TopicEntryRow>()
    conn.prepareStatement(
        """
        SELECT e.id, e.parentId, e.level, t.text, e.lineId, l.lineIndex, e.hasChildren
        FROM alt_toc_entry e JOIN tocText t ON t.id = e.textId LEFT JOIN line l ON l.id = e.lineId
        WHERE e.structureId = ? ORDER BY e.id
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, structureId)
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val parentId = rs.getLong(2).let { if (rs.wasNull()) null else it }
                val lineId = rs.getLong(5).let { if (rs.wasNull()) null else it }
                val lineIndex = rs.getLong(6).let { if (rs.wasNull()) null else it }
                entries += TopicEntryRow(
                    rs.getLong(1), parentId, rs.getInt(3), rs.getString(4), lineId, lineIndex, rs.getInt(7) != 0,
                )
            }
        }
    }
    return entries
}

/** Per book without names of its own: its dominant COMMENTARY source, that source's link share and siman coverage. */
internal fun readBorrowEvidence(
    conn: Connection,
    books: List<SimanNamesBookSnapshot>,
): Map<Long, BorrowEvidence> {
    val evidence = HashMap<Long, BorrowEvidence>()
    conn.prepareStatement(
        """
        SELECT k.sourceBookId, COUNT(*)
        FROM link k INDEXED BY idx_link_target_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        WHERE k.targetBookId = ? AND c.name = 'COMMENTARY'
        GROUP BY k.sourceBookId
        ORDER BY COUNT(*) DESC, k.sourceBookId
        """.trimIndent(),
    ).use { counts ->
        conn.prepareStatement(
            """
            SELECT DISTINCT l.lineIndex
            FROM link k INDEXED BY idx_link_target_book
            JOIN connection_type c ON c.id = k.connectionTypeId
            JOIN line l ON l.id = k.targetLineId
            WHERE k.targetBookId = ? AND k.sourceBookId = ? AND c.name = 'COMMENTARY'
            """.trimIndent(),
        ).use { linked ->
            for (book in books) {
                if (ownSimanNames(book.simanim).isNotEmpty()) continue
                counts.setLong(1, book.bookId)
                val bySource = mutableListOf<Pair<Long, Long>>()
                counts.executeQuery().use { rs -> while (rs.next()) bySource += rs.getLong(1) to rs.getLong(2) }
                val total = bySource.sumOf { it.second }
                if (total == 0L) continue
                val (sourceId, count) = bySource.first()
                linked.setLong(1, book.bookId)
                linked.setLong(2, sourceId)
                val lines = linked.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) } }
                evidence[book.bookId] = BorrowEvidence(sourceId, count.toDouble() / total, linkedSimanShare(book.simanim, lines))
            }
        }
    }
    return evidence
}

/** Atomically applies every plan and restores JDBC ownership. */
internal fun writeSimanNames(
    conn: Connection,
    plans: List<SimanNamesPlan>,
    stableIds: AttachedBuildStateIds? = null,
): SimanNamesResult {
    check(conn.autoCommit) { "writeSimanNames requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        val dropped = dropSimanNamesStructures(conn)
        var children = 0
        var named = 0
        var relabeled = 0
        var unchanged = 0
        var separate = 0
        var separateEntries = 0
        var invisible = 0
        for (plan in plans) {
            when (plan.mode) {
                SimanNamesMode.NEST -> {
                    val written = writeNestedChildren(conn, plan, stableIds)
                    children += written.size
                    named += written.count { it.named }
                    invisible += plan.namesByTocId.size - written.count { it.named }
                }
                SimanNamesMode.RELABEL -> {
                    val outcome = relabelSimanEntries(conn, plan, stableIds)
                    relabeled += outcome.changed
                    unchanged += outcome.unchanged
                    invisible += outcome.invisible
                }
                SimanNamesMode.SEPARATE -> {
                    separate++
                    separateEntries += writeSimanNamesStructure(conn, plan, stableIds)
                }
            }
        }
        val rebuilt = plans.filter { it.mode == SimanNamesMode.SEPARATE }.map { it.book.bookId }.toSet()
        clearStaleAltStructureFlags(conn, dropped - rebuilt)
        conn.commit()
        SimanNamesResult(plans.size, children, named, relabeled, unchanged, separate, separateEntries, invisible)
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

/**
 * This stage owns the key: a rerun rebuilds each structure under the same allocator
 * ids. Returns the books that had one. tocText rows are kept, as in every synthesizer.
 */
private fun dropSimanNamesStructures(conn: Connection): Set<Long> {
    val books = conn.prepareStatement("SELECT bookId FROM alt_toc_structure WHERE key = ?").use { st ->
        st.setString(1, SIMAN_NAMES_STRUCTURE_KEY)
        st.executeQuery().use { rs -> buildSet { while (rs.next()) add(rs.getLong(1)) } }
    }
    conn.prepareStatement(
        "DELETE FROM line_alt_toc WHERE structureId IN (SELECT id FROM alt_toc_structure WHERE key = ?)",
    ).use { it.setString(1, SIMAN_NAMES_STRUCTURE_KEY); it.executeUpdate() }
    conn.prepareStatement(
        "DELETE FROM alt_toc_entry WHERE structureId IN (SELECT id FROM alt_toc_structure WHERE key = ?)",
    ).use { it.setString(1, SIMAN_NAMES_STRUCTURE_KEY); it.executeUpdate() }
    conn.prepareStatement("DELETE FROM alt_toc_structure WHERE key = ?").use {
        it.setString(1, SIMAN_NAMES_STRUCTURE_KEY)
        it.executeUpdate()
    }
    return books
}

/** A book whose only structure was ours and is no longer planned has none left. */
private fun clearStaleAltStructureFlags(conn: Connection, bookIds: Set<Long>) {
    conn.prepareStatement(
        """
        UPDATE book SET hasAltStructures = 0
        WHERE id = ? AND hasAltStructures = 1
          AND NOT EXISTS (SELECT 1 FROM alt_toc_structure s WHERE s.bookId = book.id)
        """.trimIndent(),
    ).use { st ->
        for (bookId in bookIds.sorted()) {
            st.setLong(1, bookId)
            st.addBatch()
        }
        st.executeBatch()
    }
}

private fun writeNestedChildren(
    conn: Connection,
    plan: SimanNamesPlan,
    stableIds: AttachedBuildStateIds?,
): List<PlannedSimanChild> {
    val book = plan.book
    val structureId = book.structureId!!
    val previous = ownChildIds(book)
    val roots = book.entries.filter { it.id !in previous }
    val planned = planNestedChildren(roots, book.simanim, plan.namesByTocId, inlineVisibleOnly = true)

    val previousOwnedLines = HashSet<Long>()
    if (previous.isNotEmpty()) {
        val ids = previous.joinToString(",")
        conn.createStatement().use { st ->
            st.executeQuery("SELECT lineId FROM line_alt_toc WHERE altTocEntryId IN ($ids)").use { rs ->
                while (rs.next()) previousOwnedLines += rs.getLong(1)
            }
            st.executeUpdate("DELETE FROM line_alt_toc WHERE altTocEntryId IN ($ids)")
            st.executeUpdate("DELETE FROM alt_toc_entry WHERE id IN ($ids)")
        }
    }

    var nextEntryId = queryMaxId(conn, "alt_toc_entry")
    val ordinalByParent = HashMap<Long, Int>()
    val childIds = planned.map { child ->
        val ordinal = (ordinalByParent[child.parentId] ?: 0) + 1
        ordinalByParent[child.parentId] = ordinal
        if (stableIds == null) {
            ++nextEntryId
        } else {
            stableIds.altTocEntryId(structureId, altTocChildPath(entryPath(conn, structureId, child.parentId), ordinal))
        }
    }
    val lastChildIndexByParent = planned.withIndex().associate { (i, child) -> child.parentId to i }

    insertEntries(
        conn,
        structureId,
        planned.mapIndexed { i, child ->
            PendingSimanEntry(childIds[i], child.parentId, 1, child.text, child.lineId, lastChildIndexByParent[child.parentId] == i, false)
        },
        stableIds,
    )
    conn.prepareStatement("UPDATE alt_toc_entry SET hasChildren = ? WHERE id = ?").use { st ->
        for (root in roots) {
            st.setInt(1, if (root.id in lastChildIndexByParent) 1 else 0)
            st.setLong(2, root.id)
            st.addBatch()
        }
        st.executeBatch()
    }

    // Only lines a child owns now, or an earlier child owned, change hands.
    val lines = readBookLines(conn, book.bookId)
    val childIdSet = childIds.toHashSet()
    val anchors = roots.mapNotNull { r -> r.lineIndex?.let { Triple(it, 0, r.id) } } +
        planned.mapIndexed { i, child -> Triple(child.lineIndex, 1, childIds[i]) }
    val ends = planned.withIndex().associate { (i, child) -> childIds[i] to child.endLineIndex }
    val owners = nearestPrecedingOwners(lines, anchors, ends)
    conn.prepareStatement(
        "INSERT OR REPLACE INTO line_alt_toc (lineId, structureId, altTocEntryId) VALUES (?, ?, ?)",
    ).use { upsert ->
        conn.prepareStatement("DELETE FROM line_alt_toc WHERE lineId = ? AND structureId = ?").use { delete ->
            for ((lineId, _) in lines) {
                val owner = owners[lineId]
                if (owner != null && (owner in childIdSet || lineId in previousOwnedLines)) {
                    upsert.setLong(1, lineId)
                    upsert.setLong(2, structureId)
                    upsert.setLong(3, owner)
                    upsert.addBatch()
                } else if (owner == null && lineId in previousOwnedLines) {
                    delete.setLong(1, lineId)
                    delete.setLong(2, structureId)
                    delete.addBatch()
                }
            }
            upsert.executeBatch()
            delete.executeBatch()
        }
    }
    return planned
}

private data class PendingSimanEntry(
    val id: Long,
    val parentId: Long?,
    val level: Int,
    val text: String,
    val lineId: Long,
    val isLastChild: Boolean,
    val hasChildren: Boolean,
)

private fun insertEntries(
    conn: Connection,
    structureId: Long,
    entries: List<PendingSimanEntry>,
    stableIds: AttachedBuildStateIds?,
) {
    val textIds = HashMap<String, Long>()
    conn.prepareStatement(
        """
        INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent(),
    ).use { st ->
        for (entry in entries) {
            st.setLong(1, entry.id)
            st.setLong(2, structureId)
            if (entry.parentId != null) st.setLong(3, entry.parentId) else st.setNull(3, java.sql.Types.INTEGER)
            st.setLong(4, textIds.getOrPut(entry.text) { tocTextId(conn, entry.text, stableIds) })
            st.setInt(5, entry.level)
            st.setLong(6, entry.lineId)
            st.setInt(7, if (entry.isLastChild) 1 else 0)
            st.setInt(8, if (entry.hasChildren) 1 else 0)
            st.addBatch()
        }
        st.executeBatch()
    }
}

/** "הלכות וסימנים" when the roots are halachot (borrowed from a SA part), else "סימנים". */
internal fun simanNamesHeTitle(plan: SimanNamesPlan): String =
    if (plan.createdRoots.any { it.text.trimHeadingStart().startsWith("הלכות") }) {
        SIMAN_NAMES_STRUCTURE_TITLE_HE_HALACHOT
    } else {
        SIMAN_NAMES_STRUCTURE_TITLE_HE
    }

/** Writes one [SIMAN_NAMES_STRUCTURE_KEY] structure; returns its entry count. */
private fun writeSimanNamesStructure(
    conn: Connection,
    plan: SimanNamesPlan,
    stableIds: AttachedBuildStateIds?,
): Int {
    val book = plan.book
    // Own-name structures represent only the sibling groups that passed the
    // naming gate, including their unnamed simanim. Borrowers keep their own
    // complete siman sequence. Do not fold an excluded group into the prior root.
    val acceptedParents = book.simanim.filter { it.tocId in plan.namesByTocId }.map { it.parentTocId }.toHashSet()
    val simanim = book.simanim.filter { plan.nameSource != book.bookId || it.parentTocId in acceptedParents }
        .map { it.atHeading() }
    val structureId = stableIds?.altTocStructureId(book.bookId, SIMAN_NAMES_STRUCTURE_KEY)
        ?: (queryMaxId(conn, "alt_toc_structure") + 1)
    conn.prepareStatement(
        "INSERT INTO alt_toc_structure (id, bookId, key, title, heTitle) VALUES (?, ?, ?, ?, ?)",
    ).use { st ->
        st.setLong(1, structureId)
        st.setLong(2, book.bookId)
        st.setString(3, SIMAN_NAMES_STRUCTURE_KEY)
        st.setString(4, SIMAN_NAMES_STRUCTURE_TITLE_EN)
        st.setString(5, simanNamesHeTitle(plan))
        st.executeUpdate()
    }

    var nextEntryId = queryMaxId(conn, "alt_toc_entry")
    fun idFor(path: String) = stableIds?.altTocEntryId(structureId, path) ?: ++nextEntryId

    val entries = mutableListOf<PendingSimanEntry>()
    val anchors = mutableListOf<Triple<Long, Int, Long>>()
    val ends = HashMap<Long, Long>()
    if (plan.createdRoots.isEmpty()) {
        val byId = simanim.associateBy { it.tocId }
        val flat = plan.flatSimanTocIds.map(byId::getValue)
        flat.forEachIndexed { i, s ->
            val id = idFor(altTocChildPath("", i + 1))
            val label = simanLabel(s.text.trimHeadingStart().trim(), plan.namesByTocId[s.tocId])
            entries += PendingSimanEntry(id, null, 0, label, s.anchorLineId, i == flat.lastIndex, false)
            anchors += Triple(s.anchorLineIndex, 0, id)
            ends[id] = s.endLineIndex
        }
    } else {
        val rootRows = plan.createdRoots.mapIndexed { i, root ->
            TopicEntryRow(idFor(altTocChildPath("", i + 1)), null, 0, root.text, root.lineId, root.lineIndex, false)
        }
        val children = planNestedChildren(rootRows, simanim, plan.namesByTocId)
        val childCount = children.groupingBy { it.parentId }.eachCount()
        // Generated roots organize children; they must not claim gaps or appendices.
        rootRows.forEachIndexed { i, root ->
            entries += PendingSimanEntry(root.id, null, 0, root.text, root.lineId!!, i == rootRows.lastIndex, root.id in childCount)
        }
        val pathByRoot = rootRows.withIndex().associate { (i, root) -> root.id to altTocChildPath("", i + 1) }
        val ordinalByParent = HashMap<Long, Int>()
        for (child in children) {
            val ordinal = (ordinalByParent[child.parentId] ?: 0) + 1
            ordinalByParent[child.parentId] = ordinal
            val id = idFor(altTocChildPath(pathByRoot.getValue(child.parentId), ordinal))
            entries += PendingSimanEntry(id, child.parentId, 1, child.text, child.lineId, ordinal == childCount[child.parentId], false)
            anchors += Triple(child.lineIndex, 1, id)
            ends[id] = child.endLineIndex
        }
    }
    insertEntries(conn, structureId, entries, stableIds)

    val owners = nearestPrecedingOwners(readBookLines(conn, book.bookId), anchors, ends)
    conn.prepareStatement(
        "INSERT INTO line_alt_toc (lineId, structureId, altTocEntryId) VALUES (?, ?, ?)",
    ).use { st ->
        for ((lineId, owner) in owners.entries.sortedBy { it.key }) {
            st.setLong(1, lineId)
            st.setLong(2, structureId)
            st.setLong(3, owner)
            st.addBatch()
        }
        st.executeBatch()
    }
    conn.prepareStatement("UPDATE book SET hasAltStructures = 1 WHERE id = ? AND hasAltStructures = 0").use { st ->
        st.setLong(1, book.bookId)
        st.executeUpdate()
    }
    return entries.size
}

/** The allocator path the Sefaria builder recorded for [entryId]. */
private fun entryPath(conn: Connection, structureId: Long, entryId: Long): String =
    conn.prepareStatement(
        "SELECT ancestor_path FROM seifim_state.id_alt_toc_entry WHERE structure_id = ? AND id = ?",
    ).use { st ->
        st.setLong(1, structureId)
        st.setLong(2, entryId)
        st.executeQuery().use { rs ->
            check(rs.next()) { "alt_toc_entry $entryId of structure $structureId has no build-state path" }
            rs.getString(1)
        }
    }

private data class RelabelOutcome(val changed: Int, val unchanged: Int, val invisible: Int)

/** Relabels existing siman entries anchored at their siman's first content line. */
private fun relabelSimanEntries(
    conn: Connection,
    plan: SimanNamesPlan,
    stableIds: AttachedBuildStateIds?,
): RelabelOutcome {
    var changed = 0
    var unchanged = 0
    var dropped = 0
    conn.prepareStatement("UPDATE alt_toc_entry SET textId = ? WHERE id = ?").use { st ->
        for (siman in plan.book.simanim) {
            val name = plan.namesByTocId[siman.tocId] ?: continue
            val heading = siman.text.trimHeadingStart().trim()
            val label = simanLabel(heading, name)
            for (entry in plan.book.entries) {
                if (entry.lineIndex != siman.anchorLineIndex) continue
                if (simanLabelHeading(entry.text) != heading) continue
                if (entry.text == label) {
                    unchanged++
                    continue
                }
                if (!isInlineHeadingVisible(label, siman.anchorWindow)) {
                    dropped++
                    continue
                }
                st.setLong(1, tocTextId(conn, label, stableIds))
                st.setLong(2, entry.id)
                st.addBatch()
                changed++
            }
        }
        st.executeBatch()
    }
    return RelabelOutcome(changed, unchanged, dropped)
}
