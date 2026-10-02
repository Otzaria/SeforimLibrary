package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import io.github.kdroidfilter.seforimlibrary.common.ids.altTocChildPath
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Gives a commentary that has no alternate TOC of its own the **"Chapters"**
 * structure of its base book, so a reader of e.g. "מאירי על ברכות" or "אמת
 * ליעקב על ברכות" sees the chapter names (מאימתי, היה קורא...) that the
 * tractate itself shows.
 *
 * Base book — the chapter list the data points at. Bases whose chapter-name
 * lists are identical (ברכות / רי"ף ברכות / תוספות על ברכות) form one group:
 * 1. COMMENTARY links (source line → commentary line): the group holding the
 *    most links, at least a third of all the book's COMMENTARY links, and one
 *    of the declared bases when `book_base_text` has any. Within the group the
 *    base with the most links is used (tie: the title's base, else the lowest
 *    id). Links to non-Chapters books only (Noam Yerushalmi, Ma'adanei Yom Tov
 *    on the Rosh) disqualify the book: its title names the wrong base.
 *    A second pass retries rejected books crediting links from commentaries
 *    the first pass gave a structure (הגהות הב"ח על הרי"ף ← ר"ן, רבינו יונה).
 *    A native Chapters structure its own links contradict is no base
 *    ([unreliableChaptersBases]: המאור, מלחמת השם). Its chapters are worked out
 *    from its links like a commentary's, never written, and a third pass
 *    credits them to the books still without a base (כתוב שם ← המאור הקטן).
 * 2. no COMMENTARY links: the single declared Chapters base, else
 * 3. the title "X על <base>" (optionally "על מסכת <base>").
 *
 * Chapter numbers come from the source-specific link base. Daf coordinates
 * come separately from a full, curated base book explicitly identified by the
 * title or declared bases, with no conflicting pagination. Same chapter names
 * never establish identical pagination; a commentary's first represented amud
 * is not a chapter boundary. Without such a donor the link base's own
 * pagination serves when its links prove it: nine in ten of them, and at
 * least two, land on the amud of the daf heading above them (Rif commentaries
 * headed by Rif pages). Linked lines that contradict the pagination's amud
 * intervals veto it. Without pagination only links place chapters. Otherwise
 * whichever method places more chapters wins (links on a tie — sparse links
 * place only the chapters they touch; only a donor places chapters from daf
 * headings alone):
 * - links: each linked line maps to the lowest chapter it is linked to;
 *   strays are dropped by keeping the longest non-decreasing run of
 *   chapters. A chapter opens at its first line in that run, moved back onto
 *   an earlier daf heading in the gap that already reaches the chapter's amud
 *   (not past it: a heading beyond the chapter is a cross-reference), or onto
 *   the heading lines right above it — a daf heading too, unless pagination
 *   puts it in the previous chapter: no line of that chapter stands under it.
 *   With pagination, a chapter no link touches opens at the first daf heading
 *   of its amud interval in the gap where it belongs.
 * - daf headings: the first one ("דף יג.", "יג:", "דף יג ע"א", "דף יג עמוד
 *   ב", "דף יג") whose amud reaches the chapter start. A chapter starting
 *   mid-amud therefore opens at that amud's heading, taking the previous
 *   chapter's tail on the same amud with it.
 * Neither method opens a chapter above a line of the commentary that closes
 * an earlier chapter ("הדרן עלך...", "סליקו להו...", "סליק פירקא") between
 * the heading and the chapter's own material: it opens on the line after it,
 * and the heading stays in the previous chapter with the lines it holds. When
 * the chapter's opening line ("פרק...") comes before that closing, the amud is
 * covered twice (ברכת הזבח: "חדושים", then "הגהות") and its heading opens it.
 * Chapters with no material are dropped.
 *
 * Under each chapter hang the book's own daf headings (text copied verbatim)
 * whose line is in [its anchor, the next chapter's anchor), with their
 * main-TOC sub-headings below them. Subheadings crossing a chapter boundary
 * are promoted into their own chapter so they also survive exactly once. The
 * app's find-ref resolves "<book> דף ה:" through the alt-TOC as soon as a book
 * has one, so every daf must be there. A daf heading on a chapter's first amud
 * but above its anchor stays in the previous chapter, as its lines do.
 * The dafs above the first anchor belong to no chapter's range
 * ([layoutChapters]): with pagination each goes to its amud's chapter (an
 * out-of-order one opens or joins its chapter where it stands); without, they
 * open the first chapter if it is the base's first, else the book is skipped.
 * With pagination a back-reference section ("(לעיל) דף ט.") is its daf's
 * chapter wherever it stands. A chapter's entries stay together in line
 * order: one the commentary leaves and comes back to is written once per run,
 * each run's anchor never below its first daf.
 *
 * A book whose own TOC has "פרק <n>" on a line the inherited structure gives
 * to another chapter is rejected (and gives no credit): the anchors are wrong.
 *
 * Skipped: books with any alt structure (Sefaria schemas, Otzaria alt_toc JSON,
 * earlier synthesized ones — which also makes a rerun a no-op) and books whose
 * main TOC already names chapters ("פרק ראשון - מאימתי", or headings opening
 * with two of the base's chapter names). A structure written here carries
 * [INHERITED_CHAPTERS_TITLE_EN] as its title and is never a base.
 *
 * Ids come from the attached build state like [synthesizeSeifimAltTocs]. A
 * chapter's key is its base chapter number, and every base chapter's id is
 * reserved in order when the structure is first built, so a chapter that gains
 * or loses material keeps its siblings' ids and order (the reader sorts
 * siblings by id). A chapter's later runs are keyed after every reserved
 * chapter, in chapter and line order. Daf entries are keyed by ordinal within
 * their run: a heading added to the commentary's TOC renumbers only that run's
 * dafs. The reader's order is the line order, so a sibling group whose chapter
 * order is not (a later run, or an out-of-order chapter opened above the first:
 * קרן לדוד על סוכה is לולב הגזול, סוכה, לולב הגזול) hands its keyed ids out
 * again, ascending, by line; its dafs keep theirs. Any other group keeps
 * exactly the ids of its keys.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:inheritChaptersAltToc -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("InheritChaptersAltToc")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    logger.i { "Inheriting base-book Chapters alt-TOC in $dbPath" }

    try {
        val buildStatePath = resolveInheritChaptersBuildStatePath(dbPath)
        check(Files.exists(buildStatePath)) {
            "build_state not found at $buildStatePath; run the DB generation pipeline first"
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            // AttachedBuildStateIds reads the allocator tables under this alias.
            conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
                st.setString(1, buildStatePath.toAbsolutePath().toString())
                st.execute()
            }
            val stats = InheritChaptersStats()
            val snapshots = readInheritChaptersSnapshots(conn, stats)
            logger.i { "Inherited-Chapters candidates: ${stats.summary()}" }
            stats.titleDisagreements.take(20).forEach { logger.i { "  base differs from title: $it" } }
            val result = synthesizeInheritedChapters(conn, snapshots, AttachedBuildStateIds(conn))
            logger.i { "Inherited Chapters done: structures=${result.structures} entries=${result.entries}" }
        }
        BuildStateVerifier.verifyFreshSnapshot(buildStatePath, dbPath, emptyMap())
    } catch (e: Exception) {
        logger.e(e) { "Failed to inherit Chapters alt-TOC; aborting" }
        exitProcess(1)
    }
}

private fun resolveInheritChaptersBuildStatePath(dbPath: Path): Path {
    val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
    return if (explicit != null) Paths.get(explicit) else Paths.get("$dbPath.buildstate")
}

/** One base chapter: its name, base start line and start amud (null off-daf). */
internal data class BaseChapter(val text: String, val lineIndex: Long, val amud: Int?)

internal data class ChaptersBase(
    val bookId: Long,
    val title: String,
    val chapters: List<BaseChapter>,
    val isBaseBook: Boolean = false,
) {
    /** Bases sharing this key have the same chapters, so chapter numbers are interchangeable. */
    val groupKey: String get() = chapters.joinToString("\u0001") { it.text.trim() }
}

/** A main-TOC heading; [id]/[parentId] only matter for the daf sub-tree. */
internal data class ChaptersHeading(
    val lineIndex: Long,
    val text: String,
    val id: Long = 0,
    val parentId: Long? = null,
)

/** A daf heading's amud span, encoded daf*2 (+1 for amud b); "דף יג" spans both. */
internal data class AmudSpan(val start: Int, val end: Int)

internal enum class ChaptersBaseRule { LINKS, DECLARED, TITLE }

/** [sourceIds]: the link sources whose chapter numbers anchor the book (LINKS only). */
internal data class ChaptersBaseChoice(
    val baseId: Long,
    val rule: ChaptersBaseRule,
    val sourceIds: Set<Long> = emptySet(),
)

/**
 * [index] is the chapter's position in the base's chapter list. [occurrence]
 * numbers the chapter's runs of lines in line order: a commentary that leaves
 * a chapter and comes back to it gets the chapter again (see [layoutChapters]).
 */
internal data class ChapterAnchor(
    val chapter: BaseChapter,
    val lineIndex: Long,
    val index: Int = -1,
    val occurrence: Int = 0,
)

/** An alt-TOC entry below a chapter: a daf heading or one of its sub-headings. */
internal data class DafNode(val text: String, val lineIndex: Long, val children: List<DafNode> = emptyList())

internal data class InheritChaptersSnapshot(
    val bookId: Long,
    val title: String,
    val baseId: Long,
    val rule: ChaptersBaseRule,
    val anchors: List<ChapterAnchor>,
    /** Base chapter count: every chapter's id is reserved, material or not. */
    val chapterCount: Int = anchors.maxOfOrNull { it.index + 1 } ?: 0,
    /** Per anchor, the daf entries hanging below it. */
    val dafs: List<List<DafNode>> = anchors.map { emptyList() },
    val credited: Boolean = false,
    /** Independently identified complete pagination donor, never a sparse commentary. */
    val dafBaseId: Long? = null,
    /**
     * The line-ordered anchors the chapters of linked lines come from when
     * this book is credited; [anchors] may open an out-of-order chapter above.
     */
    val lineAnchors: List<ChapterAnchor> = anchors,
)

internal data class InheritChaptersResult(val structures: Int, val entries: Int)

internal class InheritChaptersStats {
    val byRule = sortedMapOf<String, Int>()
    val skipped = sortedMapOf<String, Int>()
    val titleDisagreements = mutableListOf<String>()
    var recoveredByCredit = 0
    /** Native Chapters structures that are no base ([unreliableChaptersBases]). */
    val unreliableBases = mutableListOf<Long>()
    /** Unreliable bases whose chapters were worked out to credit the books linking to them. */
    var shadowed = 0
    /** Books whose only base is through an unreliable base's worked-out chapters. */
    var recoveredThroughUnreliableBases = 0
    val skippedBooks = sortedMapOf<String, MutableList<Long>>()

    fun skip(reason: String, bookId: Long) {
        skipped.merge(reason, 1, Int::plus)
        skippedBooks.getOrPut(reason) { mutableListOf() } += bookId
    }
    fun take(rule: ChaptersBaseRule) { byRule.merge(rule.name, 1, Int::plus) }
    fun summary(): String =
        "selected=$byRule recoveredByCredit=$recoveredByCredit unreliableBases=${unreliableBases.size} " +
            "shadowed=$shadowed recoveredThroughUnreliableBases=$recoveredThroughUnreliableBases skipped=$skipped " +
            "titleDisagreements=${titleDisagreements.size}"
}

/**
 * Picks the Chapters base of one book from its evidence (see the file KDoc),
 * or null when the evidence is missing or contradicts itself.
 *
 * [groupOfSource] maps a link source to its chapter group — bases, plus the
 * credited commentaries of the second pass; [baseOfSource] maps it to the base
 * whose chapters it carries (a base maps to itself).
 */
internal fun chooseChaptersBase(
    titleBaseId: Long?,
    declaredBaseIds: Collection<Long>,
    commentaryLinkCounts: Map<Long, Int>,
    groupOfSource: Map<Long, String>,
    baseOfSource: Map<Long, Long>,
    baseIds: Set<Long>,
): ChaptersBaseChoice? {
    val total = commentaryLinkCounts.values.sum()
    if (total > 0) {
        val byGroup = HashMap<String, Int>()
        for ((source, count) in commentaryLinkCounts) {
            groupOfSource[source]?.let { byGroup.merge(it, count, Int::plus) }
        }
        val ranked = byGroup.entries.sortedByDescending { it.value }
        val top = ranked.firstOrNull() ?: return null
        if (ranked.size > 1 && ranked[1].value == top.value) return null
        if (top.value * 3 < total) return null
        if (declaredBaseIds.isNotEmpty() && declaredBaseIds.none { groupOfSource[it] == top.key }) return null

        val sources = commentaryLinkCounts.keys.filter { groupOfSource[it] == top.key }.toSet()
        val byBase = HashMap<Long, Int>()
        for (source in sources) byBase.merge(baseOfSource.getValue(source), commentaryLinkCounts.getValue(source), Int::plus)
        val best = byBase.values.max()
        val tied = byBase.filterValues { it == best }.keys
        val baseId = titleBaseId?.takeIf { it in tied } ?: tied.min()
        return ChaptersBaseChoice(baseId, ChaptersBaseRule.LINKS, sources)
    }
    if (declaredBaseIds.isNotEmpty()) {
        return declaredBaseIds.filter { it in baseIds }.singleOrNull()
            ?.let { ChaptersBaseChoice(it, ChaptersBaseRule.DECLARED) }
    }
    return titleBaseId?.let { ChaptersBaseChoice(it, ChaptersBaseRule.TITLE) }
}

/**
 * Chapter-name equivalence only permits exchanging chapter numbers in links.
 * A daf boundary must come from a full base book explicitly named by this
 * commentary. Commentary Chapters may start at their first surviving comment,
 * even when their daf coordinates are those of the underlying tractate.
 *
 * Do not follow declared-base chains: a differently paginated work can itself
 * depend on a tractate. Conflicting/unknown declared identities and titles that
 * explicitly name a commentary leave pagination unproven; links still work.
 */
internal fun chooseDafBoundaryBase(
    titleBaseId: Long?,
    declaredBaseIds: Collection<Long>,
    chapterBase: ChaptersBase,
    bases: Map<Long, ChaptersBase>,
): ChaptersBase? {
    val explicit = if (declaredBaseIds.isEmpty()) listOfNotNull(titleBaseId) else declaredBaseIds.distinct()
    val identified = explicit.map { bases[it] ?: return null }
    val donor = identified.filter { it.isBaseBook && it.groupKey == chapterBase.groupKey }.singleOrNull() ?: return null
    // Same-name commentaries with later/other pagination are not corroboration.
    val donorAmuds = donor.chapters.map { it.amud }
    if (identified.any { it.groupKey != donor.groupKey || it.chapters.map { c -> c.amud } != donorAmuds }) return null
    if (titleBaseId != null && titleBaseId != donor.bookId) return null
    return donor.takeIf { it.chapters.all { c -> c.amud != null } }
}

/** The longest " על " suffix of [title] (without a leading "מסכת") naming a base. */
internal fun titleChaptersBase(title: String, baseIdByTitle: Map<String, Long>): Long? {
    val parts = title.split(" על ")
    for (i in 1 until parts.size) {
        val suffix = parts.drop(i).joinToString(" על ").trim()
        baseIdByTitle[suffix]?.let { return it }
        baseIdByTitle[suffix.removePrefix("מסכת ").trim()]?.let { return it }
    }
    return null
}

private val HEBREW_NUMERAL_VALUES: Map<Char, Int> = mapOf(
    'א' to 1, 'ב' to 2, 'ג' to 3, 'ד' to 4, 'ה' to 5, 'ו' to 6, 'ז' to 7, 'ח' to 8, 'ט' to 9,
    'י' to 10, 'כ' to 20, 'ל' to 30, 'מ' to 40, 'נ' to 50, 'ס' to 60, 'ע' to 70, 'פ' to 80, 'צ' to 90,
    'ק' to 100, 'ר' to 200, 'ש' to 300, 'ת' to 400,
)

private fun renderHebrewNumeral(value: Int): String = buildString {
    var rest = value
    while (rest >= 400) { append('ת'); rest -= 400 }
    for (hundred in listOf(300 to 'ש', 200 to 'ר', 100 to 'ק')) {
        if (rest >= hundred.first) { append(hundred.second); rest -= hundred.first }
    }
    if (rest == 15 || rest == 16) {
        append('ט'); append(if (rest == 15) 'ו' else 'ז'); return@buildString
    }
    HEBREW_NUMERAL_VALUES.entries.firstOrNull { it.value in 10..90 && it.value == rest / 10 * 10 }?.let {
        append(it.key); rest -= it.value
    }
    HEBREW_NUMERAL_VALUES.entries.firstOrNull { it.value == rest && rest in 1..9 }?.let { append(it.key) }
}

/** A canonical Hebrew numeral ("יג", "כ\"ב") → its value; anything else → null. */
internal fun parseHebrewNumeral(raw: String): Int? {
    val letters = raw.filterNot { it == '"' || it == '\'' || it == '״' || it == '׳' }
    if (letters.isEmpty()) return null
    var value = 0
    for (ch in letters) value += HEBREW_NUMERAL_VALUES[ch] ?: return null
    return value.takeIf { renderHebrewNumeral(it) == letters }
}

private const val NUMERAL = """([א-ת"'״׳]+?)"""
private val AMUD_HEADING = Regex(
    """^(?:דף\s+)?$NUMERAL\s*(\.|:|ע["״]\s*([אב])|עמוד\s*([אב]))(?=$|[\s,\-–])""",
)
private val DAF_ONLY_HEADING = Regex("""^דף\s+$NUMERAL(?=$|[\s,\-–])""")

/** "(לעיל) " / "[לקמן] " before a daf: the commentary returns to (or anticipates) that daf. */
private val CROSS_REFERENCE_PREFIX = Regex("""^[(\[]\s*(?:לעיל|לקמן)\s*[)\]]\s*""")

private fun String.normalizedHeading(): String = trimStart('﻿').trim()

/** "(לעיל) דף ט.": a section on that daf, wherever the commentary puts it. */
internal fun isCrossReferenceHeading(text: String): Boolean = CROSS_REFERENCE_PREFIX.containsMatchIn(text.normalizedHeading())

/**
 * The amud span a commentary heading names ("דף יג.", "יג:", "דף יג ע"א",
 * "דף יג עמוד ב", or "דף יג" for both amudim, also after "(לעיל)" or
 * "(לקמן)"), or null for any other heading.
 */
internal fun parseDafHeading(text: String): AmudSpan? {
    val heading = text.normalizedHeading().replaceFirst(CROSS_REFERENCE_PREFIX, "")
    AMUD_HEADING.find(heading)?.let { match ->
        val daf = parseHebrewNumeral(match.groupValues[1]) ?: return null
        val marker = match.groupValues[2]
        val amudB = marker == ":" || match.groupValues[3] == "ב" || match.groupValues[4] == "ב"
        val amud = daf * 2 + if (amudB) 1 else 0
        return AmudSpan(amud, amud)
    }
    DAF_ONLY_HEADING.find(heading)?.let { match ->
        val daf = parseHebrewNumeral(match.groupValues[1]) ?: return null
        return AmudSpan(daf * 2, daf * 2 + 1)
    }
    return null
}

/** The amud of a Bavli/Rif-style heRef ("ברכות, יג., טז" → 13a), else null. */
internal fun parseHeRefAmud(heRef: String?): Int? {
    val segment = heRef?.split(',')?.getOrNull(1)?.trim() ?: return null
    if (!segment.endsWith('.') && !segment.endsWith(':')) return null
    return parseDafHeading(segment)?.start
}

/**
 * Indices, ascending, of one longest non-decreasing subsequence of [values].
 * Built from the end, so a tie drops the later, lower stray (a back-link).
 */
internal fun longestNonDecreasingRun(values: List<Int>): List<Int> {
    if (values.isEmpty()) return emptyList()
    // Scanning backwards this is a longest non-increasing run; tails stay as high as possible.
    val tailValue = ArrayList<Int>()
    val tailIndex = ArrayList<Int>()
    val next = IntArray(values.size) { -1 }
    for (i in values.indices.reversed()) {
        val v = values[i]
        var lo = 0
        var hi = tailValue.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (tailValue[mid] >= v) lo = mid + 1 else hi = mid
        }
        if (lo > 0) next[i] = tailIndex[lo - 1]
        if (lo == tailValue.size) {
            tailValue += v
            tailIndex += i
        } else {
            tailValue[lo] = v
            tailIndex[lo] = i
        }
    }
    val run = ArrayList<Int>(tailIndex.size)
    var cursor = tailIndex.last()
    while (cursor >= 0) {
        run += cursor
        cursor = next[cursor]
    }
    return run
}

/**
 * Chapter anchors for one commentary (see the file KDoc). [chapterByLine]
 * maps a linked commentary lineIndex to the lowest chapter number it is
 * linked to. [dafChapters] must be an independently identified complete
 * pagination donor with the same chapter names; null disables daf inference.
 * [marks] (line order) are the commentary's own chapter marks: no chapter
 * opens above a line that closes an earlier chapter on its first amud.
 */
internal fun computeChapterAnchors(
    chapters: List<BaseChapter>,
    lineIndices: List<Long>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
    dafChapters: List<BaseChapter>? = null,
    linkPagination: List<BaseChapter>? = null,
    marks: List<ChapterMark> = emptyList(),
): List<ChapterAnchor> {
    if (chapters.isEmpty()) return emptyList()
    require(dafChapters == null || dafChapters.map { it.text.trim() } == chapters.map { it.text.trim() })
    val donor = dafChapters?.takeIf { dafHeadingsAgreeWithLinks(it, headings, chapterByLine) }
    val pagination = chapterPagination(chapters, headings, chapterByLine, dafChapters, linkPagination)
    val nextLine = nextLineOf(lineIndices)
    val byLinks = anchorsFromLinks(chapters, lineIndices, headings, chapterByLine, pagination, marks)
    // Only a named donor places chapters from daf headings alone.
    val byDaf = donor?.let { anchorsFromDafHeadings(it, headings, marks, nextLine) }
        ?.map { it.copy(chapter = chapters[it.index]) }.orEmpty()
    // Sparse links place only the chapters they touch; full daf headings then place more.
    if (byDaf.size > byLinks.size) return byDaf
    return if (pagination == null) {
        byLinks.map { it.anchor }
    } else {
        fillUnlinkedChapters(chapters, byLinks, headings, pagination, marks, nextLine)
    }
}

/** The line after [line] in the book ([lineIndices] ascending), null past the end. */
private fun nextLineOf(lineIndices: List<Long>): (Long) -> Long? = { line ->
    val i = lineIndices.binarySearch(line)
    lineIndices.getOrNull(if (i >= 0) i + 1 else -i - 1)
}

/** The daf heading lines in line order, each with its amud span. */
private fun dafHeadingLines(headings: List<ChaptersHeading>): List<Pair<Long, AmudSpan>> =
    headings.mapNotNull { h -> parseDafHeading(h.text)?.let { h.lineIndex to it } }.sortedBy { it.first }

/**
 * Moves a chapter opening at the daf heading [heading] below the closing
 * lines of earlier chapters on that heading's amud (up to the next daf
 * heading, never to or past [limit]): the lines above them, the heading
 * with them, are the previous chapter's.
 */
private fun openingBelowClosings(
    heading: Long,
    chapter: Int,
    dafs: List<Pair<Long, AmudSpan>>,
    marks: List<ChapterMark>,
    nextLine: (Long) -> Long?,
    limit: Long = Long.MAX_VALUE,
): Long {
    val nextHeading = dafs.firstOrNull { it.first > heading }?.first ?: Long.MAX_VALUE
    val opening = openingAfterClosings(marks, chapter, heading, minOf(nextHeading, limit), nextLine) ?: return heading
    return opening.takeIf { it < limit } ?: heading
}

/**
 * The amud intervals daf headings may be read against: the named donor, else
 * the link base's own pagination when the links prove it ([linkPagination],
 * see [linksProvePagination]); either is vetoed by a contradicting link.
 */
internal fun chapterPagination(
    chapters: List<BaseChapter>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
    dafChapters: List<BaseChapter>?,
    linkPagination: List<BaseChapter>?,
): List<BaseChapter>? =
    listOfNotNull(dafChapters, linkPagination).firstOrNull { pagination ->
        pagination.map { it.text.trim() } == chapters.map { it.text.trim() } &&
            pagination.all { it.amud != null } &&
            dafHeadingsAgreeWithLinks(pagination, headings, chapterByLine)
    }

private const val MIN_PAGINATION_LINKS = 2

/**
 * True when the commentary's daf headings use its link base's pagination: of
 * the links from that base whose base line names an amud ([baseAmudByLine]:
 * commentary lineIndex → those amuds), at least [MIN_PAGINATION_LINKS] and
 * nine in ten fall on the amud of the daf heading above them. Rif commentaries
 * headed by Rif pages pass; a commentary headed by Bavli pages that links
 * to the Rif does not.
 */
internal fun linksProvePagination(headings: List<ChaptersHeading>, baseAmudByLine: Map<Long, List<Int>>): Boolean {
    val dafs = headings.mapNotNull { h -> parseDafHeading(h.text)?.let { h.lineIndex to it } }.sortedBy { it.first }
    if (dafs.isEmpty()) return false
    var agree = 0
    var disagree = 0
    var cursor = 0
    var preceding: AmudSpan? = null
    for ((line, amuds) in baseAmudByLine.entries.sortedBy { it.key }) {
        while (cursor < dafs.size && dafs[cursor].first <= line) preceding = dafs[cursor++].second
        val span = preceding ?: continue
        for (amud in amuds) if (amud in span.start..span.end) agree++ else disagree++
    }
    return agree >= MIN_PAGINATION_LINKS && disagree * 10 <= agree + disagree
}

/** A linked chapter outside its heading's proven amud interval vetoes daf inference. */
private fun dafHeadingsAgreeWithLinks(
    chapters: List<BaseChapter>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
): Boolean {
    val dafs = headings.mapNotNull { h -> parseDafHeading(h.text)?.let { h.lineIndex to it } }.sortedBy { it.first }
    val linked = chapterByLine.entries.filter { it.value in chapters.indices }.sortedBy { it.key }
    val run = longestNonDecreasingRun(linked.map { it.value }).map { linked[it] }
    var cursor = 0
    var preceding: AmudSpan? = null
    for ((line, chapter) in run) {
        while (cursor < dafs.size && dafs[cursor].first <= line) preceding = dafs[cursor++].second
        val span = preceding ?: continue
        val start = chapters[chapter].amud ?: return false
        val next = chapters.getOrNull(chapter + 1)?.amud ?: Int.MAX_VALUE
        // A chapter can start mid-amud: both adjacent chapters may cite that amud.
        if (span.end < start || span.start > next) return false
    }
    return true
}

internal fun chapterOfBaseLine(chapters: List<BaseChapter>, baseLineIndex: Long): Int {
    var lo = 0
    var hi = chapters.size
    while (lo < hi) {
        val mid = (lo + hi) ushr 1
        if (chapters[mid].lineIndex <= baseLineIndex) lo = mid + 1 else hi = mid
    }
    return lo - 1
}

/** Chapter number of a commentary line from its own line-ordered anchors, -1 before the first. */
private fun chapterOfAnchoredLine(anchors: List<ChapterAnchor>, lineIndex: Long): Int =
    anchors.lastOrNull { it.lineIndex <= lineIndex }?.index ?: -1

/** A chapter placed by links, with the last linked line that keeps it open. */
private data class LinkedChapter(val anchor: ChapterAnchor, val lastLine: Long)

private fun anchorsFromLinks(
    chapters: List<BaseChapter>,
    lineIndices: List<Long>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
    dafChapters: List<BaseChapter>?,
    marks: List<ChapterMark>,
): List<LinkedChapter> {
    val linked = chapterByLine.entries
        .map { it.key to it.value }
        .filter { it.second in chapters.indices }
        .sortedBy { it.first }
    if (linked.isEmpty()) return emptyList()
    val run = longestNonDecreasingRun(linked.map { it.second }).map { linked[it] }

    val headingByLine = headings.groupBy { it.lineIndex }
    val dafHeadings = dafHeadingLines(headings)
    val positionOfLine = HashMap<Long, Int>(lineIndices.size * 2).apply {
        lineIndices.forEachIndexed { position, lineIndex -> put(lineIndex, position) }
    }
    val closingLines = marks.filter { it.closes }.mapTo(HashSet()) { it.lineIndex }
    val nextLine = nextLineOf(lineIndices)

    val anchors = mutableListOf<LinkedChapter>()
    var cursor = 0
    var previousLine = -1L
    for ((k, chapter) in chapters.withIndex()) {
        while (cursor < run.size && run[cursor].second < k) cursor++
        if (cursor >= run.size) break
        if (run[cursor].second != k) continue
        val firstLine = run[cursor].first
        var lastLine = firstLine
        while (cursor < run.size && run[cursor].second == k) lastLine = run[cursor++].first

        var anchor = firstLine
        val dafAnchor = dafChapters?.get(k)?.amud?.let { start ->
            val nextStart = dafChapters.getOrNull(k + 1)?.amud ?: Int.MAX_VALUE
            // A heading past the chapter is a stray (a cross-reference), not its
            // opening; one on the next chapter's first amud may still open it.
            dafHeadings.firstOrNull { (line, span) ->
                line > previousLine && line < firstLine && span.end >= start && span.start <= nextStart
            }
        }
        if (dafAnchor != null) {
            // The previous chapter's closing lines under that heading keep it there,
            // with its lines (רבינו יונה על ברכות: "הדרן עלך מאימתי" under ז.).
            anchor = openingAfterClosings(marks, k, dafAnchor.first, firstLine, nextLine) ?: dafAnchor.first
        } else {
            // Heading lines directly above the first line open the same material,
            // a daf heading too: no line of the previous chapter stands under it.
            // With pagination the search above took every daf heading that can
            // open the chapter; one left here is the previous chapter's.
            var position = positionOfLine[firstLine] ?: -1
            while (position > 0) {
                val candidate = lineIndices[position - 1]
                if (candidate <= previousLine) break
                val lineHeadings = headingByLine[candidate] ?: break
                if (candidate in closingLines) break
                if (dafChapters != null && lineHeadings.any { parseDafHeading(it.text) != null }) break
                anchor = candidate
                position--
            }
        }
        anchors += LinkedChapter(ChapterAnchor(chapter, anchor, k), lastLine)
        previousLine = lastLine
    }
    return anchors
}

/**
 * The amud's chapter in [pagination]: the last chapter starting at or before
 * it (a chapter starting mid-amud owns its first amud), the first chapter for
 * an amud before it.
 */
private fun chapterOfAmud(pagination: List<BaseChapter>, amud: Int): Int =
    (pagination.indexOfLast { it.amud!! <= amud }).coerceAtLeast(0)

/**
 * Adds the chapters no link touches but a daf heading reaches, between (and
 * before or after) the linked ones: the first daf heading after the previous
 * chapter's last linked line, inside this chapter's amud interval, as
 * [anchorsFromDafHeadings] does. Gaps keep their order: an anchor never passes
 * the next placed chapter, and each fill is after the one before it.
 */
private fun fillUnlinkedChapters(
    chapters: List<BaseChapter>,
    placed: List<LinkedChapter>,
    headings: List<ChaptersHeading>,
    pagination: List<BaseChapter>,
    marks: List<ChapterMark>,
    nextLine: (Long) -> Long?,
): List<ChapterAnchor> {
    val dafHeadings = dafHeadingLines(headings)
    val anchors = mutableListOf<ChapterAnchor>()
    var lowerLine = -1L
    var nextChapter = 0
    fun fill(untilChapter: Int, upperLine: Long) {
        var after = lowerLine
        for (j in nextChapter until untilChapter) {
            val start = pagination[j].amud!!
            val nextStart = pagination.getOrNull(j + 1)?.amud ?: Int.MAX_VALUE
            val heading = dafHeadings.firstOrNull { (line, span) ->
                line > after && line < upperLine && span.end >= start && span.start < nextStart
            } ?: continue
            val opening = openingBelowClosings(heading.first, j, dafHeadings, marks, nextLine, upperLine)
            anchors += ChapterAnchor(chapters[j], opening, j)
            after = opening
        }
    }
    for (chapter in placed) {
        fill(chapter.anchor.index, chapter.anchor.lineIndex)
        anchors += chapter.anchor
        lowerLine = chapter.lastLine
        nextChapter = chapter.anchor.index + 1
    }
    fill(chapters.size, Long.MAX_VALUE)
    return anchors
}

private fun anchorsFromDafHeadings(
    chapters: List<BaseChapter>,
    headings: List<ChaptersHeading>,
    marks: List<ChapterMark>,
    nextLine: (Long) -> Long?,
): List<ChapterAnchor> {
    if (chapters.any { it.amud == null }) return emptyList()
    val parsed = dafHeadingLines(headings)
    val ordered = longestNonDecreasingRun(parsed.map { it.second.start }).map { parsed[it] }

    val anchors = mutableListOf<ChapterAnchor>()
    var next = 0
    for ((k, chapter) in chapters.withIndex()) {
        val start = chapter.amud!!
        val nextStart = chapters.getOrNull(k + 1)?.amud ?: Int.MAX_VALUE
        while (next < ordered.size && ordered[next].second.end < start) next++
        if (next >= ordered.size) break
        val (line, span) = ordered[next]
        if (span.start >= nextStart) continue
        anchors += ChapterAnchor(chapter, openingBelowClosings(line, k, parsed, marks, nextLine), k)
        next++
    }
    return anchors
}

/** A heading the app treats as a daf: parsed here, or opening with the word "דף". */
private fun isDafEntryHeading(text: String): Boolean =
    parseDafHeading(text) != null || DAF_WORD_HEADING.containsMatchIn(text.normalizedHeading())

private val DAF_WORD_HEADING = Regex("""^דף(?=\s)""")

/** The daf-entry headings and every main-TOC descendant of one. */
private fun relevantDafHeadings(headings: List<ChaptersHeading>): BooleanArray {
    val originalChildren = headings.withIndex().groupBy { it.value.parentId }
    val relevant = BooleanArray(headings.size)
    val pending = java.util.ArrayDeque<Int>()
    headings.forEachIndexed { i, h -> if (isDafEntryHeading(h.text)) pending.addLast(i) }
    while (pending.isNotEmpty()) {
        val i = pending.removeLast()
        if (relevant[i]) continue
        relevant[i] = true
        if (headings[i].id != 0L) originalChildren[headings[i].id].orEmpty().forEach { pending.addLast(it.index) }
    }
    return relevant
}

private val HEADING_ORDER = compareBy<IndexedValue<ChaptersHeading>>({ it.value.lineIndex }, { it.value.id }, { it.index })

/** Position of the anchor whose line range holds [line] in line-ordered [anchors], -1 above the first. */
private fun lineOwner(anchors: List<ChapterAnchor>, line: Long): Int {
    var lo = 0
    var hi = anchors.size
    while (lo < hi) {
        val mid = (lo + hi) ushr 1
        if (anchors[mid].lineIndex <= line) lo = mid + 1 else hi = mid
    }
    return lo - 1
}

/**
 * The chapters as written: [anchors] in chapter order (each chapter's runs in
 * line order), [dafs] below each. The writer gives the reader their line order.
 */
internal data class ChaptersLayout(val anchors: List<ChapterAnchor>, val dafs: List<List<DafNode>>)

/**
 * Places the daf headings above the first anchor, which no chapter's line
 * range holds, then partitions the daf subtrees ([partitionDafs]).
 *
 * With [pagination] each of those headings belongs to its amud's chapter (a
 * non-daf one to the chapter of the daf heading above it), unless a closing
 * line of the chapter before it stands on that amud: then the amud's heading
 * is that chapter's, as its first lines are. The first chapter opens at the
 * first of its own, so it takes no heading of an earlier chapter; a heading
 * of another chapter is out of order (קרן לדוד על שבת opens on קלה. and
 * קנג:) and opens that chapter where it stands. Without pagination they are
 * the first chapter's when it is the base's first chapter; otherwise nothing
 * tells which earlier chapter they belong to, and the book gets no structure
 * (null).
 *
 * [anchors] are the line-ordered chapter anchors; a written anchor moves up
 * to the first heading it receives here, so it never sits below its dafs.
 *
 * Each chapter's entries stay together in line order: a reader's section, and
 * the app's print range, is a chapter's lines up to the next chapter's. A
 * chapter the commentary leaves and comes back to (קרן לדוד על סוכה: לח.,
 * then ט. and יא., then לא:) is written once per run of its dafs, each with
 * the dafs of that run.
 */
internal fun layoutChapters(
    chapters: List<BaseChapter>,
    anchors: List<ChapterAnchor>,
    headings: List<ChaptersHeading>,
    pagination: List<BaseChapter>? = null,
    marks: List<ChapterMark> = emptyList(),
): ChaptersLayout? {
    if (anchors.isEmpty()) return ChaptersLayout(emptyList(), emptyList())
    val relevant = relevantDafHeadings(headings)
    val first = anchors.first()
    val leading = headings.withIndex()
        .filter { relevant[it.index] && it.value.lineIndex < first.lineIndex }
        .sortedWith(HEADING_ORDER)
    val chapterByAmud = HashMap<Int, Int>()
    if (leading.isNotEmpty()) {
        if (pagination == null && first.index > 0) return null
        val dafs = dafHeadingLines(headings)
        var current = first.index
        for ((i, heading) in leading) {
            val span = if (pagination == null) null else parseDafHeading(heading.text)
            if (span != null) {
                current = chapterOfAmud(pagination!!, span.start)
                val nextHeading = dafs.firstOrNull { it.first > heading.lineIndex }?.first ?: Long.MAX_VALUE
                val closed = openingAfterClosings(marks, current, heading.lineIndex, nextHeading) { it }
                if (current > 0 && closed != null) current--
            }
            chapterByAmud[i] = current
        }
    }
    // With pagination a "(לעיל) דף ט." section is its daf's chapter wherever it stands, and so
    // are the headings under it up to the next daf heading.
    if (pagination != null) {
        var current: Int? = null
        for ((i, heading) in headings.withIndex().filter { relevant[it.index] && it.index !in chapterByAmud }.sortedWith(HEADING_ORDER)) {
            val span = parseDafHeading(heading.text)
            if (span != null) current = if (isCrossReferenceHeading(heading.text)) chapterOfAmud(pagination, span.start) else null
            current?.let { chapterByAmud[i] = it }
        }
    }
    val opened = HashMap<Int, Long>()
    for ((i, chapter) in chapterByAmud) opened.merge(chapter, headings[i].lineIndex, ::minOf)
    val main = anchors.associateBy { it.index }
    val roots = (main.keys + opened.keys).sorted().map { k ->
        val line = listOfNotNull(main[k]?.lineIndex, opened[k]).min()
        ChapterAnchor(main[k]?.chapter ?: chapters[k], line, k)
    }
    val chapterOfHeading = IntArray(headings.size) { i ->
        chapterByAmud[i] ?: anchors[lineOwner(anchors, headings[i].lineIndex).coerceAtLeast(0)].index
    }

    // Runs: the chapter's root and relevant headings in line order, cut where another chapter's entry comes between.
    data class Item(val line: Long, val chapter: Int, val heading: Int)
    val items = roots.map { Item(it.lineIndex, it.index, -1) } +
        headings.indices.filter { relevant[it] }.map { Item(headings[it].lineIndex, chapterOfHeading[it], it) }
    val ordered = items.sortedWith(compareBy({ it.line }, { if (it.heading < 0) 0 else 1 }, { headings.getOrNull(it.heading)?.id ?: 0L }, { it.heading }))
    val runStarts = HashMap<Int, MutableList<Long>>()
    val runOfHeading = IntArray(headings.size)
    var previous: Int? = null
    for (item in ordered) {
        val starts = runStarts.getOrPut(item.chapter) { mutableListOf() }
        if (item.chapter != previous) starts += item.line
        if (item.heading >= 0) runOfHeading[item.heading] = starts.lastIndex
        previous = item.chapter
    }
    val written = roots.flatMap { root ->
        runStarts.getValue(root.index).mapIndexed { run, line ->
            if (run == 0) root else root.copy(lineIndex = line, occurrence = run)
        }
    }
    val position = written.withIndex().associate { (it.value.index to it.value.occurrence) to it.index }
    val owners = IntArray(headings.size) { i -> position[chapterOfHeading[i] to runOfHeading[i]] ?: 0 }
    return ChaptersLayout(written, partitionDafs(written.size, headings, relevant, owners))
}

/**
 * Partitions the daf subtrees into [chapterCount] chapters by [owners] (a
 * position per heading), preserving each original heading exactly once. A
 * descendant whose parent belongs to another chapter is promoted into its own
 * chapter, including non-daf subheadings. Unrelated main-TOC headings are not
 * copied. Traversal visits each edge once, even for malformed cycles.
 */
private fun partitionDafs(
    chapterCount: Int,
    headings: List<ChaptersHeading>,
    relevant: BooleanArray,
    owners: IntArray,
): List<List<DafNode>> {
    val byId = headings.withIndex().filter { it.value.id != 0L }.associate { it.value.id to it.index }
    val keptParent = Array<Int?>(headings.size) { i ->
        headings[i].parentId?.let(byId::get)?.takeIf { parent ->
            relevant[parent] && owners[parent] == owners[i] && headings[parent].lineIndex <= headings[i].lineIndex
        }
    }
    val ordered = headings.indices.filter { relevant[it] }
        .sortedWith(compareBy({ headings[it].lineIndex }, { headings[it].id }, { it }))
    val children = ordered.filter { keptParent[it] != null }.groupBy { keptParent[it] }
    val byChapter = ordered.groupBy { owners[it] }
    val emitted = BooleanArray(headings.size)
    fun subtree(i: Int): DafNode {
        emitted[i] = true
        val h = headings[i]
        return DafNode(h.text, h.lineIndex, children[i].orEmpty().mapNotNull {
            if (emitted[it]) null else subtree(it)
        })
    }
    return (0 until chapterCount).map { chapter ->
        val local = byChapter[chapter].orEmpty()
        buildList {
            local.filter { keptParent[it] == null }.forEach { if (!emitted[it]) add(subtree(it)) }
            // A cyclic source component has no root. Break it at its first
            // ordered heading, retaining all of its nodes without recursion loops.
            local.forEach { if (!emitted[it]) add(subtree(it)) }
        }
    }
}

/**
 * A line that marks a chapter boundary in the commentary's own text: a
 * closing ([closes]: "הדרן עלך גיד הנשה", "סליקו להו במה טומנין", "סליק
 * פירקא", a comment ending "...: הדרן עלך פרק ראשון") or an opening (a line
 * starting "פרק", as "פרק שני. הקומץ זוטא"). A closing line stays in the
 * chapter it closes, also when it goes on with the next chapter's first words
 * ("הדרן עלך פרק הזורק המגרש את אשתו..."). [chapter] is the base chapter the
 * mark names, null when it names none or several.
 */
internal data class ChapterMark(val lineIndex: Long, val chapter: Int?, val closes: Boolean = true)

private val CLOSING_MARKER = Regex(
    """(?:^|(?<=[\s:.0]))(לא\s+)?(הדרן\s+עלך|סליק(?:ו|א)?\s+(?:להו?|פירקא|פרקא|פרק))(?=$|[\s.:,])""",
)
private val HTML_TAG = Regex("<[^>]+>")
private val NAME_PUNCTUATION = Regex("""["'״׳.,:;()\[\]]""")
private val WHITESPACE = Regex("""\s+""")
private val ORDINAL_PEREK = mapOf(
    "ראשון" to 0, "קמא" to 0, "שני" to 1, "שלישי" to 2, "רביעי" to 3, "חמישי" to 4,
    "ששי" to 5, "שישי" to 5, "שביעי" to 6, "שמיני" to 7, "תשיעי" to 8, "עשירי" to 9,
)

/** Words without punctuation or "פרק", and without a leading article ("הרגל" = "רגל"). */
private fun closingNameTokens(text: String): List<String> =
    text.replace(NAME_PUNCTUATION, " ").split(WHITESPACE)
        .filter { it.isNotEmpty() && it != "פרק" }
        .map { if (it.length > 2 && it.startsWith('ה')) it.substring(1) else it }

/** Where a closing marker may stand: the line's opening words, or its last ones. */
private const val CLOSING_EDGE_CHARS = 3
private const val CLOSING_TAIL_CHARS = 60

private val OPENING_MARKER = Regex("""^פרק(?=\s)""")

/**
 * The chapter mark in a line's text (HTML allowed), or null. A closing marker
 * counts only at the start of the line or in its last words: "סליקו להו"
 * inside an argument is Aramaic, and "לא סליק להו" never closes anything;
 * "סליק להו" must name a chapter.
 */
internal fun parseChapterMark(lineIndex: Long, content: String, chapters: List<BaseChapter>): ChapterMark? {
    val text = content.replace(HTML_TAG, " ").normalizedHeading()
    closingMark(lineIndex, text, chapters)?.let { return it }
    if (!OPENING_MARKER.containsMatchIn(text)) return null
    return ChapterMark(lineIndex, namedChapter(closingNameTokens(text), chapters), closes = false)
}

private fun closingMark(lineIndex: Long, text: String, chapters: List<BaseChapter>): ChapterMark? {
    for (match in CLOSING_MARKER.findAll(text)) {
        if (match.groups[1] != null) continue
        val atStart = match.range.first < CLOSING_EDGE_CHARS
        val rest = text.substring(match.range.last + 1)
        if (!atStart && rest.length > CLOSING_TAIL_CHARS) continue
        val name = closingNameTokens(rest)
        val chapter = namedChapter(name, chapters)
        // "סליקו להו" is a closing only when it names what it closes.
        val generic = match.groupValues[2].let { it.startsWith("הדרן") || !it.contains("לה") }
        if (chapter == null && !generic && name.firstOrNull() != "מסכת") continue
        return ChapterMark(lineIndex, chapter, closes = true)
    }
    return null
}

/**
 * The chapter [name] (words, [closingNameTokens]) starts with: an ordinal
 * ("ראשון"), the one chapter whose first word it starts with, else the one
 * whose first two words, else the longest whose whole name it starts with
 * ("כל הזבחים שקבלו דמן" over "כל הזבחים"); null when none or several.
 */
private fun namedChapter(name: List<String>, chapters: List<BaseChapter>): Int? {
    val first = name.firstOrNull() ?: return null
    ORDINAL_PEREK[first]?.let { return it.takeIf { it < chapters.size } }
    val tokens = chapters.map { closingNameTokens(it.text) }
    var candidates = chapters.indices.filter { tokens[it].firstOrNull() == first }
    if (candidates.size > 1) candidates = candidates.filter { tokens[it].getOrNull(1) == name.getOrNull(1) }
    if (candidates.size > 1) {
        val whole = candidates.filter { tokens[it] == name.take(tokens[it].size) }
        val longest = whole.maxOfOrNull { tokens[it].size }
        candidates = whole.filter { tokens[it].size == longest }
    }
    return candidates.singleOrNull()
}

/**
 * The first line [chapter] may open at after the closings in (from, until):
 * the line after the last one that closes an earlier chapter. A closing
 * naming [chapter] or a later one ends the search (it is out of place;
 * nothing below it is trusted).
 *
 * Null when nothing closes, and when the chapter already opens above that
 * closing: a commentary in two runs over each amud (ברכת הזבח: "חדושים", then
 * "הגהות") opens the chapter in the first ("פרק הקומץ זוטא") and closes the
 * previous one in the second. No single line divides such an amud.
 */
private fun openingAfterClosings(
    marks: List<ChapterMark>,
    chapter: Int,
    from: Long,
    until: Long,
    nextLine: (Long) -> Long?,
): Long? {
    var opening: Long? = null
    var closing: Long? = null
    for (mark in marks) {
        if (mark.lineIndex <= from) continue
        if (mark.lineIndex >= until) break
        if (!mark.closes) continue
        if (mark.chapter != null && mark.chapter >= chapter) break
        // A closing on the book's last line leaves nothing to open: the search stops.
        opening = nextLine(mark.lineIndex) ?: break
        closing = mark.lineIndex
    }
    if (closing == null) return null
    val openedAbove = marks.any { mark ->
        !mark.closes && mark.lineIndex > from && mark.lineIndex < closing && (mark.chapter == null || mark.chapter >= chapter)
    }
    return opening.takeUnless { openedAbove }
}

private val NUMBERED_PEREK = Regex("""^פרק\s+([א-ת"'״׳]+)(?=$|[\s,.:\-–])""")

/** False when a "פרק <n>" heading sits in an inherited chapter other than the n-th. */
internal fun perekHeadingsAgree(anchors: List<ChapterAnchor>, headings: List<ChaptersHeading>): Boolean {
    for (heading in headings) {
        val match = NUMBERED_PEREK.find(heading.text.normalizedHeading()) ?: continue
        val n = parseHebrewNumeral(match.groupValues[1]) ?: continue
        val covering = anchors.lastOrNull { it.lineIndex <= heading.lineIndex } ?: continue
        if (covering.index + 1 != n) return false
    }
    return true
}

private val PEREK_HEADING = Regex("""^פרק(?:א)?(?=$|[\s\-–])""")

private fun String.findRefTokens(): List<String> =
    normalizedHeading().replace(Regex("""["'״׳]"""), "")
        .replace(Regex("""[^\p{L}\p{N}\s]"""), " ")
        .split(Regex("""\s+""")).filter { it.isNotEmpty() }

/**
 * True when the main TOC already names the chapters: two "פרק" headings, or
 * headings whose first four words hold two distinct base chapter names.
 */
internal fun hasOwnChapterHeadings(headings: List<ChaptersHeading>, chapters: List<BaseChapter>): Boolean {
    if (headings.count { PEREK_HEADING.containsMatchIn(it.text.normalizedHeading()) } >= 2) return true
    val names = chapters.map { it.text.findRefTokens() }.filter { it.isNotEmpty() }.distinct()
    val found = HashSet<List<String>>()
    for (heading in headings) {
        val head = heading.text.findRefTokens().take(4)
        for (name in names) {
            if (name in found || name.size > head.size) continue
            if ((0..head.size - name.size).any { head.subList(it, it + name.size) == name }) found += name
        }
        if (found.size >= 2) return true
    }
    return false
}

/** Pre-existing Chapters structures — minus those this stage wrote. */
internal fun readChaptersBases(conn: Connection): Map<Long, ChaptersBase> {
    data class Root(val bookId: Long, val bookTitle: String, val text: String, val lineIndex: Long, val isBaseBook: Boolean)

    val roots = mutableListOf<Root>()
    conn.prepareStatement(
        """
        SELECT s.bookId, b.title, t.text, l.lineIndex, b.isBaseBook
        FROM alt_toc_structure s
        JOIN book b ON b.id = s.bookId
        JOIN alt_toc_entry e ON e.structureId = s.id AND e.parentId IS NULL
        JOIN tocText t ON t.id = e.textId
        JOIN line l ON l.id = e.lineId
        WHERE s.key = ? AND (s.title IS NULL OR s.title <> ?)
        ORDER BY s.bookId, l.lineIndex, e.id
        """.trimIndent(),
    ).use { st ->
        st.setString(1, CHAPTERS_STRUCTURE_KEY)
        st.setString(2, INHERITED_CHAPTERS_TITLE_EN)
        st.executeQuery().use { rs ->
            while (rs.next()) roots += Root(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getBoolean(5))
        }
    }
    val heRefAtOrAfter = conn.prepareStatement(
        "SELECT heRef FROM line WHERE bookId = ? AND lineIndex >= ? AND heRef IS NOT NULL ORDER BY lineIndex LIMIT 1",
    )
    return heRefAtOrAfter.use { st ->
        roots.groupBy { it.bookId }.mapValues { (bookId, bookRoots) ->
            val chapters = bookRoots.distinctBy { it.lineIndex }.map { root ->
                st.setLong(1, bookId)
                st.setLong(2, root.lineIndex)
                val heRef = st.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                BaseChapter(root.text, root.lineIndex, parseHeRefAmud(heRef))
            }
            ChaptersBase(bookId, bookRoots.first().bookTitle, chapters, bookRoots.first().isBaseBook)
        }
    }
}

/** Linked lines that judge a base's chapters, and the share of them it must agree on. */
private const val MIN_BASE_CHECK_LINKS = 5

/**
 * Native Chapters structures that their own links contradict, by book id. A
 * commentary's Sefaria "Chapters" can be cut from Bavli daf ranges while its
 * text is paged by the Rif (המאור, מלחמת השם: "מי שמתו" at the Rif's יח:,
 * which is תפלת השחר), and Sefaria does not show them. Each is checked against
 * the other bases linking to it with its own pagination (at least two links,
 * nine in ten from a line on the same amud): it is unreliable when one of
 * them links at least [MIN_BASE_CHECK_LINKS] of its lines inside chapters of
 * both, and none puts half of those lines in the chapter of the same name.
 * Full base books (the Bavli) are the reference and never checked.
 */
internal fun unreliableChaptersBases(conn: Connection, bases: Map<Long, ChaptersBase>): Set<Long> {
    val unreliable = HashSet<Long>()
    val names = bases.mapValues { (_, base) -> base.chapters.map { closingNameTokens(it.text) } }
    conn.prepareStatement(
        """
        SELECT k.sourceBookId, bl.lineIndex, bl.heRef, ml.lineIndex, ml.heRef
        FROM link k INDEXED BY idx_link_target_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        JOIN line ml ON ml.id = k.targetLineId
        JOIN line bl ON bl.id = k.sourceLineId
        WHERE k.targetBookId = ? AND k.sourceBookId <> k.targetBookId AND c.name = 'COMMENTARY'
        """.trimIndent(),
    ).use { st ->
        for (base in bases.values.sortedBy { it.bookId }) {
            if (base.isBaseBook) continue
            class Tally { var sameAmud = 0; var paged = 0; var agree = 0; var judged = 0 }
            val bySource = HashMap<Long, Tally>()
            st.setLong(1, base.bookId)
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    val source = bases[rs.getLong(1)] ?: continue
                    val tally = bySource.getOrPut(source.bookId) { Tally() }
                    val sourceAmud = parseHeRefAmud(rs.getString(3))
                    val ownAmud = parseHeRefAmud(rs.getString(5))
                    if (sourceAmud != null && ownAmud != null) {
                        tally.paged++
                        if (sourceAmud == ownAmud) tally.sameAmud++
                    }
                    val sourceChapter = chapterOfBaseLine(source.chapters, rs.getLong(2))
                    val ownChapter = chapterOfBaseLine(base.chapters, rs.getLong(4))
                    if (sourceChapter < 0 || ownChapter < 0) continue
                    tally.judged++
                    if (names.getValue(source.bookId)[sourceChapter] == names.getValue(base.bookId)[ownChapter]) tally.agree++
                }
            }
            val judges = bySource.values.filter {
                it.sameAmud >= MIN_PAGINATION_LINKS && it.sameAmud * 10 >= it.paged * 9 && it.judged >= MIN_BASE_CHECK_LINKS
            }
            if (judges.isNotEmpty() && judges.all { it.agree * 2 < it.judged }) unreliable += base.bookId
        }
    }
    return unreliable
}

/**
 * The book's chapter marks ([parseChapterMark]) in line order. The stage runs
 * on the working DB (text in `line.content`); a rerun may meet the finished one.
 */
private fun readChapterMarks(conn: Connection, bookId: Long, chapters: List<BaseChapter>, split: Boolean): List<ChapterMark> {
    val marks = mutableListOf<ChapterMark>()
    val content = LineContentShape.contentExpr(split)
    conn.prepareStatement(
        """
        SELECT l.lineIndex, $content FROM line l ${LineContentShape.contentJoin(split)}
        WHERE l.bookId = ?
          AND ($content LIKE '%הדרן%' OR $content LIKE '%סליק%' OR instr($content, 'פרק') BETWEEN 1 AND $OPENING_SCAN_CHARS)
        ORDER BY l.lineIndex
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            while (rs.next()) parseChapterMark(rs.getLong(1), rs.getString(2), chapters)?.let { marks += it }
        }
    }
    return marks
}

/** An opening "פרק" is the line's first word; markup before it may take this many characters. */
private const val OPENING_SCAN_CHARS = 80

/**
 * Reads every eligible commentary and returns those with at least one anchor,
 * in book-id order: a first pass on base links, then a retry of the books it
 * rejected for their base, crediting links from the first pass's commentaries.
 *
 * An unreliable base ([unreliableChaptersBases]) is no base. Its own chapters
 * are worked out in the first pass like a commentary's, from its links, and a
 * third pass credits them to the books still without a base (כתוב שם ←
 * המאור הקטן); its own structure is left as imported.
 */
internal fun readInheritChaptersSnapshots(
    conn: Connection,
    stats: InheritChaptersStats = InheritChaptersStats(),
): List<InheritChaptersSnapshot> {
    val allBases = readChaptersBases(conn)
    val unreliable = unreliableChaptersBases(conn, allBases)
    stats.unreliableBases += unreliable.sorted()
    val bases = allBases - unreliable
    if (bases.isEmpty()) return emptyList()
    val contentSplit = LineContentShape.isSplit(conn)
    val baseIdByTitle = bases.values.groupBy { it.title }
        .filterValues { it.size == 1 }
        .mapValues { it.value.single().bookId }

    val declared = HashMap<Long, MutableList<Long>>()
    conn.prepareStatement("SELECT bookId, baseBookId FROM book_base_text ORDER BY bookId, baseBookId").use { st ->
        st.executeQuery().use { rs ->
            while (rs.next()) declared.getOrPut(rs.getLong(1)) { mutableListOf() } += rs.getLong(2)
        }
    }

    data class Candidate(
        val bookId: Long,
        val title: String,
        val titleBaseId: Long?,
        val declared: List<Long>,
        val links: Map<Long, Int>,
        /** An unreliable base: its chapters only credit the books linking to it. */
        val shadow: Boolean = false,
    )

    val candidates = mutableListOf<Candidate>()
    conn.prepareStatement(
        """
        SELECT b.id, b.title FROM book b
        WHERE NOT EXISTS (SELECT 1 FROM alt_toc_structure s WHERE s.bookId = b.id)
        ORDER BY b.id
        """.trimIndent(),
    ).use { st ->
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val bookId = rs.getLong(1)
                val title = rs.getString(2)
                val titleBaseId = titleChaptersBase(title, baseIdByTitle)
                val declaredIds = declared[bookId].orEmpty()
                if (titleBaseId == null && declaredIds.none { it in bases || it in unreliable }) continue
                candidates += Candidate(bookId, title, titleBaseId, declaredIds, emptyMap())
            }
        }
    }
    for (bookId in unreliable.sorted()) {
        val base = allBases.getValue(bookId)
        candidates += Candidate(bookId, base.title, titleChaptersBase(base.title, baseIdByTitle), declared[bookId].orEmpty(), emptyMap(), shadow = true)
    }
    val withLinks = candidates.map { it.copy(links = readCommentaryLinkCounts(conn, it.bookId)) }

    val groupOfSource = HashMap<Long, String>()
    val baseOfSource = HashMap<Long, Long>()
    for (base in bases.values) {
        groupOfSource[base.bookId] = base.groupKey
        baseOfSource[base.bookId] = base.bookId
    }
    val anchorsOfSource = HashMap<Long, List<ChapterAnchor>>()

    val snapshots = HashMap<Long, InheritChaptersSnapshot>()
    val shadows = HashMap<Long, InheritChaptersSnapshot>()
    val rejectedForBase = mutableListOf<Candidate>()
    val rejectedAgain = mutableListOf<Candidate>()
    for (pass in 1..3) {
        val todo = when (pass) {
            1 -> withLinks
            2 -> rejectedForBase.toList()
            else -> rejectedAgain.toList()
        }
        // Only first-pass books are credited, so later results never feed each other.
        // An unreliable base's chapters only serve a book nothing else gives a base.
        val credited = when (pass) {
            1 -> emptyList()
            2 -> snapshots.values.toList()
            else -> shadows.values.toList()
        }
        for (snapshot in credited) {
            groupOfSource[snapshot.bookId] = bases.getValue(snapshot.baseId).groupKey
            baseOfSource[snapshot.bookId] = snapshot.baseId
            anchorsOfSource[snapshot.bookId] = snapshot.lineAnchors
        }
        for (candidate in todo) {
            fun skip(reason: String) = stats.skip(if (candidate.shadow) "unreliable-base:$reason" else reason, candidate.bookId)
            val choice = chooseChaptersBase(
                candidate.titleBaseId, candidate.declared, candidate.links, groupOfSource, baseOfSource, bases.keys,
            )
            if (choice == null) {
                when {
                    candidate.shadow -> skip("no-consistent-base")
                    pass == 1 -> rejectedForBase += candidate
                    pass == 2 -> rejectedAgain += candidate
                    else -> stats.skip("no-consistent-base", candidate.bookId)
                }
                continue
            }
            val base = bases.getValue(choice.baseId)
            if (candidate.titleBaseId != null && candidate.titleBaseId != choice.baseId) {
                stats.titleDisagreements += "${candidate.bookId} '${candidate.title}' → ${base.title} (${choice.rule})"
            }

            val headings = readChaptersHeadings(conn, candidate.bookId)
            if (hasOwnChapterHeadings(headings, base.chapters)) {
                skip("own-chapter-headings")
                continue
            }
            val chapterByLine = if (choice.sourceIds.isEmpty()) {
                emptyMap()
            } else {
                readChapterByLine(conn, candidate.bookId, choice.sourceIds) { source, lineIndex ->
                    bases[source]?.let { chapterOfBaseLine(it.chapters, lineIndex) }
                        ?: chapterOfAnchoredLine(anchorsOfSource.getValue(source), lineIndex)
                }
            }
            val dafBase = chooseDafBoundaryBase(candidate.titleBaseId, candidate.declared, base, bases)
            val linkPagination = base.chapters.takeIf { chapters ->
                choice.rule == ChaptersBaseRule.LINKS && chapters.all { it.amud != null } &&
                    linksProvePagination(headings, readBaseAmudByLine(conn, candidate.bookId, choice.baseId))
            }
            val marks = readChapterMarks(conn, candidate.bookId, base.chapters, contentSplit)
            val anchors = computeChapterAnchors(
                chapters = base.chapters,
                lineIndices = readBookLineIndex(conn, candidate.bookId).map { it.second },
                headings = headings,
                chapterByLine = chapterByLine,
                dafChapters = dafBase?.chapters,
                linkPagination = linkPagination,
                marks = marks,
            )
            if (anchors.isEmpty()) {
                skip("no-anchor")
                continue
            }
            if (!perekHeadingsAgree(anchors, headings)) {
                skip("perek-mismatch")
                continue
            }
            val pagination = chapterPagination(base.chapters, headings, chapterByLine, dafBase?.chapters, linkPagination)
            val layout = layoutChapters(base.chapters, anchors, headings, pagination, marks)
            if (layout == null) {
                skip("unplaced-leading-dafs")
                continue
            }
            if (candidate.shadow) stats.shadowed++ else stats.take(choice.rule)
            if (pass == 2) stats.recoveredByCredit++
            if (pass == 3) stats.recoveredThroughUnreliableBases++
            (if (candidate.shadow) shadows else snapshots)[candidate.bookId] = InheritChaptersSnapshot(
                bookId = candidate.bookId,
                title = candidate.title,
                baseId = choice.baseId,
                rule = choice.rule,
                anchors = layout.anchors,
                chapterCount = base.chapters.size,
                dafs = layout.dafs,
                credited = pass == 2,
                dafBaseId = dafBase?.bookId,
                lineAnchors = anchors,
            )
        }
    }
    return snapshots.values.sortedBy { it.bookId }
}

// INDEXED BY as in the Seifim synthesizer: without ANALYZE stats the planner
// picks a connection-type index and scans every COMMENTARY link per book.
private fun readCommentaryLinkCounts(conn: Connection, bookId: Long): Map<Long, Int> {
    val counts = LinkedHashMap<Long, Int>()
    conn.prepareStatement(
        """
        SELECT k.sourceBookId, COUNT(*)
        FROM link k INDEXED BY idx_link_target_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        WHERE k.targetBookId = ? AND k.sourceBookId <> k.targetBookId AND c.name = 'COMMENTARY'
        GROUP BY k.sourceBookId
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs -> while (rs.next()) counts[rs.getLong(1)] = rs.getInt(2) }
    }
    return counts
}

/** Commentary lineIndex → lowest chapter number any of its links from [sourceIds] carries. */
private fun readChapterByLine(
    conn: Connection,
    bookId: Long,
    sourceIds: Set<Long>,
    chapterOf: (source: Long, sourceLineIndex: Long) -> Int,
): Map<Long, Int> {
    val byLine = HashMap<Long, Int>()
    conn.prepareStatement(
        """
        SELECT ml.lineIndex, k.sourceBookId, bl.lineIndex
        FROM link k INDEXED BY idx_link_target_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        JOIN line ml ON ml.id = k.targetLineId
        JOIN line bl ON bl.id = k.sourceLineId
        WHERE k.targetBookId = ? AND k.sourceBookId IN (${sourceIds.joinToString(",") { "?" }})
          AND c.name = 'COMMENTARY'
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        sourceIds.sorted().forEachIndexed { i, id -> st.setLong(i + 2, id) }
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val chapter = chapterOf(rs.getLong(2), rs.getLong(3))
                if (chapter < 0) continue
                byLine.merge(rs.getLong(1), chapter, ::minOf)
            }
        }
    }
    return byLine
}

/** Commentary lineIndex → the amuds of the [baseId] lines linked to it (heRefs naming one). */
private fun readBaseAmudByLine(conn: Connection, bookId: Long, baseId: Long): Map<Long, List<Int>> {
    val byLine = HashMap<Long, MutableList<Int>>()
    conn.prepareStatement(
        """
        SELECT ml.lineIndex, bl.heRef
        FROM link k INDEXED BY idx_link_target_book
        JOIN connection_type c ON c.id = k.connectionTypeId
        JOIN line ml ON ml.id = k.targetLineId
        JOIN line bl ON bl.id = k.sourceLineId
        WHERE k.targetBookId = ? AND k.sourceBookId = ? AND c.name = 'COMMENTARY'
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.setLong(2, baseId)
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val amud = parseHeRefAmud(rs.getString(2)) ?: continue
                byLine.getOrPut(rs.getLong(1)) { mutableListOf() } += amud
            }
        }
    }
    return byLine
}

private fun readChaptersHeadings(conn: Connection, bookId: Long): List<ChaptersHeading> {
    val headings = mutableListOf<ChaptersHeading>()
    conn.prepareStatement(
        """
        SELECT l.lineIndex, t.text, e.id, e.parentId
        FROM tocEntry e
        JOIN tocText t ON t.id = e.textId
        JOIN line l ON l.id = e.lineId
        WHERE e.bookId = ?
        ORDER BY l.lineIndex, e.level, e.id
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val parentId = rs.getLong(4).let { if (rs.wasNull()) null else it }
                headings += ChaptersHeading(rs.getLong(1), rs.getString(2), rs.getLong(3), parentId)
            }
        }
    }
    return headings
}

private fun readBookLineIndex(conn: Connection, bookId: Long): List<Pair<Long, Long>> {
    val lines = mutableListOf<Pair<Long, Long>>()
    conn.prepareStatement("SELECT id, lineIndex FROM line WHERE bookId = ? ORDER BY lineIndex").use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs -> while (rs.next()) lines += rs.getLong(1) to rs.getLong(2) }
    }
    return lines
}

/** Atomically writes every snapshot's structure; see [synthesizeSeifimAltTocs]. */
internal fun synthesizeInheritedChapters(
    conn: Connection,
    snapshots: List<InheritChaptersSnapshot>,
    stableIds: AttachedBuildStateIds? = null,
): InheritChaptersResult {
    check(conn.autoCommit) { "synthesizeInheritedChapters requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        var entries = 0
        for (snapshot in snapshots) entries += writeInheritedChapters(conn, snapshot, stableIds)
        conn.prepareStatement(
            """
            UPDATE book SET hasAltStructures = 1
            WHERE hasAltStructures = 0
              AND EXISTS (SELECT 1 FROM alt_toc_structure s WHERE s.bookId = book.id AND s.key = ? AND s.title = ?)
            """.trimIndent(),
        ).use { st ->
            st.setString(1, CHAPTERS_STRUCTURE_KEY)
            st.setString(2, INHERITED_CHAPTERS_TITLE_EN)
            st.executeUpdate()
        }
        conn.commit()
        InheritChaptersResult(snapshots.size, entries)
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

private fun writeInheritedChapters(
    conn: Connection,
    snapshot: InheritChaptersSnapshot,
    stableIds: AttachedBuildStateIds?,
): Int {
    val structureId = stableIds?.altTocStructureId(snapshot.bookId, CHAPTERS_STRUCTURE_KEY)
        ?: (maxId(conn, "alt_toc_structure") + 1)
    conn.prepareStatement(
        "INSERT INTO alt_toc_structure (id, bookId, key, title, heTitle) VALUES (?, ?, ?, ?, ?)",
    ).use { st ->
        st.setLong(1, structureId)
        st.setLong(2, snapshot.bookId)
        st.setString(3, CHAPTERS_STRUCTURE_KEY)
        st.setString(4, INHERITED_CHAPTERS_TITLE_EN)
        st.setString(5, SefariaAltStructureNames.forKey(CHAPTERS_STRUCTURE_KEY) ?: snapshot.title)
        st.executeUpdate()
    }

    data class Pending(
        val id: Long,
        val parentId: Long?,
        val text: String,
        val level: Int,
        val lineIndex: Long,
        val hasChildren: Boolean,
        val isLastChild: Boolean,
    )

    var nextEntryId = maxId(conn, "alt_toc_entry")
    fun entryId(path: String): Long = stableIds?.altTocEntryId(structureId, path) ?: ++nextEntryId

    // Every base chapter's id first, in chapter order — see the file KDoc. A
    // chapter's later runs come after them, in chapter and line order. These
    // are the ids drawn; [inLineOrder] decides which entry of a sibling group
    // gets which of them.
    val chapterIds = (0 until snapshot.chapterCount).map { entryId(altTocChildPath("", it + 1)) }
    var laterRuns = 0
    val drawn = mutableListOf<Pending>()
    fun addDafs(nodes: List<DafNode>, parentId: Long, parentPath: String, level: Int) {
        nodes.forEachIndexed { i, node ->
            val path = altTocChildPath(parentPath, i + 1)
            val id = entryId(path)
            drawn += Pending(id, parentId, node.text, level, node.lineIndex, node.children.isNotEmpty(), i == nodes.lastIndex)
            addDafs(node.children, id, path, level + 1)
        }
    }
    snapshot.anchors.forEachIndexed { i, anchor ->
        val path = altTocChildPath("", if (anchor.occurrence == 0) anchor.index + 1 else snapshot.chapterCount + ++laterRuns)
        val id = if (anchor.occurrence == 0) chapterIds[anchor.index] else entryId(path)
        val dafs = snapshot.dafs[i]
        drawn += Pending(id, null, anchor.chapter.text, 0, anchor.lineIndex, dafs.isNotEmpty(), i == snapshot.anchors.lastIndex)
        addDafs(dafs, id, path, 1)
    }

    /**
     * The reader lists siblings by id, so each sibling group hands the ids it
     * drew out again, ascending, in the line order of its entries (a shared
     * line keeps the written order), and flags the last of them by line.
     * Chapters are written in chapter order and dafs in line order, so this
     * only moves ids where chapter order is not the line order: a chapter's
     * later run, or an out-of-order chapter opened above the first one (and
     * dafs only past a cyclic TOC's break, [partitionDafs]). Everywhere else
     * every id stays the one drawn for its entry's path, and the rows are those
     * written before.
     */
    fun inLineOrder(entries: List<Pending>): List<Pending> {
        val renamed = HashMap<Long, Long>(entries.size)
        val last = HashSet<Long>()
        for (siblings in entries.groupBy { it.parentId }.values) {
            val ids = siblings.map { it.id }.sorted()
            val byLine = siblings.sortedBy { it.lineIndex }
            byLine.forEachIndexed { rank, entry -> renamed[entry.id] = ids[rank] }
            last += byLine.last().id
        }
        return entries.map { entry ->
            entry.copy(
                id = renamed.getValue(entry.id),
                parentId = entry.parentId?.let(renamed::getValue),
                isLastChild = entry.id in last,
            )
        }
    }
    // Still in the written order: parents before children, and the line
    // owners below break a shared line's tie the same way.
    val pending = inLineOrder(drawn)

    // Lines are re-read here so the snapshots of a whole library stay small.
    val lines = readBookLineIndex(conn, snapshot.bookId)
    val lineIdByIndex = lines.associate { it.second to it.first }
    conn.prepareStatement(
        """
        INSERT INTO alt_toc_entry
            (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent(),
    ).use { st ->
        for (entry in pending) {
            st.setLong(1, entry.id)
            st.setLong(2, structureId)
            if (entry.parentId != null) st.setLong(3, entry.parentId) else st.setNull(3, java.sql.Types.INTEGER)
            st.setLong(4, tocTextIdFor(conn, entry.text, stableIds))
            st.setInt(5, entry.level)
            st.setLong(6, lineIdByIndex.getValue(entry.lineIndex))
            st.setInt(7, if (entry.isLastChild) 1 else 0)
            st.setInt(8, if (entry.hasChildren) 1 else 0)
            st.addBatch()
        }
        st.executeBatch()
    }

    // Nearest preceding entry owns a line; on a shared line the deepest wins.
    val owners = pending.sortedWith(compareBy({ it.lineIndex }, { it.level }))
    conn.prepareStatement(
        "INSERT OR REPLACE INTO line_alt_toc (lineId, structureId, altTocEntryId) VALUES (?, ?, ?)",
    ).use { st ->
        var next = 0
        var owner: Long? = null
        for ((lineId, lineIndex) in lines) {
            while (next < owners.size && owners[next].lineIndex <= lineIndex) owner = owners[next++].id
            val entryId = owner ?: continue
            st.setLong(1, lineId)
            st.setLong(2, structureId)
            st.setLong(3, entryId)
            st.addBatch()
        }
        st.executeBatch()
    }
    return pending.size
}

private fun maxId(conn: Connection, table: String): Long =
    conn.prepareStatement("SELECT COALESCE(MAX(id), 0) FROM $table").use { st ->
        st.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
    }

private fun tocTextIdFor(conn: Connection, text: String, stableIds: AttachedBuildStateIds?): Long {
    conn.prepareStatement("SELECT id FROM tocText WHERE text = ?").use { st ->
        st.setString(1, text)
        st.executeQuery().use { rs -> if (rs.next()) return rs.getLong(1) }
    }
    val id = stableIds?.tocTextId(text) ?: (maxId(conn, "tocText") + 1)
    conn.prepareStatement("INSERT INTO tocText (id, text) VALUES (?, ?)").use { st ->
        st.setLong(1, id)
        st.setString(2, text)
        st.executeUpdate()
    }
    return id
}

internal const val CHAPTERS_STRUCTURE_KEY = "Chapters"

/** Title of every structure this stage writes; also how a rerun tells them from real bases. */
internal const val INHERITED_CHAPTERS_TITLE_EN = "Chapters (inherited)"
