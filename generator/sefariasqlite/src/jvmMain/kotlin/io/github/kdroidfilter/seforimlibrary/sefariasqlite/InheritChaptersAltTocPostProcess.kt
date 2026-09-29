package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
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
 * 2. no COMMENTARY links: the single declared Chapters base, else
 * 3. the title "X על <base>" (optionally "על מסכת <base>").
 *
 * Anchors, per base chapter (start line S_k, start amud from the base line's
 * heRef "ברכות, יג., טז"), by whichever method places more chapters (links on
 * a tie — sparse links place only the chapters they touch):
 * - links: each linked line maps to the lowest chapter it is linked to;
 *   strays are dropped by keeping the longest non-decreasing run of
 *   chapters. A chapter opens at its first line in that run, moved back onto
 *   an earlier daf heading in the gap that already reaches the chapter's amud,
 *   or onto the non-daf heading lines right above it.
 * - daf headings: the first one ("דף יג.", "יג:", "דף יג ע"א", "דף יג עמוד
 *   ב", "דף יג") whose amud reaches the chapter start. A chapter starting
 *   mid-amud therefore opens at that amud's heading, taking the previous
 *   chapter's tail on the same amud with it.
 * Chapters with no material are dropped.
 *
 * Under each chapter hang the book's own daf headings (text copied verbatim)
 * whose line is in [its anchor, the next chapter's anchor) — the first chapter
 * also takes the dafs above its anchor — with their main-TOC sub-headings below
 * them: the app's find-ref resolves "<book> דף ה:" through the alt-TOC as soon
 * as a book has one, so every daf must be there. A daf heading on a chapter's
 * first amud but above its anchor stays in the previous chapter, as its lines do.
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
 * siblings by id). Daf entries are keyed by ordinal within their chapter: a
 * heading added to the commentary's TOC renumbers only that chapter's dafs.
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

internal data class ChaptersBase(val bookId: Long, val title: String, val chapters: List<BaseChapter>) {
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

/** [index] is the chapter's position in the base's chapter list. */
internal data class ChapterAnchor(val chapter: BaseChapter, val lineIndex: Long, val index: Int = -1)

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
)

internal data class InheritChaptersResult(val structures: Int, val entries: Int)

internal class InheritChaptersStats {
    val byRule = sortedMapOf<String, Int>()
    val skipped = sortedMapOf<String, Int>()
    val titleDisagreements = mutableListOf<String>()
    var recoveredByCredit = 0
    val skippedBooks = sortedMapOf<String, MutableList<Long>>()

    fun skip(reason: String, bookId: Long) {
        skipped.merge(reason, 1, Int::plus)
        skippedBooks.getOrPut(reason) { mutableListOf() } += bookId
    }
    fun take(rule: ChaptersBaseRule) { byRule.merge(rule.name, 1, Int::plus) }
    fun summary(): String =
        "selected=$byRule recoveredByCredit=$recoveredByCredit skipped=$skipped " +
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

private fun String.normalizedHeading(): String = trimStart('﻿').trim()

/**
 * The amud span a commentary heading names ("דף יג.", "יג:", "דף יג ע"א",
 * "דף יג עמוד ב", or "דף יג" for both amudim), or null for any other heading.
 */
internal fun parseDafHeading(text: String): AmudSpan? {
    val heading = text.normalizedHeading()
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
 * linked to.
 */
internal fun computeChapterAnchors(
    chapters: List<BaseChapter>,
    lineIndices: List<Long>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
): List<ChapterAnchor> {
    if (chapters.isEmpty()) return emptyList()
    val byLinks = anchorsFromLinks(chapters, lineIndices, headings, chapterByLine)
    val byDaf = anchorsFromDafHeadings(chapters, headings)
    // Sparse links place only the chapters they touch; full daf headings then place more.
    return if (byDaf.size > byLinks.size) byDaf else byLinks
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

/** Chapter number of a commentary line from its own anchors, -1 before the first. */
private fun chapterOfAnchoredLine(anchors: List<ChapterAnchor>, lineIndex: Long): Int =
    anchors.lastOrNull { it.lineIndex <= lineIndex }?.index ?: -1

private fun anchorsFromLinks(
    chapters: List<BaseChapter>,
    lineIndices: List<Long>,
    headings: List<ChaptersHeading>,
    chapterByLine: Map<Long, Int>,
): List<ChapterAnchor> {
    val linked = chapterByLine.entries
        .map { it.key to it.value }
        .filter { it.second in chapters.indices }
        .sortedBy { it.first }
    if (linked.isEmpty()) return emptyList()
    val run = longestNonDecreasingRun(linked.map { it.second }).map { linked[it] }

    val headingByLine = headings.groupBy { it.lineIndex }
    val dafHeadings = headings.mapNotNull { h -> parseDafHeading(h.text)?.let { h.lineIndex to it } }
        .sortedBy { it.first }
    val positionOfLine = HashMap<Long, Int>(lineIndices.size * 2).apply {
        lineIndices.forEachIndexed { position, lineIndex -> put(lineIndex, position) }
    }

    val anchors = mutableListOf<ChapterAnchor>()
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
        val dafAnchor = chapter.amud?.let { start ->
            dafHeadings.firstOrNull { (line, span) -> line > previousLine && line < firstLine && span.end >= start }
        }
        if (dafAnchor != null) {
            anchor = dafAnchor.first
        } else {
            // Heading lines directly above the first line open the same material.
            var position = positionOfLine[firstLine] ?: -1
            while (position > 0) {
                val candidate = lineIndices[position - 1]
                if (candidate <= previousLine) break
                val lineHeadings = headingByLine[candidate] ?: break
                if (lineHeadings.any { parseDafHeading(it.text) != null }) break
                anchor = candidate
                position--
            }
        }
        anchors += ChapterAnchor(chapter, anchor, k)
        previousLine = lastLine
    }
    return anchors
}

private fun anchorsFromDafHeadings(
    chapters: List<BaseChapter>,
    headings: List<ChaptersHeading>,
): List<ChapterAnchor> {
    if (chapters.any { it.amud == null }) return emptyList()
    val parsed = headings.sortedBy { it.lineIndex }
        .mapNotNull { h -> parseDafHeading(h.text)?.let { h.lineIndex to it } }
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
        anchors += ChapterAnchor(chapter, line, k)
        next++
    }
    return anchors
}

/** A heading the app treats as a daf: parsed here, or opening with the word "דף". */
private fun isDafEntryHeading(text: String): Boolean =
    parseDafHeading(text) != null || DAF_WORD_HEADING.containsMatchIn(text.normalizedHeading())

private val DAF_WORD_HEADING = Regex("""^דף(?=\s)""")

/**
 * The daf entries of each anchor: every top-most daf heading whose line is in
 * [anchor, next anchor), with its main-TOC sub-headings that stay in range.
 */
internal fun buildDafChildren(anchors: List<ChapterAnchor>, headings: List<ChaptersHeading>): List<List<DafNode>> {
    val byId = headings.filter { it.id != 0L }.associateBy { it.id }
    val childrenOf = headings.filter { it.parentId != null }.groupBy { it.parentId }
    val daf = headings.filter { isDafEntryHeading(it.text) }
    val dafIds = daf.map { it.id }.toHashSet()

    fun hasDafAncestor(h: ChaptersHeading): Boolean {
        var parent = h.parentId?.let(byId::get)
        val seen = HashSet<Long>()
        while (parent != null && seen.add(parent.id)) {
            if (parent.id in dafIds) return true
            parent = parent.parentId?.let(byId::get)
        }
        return false
    }

    fun subtree(h: ChaptersHeading, end: Long): DafNode = DafNode(
        text = h.text,
        lineIndex = h.lineIndex,
        children = childrenOf[h.id].orEmpty()
            .filter { it.lineIndex >= h.lineIndex && it.lineIndex < end }
            .sortedWith(compareBy({ it.lineIndex }, { it.id }))
            .map { subtree(it, end) },
    )

    return anchors.mapIndexed { i, anchor ->
        val start = if (i == 0) Long.MIN_VALUE else anchor.lineIndex
        val end = anchors.getOrNull(i + 1)?.lineIndex ?: Long.MAX_VALUE
        daf.filter { it.lineIndex >= start && it.lineIndex < end && !hasDafAncestor(it) }
            .sortedWith(compareBy({ it.lineIndex }, { it.id }))
            .map { subtree(it, end) }
    }
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
    data class Root(val bookId: Long, val bookTitle: String, val text: String, val lineIndex: Long)

    val roots = mutableListOf<Root>()
    conn.prepareStatement(
        """
        SELECT s.bookId, b.title, t.text, l.lineIndex
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
            while (rs.next()) roots += Root(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4))
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
            ChaptersBase(bookId, bookRoots.first().bookTitle, chapters)
        }
    }
}

/**
 * Reads every eligible commentary and returns those with at least one anchor,
 * in book-id order: a first pass on base links, then a retry of the books it
 * rejected for their base, crediting links from the first pass's commentaries.
 */
internal fun readInheritChaptersSnapshots(
    conn: Connection,
    stats: InheritChaptersStats = InheritChaptersStats(),
): List<InheritChaptersSnapshot> {
    val bases = readChaptersBases(conn)
    if (bases.isEmpty()) return emptyList()
    val baseIdByTitle = bases.values.groupBy { it.title }
        .filterValues { it.size == 1 }
        .mapValues { it.value.single().bookId }

    val declared = HashMap<Long, MutableList<Long>>()
    conn.prepareStatement("SELECT bookId, baseBookId FROM book_base_text ORDER BY bookId, baseBookId").use { st ->
        st.executeQuery().use { rs ->
            while (rs.next()) declared.getOrPut(rs.getLong(1)) { mutableListOf() } += rs.getLong(2)
        }
    }

    data class Candidate(val bookId: Long, val title: String, val titleBaseId: Long?, val declared: List<Long>, val links: Map<Long, Int>)

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
                if (titleBaseId == null && declaredIds.none { it in bases }) continue
                candidates += Candidate(bookId, title, titleBaseId, declaredIds, emptyMap())
            }
        }
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
    val rejectedForBase = mutableListOf<Candidate>()
    for (pass in 1..2) {
        val todo = if (pass == 1) withLinks else rejectedForBase.toList()
        if (pass == 2) {
            // Only first-pass books are credited, so pass-2 results never feed each other.
            for (snapshot in snapshots.values) {
                groupOfSource[snapshot.bookId] = bases.getValue(snapshot.baseId).groupKey
                baseOfSource[snapshot.bookId] = snapshot.baseId
                anchorsOfSource[snapshot.bookId] = snapshot.anchors
            }
        }
        for (candidate in todo) {
            val choice = chooseChaptersBase(
                candidate.titleBaseId, candidate.declared, candidate.links, groupOfSource, baseOfSource, bases.keys,
            )
            if (choice == null) {
                if (pass == 1) rejectedForBase += candidate else stats.skip("no-consistent-base", candidate.bookId)
                continue
            }
            val base = bases.getValue(choice.baseId)
            if (candidate.titleBaseId != null && candidate.titleBaseId != choice.baseId) {
                stats.titleDisagreements += "${candidate.bookId} '${candidate.title}' → ${base.title} (${choice.rule})"
            }

            val headings = readChaptersHeadings(conn, candidate.bookId)
            if (hasOwnChapterHeadings(headings, base.chapters)) {
                stats.skip("own-chapter-headings", candidate.bookId)
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
            val anchors = computeChapterAnchors(
                chapters = base.chapters,
                lineIndices = readBookLineIndex(conn, candidate.bookId).map { it.second },
                headings = headings,
                chapterByLine = chapterByLine,
            )
            if (anchors.isEmpty()) {
                stats.skip("no-anchor", candidate.bookId)
                continue
            }
            if (!perekHeadingsAgree(anchors, headings)) {
                stats.skip("perek-mismatch", candidate.bookId)
                continue
            }
            stats.take(choice.rule)
            if (pass == 2) stats.recoveredByCredit++
            snapshots[candidate.bookId] = InheritChaptersSnapshot(
                bookId = candidate.bookId,
                title = candidate.title,
                baseId = choice.baseId,
                rule = choice.rule,
                anchors = anchors,
                chapterCount = base.chapters.size,
                dafs = buildDafChildren(anchors, headings),
                credited = pass == 2,
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
        st.setString(5, snapshot.title)
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

    // Every base chapter's id first, in chapter order — see the file KDoc.
    val chapterIds = (0 until snapshot.chapterCount).map { entryId(altTocChildPath("", it + 1)) }
    val pending = mutableListOf<Pending>()
    fun addDafs(nodes: List<DafNode>, parentId: Long, parentPath: String, level: Int) {
        nodes.forEachIndexed { i, node ->
            val path = altTocChildPath(parentPath, i + 1)
            val id = entryId(path)
            pending += Pending(id, parentId, node.text, level, node.lineIndex, node.children.isNotEmpty(), i == nodes.lastIndex)
            addDafs(node.children, id, path, level + 1)
        }
    }
    snapshot.anchors.forEachIndexed { i, anchor ->
        val id = chapterIds[anchor.index]
        val dafs = snapshot.dafs[i]
        pending += Pending(id, null, anchor.chapter.text, 0, anchor.lineIndex, dafs.isNotEmpty(), i == snapshot.anchors.lastIndex)
        addDafs(dafs, id, altTocChildPath("", anchor.index + 1), 1)
    }

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
