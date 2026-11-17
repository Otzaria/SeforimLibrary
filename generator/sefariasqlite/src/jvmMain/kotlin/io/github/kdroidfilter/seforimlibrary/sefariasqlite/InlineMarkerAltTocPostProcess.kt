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
 * Alt-TOC from markers printed inside the text — `[רמז תסה]` in the middle of a
 * paragraph — for the books listed in ForDB [INLINE_MARKER_ALT_TOC_FILE].
 *
 * Every marker becomes an entry anchored at its line (the app scrolls to the
 * bracketed marker within it), grouped under the main-TOC headings above it down
 * to the deepest one that starts with the rule's `groupBy` ("פרק"); a book's sole
 * main-TOC root (its own title) is not repeated. The text itself is not touched.
 *
 * This stage owns the rule's structure key for the listed books: it replaces a
 * structure of that key from any source (Sefaria's ten-remez ranges included).
 * Ids come from the build-state allocator keyed on ancestor paths.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:buildInlineMarkerAltToc -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("InlineMarkerAltToc")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    try {
        // An archive published before the file existed has no rules.
        val rules = downloadOptionalForDbFile(INLINE_MARKER_ALT_TOC_FILE, logger)
            ?.let(::parseInlineMarkerRules).orEmpty()
        if (rules.isEmpty()) {
            logger.i { "No $INLINE_MARKER_ALT_TOC_FILE rules" }
            return
        }
        val buildStatePath = resolveSeifimBuildStatePath(dbPath)
        check(Files.exists(buildStatePath)) {
            "build_state not found at $buildStatePath; run the DB generation pipeline first"
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
                st.setString(1, buildStatePath.toAbsolutePath().toString())
                st.execute()
            }
            val written = writeInlineMarkerAltTocs(conn, rules, AttachedBuildStateIds(conn))
            for ((title, count) in written) logger.i { "  $title: $count entries" }
        }
        BuildStateVerifier.verifyFreshSnapshot(buildStatePath, dbPath, emptyMap())
    } catch (e: Exception) {
        logger.e(e) { "Failed to build inline-marker alt-TOCs; aborting" }
        exitProcess(1)
    }
}

internal const val INLINE_MARKER_ALT_TOC_FILE = "inline_marker_alt_toc.csv"
private const val INLINE_MARKER_HEADER = "bookName,marker,structureKey,structureHeTitle,groupBy"

/** Keys the app gives other meanings (label or heading injection, sidebar order). */
private val RESERVED_STRUCTURE_KEYS = setOf("Simanim", "Seifim", "Topic", "Parasha", "SimanNames")
private val STRUCTURE_KEY_REGEX = Regex("[A-Za-z][A-Za-z0-9]*")
private val MARKER_WORD_REGEX = Regex("[א-ת]+")

internal data class InlineMarkerRule(
    val bookTitle: String,
    val marker: String,
    val structureKey: String,
    val structureHeTitle: String,
    /** Heading prefix to group by; blank for a flat list. */
    val groupBy: String,
)

/** Fails on the first malformed row: the build must not silently drop a rule. */
internal fun parseInlineMarkerRules(lines: List<String>): List<InlineMarkerRule> {
    val rows = forDbText(lines).lines().map { it.trimEnd('\r') }.filter { it.isNotBlank() }
    if (rows.isEmpty()) return emptyList()
    require(rows.first().trim() == INLINE_MARKER_HEADER) {
        "$INLINE_MARKER_ALT_TOC_FILE: header must be '$INLINE_MARKER_HEADER'"
    }
    val rules = rows.drop(1).mapIndexed { i, row ->
        val cells = row.split(',').map { it.trim() }
        val where = "$INLINE_MARKER_ALT_TOC_FILE row ${i + 2}"
        require(cells.size == 5) { "$where: expected 5 cells, got ${cells.size}" }
        val (book, marker, key, heTitle) = cells
        require(book.isNotEmpty() && heTitle.isNotEmpty()) { "$where: bookName and structureHeTitle are required" }
        require(MARKER_WORD_REGEX.matches(marker)) { "$where: marker must be one Hebrew word, got '$marker'" }
        require(STRUCTURE_KEY_REGEX.matches(key) && key !in RESERVED_STRUCTURE_KEYS) {
            "$where: structureKey '$key' must be an English identifier outside $RESERVED_STRUCTURE_KEYS"
        }
        InlineMarkerRule(book, marker, key, heTitle, cells[4])
    }
    val duplicate = rules.groupBy { it.bookTitle }.filterValues { it.size > 1 }.keys
    require(duplicate.isEmpty()) { "$INLINE_MARKER_ALT_TOC_FILE: more than one rule for $duplicate" }
    return rules
}

internal data class MarkerTocRow(val id: Long, val parentId: Long?, val text: String, val lineId: Long?, val lineIndex: Int?)
internal data class MarkerLineRow(val id: Long, val index: Int, val content: String, val tocEntryId: Long?)

internal data class PlannedMarkerEntry(
    val path: String,
    val parentPath: String?,
    val level: Int,
    val text: String,
    val lineId: Long,
    val lineIndex: Int,
    /** Marker entries own the lines from theirs on; group entries own none. */
    val isMarker: Boolean,
)

private fun markerRegex(marker: String) = Regex("""\[${Regex.escape(marker)} ([א-ת"'׳״]{1,6})]""")

/** Groups first, then each marker under them, in reading order. */
internal fun planInlineMarkerEntries(
    rule: InlineMarkerRule,
    lines: List<MarkerLineRow>,
    toc: List<MarkerTocRow>,
): List<PlannedMarkerEntry> {
    val tocById = toc.associateBy { it.id }
    val soleRoot = toc.filter { it.parentId == null }.singleOrNull()?.id
    fun groupChain(tocEntryId: Long?): List<MarkerTocRow> {
        if (rule.groupBy.isEmpty()) return emptyList()
        val chain = generateSequence(tocEntryId?.let(tocById::get)) { it.parentId?.let(tocById::get) }
            .toList().asReversed().filter { it.id != soleRoot }
        val deepest = chain.indexOfLast { it.text.trimHeadingStart().startsWith(rule.groupBy) }
        return if (deepest < 0) emptyList() else chain.subList(0, deepest + 1)
    }

    val regex = markerRegex(rule.marker)
    val planned = mutableListOf<PlannedMarkerEntry>()
    val pathByGroup = HashMap<Long, String>()
    val ordinalByParent = HashMap<String, Int>()
    fun childPath(parentPath: String): String {
        val ordinal = (ordinalByParent[parentPath] ?: 0) + 1
        ordinalByParent[parentPath] = ordinal
        return altTocChildPath(parentPath, ordinal)
    }
    for (line in lines) {
        for (match in regex.findAll(line.content)) {
            var parentPath = ""
            val groups = groupChain(line.tocEntryId)
            groups.forEachIndexed { depth, group ->
                parentPath = pathByGroup.getOrPut(group.id) {
                    val path = childPath(parentPath)
                    planned += PlannedMarkerEntry(
                        path, parentPath.ifEmpty { null }, depth, group.text.trimHeadingStart().trim(),
                        group.lineId ?: line.id, group.lineIndex ?: line.index, isMarker = false,
                    )
                    path
                }
            }
            planned += PlannedMarkerEntry(
                childPath(parentPath), parentPath.ifEmpty { null }, groups.size,
                "${rule.marker} ${match.groupValues[1]}", line.id, line.index, isMarker = true,
            )
        }
    }
    return planned
}

/** Atomically replaces each rule's structure; returns entry counts by book title. */
internal fun writeInlineMarkerAltTocs(
    conn: Connection,
    rules: List<InlineMarkerRule>,
    stableIds: AttachedBuildStateIds? = null,
): Map<String, Int> {
    check(conn.autoCommit) { "writeInlineMarkerAltTocs requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        val written = rules.associate { rule ->
            val bookId = conn.prepareStatement("SELECT id FROM book WHERE title = ?").use { st ->
                st.setString(1, rule.bookTitle)
                st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else null }
            } ?: error("$INLINE_MARKER_ALT_TOC_FILE: no book titled '${rule.bookTitle}'")
            val lines = readMarkerLines(conn, bookId)
            val planned = planInlineMarkerEntries(rule, lines, readMarkerToc(conn, bookId))
            check(planned.any { it.isMarker }) { "$INLINE_MARKER_ALT_TOC_FILE: no '[${rule.marker} …]' marker in '${rule.bookTitle}'" }
            rule.bookTitle to writeStructure(conn, bookId, rule, planned, lines, stableIds)
        }
        conn.commit()
        written
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

private fun readMarkerLines(conn: Connection, bookId: Long): List<MarkerLineRow> =
    conn.prepareStatement(
        """
        SELECT l.id, l.lineIndex, l.content, lt.tocEntryId
        FROM line l LEFT JOIN line_toc lt ON lt.lineId = l.id
        WHERE l.bookId = ? ORDER BY l.lineIndex
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            buildList {
                while (rs.next()) {
                    add(MarkerLineRow(rs.getLong(1), rs.getInt(2), rs.getString(3), rs.getLong(4).takeUnless { rs.wasNull() }))
                }
            }
        }
    }

private fun readMarkerToc(conn: Connection, bookId: Long): List<MarkerTocRow> =
    conn.prepareStatement(
        """
        SELECT t.id, t.parentId, tt.text, t.lineId, l.lineIndex
        FROM tocEntry t JOIN tocText tt ON tt.id = t.textId LEFT JOIN line l ON l.id = t.lineId
        WHERE t.bookId = ? ORDER BY t.id
        """.trimIndent(),
    ).use { st ->
        st.setLong(1, bookId)
        st.executeQuery().use { rs ->
            buildList {
                while (rs.next()) {
                    val parentId = rs.getLong(2).takeUnless { rs.wasNull() }
                    val lineId = rs.getLong(4).takeUnless { rs.wasNull() }
                    val lineIndex = rs.getInt(5).takeUnless { rs.wasNull() }
                    add(MarkerTocRow(rs.getLong(1), parentId, rs.getString(3), lineId, lineIndex))
                }
            }
        }
    }

private fun writeStructure(
    conn: Connection,
    bookId: Long,
    rule: InlineMarkerRule,
    planned: List<PlannedMarkerEntry>,
    lines: List<MarkerLineRow>,
    stableIds: AttachedBuildStateIds?,
): Int {
    for (sql in listOf(
        "DELETE FROM line_alt_toc WHERE structureId IN (SELECT id FROM alt_toc_structure WHERE bookId = ? AND key = ?)",
        "DELETE FROM alt_toc_entry WHERE structureId IN (SELECT id FROM alt_toc_structure WHERE bookId = ? AND key = ?)",
        "DELETE FROM alt_toc_structure WHERE bookId = ? AND key = ?",
    )) {
        conn.prepareStatement(sql).use { st ->
            st.setLong(1, bookId)
            st.setString(2, rule.structureKey)
            st.executeUpdate()
        }
    }
    val structureId = stableIds?.altTocStructureId(bookId, rule.structureKey)
        ?: (queryMaxId(conn, "alt_toc_structure") + 1)
    conn.prepareStatement(
        "INSERT INTO alt_toc_structure (id, bookId, key, title, heTitle) VALUES (?, ?, ?, ?, ?)",
    ).use { st ->
        st.setLong(1, structureId)
        st.setLong(2, bookId)
        st.setString(3, rule.structureKey)
        st.setString(4, rule.structureKey)
        st.setString(5, rule.structureHeTitle)
        st.executeUpdate()
    }

    var nextEntryId = queryMaxId(conn, "alt_toc_entry")
    val idByPath = planned.associate { it.path to (stableIds?.altTocEntryId(structureId, it.path) ?: ++nextEntryId) }
    val parents = planned.mapNotNull { it.parentPath }.toSet()
    val lastByParent = planned.groupBy { it.parentPath }.mapValues { (_, siblings) -> siblings.last().path }
    conn.prepareStatement(
        """
        INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent(),
    ).use { st ->
        for (entry in planned) {
            st.setLong(1, idByPath.getValue(entry.path))
            st.setLong(2, structureId)
            entry.parentPath?.let { st.setLong(3, idByPath.getValue(it)) } ?: st.setNull(3, java.sql.Types.INTEGER)
            st.setLong(4, tocTextId(conn, entry.text, stableIds))
            st.setInt(5, entry.level)
            st.setLong(6, entry.lineId)
            st.setInt(7, if (lastByParent[entry.parentPath] == entry.path) 1 else 0)
            st.setInt(8, if (entry.path in parents) 1 else 0)
            st.addBatch()
        }
        st.executeBatch()
    }

    // Each line belongs to the last marker at or before it; one line with several
    // markers maps to its last (the app keeps a clicked earlier one selected).
    val ownerByLineIndex = planned.filter { it.isMarker }.associate { it.lineIndex to idByPath.getValue(it.path) }
    conn.prepareStatement("INSERT INTO line_alt_toc (lineId, structureId, altTocEntryId) VALUES (?, ?, ?)").use { st ->
        var owner: Long? = null
        for (line in lines) {
            owner = ownerByLineIndex[line.index] ?: owner
            st.setLong(1, line.id)
            st.setLong(2, structureId)
            st.setLong(3, owner ?: continue)
            st.addBatch()
        }
        st.executeBatch()
    }
    conn.prepareStatement("UPDATE book SET hasAltStructures = 1 WHERE id = ?").use { st ->
        st.setLong(1, bookId)
        st.executeUpdate()
    }
    return planned.size
}
