package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Synthesizes a **"Seifim"** alternate-TOC for nosei-kelim on the Shulchan
 * Aruch (Mishnah Berurah, Shach, Taz, Magen Avraham, Be'er Heitev...).
 *
 * These books ship from Sefaria as flat siman × se'if-katan structures: the
 * running text never says which SA se'if a group of se'ifim-ketanim explains,
 * so a standalone reader cannot tell where one se'if ends and the next
 * begins. The association does exist — every ס"ק line is the target of a
 * COMMENTARY link from the exact SA se'if line — so this post-process derives
 * it once at build time and materializes it in the regular alt_toc tables:
 *
 * - containers mirror the book's own main-TOC headings (סימן א, סימן ב...),
 * - one leaf "סעיף X" per contiguous ס"ק group hangs beneath its heading,
 *   pointing at the group's first content line.
 *
 * Only `alt_toc_*` rows are written — no content line is created or moved.
 * Candidates are books whose *declared* base text (`book_base_text`, Sefaria
 * `base_text_titles`) is a שולחן ערוך book; the SA gate is deliberate — on
 * other bases (e.g. Rashi on Torah) a per-verse marker would be noise, not
 * structure. A book that already has a Seifim structure is skipped, so the
 * task is idempotent.
 *
 * All writes run on one JDBC connection in a single transaction with batched
 * statements — the line_alt_toc map alone is hundreds of thousands of rows,
 * and per-row autocommit turns that into hours of fsyncs. Structure and newly
 * created tocText ids come from the build-state allocator, so they remain
 * stable across releases. Entry ids follow the existing alt-TOC policy: a
 * deterministic wholesale rebuild per book.
 *
 * Runs AFTER all book- and link-writing stages (Sefaria links carry the
 * COMMENTARY rows this reads).
 *
 * Usage:
 *   ./gradlew :sefariasqlite:synthesizeSeifimAltToc -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SynthesizeSeifimAltToc")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    logger.i { "Synthesizing Seifim alt-TOC in $dbPath" }

    try {
        val buildStatePath = resolveSeifimBuildStatePath(dbPath)
        check(Files.exists(buildStatePath)) {
            "build_state not found at $buildStatePath; run the DB generation pipeline first"
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.prepareStatement("ATTACH DATABASE ? AS seifim_state").use { st ->
                st.setString(1, buildStatePath.toAbsolutePath().toString())
                st.execute()
            }
            val stableIds = AttachedBuildStateIds(conn)
            val snapshots = readSeifimCandidateSnapshots(conn)
            logger.i { "Candidates with derivable seif markers: ${snapshots.size}" }
            val result = synthesizeSeifimAltTocs(conn, snapshots, stableIds)
            logger.i { "Seifim alt-TOC done: structures=${result.structures} leaves=${result.leaves}" }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to synthesize Seifim alt-TOC; aborting" }
        exitProcess(1)
    }
}

/** Resolves -DbuildStatePath / BUILD_STATE_PATH, else `<db>.buildstate`. */
private fun resolveSeifimBuildStatePath(dbPath: Path): Path {
    val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
    return if (explicit != null) Paths.get(explicit) else Paths.get("$dbPath.buildstate")
}

/** One derivable-seif candidate: everything the write phase needs, pre-read. */
internal data class SeifimBookSnapshot(
    val bookId: Long,
    val title: String,
    /** Main-TOC headings with a line: (tocLevel, text, lineId, lineIndex), by lineIndex. */
    val headings: List<SeifimHeading>,
    /** Derived markers: group-opening lines, by lineIndex. */
    val markers: List<SeifMarker>,
    /** Every content line of the book: (lineId, lineIndex), by lineIndex. */
    val lines: List<Pair<Long, Long>>,
)

internal data class SeifimHeading(
    val tocLevel: Int,
    val text: String,
    val lineId: Long,
    val lineIndex: Long,
)

internal data class SeifMarker(val lineId: Long, val lineIndex: Long, val label: String)

/** One COMMENTARY link row: the book line it lands on and the SA source ref. */
internal data class SeifLinkRow(
    val lineId: Long,
    val lineIndex: Long,
    val baseHeRef: String,
)

internal data class SeifimSynthesisResult(val structures: Int, val leaves: Int)

/**
 * Small, disk-backed allocator for the two stable-id namespaces this
 * post-process touches. The build-state DB is ATTACHed to [conn], so its
 * natural-key rows and the generated library rows commit or roll back together
 * without loading the very large line/link maps into memory.
 */
internal class AttachedBuildStateIds(private val conn: Connection) {
    fun altTocStructureId(bookId: Long, key: String): Long {
        conn.prepareStatement(
            "SELECT id FROM seifim_state.id_alt_toc_structure WHERE book_id = ? AND key = ?",
        ).use { st ->
            st.setLong(1, bookId)
            st.setString(2, key)
            st.executeQuery().use { rs -> if (rs.next()) return rs.getLong(1) }
        }
        val id = allocate(IdTable.ALT_TOC_STRUCTURE, queryMaxId(conn, "alt_toc_structure"))
        conn.prepareStatement(
            "INSERT INTO seifim_state.id_alt_toc_structure(book_id, key, id) VALUES (?, ?, ?)",
        ).use { st ->
            st.setLong(1, bookId)
            st.setString(2, key)
            st.setLong(3, id)
            st.executeUpdate()
        }
        return id
    }

    fun tocTextId(text: String): Long {
        conn.prepareStatement(
            "SELECT id FROM seifim_state.id_lookup WHERE kind = 'toc_text' AND natural_key = ?",
        ).use { st ->
            st.setString(1, text)
            st.executeQuery().use { rs -> if (rs.next()) return rs.getLong(1) }
        }
        val id = allocate(IdTable.TOC_TEXT, queryMaxId(conn, "tocText"))
        conn.prepareStatement(
            "INSERT INTO seifim_state.id_lookup(kind, natural_key, id) VALUES ('toc_text', ?, ?)",
        ).use { st ->
            st.setString(1, text)
            st.setLong(2, id)
            st.executeUpdate()
        }
        return id
    }

    private fun allocate(table: IdTable, currentDbMax: Long): Long {
        val persistedNext = conn.prepareStatement(
            "SELECT next_id FROM seifim_state.id_counters WHERE table_name = ?",
        ).use { st ->
            st.setString(1, table.tableName)
            st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else 1L }
        }
        val id = maxOf(persistedNext, currentDbMax + 1, 1L)
        conn.prepareStatement(
            """
            INSERT INTO seifim_state.id_counters(table_name, next_id) VALUES (?, ?)
            ON CONFLICT(table_name) DO UPDATE SET next_id = excluded.next_id
            """.trimIndent(),
        ).use { st ->
            st.setString(1, table.tableName)
            st.setLong(2, id + 1)
            st.executeUpdate()
        }
        return id
    }
}

/** Atomically writes all candidate structures and restores JDBC ownership. */
internal fun synthesizeSeifimAltTocs(
    conn: Connection,
    snapshots: List<SeifimBookSnapshot>,
    stableIds: AttachedBuildStateIds? = null,
): SeifimSynthesisResult {
    check(conn.autoCommit) { "synthesizeSeifimAltTocs requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        var structures = 0
        var leaves = 0
        for (snapshot in snapshots) {
            val written = writeSeifimAltToc(conn, snapshot, stableIds)
            if (written > 0) {
                structures++
                leaves += written
            }
        }
        // The catalog and clients use this denormalized flag as their cheap
        // capability gate. Repair it for both newly-created structures and a
        // structure left by an older run whose flag was not updated.
        conn.prepareStatement(
            """
            UPDATE book
            SET hasAltStructures = 1
            WHERE hasAltStructures = 0
              AND EXISTS (
                SELECT 1 FROM alt_toc_structure s
                WHERE s.bookId = book.id AND s.key = ?
              )
            """.trimIndent(),
        ).use { st ->
            st.setString(1, SEIFIM_STRUCTURE_KEY)
            st.executeUpdate()
        }
        conn.commit()
        SeifimSynthesisResult(structures, leaves)
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

/**
 * Derives the group-opening markers from COMMENTARY link rows.
 *
 * [rows] must be ordered by `(lineIndex, base lineIndex)` so the first link
 * of a multi-linked line decides its se'if. The group key is the base line's
 * full heRef (siman+se'if) — not the se'if letter alone — so a new siman
 * whose first commented se'if carries the same letter still opens a group.
 * The label is the se'if alone: the trailing comma-separated heRef segment.
 */
internal fun computeSeifMarkers(rows: List<SeifLinkRow>): List<SeifMarker> {
    val refByLine = LinkedHashMap<Long, Triple<Long, String, String>>() // lineIndex -> (lineId, key, seif)
    for (row in rows) {
        if (refByLine.containsKey(row.lineIndex)) continue
        val comma = row.baseHeRef.lastIndexOf(',')
        if (comma < 0) continue
        val seif = row.baseHeRef.substring(comma + 1).trim()
        if (seif.isEmpty()) continue
        refByLine[row.lineIndex] = Triple(row.lineId, row.baseHeRef, seif)
    }

    val markers = mutableListOf<SeifMarker>()
    var previousKey: String? = null
    for (lineIndex in refByLine.keys.sorted()) {
        val (lineId, key, seif) = refByLine.getValue(lineIndex)
        if (key != previousKey) {
            markers += SeifMarker(lineId = lineId, lineIndex = lineIndex, label = "סעיף $seif")
            previousKey = key
        }
    }
    return markers
}

/**
 * Reads every candidate book (declared base = שולחן ערוך, no existing Seifim
 * structure) and returns those whose links yield at least one marker.
 */
internal fun readSeifimCandidateSnapshots(conn: Connection): List<SeifimBookSnapshot> {
    data class Candidate(
        val bookId: Long,
        val title: String,
        val baseBookIds: MutableList<Long>,
        val shulchanAruchBaseBookIds: MutableList<Long>,
    )
    data class HeadingRow(val id: Long, val parentId: Long?, val heading: SeifimHeading)

    // ספר יכול להצהיר על כמה ספרי בסיס (קול יעקב: שולחן ערוך + שולחן ערוך
    // הרב) — מקבצים לפי bookId כדי לא לנסות ליצור מבנה כפול.
    val candidatesByBook = LinkedHashMap<Long, Candidate>()
    conn.prepareStatement(
        """
        SELECT bbt.bookId, bbt.baseBookId, bb.title, b.title
        FROM book_base_text bbt
        JOIN book b ON b.id = bbt.bookId
        JOIN book bb ON bb.id = bbt.baseBookId
        WHERE EXISTS (
            SELECT 1
            FROM book_base_text sa_bbt
            JOIN book sa ON sa.id = sa_bbt.baseBookId
            WHERE sa_bbt.bookId = bbt.bookId AND sa.title LIKE 'שולחן ערוך%'
          )
          AND NOT EXISTS (
            SELECT 1 FROM alt_toc_structure s
            WHERE s.bookId = bbt.bookId AND s.key = '$SEIFIM_STRUCTURE_KEY'
          )
        ORDER BY bbt.bookId, bbt.baseBookId
        """.trimIndent(),
    ).use { st ->
        st.executeQuery().use { rs ->
            while (rs.next()) {
                val bookId = rs.getLong(1)
                val candidate = candidatesByBook.getOrPut(bookId) {
                    Candidate(bookId, rs.getString(4), mutableListOf(), mutableListOf())
                }
                candidate.baseBookIds += rs.getLong(2)
                if (rs.getString(3).startsWith("שולחן ערוך")) {
                    candidate.shulchanAruchBaseBookIds += rs.getLong(2)
                }
            }
        }
    }

    val snapshots = mutableListOf<SeifimBookSnapshot>()
    for (candidate in candidatesByBook.values) {
        val linkRows = mutableListOf<SeifLinkRow>()
        val basePlaceholders = candidate.baseBookIds.joinToString(",") { "?" }
        val shulchanAruchBaseIds = candidate.shulchanAruchBaseBookIds.toHashSet()
        var declaredBaseLinkCount = 0
        conn.prepareStatement(
            """
            SELECT ml.id, ml.lineIndex, bl.heRef, k.sourceBookId
            FROM link k INDEXED BY idx_link_target_book
            JOIN connection_type c ON c.id = k.connectionTypeId
            JOIN line ml ON ml.id = k.targetLineId
            JOIN line bl ON bl.id = k.sourceLineId
            WHERE k.sourceBookId IN ($basePlaceholders) AND k.targetBookId = ?
              AND c.name = 'COMMENTARY' AND bl.heRef IS NOT NULL
            ORDER BY ml.lineIndex, bl.lineIndex, k.sourceBookId, k.id
            """.trimIndent(),
        ).use { st ->
            candidate.baseBookIds.forEachIndexed { i, baseId -> st.setLong(i + 1, baseId) }
            st.setLong(candidate.baseBookIds.size + 1, candidate.bookId)
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    declaredBaseLinkCount++
                    if (rs.getLong(4) in shulchanAruchBaseIds) {
                        linkRows += SeifLinkRow(rs.getLong(1), rs.getLong(2), rs.getString(3))
                    }
                }
            }
        }

        // A few incidental direct-SA links do not turn a super-commentary
        // into a usable se'if-by-se'if commentary. Require the direct SA links
        // to be at least half of its links from all declared bases. This keeps
        // genuine multi-SA works (e.g. Kol Yaakov) while excluding works such
        // as Pri Megadim whose real base is Magen Avraham/Taz.
        if (linkRows.size * 2 < declaredBaseLinkCount) continue

        val markers = computeSeifMarkers(linkRows)
        if (markers.isEmpty()) continue

        val headingRows = mutableListOf<HeadingRow>()
        conn.prepareStatement(
            """
            SELECT e.id, e.parentId, e.level, t.text, l.id, l.lineIndex
            FROM tocEntry e
            JOIN tocText t ON t.id = e.textId
            JOIN line l ON l.id = e.lineId
            WHERE e.bookId = ?
            ORDER BY l.lineIndex, e.level, e.id
            """.trimIndent(),
        ).use { st ->
            st.setLong(1, candidate.bookId)
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    val parentId = rs.getLong(2).let { if (rs.wasNull()) null else it }
                    headingRows += HeadingRow(
                        id = rs.getLong(1),
                        parentId = parentId,
                        heading = SeifimHeading(rs.getInt(3), rs.getString(4), rs.getLong(5), rs.getLong(6)),
                    )
                }
            }
        }
        val headingById = headingRows.associateBy { it.id }
        val relevantHeadingIds = HashSet<Long>()
        for (row in headingRows) {
            if (!isSimanHeading(row.heading.text)) continue
            var current: HeadingRow? = row
            while (current != null && relevantHeadingIds.add(current.id)) {
                current = current.parentId?.let(headingById::get)
            }
        }
        // Keep only siman headings and their structural ancestors. Lower-level
        // headings such as "סעיף קטן א" are content inside the new se'if
        // group, not containers above it.
        val headings = headingRows
            .filter { it.id in relevantHeadingIds && it.heading.tocLevel >= 1 }
            .map { it.heading }
        if (headings.isEmpty()) continue

        val lines = mutableListOf<Pair<Long, Long>>()
        conn.prepareStatement(
            "SELECT id, lineIndex FROM line WHERE bookId = ? ORDER BY lineIndex",
        ).use { st ->
            st.setLong(1, candidate.bookId)
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    lines += rs.getLong(1) to rs.getLong(2)
                }
            }
        }

        snapshots += SeifimBookSnapshot(
            bookId = candidate.bookId,
            title = candidate.title,
            headings = headings,
            markers = markers,
            lines = lines,
        )
    }
    return snapshots
}

private fun isSimanHeading(text: String): Boolean {
    val trimmed = text.trimStart()
    return trimmed.startsWith("סימן") &&
        trimmed.length > "סימן".length &&
        trimmed["סימן".length].isWhitespace()
}

private fun queryMaxId(conn: Connection, table: String): Long =
    conn.prepareStatement("SELECT COALESCE(MAX(id), 0) FROM $table").use { st ->
        st.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
    }

/** Get-or-create over tocText's UNIQUE text, using build-state for a new id. */
private fun tocTextId(conn: Connection, text: String, stableIds: AttachedBuildStateIds?): Long {
    conn.prepareStatement("SELECT id FROM tocText WHERE text = ?").use { st ->
        st.setString(1, text)
        st.executeQuery().use { rs -> if (rs.next()) return rs.getLong(1) }
    }
    val stableId = stableIds?.tocTextId(text)
    val sql = if (stableId == null) {
        "INSERT INTO tocText (text) VALUES (?)"
    } else {
        "INSERT INTO tocText (id, text) VALUES (?, ?)"
    }
    conn.prepareStatement(sql).use { st ->
        var parameter = 1
        if (stableId != null) st.setLong(parameter++, stableId)
        st.setString(parameter, text)
        st.executeUpdate()
    }
    conn.prepareStatement("SELECT id FROM tocText WHERE text = ?").use { st ->
        st.setString(1, text)
        st.executeQuery().use { rs -> rs.next(); return rs.getLong(1) }
    }
}

/**
 * Writes one book's Seifim structure: the heading mirror, the se'if leaves,
 * and the per-line owner map — same shape as the synthesized Simanim alt-TOC
 * ([SefariaAltTocBuilder]), but built fully in memory first so `hasChildren`
 * and `isLastChild` land in the initial batched INSERTs. Returns the number
 * of leaves written (0 = nothing usable, and then nothing is written at all).
 */
internal fun writeSeifimAltToc(
    conn: Connection,
    snapshot: SeifimBookSnapshot,
    stableIds: AttachedBuildStateIds? = null,
): Int {
    val headingLineIndices = snapshot.headings.map { it.lineIndex }.toHashSet()
    // A marker can only hang beneath a preceding heading; a heading line
    // itself never opens a group (headings carry no ס"ק content).
    val usableMarkers = snapshot.markers.filter { marker ->
        marker.lineIndex !in headingLineIndices &&
            snapshot.headings.first().lineIndex < marker.lineIndex
    }
    if (usableMarkers.isEmpty()) return 0

    // Build the whole entry tree in memory.
    data class PendingEntry(
        val id: Long,
        val parentId: Long?,
        val text: String,
        val level: Int,
        val lineId: Long,
        var isLastChild: Boolean = false,
        var hasChildren: Boolean = false,
    )

    var nextEntryId = queryMaxId(conn, "alt_toc_entry")
    val entries = mutableListOf<PendingEntry>()
    val entryById = HashMap<Long, PendingEntry>()
    val childrenByParent = LinkedHashMap<Long?, MutableList<PendingEntry>>()
    val headingByLine = HashMap<Long, PendingEntry>() // headingLine -> entry
    val lineToEntryId = HashMap<Long, Long>() // owning line -> entry id
    val stack = ArrayDeque<Pair<Int, PendingEntry>>() // (tocLevel, entry)

    for (heading in snapshot.headings) {
        while (stack.isNotEmpty() && stack.last().first >= heading.tocLevel) stack.removeLast()
        val parent = stack.lastOrNull()?.second
        val entry = PendingEntry(
            id = ++nextEntryId,
            parentId = parent?.id,
            text = heading.text,
            level = stack.size,
            lineId = heading.lineId,
        )
        entries += entry
        entryById[entry.id] = entry
        childrenByParent.getOrPut(parent?.id) { mutableListOf() }.add(entry)
        stack.addLast(heading.tocLevel to entry)
        headingByLine[heading.lineIndex] = entry
        lineToEntryId[heading.lineIndex] = entry.id
    }

    val sortedHeadingLines = snapshot.headings.map { it.lineIndex }.sorted()
    var leaves = 0
    for (marker in usableMarkers) {
        val parentLine = sortedHeadingLines.lastOrNull { it < marker.lineIndex } ?: continue
        val parent = headingByLine.getValue(parentLine)
        val entry = PendingEntry(
            id = ++nextEntryId,
            parentId = parent.id,
            text = marker.label,
            level = parent.level + 1,
            lineId = marker.lineId,
        )
        entries += entry
        entryById[entry.id] = entry
        childrenByParent.getOrPut(parent.id) { mutableListOf() }.add(entry)
        lineToEntryId[marker.lineIndex] = entry.id
        leaves++
    }

    for ((parentId, children) in childrenByParent) {
        if (children.isEmpty()) continue
        if (parentId != null) entryById.getValue(parentId).hasChildren = true
        children.last().isLastChild = true
    }

    // Flush: structure row, entry batch, then the per-line owner map.
    val structureId = stableIds?.altTocStructureId(snapshot.bookId, SEIFIM_STRUCTURE_KEY)
        ?: (queryMaxId(conn, "alt_toc_structure") + 1)
    conn.prepareStatement(
        "INSERT INTO alt_toc_structure (id, bookId, key, title, heTitle) VALUES (?, ?, ?, ?, ?)",
    ).use { st ->
        st.setLong(1, structureId)
        st.setLong(2, snapshot.bookId)
        st.setString(3, SEIFIM_STRUCTURE_KEY)
        st.setString(4, SEIFIM_STRUCTURE_TITLE_EN)
        st.setString(5, SEIFIM_STRUCTURE_TITLE_HE)
        st.executeUpdate()
    }

    val textIds = HashMap<String, Long>()
    conn.prepareStatement(
        """
        INSERT INTO alt_toc_entry
            (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren)
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

    // line_alt_toc: map every content line to its nearest preceding entry.
    conn.prepareStatement(
        "INSERT OR REPLACE INTO line_alt_toc (lineId, structureId, altTocEntryId) VALUES (?, ?, ?)",
    ).use { st ->
        val ownerLines = lineToEntryId.keys.sorted()
        var oi = 0
        var currentEntryId: Long? = null
        for ((lineId, lineIndex) in snapshot.lines) {
            while (oi < ownerLines.size && ownerLines[oi] <= lineIndex) {
                currentEntryId = lineToEntryId[ownerLines[oi]]
                oi++
            }
            val entryId = currentEntryId ?: continue
            st.setLong(1, lineId)
            st.setLong(2, structureId)
            st.setLong(3, entryId)
            st.addBatch()
        }
        st.executeBatch()
    }
    return leaves
}

internal const val SEIFIM_STRUCTURE_KEY = "Seifim"
internal const val SEIFIM_STRUCTURE_TITLE_EN = "Seifim"
internal const val SEIFIM_STRUCTURE_TITLE_HE = "סעיפים"
