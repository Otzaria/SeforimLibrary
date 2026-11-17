package io.github.kdroidfilter.seforimlibrary.common.anchors

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.visibleUnits
import java.sql.Connection

data class CommentarySideAnchorReport(
    /** Labelled commentary links without a side-1 anchor. */
    val candidates: Int,
    val written: Int,
    /** Candidates whose commentary line does not open with "(label)". */
    val noMarker: Int,
    /** Candidates skipped because their base-side anchors disagree on the label. */
    val conflictingLabels: Int,
    /** Commentary lines that two or more matching links share; none of them is anchored. */
    val sharedLines: Int,
)

/**
 * Side-1 anchors over the printed marker that opens a commentary line
 * (`:generator-common:buildCommentarySideAnchors`, Otzaria/otzaria#3785).
 *
 * A COMMENTARY or SUPER_COMMENTARY link whose base side has a labelled anchor (the marker
 * in the base text) gets a range anchor on its commentary side when the commentary line
 * opens with the same printed "(label)". The reader then shows that marker as a link to the
 * base text instead of plain text, the reverse of the base-side marker. The anchor keeps the
 * label: without it the reader's link lists fall back to the last letter of the base heRef.
 *
 * Offsets are visible chars of the full line (footnote bodies included), like every other
 * `link_anchor` writer, so this runs before `buildLineNoteIndex`. Links that already have a
 * side-1 anchor are left as they are, which also makes a rerun a no-op.
 */
fun buildCommentarySideMarkerAnchors(
    conn: Connection,
    logger: Logger,
    batchSize: Int = 5_000,
): CommentarySideAnchorReport {
    var candidates = 0
    var noMarker = 0
    var conflicting = 0
    // targetLineId -> matches; a line matched by two links is ambiguous and skipped.
    val byLine = LinkedHashMap<Long, MutableList<MarkerMatch>>()

    conn.createStatement().use { st ->
        st.executeQuery(
            """
            SELECT l.id, l.targetLineId, MIN(la.label), MAX(la.label), tl.content
            FROM link l
            JOIN connection_type ct ON ct.id = l.connectionTypeId
            JOIN link_anchor la ON la.linkId = l.id AND la.side = 0
            JOIN line tl ON tl.id = l.targetLineId
            WHERE ct.name IN ('COMMENTARY', 'SUPER_COMMENTARY')
              AND la.label IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM link_anchor x WHERE x.linkId = l.id AND x.side = 1)
            GROUP BY l.id
            """.trimIndent(),
        ).use { rs ->
            while (rs.next()) {
                candidates++
                val label = rs.getString(3)
                if (label != rs.getString(4)) {
                    conflicting++
                    continue
                }
                val range = openingMarkerRange(rs.getString(5), label)
                if (range == null) {
                    noMarker++
                    continue
                }
                byLine.getOrPut(rs.getLong(2)) { ArrayList(1) } += MarkerMatch(rs.getLong(1), range, label)
            }
        }
    }

    var written = 0
    var shared = 0
    val autoCommit = conn.autoCommit
    conn.autoCommit = false
    try {
        conn.prepareStatement(
            "INSERT OR IGNORE INTO link_anchor(linkId, side, charStart, charEnd, label) VALUES (?, 1, ?, ?, ?)",
        ).use { ps ->
            var pending = 0
            for (matches in byLine.values) {
                if (matches.size > 1) {
                    shared++
                    continue
                }
                val match = matches.single()
                ps.setLong(1, match.linkId)
                ps.setInt(2, match.range.first)
                ps.setInt(3, match.range.last + 1)
                ps.setString(4, match.label)
                ps.addBatch()
                if (++pending >= batchSize) {
                    written += ps.executeBatch().count { it > 0 }
                    pending = 0
                }
            }
            if (pending > 0) written += ps.executeBatch().count { it > 0 }
        }
        conn.commit()
    } finally {
        conn.autoCommit = autoCommit
    }

    return CommentarySideAnchorReport(candidates, written, noMarker, conflicting, shared).also {
        logger.i { "Commentary-side marker anchors: $it" }
    }
}

private data class MarkerMatch(val linkId: Long, val range: IntRange, val label: String)

/**
 * The visible-char range of "(label)" when it is the first visible text of [content],
 * else null. Tags do not count; an entity counts as one char.
 */
internal fun openingMarkerRange(content: String, label: String): IntRange? {
    val marker = "(${label.trim().removePrefix("(").removeSuffix(")").trim()})"
    if (marker.length < 3) return null
    val units = visibleUnits(content)
    val start = units.indexOfFirst { it.isNotBlank() }
    if (start < 0 || start + marker.length > units.size) return null
    for (i in marker.indices) {
        if (units[start + i] != marker[i].toString()) return null
    }
    return start until start + marker.length
}
