package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Range extent of a [Link] whose citation spans multiple lines (e.g.
 * "Exodus 1:1-6:1"). The link row itself is anchored at the range's first
 * line; this row records the last line of the ranged side.
 *
 * @property linkId The link this range belongs to
 * @property side 0 = the range is on the stored source side, 1 = target side
 * @property endLineId Last line of the range (inclusive)
 * @property endLineIndex 0-based index of [endLineId] within its book (denormalized)
 */
@Serializable
data class LinkRange(
    val linkId: Long,
    val side: Int,
    val endLineId: Long,
    val endLineIndex: Int,
)

/**
 * One line covered by a ranged link side, beyond the range's first line
 * (the first line is matched by the equality queries on the link row itself).
 */
@Serializable
data class LinkCoverage(
    val lineId: Long,
    val linkId: Long,
    val side: Int,
)

/**
 * One side of a [Link] that Sefaria would not surface, with the reason(s) why.
 *
 * The decision is made by SefariaExport (which alone has the TermSet, both
 * `Ref`s and their `index_node` depths) and shipped per links-CSV row; the
 * importer keeps the side hidden only when every merged contribution is
 * hidden, and OR-s their diagnostic reasons.
 *
 * @property linkId The link this applies to
 * @property side 0 = the stored source side is hidden, 1 = the target side
 * @property reasonMask Bitwise OR of [SuppressionReason] values; never 0
 */
@Serializable
data class LinkSuppressedSide(
    val linkId: Long,
    val side: Int,
    val reasonMask: Int,
)

/**
 * Why a link side is hidden. Mirrors the `continue` branches of Sefaria's
 * `get_links()`; values match SefariaExport's `Suppression Mask 1/2` columns.
 */
object SuppressionReason {
    /** The anchor ref is not at segment level (`len(sections) != node_depth`). */
    const val ANCHOR_NOT_SEGMENT = 1

    /** The other side sits more than one level above its own segment depth. */
    const val OTHER_TOO_COARSE = 2

    /** The anchor ref is a whole Talmud/Mishnah/Tosefta perek. */
    const val WHOLE_PEREK = 4

    /** The anchor ref is a whole Torah parasha. */
    const val WHOLE_PARASHA = 8

    /** Every bit currently defined — anything outside this is an unknown reason. */
    const val ALL = ANCHOR_NOT_SEGMENT or OTHER_TOO_COARSE or WHOLE_PEREK or WHOLE_PARASHA
}
