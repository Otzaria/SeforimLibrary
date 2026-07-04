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
