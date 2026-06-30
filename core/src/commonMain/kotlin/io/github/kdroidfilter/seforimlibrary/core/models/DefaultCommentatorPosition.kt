package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * A commentator's placement in a book's default-commentator list.
 *
 * Positions may be non-contiguous: an absent commentator leaves its slot empty rather than
 * shifting the rest up, so every commentator keeps a stable column in fixed-slot page layouts.
 * Honoring [position] is opt-in — a consumer that reads the list in order and ignores it is
 * unaffected; it simply renders the commentators consecutively, with no gap.
 *
 * @property commentatorBookId The book id of the commentator
 * @property position The position the commentator occupies in the list
 */
@Serializable
data class DefaultCommentatorPosition(
    val commentatorBookId: Long,
    val position: Int
)
