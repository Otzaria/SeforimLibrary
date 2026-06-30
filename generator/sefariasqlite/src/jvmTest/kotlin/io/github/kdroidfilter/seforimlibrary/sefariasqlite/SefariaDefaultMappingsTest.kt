package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.DefaultCommentatorPosition
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaDefaultMappingsTest {
    private val titleToId = mapOf("א" to 10L, "ב" to 20L, "ג" to 30L)

    @Test
    fun contiguousSlotsGetSequentialPositions() {
        val result =
            resolvePositionedCommentators(listOf("א", "ב", "ג"), 99L, titleToId)
        assertEquals(
            listOf(
                DefaultCommentatorPosition(10L, 0),
                DefaultCommentatorPosition(20L, 1),
                DefaultCommentatorPosition(30L, 2)
            ),
            result
        )
    }

    @Test
    fun nullSlotLeavesAGap() {
        // א ב-position 0, slot ריק (null) מדלג ל-1, ב נוחת על position 2.
        val result =
            resolvePositionedCommentators(listOf("א", null, "ב"), 99L, titleToId)
        assertEquals(
            listOf(DefaultCommentatorPosition(10L, 0), DefaultCommentatorPosition(20L, 2)),
            result
        )
    }

    @Test
    fun missingCommentatorIsPackedNotGapped() {
        // מפרש שאינו ב-map מתקבץ (position לא מתקדם) — תאימות לאחור.
        val result =
            resolvePositionedCommentators(listOf("א", "לא קיים", "ב"), 99L, titleToId)
        assertEquals(
            listOf(DefaultCommentatorPosition(10L, 0), DefaultCommentatorPosition(20L, 1)),
            result
        )
    }

    @Test
    fun duplicatesAndSelfReferenceAreSkipped() {
        assertEquals(
            listOf(DefaultCommentatorPosition(10L, 0), DefaultCommentatorPosition(20L, 1)),
            resolvePositionedCommentators(listOf("א", "א", "ב"), 99L, titleToId)
        )
        // baseBookId == commentator → מדולג (ספר אינו מפרש על עצמו)
        assertEquals(
            listOf(DefaultCommentatorPosition(20L, 0)),
            resolvePositionedCommentators(listOf("א", "ב"), 10L, titleToId)
        )
    }
}
