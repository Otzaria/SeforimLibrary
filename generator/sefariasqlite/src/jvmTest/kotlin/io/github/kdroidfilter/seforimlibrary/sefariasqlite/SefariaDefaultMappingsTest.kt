package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaDefaultMappingsTest {
    private val titleToId = mapOf("א" to 10L, "ב" to 20L, "ג" to 30L)

    @Test
    fun contiguousSlotsGetSequentialPositions() {
        val result =
            resolvePositionedCommentators(listOf("א", "ב", "ג"), 99L, titleToId)
        assertEquals(listOf(10L to 0, 20L to 1, 30L to 2), result)
    }

    @Test
    fun nullSlotLeavesAGap() {
        // א ב-position 0, slot ריק (null) מדלג ל-1, ב נוחת על position 2.
        val result =
            resolvePositionedCommentators(listOf("א", null, "ב"), 99L, titleToId)
        assertEquals(listOf(10L to 0, 20L to 2), result)
    }

    @Test
    fun missingCommentatorIsPackedNotGapped() {
        // מפרש שאינו ב-map מתקבץ (position לא מתקדם) — תאימות לאחור.
        val result =
            resolvePositionedCommentators(listOf("א", "לא קיים", "ב"), 99L, titleToId)
        assertEquals(listOf(10L to 0, 20L to 1), result)
    }

    @Test
    fun duplicatesAndSelfReferenceAreSkipped() {
        assertEquals(
            listOf(10L to 0, 20L to 1),
            resolvePositionedCommentators(listOf("א", "א", "ב"), 99L, titleToId)
        )
        // baseBookId == commentator → מדולג (ספר אינו מפרש על עצמו)
        assertEquals(
            listOf(20L to 0),
            resolvePositionedCommentators(listOf("א", "ב"), 10L, titleToId)
        )
    }
}
