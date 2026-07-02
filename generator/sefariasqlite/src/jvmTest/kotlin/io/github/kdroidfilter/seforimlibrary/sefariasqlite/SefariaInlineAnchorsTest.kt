package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaInlineAnchorsTest {

    @Test
    fun parsesWellFormedItagWithOrderAndLabel() {
        // Real shape from Shulchan Arukh, Orach Chayim 1:1
        val line = """<b>דין השכמת הבוקר:</b> <i data-commentator="Be'er HaGolah" data-label="א" data-order="1"></i>יתגבר"""
        val itags = parseInlineItags(line)
        assertEquals(1, itags.size)
        assertEquals("Be'er HaGolah", itags[0].commentator)
        assertEquals(1, itags[0].order)
        assertEquals("א", itags[0].label)
        // Visible prefix: "דין השכמת הבוקר: " = 17 visible chars (tags skipped)
        assertEquals(17, itags[0].charStart)
    }

    @Test
    fun parsesMalformedItagMissingOpeningQuote() {
        // Sefaria ships a handful of these (all Mishnah Berurah, incl. OC 1:1)
        val line = """יתגבר <i data-commentator=Mishnah Berurah" data-label="א"></i>לעבודת בוראו"""
        val itags = parseInlineItags(line)
        assertEquals(1, itags.size)
        assertEquals("Mishnah Berurah", itags[0].commentator)
        assertNull(itags[0].order)
        assertEquals("א", itags[0].label)
        assertEquals(6, itags[0].charStart)
    }

    @Test
    fun countsEntityAsSingleVisibleChar() {
        val line = """א&nbsp;ב<i data-commentator="Taz" data-order="2"></i>ג"""
        val itags = parseInlineItags(line)
        // "א" + entity + "ב" = 3 visible chars before the tag
        assertEquals(3, itags[0].charStart)
        assertEquals(2, itags[0].order)
    }

    @Test
    fun multipleItagsShareLineWithDistinctOffsets() {
        val line = """<i data-commentator="A" data-order="1"></i>אב <i data-commentator="B" data-order="2"></i>גד"""
        val itags = parseInlineItags(line)
        assertEquals(listOf(0, 3), itags.map { it.charStart })
        assertEquals(listOf("A", "B"), itags.map { it.commentator })
    }

    @Test
    fun ignoresNonItagTags() {
        assertEquals(emptyList(), parseInlineItags("<b>בלי עוגנים</b> כלל"))
    }

    @Test
    fun gematriaDecodesPlainAndSpecialValues() {
        assertEquals(1, gematriaValue("א"))
        assertEquals(15, gematriaValue("טו"))
        assertEquals(16, gematriaValue("טז"))
        assertEquals(129, gematriaValue("קכט"))
        // Gershayim-punctuated labels decode the same
        assertEquals(129, gematriaValue("קכ\"ט"))
        // Final letters carry their regular values
        assertEquals(20, gematriaValue("ך"))
    }

    @Test
    fun gematriaRejectsNonHebrewLabels() {
        assertNull(gematriaValue("12"))
        assertNull(gematriaValue(""))
        assertNull(gematriaValue("a"))
    }

    @Test
    fun buildsCommentRefForSimpleAddress() {
        assertEquals(
            "Turei Zahav on Shulchan Arukh, Orach Chayim 1:7",
            buildCommentRef(
                baseRef = "Shulchan Arukh, Orach Chayim 1:5",
                baseEnTitle = "Shulchan Arukh, Orach Chayim",
                commentaryEnTitle = "Turei Zahav on Shulchan Arukh, Orach Chayim",
                order = 7,
            )
        )
    }

    @Test
    fun buildsCommentRefForDafAddress() {
        assertEquals(
            "Hagahot HaBach on Rif Berakhot 2a:3",
            buildCommentRef(
                baseRef = "Rif Berakhot 2a:5",
                baseEnTitle = "Rif Berakhot",
                commentaryEnTitle = "Hagahot HaBach on Rif Berakhot",
                order = 3,
            )
        )
    }

    @Test
    fun buildsCommentRefForDepthOneBase() {
        assertEquals(
            "Beit Yosef, Orach Chayim 1:2",
            buildCommentRef(
                baseRef = "Tur, Orach Chayim 1",
                baseEnTitle = "Tur",
                commentaryEnTitle = "Beit Yosef",
                order = 2,
            )
        )
    }

    @Test
    fun buildCommentRefRejectsForeignPrefix() {
        assertNull(
            buildCommentRef(
                baseRef = "Genesis 1:1",
                baseEnTitle = "Exodus",
                commentaryEnTitle = "Rashi on Exodus",
                order = 1,
            )
        )
    }
}
