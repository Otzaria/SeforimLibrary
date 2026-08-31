package io.github.kdroidfilter.seforimlibrary.common.dh

import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor.Format
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** Shapes below are verbatim from a real seforim.db unless noted. */
class DhExtractorTest {

    // ── DASH format (Sefaria Talmud commentaries) ──────────────────────────

    @Test
    fun `dash-separated dibbur is extracted and normalised`() {
        assertEquals(
            "עד סוף האשמורה הראשונה",
            DhExtractor.extract(
                "עד סוף האשמורה הראשונה – שליש הלילה כדמפרש בגמרא",
                Format.DASH,
            ),
        )
    }

    @Test
    fun `plain hyphen works as separator too`() {
        assertEquals(
            "אי הכי סיפא דקתני שחרית ברישא",
            DhExtractor.extract(
                "אי הכי סיפא דקתני שחרית ברישא - אי אמרת בשלמא דסמיך אקרא",
                Format.DASH,
            ),
        )
    }

    @Test
    fun `a daf's first comment ends its dibbur at the sentence break`() {
        // Sefaria's first comment per daf ends with '. ' instead of ' – ';
        // the naive dash cut would swallow the whole quoted mishnah.
        assertEquals(
            "מאימתי קורין את שמע בערבין",
            DhExtractor.extract(
                "מאימתי קורין את שמע בערבין. משעה שהכהנים נכנסים לאכול בתרומתן – כהנים שנטמאו וטבלו",
                Format.DASH,
            ),
        )
    }

    @Test
    fun `maqaf does not separate a dibbur`() {
        assertNull(DhExtractor.extract("בית־השלחין שדה שצריך להשקותה", Format.DASH))
    }

    @Test
    fun `a line without a spaced dash yields nothing`() {
        assertNull(DhExtractor.extract("שורה רגילה בלי מפריד כלל", Format.DASH))
    }

    @Test
    fun `a dash with nothing after it yields nothing`() {
        assertNull(DhExtractor.extract("עד סוף האשמורה - ", Format.DASH))
    }

    @Test
    fun `an implausibly long dash prefix without a sentence break yields nothing`() {
        val prefix = "מילים ".repeat(30).trim()
        assertNull(DhExtractor.extract("$prefix - פירוש", Format.DASH))
    }

    // ── BOLD format (Rashi on Tanakh, Mishnah commentaries) ────────────────

    @Test
    fun `bold dibbur is extracted, nikud stripped`() {
        assertEquals(
            "בראשית",
            DhExtractor.extract(
                "<b>בְּרֵאשִׁית.</b> אָמַר רַבִּי יִצְחָק לֹא הָיָה צָרִיךְ",
                Format.BOLD,
            ),
        )
    }

    @Test
    fun `whole-line bold is a decorated heading, not a dibbur`() {
        assertNull(DhExtractor.extract("<b>הדרן עלך מאימתי</b>", Format.BOLD))
    }

    @Test
    fun `structural markers are not dibburim`() {
        assertNull(DhExtractor.extract("<b>מתני'</b> ביצה שנולדה ביום טוב", Format.BOLD))
        assertNull(DhExtractor.extract("<b>גמרא</b> במאי אוקימתא", Format.BOLD))
        assertNull(DhExtractor.extract("<b>(גמרא)</b> במאי אוקימתא", Format.BOLD))
        assertNull(DhExtractor.extract("<b>גמרא!</b> במאי אוקימתא", Format.BOLD))
        assertNull(DhExtractor.extract("<b>שם</b> ד\"ה הורו, עד עפ\"י ב\"ד", Format.BOLD))
        assertNull(DhExtractor.extract("<b>תוד\"ה</b> חייב, בהקפת הראש", Format.BOLD))
        assertNull(DhExtractor.extract("<b>ד״ה</b> הורו, עד עפ״י ב״ד", Format.BOLD))
        assertNull(DhExtractor.extract("<b>בד״ה</b> הורו, עד עפ״י ב״ד", Format.BOLD))
        assertNull(DhExtractor.extract("<b>תוד״ה</b> חייב, בהקפת הראש", Format.BOLD))
        assertNull(DhExtractor.extract("<b>בד''ה</b> אתנו ב''ד", Format.BOLD))
        assertNull(DhExtractor.extract("<b>רשי'</b> פירש כאן", Format.BOLD))
    }

    @Test
    fun `a genuine dibbur that resembles a marker without its quote marks survives`() {
        // תוד"ה is a locator; תודה is a real dibbur (the korban).
        assertEquals(
            "תודה",
            DhExtractor.extract("<b>תודה.</b> הבא תודה על חטאתו", Format.BOLD),
        )
    }

    @Test
    fun `locator-style dibburim of super-commentaries are kept`() {
        // גליון הש"ס: the bold locator IS the searchable dibbur.
        assertEquals(
            "תוס דה חייב וכו בהקפת הראש חייב אף במספרים",
            DhExtractor.extract(
                "<b>תוס' ד\"ה חייב וכו' בהקפת הראש חייב אף במספרים.</b> לפ\"ז נראה דגם במלקט חייב",
                Format.BOLD,
            ),
        )
    }

    @Test
    fun `bold mid-line is not a dibbur`() {
        assertNull(DhExtractor.extract("עיין רע\"ב דאזיל בשטת הר\"ש. <b>וקשיא</b> לי ביה", Format.BOLD))
    }

    // ── shared guards ───────────────────────────────────────────────────────

    @Test
    fun `heading lines never carry a dibbur`() {
        assertNull(DhExtractor.extract("<h2>דף ב.</h2>", Format.DASH))
        assertNull(DhExtractor.extract("<h2>דף ב.</h2>", Format.BOLD))
        // BOM-prefixed heading, as emitted by some source files.
        assertNull(DhExtractor.extract("﻿<h1>רש\"י על ברכות</h1>", Format.BOLD))
    }
}
