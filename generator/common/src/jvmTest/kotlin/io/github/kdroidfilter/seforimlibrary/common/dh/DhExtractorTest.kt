package io.github.kdroidfilter.seforimlibrary.common.dh

import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor.Format
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** Shapes below are verbatim from a real seforim.db unless noted. */
class DhExtractorTest {

    private fun key(line: String, format: Format): String? = DhExtractor.extract(line, format)?.key

    // ── DASH format (Sefaria Talmud commentaries) ──────────────────────────

    @Test
    fun `dash-separated dibbur is extracted and normalised`() {
        assertEquals(
            "עד סוף האשמורה הראשונה",
            key(
                "עד סוף האשמורה הראשונה – שליש הלילה כדמפרש בגמרא",
                Format.DASH,
            ),
        )
    }

    @Test
    fun `plain hyphen works as separator too`() {
        assertEquals(
            "אי הכי סיפא דקתני שחרית ברישא",
            key(
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
            key(
                "מאימתי קורין את שמע בערבין. משעה שהכהנים נכנסים לאכול בתרומתן – כהנים שנטמאו וטבלו",
                Format.DASH,
            ),
        )
    }

    @Test
    fun `maqaf does not separate a dibbur`() {
        assertNull(key("בית־השלחין שדה שצריך להשקותה", Format.DASH))
    }

    @Test
    fun `a line without a spaced dash yields nothing`() {
        assertNull(key("שורה רגילה בלי מפריד כלל", Format.DASH))
    }

    @Test
    fun `a dash with nothing after it yields nothing`() {
        assertNull(key("עד סוף האשמורה - ", Format.DASH))
    }

    @Test
    fun `an implausibly long dash prefix without a sentence break yields nothing`() {
        val prefix = "מילים ".repeat(30).trim()
        assertNull(key("$prefix - פירוש", Format.DASH))
    }

    // ── BOLD format (Rashi on Tanakh, Mishnah commentaries) ────────────────

    @Test
    fun `bold dibbur is extracted, nikud stripped`() {
        assertEquals(
            "בראשית",
            key(
                "<b>בְּרֵאשִׁית.</b> אָמַר רַבִּי יִצְחָק לֹא הָיָה צָרִיךְ",
                Format.BOLD,
            ),
        )
    }

    @Test
    fun `whole-line bold is a decorated heading, not a dibbur`() {
        assertNull(key("<b>הדרן עלך מאימתי</b>", Format.BOLD))
    }

    @Test
    fun `structural markers are not dibburim`() {
        assertNull(key("<b>מתני'</b> ביצה שנולדה ביום טוב", Format.BOLD))
        assertNull(key("<b>גמרא</b> במאי אוקימתא", Format.BOLD))
        assertNull(key("<b>(גמרא)</b> במאי אוקימתא", Format.BOLD))
        assertNull(key("<b>גמרא!</b> במאי אוקימתא", Format.BOLD))
        assertNull(key("<b>שם</b> ד\"ה הורו, עד עפ\"י ב\"ד", Format.BOLD))
        assertNull(key("<b>תוד\"ה</b> חייב, בהקפת הראש", Format.BOLD))
        assertNull(key("<b>ד״ה</b> הורו, עד עפ״י ב״ד", Format.BOLD))
        assertNull(key("<b>בד״ה</b> הורו, עד עפ״י ב״ד", Format.BOLD))
        assertNull(key("<b>תוד״ה</b> חייב, בהקפת הראש", Format.BOLD))
        assertNull(key("<b>בד''ה</b> אתנו ב''ד", Format.BOLD))
        assertNull(key("<b>רשי'</b> פירש כאן", Format.BOLD))
    }

    @Test
    fun `a genuine dibbur that resembles a marker without its quote marks survives`() {
        // תוד"ה is a locator; תודה is a real dibbur (the korban).
        assertEquals(
            "תודה",
            key("<b>תודה.</b> הבא תודה על חטאתו", Format.BOLD),
        )
    }

    @Test
    fun `locator-style dibburim of super-commentaries are kept`() {
        // גליון הש"ס: the bold locator IS the searchable dibbur.
        assertEquals(
            "תוס דה חייב וכו בהקפת הראש חייב אף במספרים",
            key(
                "<b>תוס' ד\"ה חייב וכו' בהקפת הראש חייב אף במספרים.</b> לפ\"ז נראה דגם במלקט חייב",
                Format.BOLD,
            ),
        )
    }

    @Test
    fun `bold mid-line is not a dibbur`() {
        assertNull(key("עיין רע\"ב דאזיל בשטת הר\"ש. <b>וקשיא</b> לי ביה", Format.BOLD))
    }

    // ── shared guards ───────────────────────────────────────────────────────

    @Test
    fun `heading lines never carry a dibbur`() {
        assertNull(key("<h2>דף ב.</h2>", Format.DASH))
        assertNull(key("<h2>דף ב.</h2>", Format.BOLD))
        // BOM-prefixed heading, as emitted by some source files.
        assertNull(key("﻿<h1>רש\"י על ברכות</h1>", Format.BOLD))
    }

    // ── display form ───────────────────────────────────────────────────────

    @Test
    fun `display keeps points and quote marks, drops the closing period`() {
        assertEquals(
            DhExtractor.Dh(key = "בראשית", display = "בְּרֵאשִׁית"),
            DhExtractor.extract(
                "<b>בְּרֵאשִׁית.</b> אָמַר רַבִּי יִצְחָק לֹא הָיָה צָרִיךְ",
                Format.BOLD,
            ),
        )
        assertEquals(
            DhExtractor.Dh(key = "אר וכו", display = "א\"ר וכו'"),
            DhExtractor.extract("א\"ר וכו' – פירוש הדברים", Format.DASH),
        )
    }

    @Test
    fun `display collapses inner whitespace like the key does`() {
        assertEquals(
            "עד סוף האשמורה",
            DhExtractor.extract("עד  סוף\tהאשמורה – שליש הלילה", Format.DASH)?.display,
        )
    }

    // ── BOLD_LEAD format (Maharsha, Chiddushei Aggadot) ─────────────────────

    @Test
    fun `lead-bold dibbur runs from the bold word to the closing marker`() {
        assertEquals(
            "אין שלום",
            key("<b>אין</b> שלום כו'. הכא ניחא דמשמע ליה דמשפט ושלום איירי", Format.BOLD_LEAD),
        )
        assertEquals(
            "אין שלום",
            DhExtractor.extract("<b>אין</b> שלום כו'. הכא ניחא", Format.BOLD_LEAD)?.display,
        )
    }

    @Test
    fun `lead-bold accepts וכו and וגו with a Hebrew geresh`() {
        assertEquals("וכתיב ואצוה אתכם", key("<b>וכתיב</b> ואצוה אתכם וגו׳. לפי פשוטו", Format.BOLD_LEAD))
        assertEquals("אמר רא למרק ב חזיונות", key("<b>אמר</b> ר\"א למרק ב' חזיונות וכו'. סיפא דקרא", Format.BOLD_LEAD))
    }

    @Test
    fun `lead-bold yields nothing without a marker, or when the marker is too far`() {
        assertNull(key("<b>אין</b> שלום. הכא ניחא דמשמע ליה", Format.BOLD_LEAD))
        assertNull(
            key(
                "<b>אין</b> שלום אמר ה' לרשעים ומשפט ושלום ואמת בשעריכם תשפטו ולא תשקרו כו'. ביאור",
                Format.BOLD_LEAD,
            ),
        )
    }

    @Test
    fun `lead-bold stops at a sentence break before the marker`() {
        // Sha'arei Korban: the bold is the whole dibbur; the כו' belongs to the comment.
        assertNull(key("<b>האיש מקדש.</b> וידא אמר דא כו' דתנן", Format.BOLD_LEAD))
        assertNull(key("<b>רש\"י ד\"ה בסייף.</b> כדי שיהא שמאלו של עובד כוכבים כו' ולא", Format.BOLD_LEAD))
    }

    @Test
    fun `lead-bold needs commentary text after the marker`() {
        assertNull(key("<b>אין</b> שלום כו'.", Format.BOLD_LEAD))
        assertNull(key("<b>אין</b> שלום כו'", Format.BOLD_LEAD))
    }

    @Test
    fun `lead-bold does not fire on a plain bold dibbur`() {
        assertNull(key("<b>מאימתי קורין.</b> משעה שהכהנים נכנסין לאכול", Format.BOLD_LEAD))
    }
}
