package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaDashlessDibburimTest {

    private val book = "תוספות על סוכה"

    @Test
    fun `first sentence becomes the dibbur, separated by a spaced en dash`() {
        // Verbatim from Tosafot on Sukkah 2a in seforim.db.
        assertEquals(
            "מאי שנא גבי סוכה דתני פסולה ומאי שנא גבי מבוי דתני תקנתא – והא דלא פריך מאי שנא גבי הדס",
            SefariaDashlessDibburim.separate(
                book,
                "מאי שנא גבי סוכה דתני פסולה ומאי שנא גבי מבוי דתני תקנתא. והא דלא פריך מאי שנא גבי הדס",
            ),
        )
    }

    @Test
    fun `only the first period is touched`() {
        assertEquals(
            "אמר רבה – קצת קשה. ותירץ דאין הכי נמי.",
            SefariaDashlessDibburim.separate(book, "אמר רבה. קצת קשה. ותירץ דאין הכי נמי."),
        )
    }

    @Test
    fun `a line that already has a dash is left alone`() {
        val line = "סוכה דאורייתא תני פסולה – פרש\"י דשייך למיתני בה לשון פסול. ועוד"
        assertEquals(line, SefariaDashlessDibburim.separate(book, line))
    }

    @Test
    fun `a long first sentence is commentary, not a dibbur`() {
        val line = "ואם תאמר מאי שנא גבי סוכה דתני פסולה ומאי שנא גבי מבוי דתני תקנתא והא דלא פריך. ויש לומר"
        assertEquals(line, SefariaDashlessDibburim.separate(book, line))
    }

    @Test
    fun `headings, tagged prefixes and lines without a sentence break are left alone`() {
        assertEquals("<h2>דף ב.</h2>", SefariaDashlessDibburim.separate(book, "<h2>דף ב.</h2>"))
        assertEquals("<b>תוספות</b>. ביאור", SefariaDashlessDibburim.separate(book, "<b>תוספות</b>. ביאור"))
        assertEquals("תוספות", SefariaDashlessDibburim.separate(book, "תוספות"))
        assertEquals("אמר רבה. ", SefariaDashlessDibburim.separate(book, "אמר רבה. "))
    }

    @Test
    fun `structural markers from the real corpus are not rewritten as dibburim`() {
        // Verbatim from Tosafot on Bava Batra 139b and 163b in seforim.db.
        val mishnah = "מתני'. והבנות יזונו. מה שהזכיר רשב\"ם"
        val gloss = "(הג\"ה. שיטה ומחצה. נראה ליישב כגון שחתומים עדים"
        assertEquals(mishnah, SefariaDashlessDibburim.separate("תוספות על בבא בתרא", mishnah))
        assertEquals(gloss, SefariaDashlessDibburim.separate("תוספות על בבא בתרא", gloss))
    }

    @Test
    fun `books outside the list are never changed`() {
        val line = "מאימתי קורין. משעה שהכהנים נכנסין לאכול"
        assertEquals(line, SefariaDashlessDibburim.separate("רש\"י על ברכות", line))
    }
}
