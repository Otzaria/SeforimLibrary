package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.dh.DhExtractor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SefariaTalmudDibburBoldTest {

    private val bavliRashi = listOf("Talmud", "Bavli", "Rishonim on Talmud", "Rashi", "Seder Zeraim")

    // Verbatim from Rashi on Berakhot 2a in seforim.db.
    private val plain = "עד סוף האשמורה הראשונה – שליש הלילה כדמפרש בגמרא"
    private val firstComment =
        "מאימתי קורין את שמע בערבין. משעה שהכהנים נכנסים לאכול בתרומתן – כהנים שנטמאו וטבלו"

    @Test
    fun `the dibbur before the dash is bolded and the dash is kept`() {
        assertEquals(
            "<b>עד סוף האשמורה הראשונה</b> – שליש הלילה כדמפרש בגמרא",
            SefariaTalmudDibburBold.bold(plain),
        )
    }

    @Test
    fun `a daf's first comment is bolded up to its sentence break`() {
        assertEquals(
            "<b>מאימתי קורין את שמע בערבין</b>. משעה שהכהנים נכנסים לאכול בתרומתן – כהנים שנטמאו וטבלו",
            SefariaTalmudDibburBold.bold(firstComment),
        )
    }

    @Test
    fun `line_dh reads the bolded line exactly as it read the dash line`() {
        for (line in listOf(plain, firstComment, "א\"ר וכו' – פירוש הדברים")) {
            assertEquals(
                DhExtractor.extract(line, DhExtractor.Format.DASH),
                DhExtractor.extract(SefariaTalmudDibburBold.bold(line), DhExtractor.Format.BOLD),
            )
        }
    }

    @Test
    fun `bolding adds no visible chars, so char-level anchors stay valid`() {
        assertEquals(countVisibleChars(plain), countVisibleChars(SefariaTalmudDibburBold.bold(plain)))
    }

    @Test
    fun `lines without a dash dibbur are left alone`() {
        for (line in listOf("<h2>דף ב.</h2>", "שורה רגילה בלי מפריד", "מתני' – פירוש", "<b>כבר</b> – מודגש")) {
            assertEquals(line, SefariaTalmudDibburBold.bold(line))
        }
    }

    @Test
    fun `applies only to Bavli commentaries whose lines mostly open with a dash dibbur`() {
        val lines = listOf("<h2>דף ב.</h2>", plain, firstComment, "שורה בלי מפריד")
        val commentary = Dependence.COMMENTARY
        assertTrue(SefariaTalmudDibburBold.appliesTo(bavliRashi, commentary, lines))
        assertFalse(SefariaTalmudDibburBold.appliesTo(listOf("Tanakh", "Rishonim on Tanakh", "Rashi"), commentary, lines))
        assertFalse(
            SefariaTalmudDibburBold.appliesTo(
                bavliRashi,
                commentary,
                listOf(plain, "שורה אחת", "שורה שנייה", "שורה שלישית"),
            ),
        )
    }

    @Test
    fun `the Gemara's own punctuated dashes are never bolded`() {
        // Verbatim from Bava Kamma 4a in seforim.db.
        val gemara = listOf(
            "הַצַּד הַשָּׁוֶה שֶׁבָּהֶן – שֶׁדַּרְכָּן לְהַזִּיק, וּשְׁמִירָתָן עָלֶיךָ",
            "וּלְרַבִּי אֱלִיעֶזֶר – דִּמְחַיֵּיב אַתּוֹלָדָה בִּמְקוֹם אָב",
        )
        assertFalse(SefariaTalmudDibburBold.appliesTo(listOf("Talmud", "Bavli", "Seder Nezikin"), null, gemara))
    }
    @Test
    fun `coverage includes every declared dependant kind but excludes independent text`() {
        for (dependence in Dependence.entries) {
            assertTrue(SefariaTalmudDibburBold.appliesTo(bavliRashi, dependence, listOf(plain)), dependence.name)
        }
        assertFalse(SefariaTalmudDibburBold.appliesTo(bavliRashi, null, listOf(plain)))
    }

    @Test
    fun `coverage threshold excludes headings and blanks and includes exactly forty percent`() {
        val content = listOf(plain, firstComment, "אחת", "שתיים", "שלוש")
        assertTrue(SefariaTalmudDibburBold.appliesTo(bavliRashi, Dependence.COMMENTARY, content + listOf("", "<h2>דף א</h2>")))
        assertFalse(SefariaTalmudDibburBold.appliesTo(bavliRashi, Dependence.COMMENTARY, content + "ארבע"))
    }

    @Test
    fun `sentence recut cannot cross the dash or rescue an overlong raw dibbur`() {
        val before = "דיבור ".repeat(7).trim()
        val sentenceAfterDash = "$before – פירוש. המשך"
        val bold = SefariaTalmudDibburBold.bold(sentenceAfterDash)
        assertEquals("<b>$before</b> – פירוש. המשך", bold)
        assertEquals(DhExtractor.extract(sentenceAfterDash, DhExtractor.Format.DASH), DhExtractor.extract(bold, DhExtractor.Format.BOLD))
        val overlong = "דיבור ראשון. " + "מילה ".repeat(25) + "– פירוש"
        assertEquals(overlong, SefariaTalmudDibburBold.bold(overlong))
    }

}
