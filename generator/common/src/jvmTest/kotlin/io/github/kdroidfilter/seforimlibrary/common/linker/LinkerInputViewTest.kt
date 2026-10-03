package io.github.kdroidfilter.seforimlibrary.common.linker

import io.github.kdroidfilter.seforimlibrary.common.linker.LinkerInputView.Mode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LinkerInputViewTest {

    private fun view(content: String, mode: Mode = Mode.PLAIN) = LinkerInputView.linkerContent(mode, content)

    @Test
    fun aLineThatOnlyGainedTheSeifPrefixIsHandedOverAsItWas() {
        // באר היטב חשן משפט line 42 in v29 and in v30.
        val v29 = " <b>כפרן. </b> כמ\"ש בסי' ע\"ט ס\"ט"
        assertEquals(v29, view("(ח) $v29"))
        assertEquals(4, LinkerInputView.strippedPrefixLength(Mode.PLAIN, "(ח) $v29"))
        assertEquals("<small>זאת מצאנו</small>", view("(קכח) <small>זאת מצאנו</small>"))
        // One label only: a second one is real text the linker's own filter still sees.
        assertEquals("[א] <b>יתגבר כארי</b>", view("(א) [א] <b>יתגבר כארי</b>"))
        assertEquals("(א) רצף שבור", view("(א) (א) רצף שבור"))
    }

    @Test
    fun everySefariaBookUsesThePlainView() {
        assertEquals(Mode.PLAIN, LinkerInputView.modeFor("Sefaria", listOf("טקסט")))
        // A tagged label is a Sefaria book's text: v29 handed it over that way.
        assertEquals("<b>(א)</b> טקסט", view("<b>(א)</b> טקסט", Mode.PLAIN))
    }

    @Test
    fun anOtherBookIsStrippedOnlyFromTheThresholdOn() {
        val lines = (1..19).map { "(${hebrew(it)}) סעיף" } + listOf("פתיחה", "(שם) כמבואר")
        assertEquals(Mode.NONE, LinkerInputView.modeFor("DictaToOtzaria", lines))
        assertEquals("(א) סעיף", view("(א) סעיף", Mode.NONE))
        // The twentieth labelled line, plain or tagged, turns the view on.
        assertEquals(Mode.PLAIN_AND_TAGGED, LinkerInputView.modeFor("DictaToOtzaria", lines + "<b>(כ)</b> סעיף"))
        assertEquals(Mode.PLAIN_AND_TAGGED, LinkerInputView.modeFor("MoreBooks", lines + "(כ) סעיף"))
        assertEquals(20, LinkerInputView.MIN_LABELLED_LINES)
    }

    @Test
    fun aLabelWrappedAloneInItsTagsGoesWithItsTags() {
        val tagged = Mode.PLAIN_AND_TAGGED
        // ילקוט הגרשוני, הערות על שות רבי משולם איגרא.
        assertEquals("<b>במג\"א</b> סק\"ו", view("<b>(א)</b> <b>במג\"א</b> סק\"ו", tagged))
        assertEquals("בפ\"ג מה' ציצית", view("<sup style=\"color:blue;\">(א)</sup> בפ\"ג מה' ציצית", tagged))
        assertEquals("טקסט", view("<b><i>(קכח)</i></b> טקסט", tagged))
        assertEquals("", view("<b>(א)</b>", tagged))
        // Not a balanced fragment holding the label alone: left as is.
        assertEquals("<b>(א) נידון</b> השאלה", view("<b>(א) נידון</b> השאלה", tagged))
        assertEquals("<b><i>(א)</b></i> x", view("<b><i>(א)</b></i> x", tagged))
        assertEquals("<b>(שם)</b> x", view("<b>(שם)</b> x", tagged))
        assertEquals("<br/>(א) x", view("<br/>(א) x", tagged))
        // Headings are never touched.
        assertEquals("<h4>(א)</h4>", view("<h4>(א)</h4>", tagged))
    }

    @Test
    fun offsetsIntoAViewMapBackByThePrefixLength() {
        val stored = "<b>(יב)</b> כמ\"ש בסי' ע\"ט"
        val view = view(stored, Mode.PLAIN_AND_TAGGED)
        val strip = LinkerInputView.offsetShift(stored) { it == view }
        assertEquals(stored.length - view.length, strip)
        assertEquals(view.substring(3, 8), stored.substring(3 + strip!!, 8 + strip))
        assertEquals(0, LinkerInputView.offsetShift(stored) { it == stored })
        assertEquals(null, LinkerInputView.offsetShift(stored) { false })
        assertEquals(4, LinkerInputView.offsetShift("(ח) טקסט") { it == "טקסט" })
    }

    private fun hebrew(n: Int): String = listOf(
        "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "יא", "יב", "יג", "יד", "טו", "טז", "יז", "יח", "יט", "כ",
    )[n - 1]

    @Test
    fun theLabelIsRecognisedExactlyAsTheLinkerRecognisesIt() {
        // The cases of LinkerToOtzaria tests/test_verse_marker_anchor.py.
        val end = { s: String -> LinkerInputView.leadingNumeralMarkerEnd(s) }
        assertTrue(end("(קכח) טקסט") > 0)
        assertTrue(end("(ט״ו) טקסט") > 0)
        assertTrue(end("(ט\"ז) טקסט") > 0)
        assertEquals(0, end("(ית) ציטוט")) // ascending, not a numeral
        assertEquals(0, end("(מן) ציטוט")) // final letters are not numerals
        assertEquals(0, end("(שם) כמבואר לעיל"))
        assertEquals(0, end("(ב״י) עיין בבית יוסף"))
        assertEquals(0, end("(יתרו) נאמר בפרשה"))
        assertEquals(0, end("ראה (נח) וגם נח איש צדיק"))
        // Python's \s, including a no-break space, may precede it; the end excludes the space after.
        assertEquals(6, end("  (טו) טקסט"))
        assertEquals(4, end(" (ה)"))
        assertEquals(0, end("(״) טקסט"))
        assertEquals(0, end("(א"))
    }

    @Test
    fun aCountOfLabelledLinesDecidesTheSameViewAsTheLinesThemselves() {
        assertEquals(Mode.PLAIN, LinkerInputView.fixedModeFor("Sefaria"))
        assertEquals(null, LinkerInputView.fixedModeFor("DictaToOtzaria"))
        assertEquals(null, LinkerInputView.fixedModeFor("sefaria"))
        val samples = listOf(
            "(א) סעיף", "<b>(כ)</b> סעיף", "<sup style=\"color:blue;\">(א)</sup> x", " (טו) x", "(א)",
            "(שם) כמבואר", "ראה (נח) שם", "<h4>(א)</h4>", "<b>(א) נידון</b>", "פתיחה", "", "（א） x",
        )
        for (count in listOf(0, 1, 19, 20, 21, 40)) {
            val lines = List(count) { samples[it % 5] } + samples.drop(5)
            assertEquals(count, lines.count(LinkerInputView::isLabelled))
            for (source in listOf("Sefaria", "DictaToOtzaria")) {
                val streamed = LinkerInputView.fixedModeFor(source) ?: LinkerInputView.modeForLabelledLines(count)
                assertEquals(LinkerInputView.modeFor(source, lines), streamed, "$source, $count labelled")
            }
        }
        // A reader that counts may skip every line without the opening parenthesis.
        for (line in samples) {
            if (LinkerInputView.isLabelled(line)) assertTrue(LinkerInputView.LABEL_OPEN in line, line)
        }
    }

    @Test
    fun aLabelWithNothingAfterItLeavesAnEmptyLine() {
        assertEquals("", view("(א)"))
        assertEquals("", view("(א) "))
        assertEquals("טקסט", view("\t(יא) טקסט"))
        assertEquals("טקסט", view("(א)טקסט"))
    }
}
