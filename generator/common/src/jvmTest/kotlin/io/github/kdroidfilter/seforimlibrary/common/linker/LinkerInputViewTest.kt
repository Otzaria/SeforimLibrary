package io.github.kdroidfilter.seforimlibrary.common.linker

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LinkerInputViewTest {

    private fun view(content: String, source: String = "Sefaria") = LinkerInputView.linkerContent(source, content)

    @Test
    fun aLineThatOnlyGainedTheSeifPrefixIsHandedOverAsItWas() {
        // באר היטב חשן משפט line 42 in v29 and in v30.
        val v29 = " <b>כפרן. </b> כמ\"ש בסי' ע\"ט ס\"ט"
        assertEquals(v29, view("(ח) $v29"))
        assertEquals(4, LinkerInputView.strippedPrefixLength("Sefaria", "(ח) $v29"))
        assertEquals("<small>זאת מצאנו</small>", view("(קכח) <small>זאת מצאנו</small>"))
        // One label only: a second one is real text the linker's own filter still sees.
        assertEquals("[א] <b>יתגבר כארי</b>", view("(א) [א] <b>יתגבר כארי</b>"))
        assertEquals("(א) רצף שבור", view("(א) (א) רצף שבור"))
    }

    @Test
    fun onlySefariaLinesAreStripped() {
        assertEquals("(א) טקסט", view("(א) טקסט", source = "DictaToOtzaria"))
        assertEquals(0, LinkerInputView.strippedPrefixLength("MoreBooks", "(א) טקסט"))
    }

    @Test
    fun theLabelIsRecognisedExactlyAsTheLinkerRecognisesIt() {
        // The cases of LinkerToOtzaria tests/test_verse_marker_anchor.py.
        val end = LinkerInputView::leadingNumeralMarkerEnd
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
    fun aLabelWithNothingAfterItLeavesAnEmptyLine() {
        assertEquals("", view("(א)"))
        assertEquals("", view("(א) "))
        assertEquals("טקסט", view("\t(יא) טקסט"))
        assertEquals("טקסט", view("(א)טקסט"))
    }
}
