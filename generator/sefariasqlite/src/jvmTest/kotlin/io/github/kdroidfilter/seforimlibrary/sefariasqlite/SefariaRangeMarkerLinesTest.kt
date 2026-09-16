package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SefariaRangeMarkerLinesTest {

    @Test
    fun `bare ranges from the real corpus are markers`() {
        // Verbatim from Maggid Mishneh (Fasts 5:3, 2:17), Malbim and Lechem Mishneh in seforim.db.
        for (line in listOf("(א-ג) ", "(טז-יז) ", "(לא-לג) ", "(יח-יט)", "(ב-ג) ")) {
            assertTrue(SefariaRangeMarkerLines.isRangeMarker(line), line)
        }
    }

    @Test
    fun `dash variants, spacing and gershayim are tolerated`() {
        assertTrue(SefariaRangeMarkerLines.isRangeMarker("( א – ג )"))
        assertTrue(SefariaRangeMarkerLines.isRangeMarker("(ט\"ו-ט\"ז)"))
    }

    @Test
    fun `lines with any text beyond the range are not markers`() {
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(א-ג) <b>יש שם וכו'.</b> ארבעת ימי הצומות"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(א) (א-ג)"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("<small>(א-ג)</small>"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(ראה א-ג)"))
    }

    @Test
    fun `single numbers, citations and headings are not markers`() {
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(א) "))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(שם)"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("(1-3)"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker("<h3>הלכה א-ג</h3>"))
        assertFalse(SefariaRangeMarkerLines.isRangeMarker(""))
    }
}
