package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.sefariasqlite.SefariaTurSeifMarkers.Point
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Otzaria/otzaria#4377: the Shulchan Arukh se'ifim are marked inside the Tur's single-segment simanim.
class SefariaTurSeifMarkersTest {

    private val itag = """<i data-commentator="Beit Yosef" data-order="2.1"></i>"""
    private val line = "נוהגין שלא לישא אשה ${itag}ויש מקומות שנהגו שלא להסתפר ומצאתי כתוב מנהג"

    private fun point(word: String, seif: String, context: String, shift: Int = 0) =
        Point(line.indexOf(word) + shift, seif, context)

    @Test
    fun `marks the start of the siman and a point mid-line before its commentator markers`() {
        val marked = SefariaTurSeifMarkers.insert(
            line,
            listOf(point("נוהגין", "א", "נוהגין שלא לישא"), point("ויש", "ב", "ויש מקומות שנהגו")),
        )
        assertEquals(
            "<b>[סעיף א]</b> נוהגין שלא לישא אשה <b>[סעיף ב]</b> ${itag}ויש מקומות שנהגו שלא להסתפר ומצאתי כתוב מנהג",
            marked,
        )
    }

    @Test
    fun `keeps the Tur's own order and joins se'ifim that start together`() {
        val marked = SefariaTurSeifMarkers.insert(
            line,
            listOf(
                point("ומצאתי", "ד", "ומצאתי כתוב מנהג"),
                point("נוהגין", "ב", "נוהגין שלא לישא"),
                point("נוהגין", "א", "נוהגין שלא לישא"),
            ),
        )
        assertTrue(marked.startsWith("<b>[סעיפים א, ב]</b> נוהגין"), marked)
        assertTrue("<b>[סעיף ד]</b> ומצאתי" in marked, marked)
    }

    @Test
    fun `finds a point whose words moved and drops one whose words are gone`() {
        val moved = SefariaTurSeifMarkers.insert(line, listOf(point("ומצאתי", "ג", "ומצאתי כתוב מנהג", shift = -7)))
        assertTrue("<b>[סעיף ג]</b> ומצאתי" in moved, moved)

        val gone = SefariaTurSeifMarkers.insert(line, listOf(point("ומצאתי", "ג", "ומצאתי כתוב אחר")))
        assertEquals(line, gone)
    }

    @Test
    fun `leaves other books and refs without points untouched`() {
        assertEquals(line, SefariaTurSeifMarkers.apply("שולחן ערוך, אורח חיים", "טור, אורח חיים,  א, א", line))
        assertEquals(line, SefariaTurSeifMarkers.apply("טור", "טור, אורח חיים,  תתתת, א", line))
    }

    @Test
    fun `every resource point names a Tur segment by its heRef, a se'if letter and its following words`() {
        val points = SefariaTurSeifMarkers.loadPoints(SefariaTurSeifMarkers::class.java.classLoader)
        assertTrue(points.isNotEmpty())
        val ref = Regex("""^טור, (אורח חיים|יורה דעה|אבן העזר|חושן משפט), +[א-ת]{1,4}, +[א-ת]{1,3}$""")
        val seif = Regex("""^[א-ת]{1,3}$""")
        for ((r, list) in points) {
            assertTrue(ref.matches(r), r)
            for (p in list) {
                assertTrue(p.offset >= 0 && seif.matches(p.seif) && p.context.isNotBlank(), "$r $p")
            }
        }
    }
}
