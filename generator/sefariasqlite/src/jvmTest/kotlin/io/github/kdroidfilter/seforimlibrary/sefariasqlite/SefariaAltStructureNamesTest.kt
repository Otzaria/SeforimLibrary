package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaAltStructureNamesTest {
    @Test
    fun `structures that repeat the book title are named by their key`() {
        // Zohar: Sefaria labels both Daf and Essay "ספר הזהר".
        assertEquals("דפים", SefariaAltStructureNames.heTitle("Daf", "ספר הזהר", "ספר הזהר"))
        assertEquals("מאמרים", SefariaAltStructureNames.heTitle("Essay", "ספר הזהר", "ספר הזהר"))
        assertEquals("דפוס ונציה", SefariaAltStructureNames.heTitle("Venice", null, "תלמוד ירושלמי ברכות"))
    }

    @Test
    fun `a structure title of its own is kept`() {
        assertEquals("ספר ראשון", SefariaAltStructureNames.heTitle("Book", "ספר ראשון", "תהילים"))
    }

    @Test
    fun `an unknown key keeps what Sefaria gave`() {
        assertEquals("ספר הזהר", SefariaAltStructureNames.heTitle("Mystery", "ספר הזהר", "ספר הזהר"))
    }
}
