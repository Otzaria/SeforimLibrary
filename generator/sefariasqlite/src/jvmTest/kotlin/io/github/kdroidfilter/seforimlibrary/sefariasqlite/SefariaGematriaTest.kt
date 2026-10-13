package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaGematriaTest {

    @Test
    fun numbersBelowTwelveHundredRepeatTav() {
        assertEquals("תתקצט", toGematria(999))
        assertEquals("תתר", toGematria(1000))
        assertEquals("תתרא", toGematria(1001))
        assertEquals("תתרטו", toGematria(1015))
        assertEquals("תתרפה", toGematria(1085))
        assertEquals("תתשצט", toGematria(1199))
    }
}
