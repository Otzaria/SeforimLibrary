package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import kotlin.test.Test
import kotlin.test.assertEquals

class LegacyHeRefKeyTest {
    @Test
    fun separatedAndGluedSpellingsShareOneKey() {
        assertEquals(
            "רש\"י על חולין ו., ה, ב",
            legacyHeRefKey("רש\"י על חולין", "רש\"י על חולין, ו., ה, ב"),
        )
        assertEquals(
            "רש\"י על חולין ו., ה, ב",
            legacyHeRefKey("רש\"י על חולין", "רש\"י על חולין ו., ה, ב"),
        )
    }

    @Test
    fun titlesContainingACommaKeepTheirOwnBoundary() {
        assertEquals(
            "שולחן ערוך, אורח חיים יא, ב",
            legacyHeRefKey("שולחן ערוך, אורח חיים", "שולחן ערוך, אורח חיים, יא, ב"),
        )
        assertEquals(
            "שולחן ערוך, אורח חיים יא, ב",
            legacyHeRefKey("שולחן ערוך, אורח חיים", "שולחן ערוך, אורח חיים יא, ב"),
        )
    }

    @Test
    fun unrelatedRefsPassThroughVerbatim() {
        assertEquals("יעד אחר, א", legacyHeRefKey("ספר", "יעד אחר, א"))
        assertEquals("ספר", legacyHeRefKey("ספר", "ספר"))
    }
}
