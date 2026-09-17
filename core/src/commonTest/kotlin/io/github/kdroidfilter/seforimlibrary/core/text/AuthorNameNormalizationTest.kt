package io.github.kdroidfilter.seforimlibrary.core.text

import kotlin.test.Test
import kotlin.test.assertEquals

class AuthorNameNormalizationTest {

    // --- the two real splits this exists to close ---

    @Test
    fun nikudSpellingCollapsesOntoThePlainOne() {
        // Dicta's ראשון לציון volumes vs Sefaria's — one man, two author rows.
        assertEquals("חיים בן עטר", normalizeAuthorName("חיים בן עֲטַר"))
    }

    @Test
    fun trailingBidiMarkCollapsesOntoThePlainName() {
        assertEquals("מנחם מנדל שניאורסון", normalizeAuthorName("מנחם מנדל שניאורסון‏"))
        assertEquals("ישראל ליפשיץ", normalizeAuthorName("ישראל ליפשיץ‎‎"))
    }

    @Test
    fun doubledSpaceCollapses() {
        assertEquals("עקיבא איגר", normalizeAuthorName("עֲקִיבָא  אֵיגֶר"))
    }

    // --- what it must NOT touch ---

    @Test
    fun maqafSurvives() {
        // U+05BE sits between the two nikud ranges on purpose: it joins a name.
        assertEquals("ברוך תאומים־פרנקל", normalizeAuthorName("ברוך תאומים־פרנקל"))
    }

    @Test
    fun gershayimIsNotFoldedIntoAsciiQuote() {
        assertEquals("רבנו דוד ב\"ר יוסף קמחי", normalizeAuthorName("רבנו דוד ב\"ר יוסף קמחי"))
        assertEquals("רבנו הרמח״ל", normalizeAuthorName("רבנו הרמח״ל"))
    }

    @Test
    fun aDifferentSpellingStaysADifferentName() {
        // Merging these is a human decision, and lives in the ForDB CSV.
        assertEquals("יעקב בן יעקב משה מליסא", normalizeAuthorName("יעקב בן יעקב משה מליסא"))
    }

    @Test
    fun alreadyCleanNamesAreReturnedUnchanged() {
        assertEquals("חיים בן עטר", normalizeAuthorName("חיים בן עטר"))
        assertEquals("", normalizeAuthorName("   "))
    }

    @Test
    fun nonBreakingSpaceIsRealWhitespace() {
        assertEquals("יוסף ענגיל", normalizeAuthorName("יוסף ענגיל"))
    }

    @Test
    fun isIdempotent() {
        val once = normalizeAuthorName("עֲקִיבָא  אֵיגֶר‏")
        assertEquals(once, normalizeAuthorName(once))
    }
}
