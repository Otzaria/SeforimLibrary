package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SefariaAuthorCanonicalNamesTest {

    private val header = "שם בספריא,שם קנוני"

    private fun parse(vararg rows: String) =
        SefariaAuthorCanonicalNames.parse(listOf(header) + rows.toList())

    @Test
    fun rewritesAListedSefariaNameAndLeavesTheRestAlone() {
        val names = parse("יעקב בן יעקב משה מליסא,רבי יעקב בן יעקב משה לורברבוים מליסא")
        assertEquals(
            "רבי יעקב בן יעקב משה לורברבוים מליסא",
            names.canonical("יעקב בן יעקב משה מליסא"),
        )
        assertEquals("חיים בן עטר", names.canonical("חיים בן עטר"))
    }

    @Test
    fun normalizesEvenWhenNoRowMatches() {
        // The mechanical cleanup applies to every name, listed or not.
        assertEquals("חיים בן עטר", SefariaAuthorCanonicalNames.EMPTY.canonical("חיים בן עֲטַר"))
    }

    @Test
    fun aRowWrittenWithNikudStillMatchesAPlainName() {
        // Both sides are normalized on the way in, so the row cannot go dead.
        val names = parse("חיים בן עֲטַר,רבי חיים בן עטר")
        assertEquals("רבי חיים בן עטר", names.canonical("חיים בן עטר"))
    }

    @Test
    fun blankLinesAreIgnored() {
        val names = SefariaAuthorCanonicalNames.parse(
            listOf(header, "", "אבן עזרא,אברהם אבן עזרא", "   "),
        )
        assertEquals(1, names.size)
        assertEquals("אברהם אבן עזרא", names.canonical("אבן עזרא"))
    }

    @Test
    fun missingHeaderIsRejected() {
        val error = assertFailsWith<IllegalArgumentException> {
            SefariaAuthorCanonicalNames.parse(listOf("אבן עזרא,אברהם אבן עזרא"))
        }
        assertTrue("שם בספריא" in error.message.orEmpty())
    }

    @Test
    fun anEmptyFileIsAnEmptyMapNotAFailure() {
        assertEquals(0, SefariaAuthorCanonicalNames.parse(emptyList()).size)
    }

    @Test
    fun aRowMissingItsSecondColumnIsRejected() {
        assertFailsWith<IllegalArgumentException> { parse("אבן עזרא") }
        assertFailsWith<IllegalArgumentException> { parse("אבן עזרא,") }
    }

    @Test
    fun aSelfMappingRowIsRejected() {
        // Almost always a half-finished edit, and it never does anything.
        val error = assertFailsWith<IllegalArgumentException> { parse("אבן עזרא,אבן עזרא") }
        assertTrue("itself" in error.message.orEmpty())
    }

    @Test
    fun conflictingTargetsForOneNameAreRejected() {
        val error = assertFailsWith<IllegalArgumentException> {
            parse("אבן עזרא,אברהם אבן עזרא", "אבן עזרא,אברהם בן מאיר אבן עזרא")
        }
        assertTrue("both" in error.message.orEmpty())
    }

    @Test
    fun aChainOfRenamesIsRejected() {
        // One pass, so 'א->ב' + 'ב->ג' would leave books under both ב and ג
        // depending on row order — exactly the split the file exists to remove.
        val error = assertFailsWith<IllegalArgumentException> {
            parse("שלום מרדכי שבדרון,שלום מרדכי הכהן שבדרון", "שלום מרדכי הכהן שבדרון,מהרש\"ם")
        }
        assertTrue("chains" in error.message.orEmpty())
    }

    @Test
    fun aDuplicateRowWithTheSameTargetIsFine() {
        val names = parse("אבן עזרא,אברהם אבן עזרא", "אבן עזרא,אברהם אבן עזרא")
        assertEquals(1, names.size)
    }
}
