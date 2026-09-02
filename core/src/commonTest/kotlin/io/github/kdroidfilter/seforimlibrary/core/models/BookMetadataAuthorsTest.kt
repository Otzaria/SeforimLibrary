package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class BookMetadataAuthorsTest {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private fun parse(s: String) = json.decodeFromString<BookMetadata>(s)

    // --- parsing: both shapes must keep working ---

    @Test
    fun singleAuthorStillParsesAndLeavesAuthorsNull() {
        val m = parse("""{"title":"אגרת התשובה","author":"שניאור זלמן מליאדי"}""")
        assertEquals("שניאור זלמן מליאדי", m.author)
        assertNull(m.authors)
    }

    @Test
    fun authorsListParses() {
        val m = parse("""{"title":"אספת גאונים","authors":["רבי יעקב מלובלין","רבי העשיל מקראקא"]}""")
        assertEquals(listOf("רבי יעקב מלובלין", "רבי העשיל מקראקא"), m.authors)
        assertNull(m.author)
    }

    // --- resolveAuthorNames: the logic the generator actually depends on ---

    @Test
    fun resolvesSingleAuthor() {
        assertEquals(listOf("א"), parse("""{"title":"X","author":"א"}""").resolveAuthorNames())
    }

    @Test
    fun resolvesListInOrder() {
        assertEquals(
            listOf("א", "ב", "ג"),
            parse("""{"title":"X","authors":["א","ב","ג"]}""").resolveAuthorNames(),
        )
    }

    @Test
    fun listWinsOverSingle() {
        assertEquals(
            listOf("ב", "ג"),
            parse("""{"title":"X","author":"א","authors":["ב","ג"]}""").resolveAuthorNames(),
        )
    }

    /** An empty `authors` must not erase a good `author`. */
    @Test
    fun emptyListFallsBackToSingle() {
        assertEquals(listOf("א"), parse("""{"title":"X","author":"א","authors":[]}""").resolveAuthorNames())
    }

    /** Same for a list that holds only blanks. */
    @Test
    fun allBlankListFallsBackToSingle() {
        assertEquals(
            listOf("א"),
            parse("""{"title":"X","author":"א","authors":["","  "]}""").resolveAuthorNames(),
        )
    }

    @Test
    fun blanksAreDroppedFromAMixedList() {
        assertEquals(
            listOf("א", "ב"),
            parse("""{"title":"X","authors":["א","","  ","ב"]}""").resolveAuthorNames(),
        )
    }

    @Test
    fun namesAreTrimmed() {
        assertEquals(listOf("א"), parse("""{"title":"X","author":"  א  "}""").resolveAuthorNames())
        assertEquals(listOf("א", "ב"), parse("""{"title":"X","authors":[" א ","ב "]}""").resolveAuthorNames())
    }

    @Test
    fun duplicatesAreCollapsed() {
        assertEquals(
            listOf("א", "ב"),
            parse("""{"title":"X","authors":["א","ב","א"," א "]}""").resolveAuthorNames(),
        )
    }

    @Test
    fun blankSingleAuthorResolvesToNothing() {
        assertEquals(emptyList(), parse("""{"title":"X","author":"   "}""").resolveAuthorNames())
    }

    @Test
    fun noAuthorFieldsResolvesToNothing() {
        assertEquals(emptyList(), parse("""{"title":"X"}""").resolveAuthorNames())
    }

    /**
     * A comma is part of a catalogue-order name, never a separator — 95 existing
     * records rely on this.
     */
    @Test
    fun commaInsideANameIsNotASeparator() {
        assertEquals(
            listOf("אלגאזי, ישראל יעקב בן יום טוב"),
            parse("""{"title":"X","author":"אלגאזי, ישראל יעקב בן יום טוב"}""").resolveAuthorNames(),
        )
    }
}
