package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * `authors` (list) is the co-authored form; `author` (single) is what the
 * original metadata dump writes. Both must keep parsing, in either order.
 */
class BookMetadataAuthorsTest {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    @Test
    fun singleAuthorStillParsesAndLeavesAuthorsNull() {
        val m = json.decodeFromString<BookMetadata>(
            """{"title":"אגרת התשובה","author":"שניאור זלמן מליאדי"}"""
        )
        assertEquals("שניאור זלמן מליאדי", m.author)
        assertNull(m.authors)
    }

    @Test
    fun authorsListParsesAndPreservesOrder() {
        val m = json.decodeFromString<BookMetadata>(
            """{"title":"אספת גאונים","authors":["רבי יעקב מלובלין","רבי העשיל מקראקא"]}"""
        )
        assertEquals(listOf("רבי יעקב מלובלין", "רבי העשיל מקראקא"), m.authors)
        assertNull(m.author)
    }

    @Test
    fun bothFieldsMayCoexist() {
        val m = json.decodeFromString<BookMetadata>(
            """{"title":"X","author":"א","authors":["א","ב"]}"""
        )
        assertEquals("א", m.author)
        assertEquals(listOf("א", "ב"), m.authors)
    }

    @Test
    fun recordWithNeitherFieldParses() {
        val m = json.decodeFromString<BookMetadata>("""{"title":"X"}""")
        assertNull(m.author)
        assertNull(m.authors)
    }
}
