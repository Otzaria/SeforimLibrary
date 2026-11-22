package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Parsing and source-matching for the seedAllMetadata post-process. */
class SeedAllMetadataTest {

    // --- parseForDbCsvRecords: quoted multi-line records ---

    @Test
    fun recordsParser_stitchesQuotedNewlineFields() {
        val text = "h1,h2\n" +
            "\"line one\nline two\",plain\n" +
            "a,b"
        val records = parseForDbCsvRecords(text)
        assertEquals(
            listOf(
                listOf("h1", "h2"),
                listOf("line one\nline two", "plain"),
                listOf("a", "b"),
            ),
            records,
        )
    }

    @Test
    fun recordsParser_handlesCrlfAndBlankLinesAndEscapedQuotes() {
        val text = "a,\"b\r\nc\"\r\n\r\n\"d\"\"e\",f\r\n"
        val records = parseForDbCsvRecords(text)
        assertEquals(
            listOf(
                listOf("a", "b\r\nc"),
                listOf("d\"e", "f"),
            ),
            records,
        )
    }

    // --- parseDescriptionOverrides: real-shape CSV with a multi-line description ---

    @Test
    fun descriptions_readMultilineHeShortDescAndHeDescNew() {
        val lines = listOf(
            "categoryPath,title,author,heShortDesc,heDesc,heDescNew",
            // heShortDesc spans two physical lines (matches the real release data).
            "\"שו\"\"ת/ראשונים\",שות מהרם פדוואה,ר' מאיר,\"קובץ שאלות",
            "ותשובות מהמאה ה-16\",old desc,new long desc",
            "cat,בראשית,author,short,old,new",
        )
        val map = parseDescriptionOverrides(lines)

        assertEquals(2, map.size)
        val mahram = map.getValue("שות מהרם פדוואה")
        assertEquals(DescriptionEdit.Replace("קובץ שאלות\nותשובות מהמאה ה-16"), mahram.shortEdit)
        // heDesc comes from heDescNew (column index 5), not the original heDesc (index 4).
        assertEquals(DescriptionEdit.Replace("new long desc"), mahram.longEdit)
        assertEquals(DescriptionEdit.Replace("new"), map.getValue("בראשית").longEdit)
    }

    @Test
    fun descriptions_deleteMarkerClearsOnlyItsCell() {
        val map = parseDescriptionOverrides(
            listOf(
                "categoryPath,title,author,heShortDesc,heDesc,heDescNew",
                "cat,שני התאים,author,[מחק],old,[מחק]",
                "cat,רק הקצר,author,[מחק],old,new long",
                "cat,רק הארוך,author, ,[מחק],\" [מחק] \"",
                "cat,ריק,author,,[מחק],",
            ),
        )

        assertEquals(Description(DescriptionEdit.Clear, DescriptionEdit.Clear), map.getValue("שני התאים"))
        assertEquals(Description(DescriptionEdit.Clear, DescriptionEdit.Replace("new long")), map.getValue("רק הקצר"))
        assertEquals(Description(longEdit = DescriptionEdit.Clear), map.getValue("רק הארוך"))
        assertEquals(Description(), map.getValue("ריק"))
    }

    @Test
    fun descriptions_requireHeaderRow() {
        assertFailsWith<IllegalArgumentException> {
            parseDescriptionOverrides(listOf("cat,בראשית,author,short,old,new"))
        }
    }

    @Test
    fun descriptions_skipRowsWithoutTitle_andEmptyInput() {
        assertTrue(parseDescriptionOverrides(emptyList()).isEmpty())
        val map = parseDescriptionOverrides(
            listOf(
                "categoryPath,title,author,heShortDesc,heDesc,heDescNew",
                "cat,,author,short,old,new",
            ),
        )
        assertTrue(map.isEmpty())
    }

    // --- parseBulkMetadata: all_metadata.json shape ---

    @Test
    fun bulk_parsesPubDatesAndPlace() {
        val json = """
            [
              {"title":"אחיעזר","pubDate":[1922],"pubPlaceStringHe":"וילנא","Sourcefolder":"Dicta"},
              {"title":"בלי מקור"},
              {"pubDate":[1900]}
            ]
        """.trimIndent()
        val map = parseBulkMetadata(json.lines())

        assertEquals(2, map.size)
        val a = map.getValue("אחיעזר")
        assertEquals(listOf(1922), a.pubDates)
        assertEquals("וילנא", a.pubPlaceHe)
        val b = map.getValue("בלי מקור")
        assertTrue(b.pubDates.isEmpty())
        assertNull(b.pubPlaceHe)
    }
}
