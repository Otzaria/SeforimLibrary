package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
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
        assertEquals("קובץ שאלות\nותשובות מהמאה ה-16", mahram.heShortDesc)
        // heDesc comes from heDescNew (column index 5), not the original heDesc (index 4).
        assertEquals("new long desc", mahram.heDesc)
        assertEquals("new", map.getValue("בראשית").heDesc)
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
    fun bulk_parsesPubDatesPlaceAndSourcefolder() {
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
        assertEquals("Dicta", a.sourcefolder)
        // missing fields default cleanly
        val b = map.getValue("בלי מקור")
        assertTrue(b.pubDates.isEmpty())
        assertNull(b.pubPlaceHe)
        assertNull(b.sourcefolder)
    }

    // --- resolveSourceFolders: explicit mapping, fail-loud on unknown ---

    private val logger = Logger.withTag("test")

    private fun bulkWith(vararg folders: String?): Map<String, BulkMetadata> =
        folders.mapIndexed { i, f -> "book$i" to BulkMetadata(emptyList(), null, f) }.toMap()

    @Test
    fun sources_mapFolderToDbSourceId() {
        val dbSources = mapOf("dictatootzaria" to 6L, "morebooks" to 12L)
        val resolved = resolveSourceFolders(bulkWith("Dicta", "MoreBooks"), dbSources, logger)
        assertEquals(6L, resolved["Dicta"])
        assertEquals(12L, resolved["MoreBooks"])
    }

    @Test
    fun sources_unmappedFolderIsFatal() {
        val ex = assertFailsWith<IllegalArgumentException> {
            resolveSourceFolders(bulkWith("BrandNewSource"), emptyMap(), logger)
        }
        assertTrue("BrandNewSource" in ex.message!!)
    }

    @Test
    fun sources_mappedButAbsentFromDb_resolvesToNothing() {
        // Sourcefolder is known, but this DB build has no such source row → no entry.
        val resolved = resolveSourceFolders(bulkWith("Dicta"), emptyMap(), logger)
        assertTrue(resolved.isEmpty())
    }

    @Test
    fun sources_everyMappedNameMatchesDbCasing() {
        // Guard the mapping against the real built DB's source names.
        val dbNames = setOf(
            "Ben-YehudaToOtzaria", "DictaToOtzaria", "MoreBooks", "OnYourWayToOtzaria",
            "OraytaToOtzaria", "ToratEmetToOtzaria", "pninimToOtzaria", "tashmaToOtzaria",
            "wikiJewishBooksToOtzaria", "wikisourceToOtzaria",
        ).map { it.lowercase() }.toSet()
        val bulk = bulkWith(*SOURCEFOLDER_TO_DB_SOURCE.keys.toTypedArray())
        val dbSources = dbNames.withIndex().associate { (i, n) -> n to i.toLong() }
        val resolved = resolveSourceFolders(bulk, dbSources, logger)
        assertEquals(SOURCEFOLDER_TO_DB_SOURCE.size, resolved.size)
    }
}
