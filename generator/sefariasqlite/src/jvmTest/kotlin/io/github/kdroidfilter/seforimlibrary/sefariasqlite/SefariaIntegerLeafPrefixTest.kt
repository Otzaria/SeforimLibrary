package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse

// "Integer" leaves get "(אות) " by their Hebrew name; paragraph/comment/line/blank names stay bare.
class SefariaIntegerLeafPrefixTest {

    private fun readBook(): BookPayload = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve("IntLeaf"))
        Files.writeString(schemaDir.resolve("IntLeaf.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaIntegerLeafPrefixTest")
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)).single()
    }

    @Test
    fun integerLeafWithRealUnitNameIsNumbered() {
        val book = readBook()
        assertContains(book.lines, "(א) פסוק ראשון")
        assertContains(book.lines, "(ב) פסוק שני")
        // Generated prefix is recorded, so the line key and char anchors see the raw segment.
        val idx = book.lines.indexOf("(ב) פסוק שני")
        assertEquals(4, book.cleanShiftByLineIndex[idx])
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash("פסוק שני"),
            requireNotNull(book.precomputed).lineKeyHashes!![idx],
        )
    }

    @Test
    fun paragraphCommentLineAndBlankIntegerLeavesStayBare() {
        val lines = readBook().lines
        listOf("פסקה", "פירוש", "פרשנות", "שורה", "מדרש", "קטע").forEach { name ->
            assertContains(lines, "$name ראשון")
            assertContains(lines, "$name שני")
            assertFalse(lines.any { it.startsWith("(") && it.endsWith("$name ראשון") }, name)
        }
    }

    @Test
    fun nonIntegerLeafIsUnchanged() {
        val lines = readBook().lines
        assertContains(lines, "(א) סעיף ראשון")
        assertContains(lines, "(ב) סעיף שני")
    }

    @Test
    fun sourceNumberedIntegerLeafIsNotDoubled() {
        val lines = readBook().lines
        assertContains(lines, "(א) ממוספר ראשון")
        assertContains(lines, "(ב) ממוספר שני")
        assertFalse(lines.any { it.startsWith("(א) (א)") })
    }

    companion object {
        private fun node(title: String, types: String, en: String, he: String) = """
            {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": $types,
             "sectionNames": $en, "heSectionNames": $he,
             "title": "$title", "heTitle": "$title", "key": "$title"}
        """

        private val schemaJson = """
            {"title": "IntLeaf", "heTitle": "בדיקת עלה",
             "schema": {"title": "IntLeaf", "heTitle": "בדיקת עלה", "key": "IntLeaf", "nodes": [
               ${node("Pasuk", """["Integer","Integer"]""", """["Chapter","Verse"]""", """["פרק","פסוק"]""")},
               ${node("Paragraph", """["Integer","Integer"]""", """["Chapter","Paragraph"]""", """["פרק","פסקה"]""")},
               ${node("Comment", """["Integer","Integer"]""", """["Chapter","Comment"]""", """["פרק","פירוש"]""")},
               ${node("Parshanut", """["Integer","Integer"]""", """["Chapter","Parshanut"]""", """["פרק","פרשנות"]""")},
               ${node("Line", """["Integer","Integer"]""", """["Chapter","Line"]""", """["פרק","שורה"]""")},
               ${node("Midrash", """["Integer","Integer"]""", """["Chapter","Midrash"]""", """["פרק","מדרש"]""")},
               ${node("Blank", """["Integer","Integer"]""", """["Chapter","Segment"]""", """["פרק",""]""")},
               ${node("Seif", """["Siman","Seif"]""", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Numbered", """["Integer","Integer"]""", """["Chapter","Verse"]""", """["פרק","פסוק"]""")}
             ]}}
        """.trimIndent()

        private val mergedJson = """
            {"title": "IntLeaf", "heTitle": "בדיקת עלה", "text": {
              "Pasuk": [["פסוק ראשון", "פסוק שני"]],
              "Paragraph": [["פסקה ראשון", "פסקה שני"]],
              "Comment": [["פירוש ראשון", "פירוש שני"]],
              "Parshanut": [["פרשנות ראשון", "פרשנות שני"]],
              "Line": [["שורה ראשון", "שורה שני"]],
              "Midrash": [["מדרש ראשון", "מדרש שני"]],
              "Blank": [["קטע ראשון", "קטע שני"]],
              "Seif": [["סעיף ראשון", "סעיף שני"]],
              "Numbered": [["(א) ממוספר ראשון", "(ב) ממוספר שני"]]
            }}
        """.trimIndent()
    }
}
