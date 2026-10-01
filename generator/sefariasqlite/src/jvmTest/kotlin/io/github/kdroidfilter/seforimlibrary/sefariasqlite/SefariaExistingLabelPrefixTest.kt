package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertNull

// לא מוסיפים "(אות) " לשורה שכבר נפתחת בסימון משלה לאותו מספר, בכל צורות הסימון
// שבספריא: [א] (אליה רבה), א) (ביאור רס"ג), <b>ב</b> (פרקי אבות בסידור ספרד), א. ו-[ב]:.
class SefariaExistingLabelPrefixTest {

    private fun readBook(): BookPayload = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve("Labels"))
        Files.writeString(schemaDir.resolve("Labels.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaExistingLabelPrefixTest")
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)).single()
    }

    @Test
    fun squareBracketLabelsAreNotDoubled() {
        val book = readBook()
        assertContains(book.lines, "[א] <b>יתגבר כארי וכו'.</b> כתבו")
        assertContains(book.lines, "[ב] <b>בבוקר וכו'.</b> שומרים")
        assertFalse(book.lines.any { it.startsWith("(א) [א]") || it.startsWith("(ב) [ב]") })
        // No generated prefix, so the line key and char anchors use the line as is.
        val idx = book.lines.indexOf("[ב] <b>בבוקר וכו'.</b> שומרים")
        assertNull(book.cleanShiftByLineIndex[idx])
    }

    @Test
    fun bareParenthesisLabelsAreNotDoubledEvenWhenGlued() {
        val lines = readBook().lines
        assertContains(lines, "<b>פרטי המצות</b>")
        assertContains(lines, "ב) תפלה")
        assertContains(lines, "ג)ק\"ש ערבית")
        assertFalse(lines.any { it.startsWith("(ב) ב)") || it.startsWith("(ג) ג)") || it.startsWith("(א) <b>פרטי") })
    }

    @Test
    fun boldBareNumeralAfterAnIntroIsALabel() {
        val lines = readBook().lines
        assertContains(lines, "<big><b>פרקי אבות</b></big>")
        assertContains(lines, "<b>ב</b> שמעון הצדיק")
        assertContains(lines, "<b>ג</b> אנטיגנוס")
    }

    @Test
    fun dottedAndColonLabelsAreNotDoubled() {
        val lines = readBook().lines
        assertContains(lines, "א. אמר שמואל")
        assertContains(lines, "ב. מיד יטול")
        assertContains(lines, "[א]: תשובה ראשונה")
        assertContains(lines, "[ב]: תשובה שנייה")
    }

    @Test
    fun partiallyNumberedArrayDropsOnlyTheMatchingSelfLabels() {
        val lines = readBook().lines
        // Same number as the generated prefix: the item keeps its own label alone.
        assertContains(lines, "[ב] שני ממוספר")
        assertContains(lines, "[ד] רביעי ממוספר")
        // Unlabelled items, and a label for another number, keep the generated prefix.
        assertContains(lines, "(א) פתיחה בלי סימון")
        assertContains(lines, "(ג) גוף בלי סימון")
        assertContains(lines, "(ה) [ח] סימון של מספר אחר")
    }

    @Test
    fun dibburLookalikesKeepThePrefix() {
        val lines = readBook().lines
        // באר היטב: a bold dibbur that starts with a number is not a label.
        assertContains(lines, "(א) <b>א' פעמים. </b> ב\"ח")
        assertContains(lines, "(ב) <b>ב' פעמים. </b> ומ\"א")
        assertContains(lines, "(ג) <b>ט'. </b> וכתב בש\"ך")
        assertContains(lines, "(ד) לא יקשור הקמיע")
    }

    companion object {
        private fun node(title: String, types: String, he: String) = """
            {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": $types,
             "sectionNames": ["Siman","Seif"], "heSectionNames": $he,
             "title": "$title", "heTitle": "$title", "key": "$title"}
        """

        private val schemaJson = """
            {"title": "Labels", "heTitle": "בדיקת סימונים",
             "schema": {"title": "Labels", "heTitle": "בדיקת סימונים", "key": "Labels", "nodes": [
               ${node("Square", """["Siman","Integer"]""", """["סימן","סעיף קטן"]""")},
               ${node("Paren", """["Integer","Integer"]""", """["פרק","מצוה"]""")},
               ${node("Bold", """["Integer","Integer"]""", """["פרק","משנה"]""")},
               ${node("Dotted", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Partial", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Dibbur", """["Siman","Seif"]""", """["סימן","סעיף קטן"]""")}
             ]}}
        """.trimIndent()

        private val mergedJson = """
            {"title": "Labels", "heTitle": "בדיקת סימונים", "text": {
              "Square": [["[א] <b>יתגבר כארי וכו'.</b> כתבו", "[ב] <b>בבוקר וכו'.</b> שומרים"]],
              "Paren": [["<b>פרטי המצות</b>", "ב) תפלה", "ג)ק\"ש ערבית"]],
              "Bold": [["<big><b>פרקי אבות</b></big>", "<b>ב</b> שמעון הצדיק", "<b>ג</b> אנטיגנוס"]],
              "Dotted": [["א. אמר שמואל", "ב. מיד יטול"], ["[א]: תשובה ראשונה", "[ב]: תשובה שנייה"]],
              "Partial": [["פתיחה בלי סימון", "[ב] שני ממוספר", "גוף בלי סימון", "[ד] רביעי ממוספר", "[ח] סימון של מספר אחר"]],
              "Dibbur": [["<b>א' פעמים. </b> ב\"ח", "<b>ב' פעמים. </b> ומ\"א", "<b>ט'. </b> וכתב בש\"ך", "לא יקשור הקמיע"]]
            }}
        """.trimIndent()
    }
}
