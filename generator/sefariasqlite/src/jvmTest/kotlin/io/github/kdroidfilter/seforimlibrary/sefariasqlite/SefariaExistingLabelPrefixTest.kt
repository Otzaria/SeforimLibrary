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
        // The unlabelled intro is segment א and keeps its generated number.
        assertContains(lines, "(א) <b>פרטי המצות</b>")
        assertContains(lines, "ב) תפלה")
        assertContains(lines, "ג)ק\"ש ערבית")
        assertFalse(lines.any { it.startsWith("(ב) ב)") || it.startsWith("(ג) ג)") })
    }

    @Test
    fun boldBareNumeralAfterAnIntroIsALabel() {
        val lines = readBook().lines
        assertContains(lines, "(א) <big><b>פרקי אבות</b></big>")
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
    fun aSourceRunShiftedByAnIntroKeepsTheGeneratedNumbers() {
        // כף אחת כ"ד: segment ב opens "א." — another number, so nothing is dropped.
        val lines = readBook().lines
        assertContains(lines, "(א) קצת אזהרות לחג הסוכות:")
        assertContains(lines, "(ב) א. יטבול ערב סוכות")
        assertContains(lines, "(ג) ב. ישתדל בסוכה")
        // The same for a shifted run of square-bracket labels.
        assertContains(lines, "(א) <b>שאלה:</b><br>המנהג בקהלתנו")
        assertContains(lines, "(ב) [א] תחלה יש לברר")
    }

    @Test
    fun aLabelRightAfterAnOpeningHeadingCounts() {
        // קשר גודל: "<big><strong>סימן א. …</strong></big><br>א. מיד".
        val lines = readBook().lines
        assertContains(lines, "<big><strong>סימן א. לקום באשמורת.</strong></big><br>א. מיד כשנעור")
        assertContains(lines, "ב. שיעור הטלית")
        // A lone label after a heading, in an array with no other self-labelled item, is a
        // summary list (תלמוד עשר הספירות), not the segment's label.
        assertContains(lines, "(א) <b><big>מבאר ד' בחינות. ובו ח' ענינים:</big></b><br><small>א. בכל העולמות</small>")
        // A bold dibbur with its comment is no heading: the later label is another segment's.
        assertContains(lines, "(א) <b>פלגש כו'. </b> דכאן ל\"ל<br> <b>א) י\"א כו'</b>")
    }

    @Test
    fun dibburLookalikesKeepThePrefix() {
        val lines = readBook().lines
        // באר היטב: a bold dibbur that starts with a number is not a label.
        assertContains(lines, "(א) <b>א' פעמים. </b> ב\"ח")
        assertContains(lines, "(ב) <b>ב' פעמים. </b> ומ\"א")
        assertContains(lines, "(ג) <b>ט'. </b> וכתב בש\"ך")
        assertContains(lines, "(ד) לא יקשור הקמיע")
        // ביאור הגר"א חו"מ ז:יא: "<b>י"א. </b>" is "יש אומרים" at the eleventh place, and
        // no other item of the array carries a label of its own.
        assertContains(lines, "(יא) <b>י\"א. </b> שבת נ\"ו ב':")
        // מעדני יום טוב: "<b>  לא  </b> פלוג" at the thirty-first place.
        assertContains(lines, "(לא) <b>  לא  </b> פלוג באדם כו'.")
    }

    @Test
    fun moreLabelShapesAreRecognised() {
        val lines = readBook().lines
        // תרומת הדשן "<b>א </b>טעם", אורחות חיים להרא"ש "<b>א.</b>", נהר מצרים "( א )",
        // אליה רבה קסב:ה "[אות ה]".
        assertContains(lines, "<b>א </b>טעם למה אנו נוהגין")
        assertContains(lines, "<b>ב </b>אם רשאי לענות")
        assertContains(lines, "<b>ג.</b> לְהִתְרַחֵק מִן הַגַּאֲוָה")
        assertContains(lines, "( ד )")
        assertContains(lines, "[אות ה] <b>וכן המטביל וכו'.</b>")
        // "סעיף א" names the Shulchan Arukh seif a commentary is on, not this line.
        assertContains(lines, "(א) <b>סעיף א</b> עיין מ\"ש")
        assertContains(lines, "(ב) <b>סעיף ב</b> שם")
    }

    @Test
    fun aPlainLabelCountsEvenAloneInItsArray() {
        // פסקי חלה: "א. אבאר" is the only labelled item of its array, and still a label.
        val lines = readBook().lines
        assertContains(lines, "א. אבאר מאימתי החלה ניטלת")
        assertContains(lines, "(ב) המשך בלי סימון")
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
               ${node("Shifted", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Heading", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Dibbur", """["Siman","Seif"]""", """["סימן","סעיף קטן"]""")},
               ${node("Shapes", """["Siman","Seif"]""", """["סימן","סעיף"]""")},
               ${node("Lonely", """["Siman","Seif"]""", """["סימן","סעיף"]""")}
             ]}}
        """.trimIndent()

        private val mergedJson = """
            {"title": "Labels", "heTitle": "בדיקת סימונים", "text": {
              "Square": [["[א] <b>יתגבר כארי וכו'.</b> כתבו", "[ב] <b>בבוקר וכו'.</b> שומרים"]],
              "Paren": [["<b>פרטי המצות</b>", "ב) תפלה", "ג)ק\"ש ערבית"]],
              "Bold": [["<big><b>פרקי אבות</b></big>", "<b>ב</b> שמעון הצדיק", "<b>ג</b> אנטיגנוס"]],
              "Dotted": [["א. אמר שמואל", "ב. מיד יטול"], ["[א]: תשובה ראשונה", "[ב]: תשובה שנייה"]],
              "Partial": [["פתיחה בלי סימון", "[ב] שני ממוספר", "גוף בלי סימון", "[ד] רביעי ממוספר", "[ח] סימון של מספר אחר"]],
              "Shifted": [["קצת אזהרות לחג הסוכות:", "א. יטבול ערב סוכות", "ב. ישתדל בסוכה"], ["<b>שאלה:</b><br>המנהג בקהלתנו", "[א] תחלה יש לברר", "[ב] אמנם"]],
              "Heading": [["<big><strong>סימן א. לקום באשמורת.</strong></big><br>א. מיד כשנעור", "ב. שיעור הטלית"], ["<b>פלגש כו'. </b> דכאן ל\"ל<br> <b>א) י\"א כו'</b>", "<b>שני כו'. </b> טעם"], ["<b><big>מבאר ד' בחינות. ובו ח' ענינים:</big></b><br><small>א. בכל העולמות</small>", "<small>תקון המסך</small>"]],
              "Shapes": [["<b>א </b>טעם למה אנו נוהגין", "<b>ב </b>אם רשאי לענות", "<b>ג.</b> לְהִתְרַחֵק מִן הַגַּאֲוָה", "( ד )", "[אות ה] <b>וכן המטביל וכו'.</b>"], ["<b>סעיף א</b> עיין מ\"ש", "<b>סעיף ב</b> שם"]],
              "Lonely": [["א. אבאר מאימתי החלה ניטלת", "המשך בלי סימון"]],
              "Dibbur": [["<b>א' פעמים. </b> ב\"ח", "<b>ב' פעמים. </b> ומ\"א", "<b>ט'. </b> וכתב בש\"ך", "לא יקשור הקמיע"], ["<b>אבל. </b> א", "<b>וכן. </b> ב", "<b>ועוד. </b> ג", "<b>שם. </b> ד", "<b>ומה. </b> ה", "<b>וכו. </b> ו", "<b>אבל. </b> ז", "<b>וכן. </b> ח", "<b>ועוד. </b> ט", "<b>אבל. </b> י", "<b>י\"א. </b> שבת נ\"ו ב':", "<b>והביא ב' שערות. </b>"], ["<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>דיבור. </b> כו", "<b>  לא  </b> פלוג באדם כו'."]]
            }}
        """.trimIndent()
    }
}
