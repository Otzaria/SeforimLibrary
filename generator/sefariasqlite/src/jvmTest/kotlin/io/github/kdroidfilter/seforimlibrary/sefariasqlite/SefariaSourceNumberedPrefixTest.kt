package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertTrue

// "(אות) " prefix is suppressed for self-numbered leaf arrays (משנה ברורה) and kept elsewhere.
class SefariaSourceNumberedPrefixTest {

    private fun readLines(): List<String> = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Bdika"))

        Files.writeString(schemaDir.resolve("Bdika.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaSourceNumberedPrefixTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single().lines
    }

    @Test
    fun selfNumberedArraysLoseThePrefixAndOthersKeepIt() {
        val lines = readLines()

        // 1. ממוספר בסוגריים עגולים — בלי prefix
        assertContains(lines, "(א) אחד עגול")
        assertContains(lines, "(ב) שנים עגול")
        // 2. ממוספר בסוגריים מסולסלים — בלי prefix
        assertContains(lines, "{א} אחד מסולסל")
        assertContains(lines, "{ב} שנים מסולסל")
        // 3. הקדמה לא ממוסמנת ואז א,ב (צורת סימן סט) — בלי prefix
        assertContains(lines, "פתיחה בלי מספור")
        assertContains(lines, "(א) אחד אחרי פתיחה")
        // 4. הרצף מתחיל ב-ב' תואם-מיקום (צורת סימן מו) — בלי prefix
        assertContains(lines, "פתיחה עם (א) בפנים")
        assertContains(lines, "(ב) שנים אחרי מיזוג")
        // 5. לא ממוספר — ה-prefix נשאר
        assertContains(lines, "(א) ראשון חשוף")
        assertContains(lines, "(ב) שני חשוף")
        // 6. ציטוט (כה) בפסקה ראשונה — לא רצף תקין, ה-prefix נשאר
        assertContains(lines, "(א) (כה) ציטוט מסימן אחר")
        assertContains(lines, "(ב) המשך אחרי ציטוט")
        // 7. רצף שבור א,ב,ד — ה-prefix נשאר רק במקום שהסימון שבמקור אינו המספר שלו
        assertContains(lines, "(א) רצף שבור")
        assertContains(lines, "(ב) המשך תקין")
        assertContains(lines, "(ג) (ד) קפיצה ברצף")
        // 8. פריט ריק ראשון ואז א,ב — בלי prefix
        assertContains(lines, "(א) אחד אחרי ריק")
        // 9. פסקה בודדת בסימן — ממילא בלי prefix
        assertContains(lines, "{א} פסקה יחידה")
        // 10. סימון בודד באמצע (נהר מצרים) — ה-prefix נשאר
        assertContains(lines, "(ג) (ב) סימון בודד באמצע")
        // 11. שאלה + "(א) תשובה" (שו"ת הר"ן) — גוש של 1, ה-prefix נשאר
        assertContains(lines, "(ב) (א) תשובה לשאלה")
        // 12. מבוא וגם זנב סביב הגוש — ה-prefix נשאר
        assertContains(lines, "(ב) (א) גוש עם זנב ומבוא")
        // 13. הערות-שוליים בסוף (במראה הבזק) — מבוא כפול, ה-prefix נשאר
        assertContains(lines, "(ג) (א) הערת שוליים")

        // אף שורה ממוספרת-במקור לא הוכפלה
        assertTrue(lines.none { it.startsWith("(א) (א) אחד") || it.startsWith("(א) {א}") })
    }

    companion object {
        private val schemaJson = """
            {
              "title": "Bdika",
              "heTitle": "בדיקה",
              "schema": {
                "title": "Bdika",
                "heTitle": "בדיקה",
                "key": "Bdika",
                "nodes": [
                  {
                    "nodeType": "JaggedArrayNode",
                    "depth": 2,
                    "addressTypes": ["Siman", "SeifKatan"],
                    "sectionNames": ["Siman", "Seif Katan"],
                    "heSectionNames": ["סימן", "סעיף קטן"],
                    "title": "",
                    "heTitle": "",
                    "key": "default",
                    "default": true
                  }
                ]
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Bdika",
              "heTitle": "בדיקה",
              "text": {
                "": [
                  ["(א) אחד עגול", "(ב) שנים עגול"],
                  ["{א} אחד מסולסל", "{ב} שנים מסולסל"],
                  ["פתיחה בלי מספור", "(א) אחד אחרי פתיחה", "(ב) שנים אחרי פתיחה"],
                  ["פתיחה עם (א) בפנים", "(ב) שנים אחרי מיזוג", "(ג) שלושה אחרי מיזוג"],
                  ["ראשון חשוף", "שני חשוף"],
                  ["(כה) ציטוט מסימן אחר", "המשך אחרי ציטוט"],
                  ["(א) רצף שבור", "(ב) המשך תקין", "(ד) קפיצה ברצף"],
                  ["", "(א) אחד אחרי ריק", "(ב) שנים אחרי ריק"],
                  ["{א} פסקה יחידה"],
                  ["פסקה רגילה", "פסקה נוספת", "(ב) סימון בודד באמצע", "פסקה אחרונה"],
                  ["שאלה בלי סימון", "(א) תשובה לשאלה"],
                  ["מבוא בלי סימון", "(א) גוש עם זנב ומבוא", "(ב) המשך הגוש", "זנב בלי סימון"],
                  ["גוף התשובה", "עוד פסקה", "(א) הערת שוליים", "(ב) הערה נוספת"]
                ]
              }
            }
        """.trimIndent()
    }
}
