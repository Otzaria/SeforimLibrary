package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertNull

// חלק של ספר שרוב הסעיפים בו נושאים סימון משלהם כותב חלק מהמספרים בצורה אחרת: "טוב."
// ל-17 ו"יוד." ל-10 (קשר גודל), "<b>רן </b>" ל-250 (המפתחות של תרומת הדשן). רק בחלק כזה
// הצורה האחרת מבטלת את ה-"(X) " שנוסף. בכל מקום אחר "אחד." בסעיף י"ג הוא דיבור.
class SefariaVariantLabelPrefixTest {

    private fun readBook(vararg simanim: List<String>): BookPayload =
        readText(SCHEMA_JSON, simanText(*simanim).toString())

    /** A book of two parts, like תרומת הדשן: a body and its index. */
    private fun readParts(body: List<String>, index: List<String>): BookPayload =
        readText(PARTS_SCHEMA_JSON, """{"Body": ${simanText(body)}, "Index": ${simanText(index)}}""")

    private fun simanText(vararg simanim: List<String>) =
        JsonArray(simanim.map { seifim -> JsonArray(seifim.map(::JsonPrimitive)) })

    private fun readText(schemaJson: String, text: String): BookPayload = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve("Variants"))
        Files.writeString(schemaDir.resolve("Variants.json"), schemaJson)
        Files.writeString(
            bookDir.resolve("merged.json"),
            """{"title": "Variants", "heTitle": "בדיקת צורות", "text": $text}""",
        )
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaVariantLabelPrefixTest"),
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)).single()
    }

    /** Seifim 1..[count], each opening "X. " with its own number, except those in [overrides]. */
    private fun labelledSiman(count: Int, overrides: Map<Int, String> = emptyMap()): List<String> =
        (1..count).map { n -> overrides[n] ?: "${toGematria(n)}. סעיף $n" }

    @Test
    fun aBookThatLabelsItsItemsDropsThePrefixBeforeAnotherSpelling() {
        val book = readBook(
            labelledSiman(
                30,
                mapOf(
                    10 to "יוד. מי שאיחר לבא",
                    17 to "טוב. לא יסיח דעתו",
                    18 to "חי. המכין מצעדי גבר",
                    21 to "אך. לא ישיח",
                    27 to "זך. מי שלא התפלל",
                ),
            ),
        )
        val lines = book.lines
        assertContains(lines, "יוד. מי שאיחר לבא")
        assertContains(lines, "טוב. לא יסיח דעתו")
        assertContains(lines, "חי. המכין מצעדי גבר")
        assertContains(lines, "אך. לא ישיח")
        assertContains(lines, "זך. מי שלא התפלל")
        assertContains(lines, "יט. סעיף 19")
        // No generated prefix is left, so the line key and char anchors use the line as is.
        assertNull(book.cleanShiftByLineIndex[lines.indexOf("טוב. לא יסיח דעתו")])
    }

    @Test
    fun anotherSpellingCountsOnlyForItsOwnNumberAndUpToFourLetters() {
        val lines = readBook(
            labelledSiman(
                30,
                mapOf(
                    // "טוב" is 17, not 22.
                    22 to "טוב. שיאמר יהיו לרצון",
                    // Five letters adding up to 19 are a word.
                    19 to "והבאה. לבית המקדש",
                ),
            ),
        ).lines
        assertContains(lines, "(כב) טוב. שיאמר יהיו לרצון")
        assertContains(lines, "(יט) והבאה. לבית המקדש")
    }

    @Test
    fun reorderedAndFinalLettersInBoldLabels() {
        // תרומת הדשן: "<b>רן </b>" for 250 and "<b>דשמ </b>" for 344, among "<b>X </b>" labels.
        val overrides = mapOf(250 to "<b>רן </b>אם מותר", 344 to "<b>דשמ </b>צבור שהוטל")
        val siman = (1..350).map { n -> overrides[n] ?: "<b>${toGematria(n)} </b>שאלה $n" }
        val lines = readBook(siman).lines
        assertContains(lines, "<b>רן </b>אם מותר")
        assertContains(lines, "<b>דשמ </b>צבור שהוטל")
    }

    @Test
    fun aBookWithFewLabelsKeepsThePrefixBeforeAnotherSpelling() {
        // Three own labels among thirty seifim are no pattern: "טוב." at 17 may be a dibbur.
        val siman = (1..30).map { n ->
            when (n) {
                1, 2, 3 -> "${toGematria(n)}. סעיף $n"
                17 -> "טוב. שיאמר"
                else -> "דיבור $n. פירוש"
            }
        }
        val lines = readBook(siman).lines
        assertContains(lines, "א. סעיף 1")
        assertContains(lines, "(יז) טוב. שיאמר")
    }

    @Test
    fun aDibburThatSpellsTheNumberKeepsThePrefixInAnUnlabelledBook() {
        // "אחד." at the thirteenth seif is 13 in letters, and a dibbur.
        val siman = (1..20).map { n -> if (n == 13) "אחד. שאמרו חכמים" else "דיבור $n. פירוש" }
        val lines = readBook(siman).lines
        assertContains(lines, "(יג) אחד. שאמרו חכמים")
    }

    @Test
    fun theGateNeedsBothShareAndCount() {
        // Twenty-five own labels among a hundred seifim are too small a share.
        val sparse = (1..100).map { n ->
            when {
                n == 17 -> "טוב. שיאמר"
                n <= 25 -> "${toGematria(n)}. סעיף $n"
                else -> "דיבור $n. פירוש"
            }
        }
        assertContains(readBook(sparse).lines, "(יז) טוב. שיאמר")
        // Seventeen own labels out of nineteen are too few.
        val short = labelledSiman(19, mapOf(17 to "טוב. שיאמר", 18 to "חי. המכין"))
        val lines = readBook(short).lines
        assertContains(lines, "(יז) טוב. שיאמר")
        assertContains(lines, "(יח) חי. המכין")
    }

    @Test
    fun theGateCountsTheWholeSection() {
        // A short siman with a variant, in a section whose other simanim label themselves.
        val lines = readBook(
            labelledSiman(15),
            labelledSiman(18, mapOf(17 to "טוב. לא יסיח", 18 to "חי. המכין")),
        ).lines
        assertContains(lines, "טוב. לא יסיח")
        assertContains(lines, "חי. המכין")
    }

    @Test
    fun theGateIsPerSection() {
        // תרומת הדשן: the body numbers its paragraphs with no labels of their own, the index
        // labels every entry. The index alone accepts "<b>רן </b>".
        val body = (1..300).map { n -> if (n == 250) "<b>רן </b>שאלה" else "שאלה $n" }
        val index = (1..260).map { n -> if (n == 250) "<b>רן </b>אם מותר" else "<b>${toGematria(n)} </b>דין $n" }
        val lines = readParts(body, index).lines
        assertContains(lines, "<b>רן </b>אם מותר")
        assertContains(lines, "(רנ) <b>רן </b>שאלה")
        assertContains(lines, "(א) שאלה 1")
    }

    companion object {
        private val PARTS_SCHEMA_JSON = """
            {"title": "Variants", "heTitle": "בדיקת צורות",
             "schema": {"title": "Variants", "heTitle": "בדיקת צורות", "key": "Variants", "nodes": [
               {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": ["Siman","Seif"],
                "sectionNames": ["Siman","Seif"], "heSectionNames": ["סימן","סעיף"],
                "title": "Body", "heTitle": "גוף", "key": "Body"},
               {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": ["Siman","Seif"],
                "sectionNames": ["Siman","Seif"], "heSectionNames": ["סימן","סעיף"],
                "title": "Index", "heTitle": "מפתח", "key": "Index"}
             ]}}
        """.trimIndent()

        private val SCHEMA_JSON = """
            {"title": "Variants", "heTitle": "בדיקת צורות",
             "schema": {"nodeType": "JaggedArrayNode", "depth": 2, "addressTypes": ["Siman","Seif"],
               "sectionNames": ["Siman","Seif"], "heSectionNames": ["סימן","סעיף"],
               "title": "Variants", "heTitle": "בדיקת צורות", "key": "Variants"}}
        """.trimIndent()
    }
}
