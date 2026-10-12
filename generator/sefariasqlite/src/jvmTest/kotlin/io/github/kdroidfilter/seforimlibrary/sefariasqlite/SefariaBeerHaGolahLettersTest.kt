package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Regression for Otzaria/otzaria#1979: the Shulchan Arukh shows Be'er HaGolah's own
 * letters (Sefaria's itag `data-label`), which skip the unlettered notes ("•") and run
 * through the alphabet (… י, כ, ל …). A generated "(N)" numbers every note, so the
 * note the Shulchan Arukh letters ב read "(ג)".
 */
class SefariaBeerHaGolahLettersTest {
    @Test
    fun notesShowNoLetterOtherThanTheOneOnTheShulchanArukh() {
        val notes = listOf("מגילה דף כ\"ח", "לאפוקי מגירסת הרא\"ש", "כך משמע שם בברכות")
        val payload = readBook(ORACH_CHAYIM, "באר הגולה על שולחן ערוך אורח חיים", notes)

        val noteByOrder = payload.refEntries.associate {
            it.ref.substringAfterLast(':').toInt() to payload.lines[it.lineIndex - 1]
        }
        for (itag in SEIFIM.flatMap(::parseInlineItags)) {
            val note = noteByOrder.getValue(itag.order!!)
            val shown = GENERATED_LABEL.find(note)?.groupValues?.get(1)
            assertTrue(
                shown == null || shown == itag.label,
                "the Shulchan Arukh letters it ${itag.label}, the note reads \"$note\"",
            )
        }
        assertEquals(notes, payload.lines.drop(2))
        // The notes keep their line ids: the key always hashed a note without its prefix.
        val keys = requireNotNull(payload.precomputed?.lineKeyHashes)
        notes.forEachIndexed { i, note ->
            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(note), keys[i + 2])
        }
    }

    @Test
    fun choshenMishpatNotesKeepTheirOwnPrintedLetters() {
        val notes = listOf("א) מעובדא דאביי", "פירוש שמעידין שבפנינו", "ב) כן כ' הטור")
        val payload = readBook(CHOSHEN_MISHPAT, "באר הגולה על שולחן ערוך חושן משפט", notes)

        assertEquals(notes, payload.lines.drop(2))
    }

    @Test
    fun yorehDeahKeepsTheNoteNumberItsAnchorsShow() {
        // Its itags carry data-order alone, so the anchor shows the note number too.
        val payload = readBook(YOREH_DEAH, "באר הגולה על שולחן ערוך יורה דעה", listOf("משנה ריש חולין", "תוס' שם"))

        assertEquals(listOf("(א) משנה ריש חולין", "(ב) תוס' שם"), payload.lines.drop(2))
    }

    /** A one-siman book: `notes` are its comments in order. */
    private fun readBook(title: String, heTitle: String, notes: List<String>) = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-beer-hagolah")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve(title))
        Files.writeString(schemaDir.resolve("${title.replace(' ', '_')}.json"), schemaJson(title, heTitle))
        val siman = JsonArray(notes.map(::JsonPrimitive))
        Files.writeString(
            bookDir.resolve("merged.json"),
            """{"title": "$title", "heTitle": "$heTitle", "text": [$siman]}""",
        )

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaBeerHaGolahLettersTest"),
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)).single()
    }

    private fun schemaJson(title: String, heTitle: String) = """
        {
          "title": "$title",
          "heTitle": "$heTitle",
          "schema": {
            "nodeType": "JaggedArrayNode",
            "depth": 2,
            "addressTypes": ["Siman", "Seif Katan"],
            "sectionNames": ["Siman", "Seif Katan"],
            "heSectionNames": ["סימן", "סעיף קטן"],
            "title": "$title",
            "heTitle": "$heTitle"
          }
        }
    """.trimIndent()

    private companion object {
        const val ORACH_CHAYIM = "Be'er HaGolah on Shulchan Arukh, Orach Chayim"
        const val CHOSHEN_MISHPAT = "Be'er HaGolah on Shulchan Arukh, Choshen Mishpat"
        const val YOREH_DEAH = "Be'er HaGolah on Shulchan Arukh, Yoreh De'ah"

        // Shulchan Arukh, Orach Chayim 151 as Sefaria prints it: the second note is unlettered.
        val SEIFIM = listOf(
            """<i data-commentator="Be'er HaGolah" data-label="א" data-order="1"></i>בתי כנסיות """ +
                """<i data-commentator="Be'er HaGolah" data-label="•" data-order="2"></i>אין נוהגין בהם קלות ראש""",
            """<i data-commentator="Be'er HaGolah" data-label="ב" data-order="3"></i>אין נכנסין בהם בחמה""",
        )

        val GENERATED_LABEL = Regex("""^\(([^)]+)\) """)
    }
}
