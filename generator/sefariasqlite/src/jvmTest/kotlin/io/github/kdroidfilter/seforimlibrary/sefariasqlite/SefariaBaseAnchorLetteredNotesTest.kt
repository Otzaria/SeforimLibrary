package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

// Otzaria/otzaria#4898: the SMK shows each Haggahot Rabbeinu Peretz anchor as the letter of
// the note's position in its siman; the notes themselves must carry that letter.
class SefariaBaseAnchorLetteredNotesTest {

    private fun strings(values: List<String>) = JsonArray(values.map(::JsonPrimitive))

    private fun readBook(enTitle: String, simanim: List<List<String>>): BookPayload = runBlocking {
        val root = Files.createTempDirectory("sefaria-anchor-letters")
        try {
            Files.createDirectories(root.resolve("schemas"))
            // The real schema's shape: Siman × an unnamed Integer segment.
            val schema = buildJsonObject {
                put("title", enTitle); put("heTitle", "הגהות רבינו פרץ על ספר מצוות קטן")
                put("schema", buildJsonObject {
                    put("title", enTitle); put("heTitle", "הגהות רבינו פרץ על ספר מצוות קטן"); put("key", enTitle)
                    put("nodeType", "JaggedArrayNode"); put("depth", 2)
                    put("sectionNames", strings(listOf("Siman", "Segment")))
                    put("heSectionNames", strings(listOf("סימן", "")))
                    put("addressTypes", strings(listOf("Siman", "Integer")))
                })
                put("categories", strings(listOf("Halakhah", "Commentary")))
            }
            Files.writeString(root.resolve("schemas/${enTitle.replace(' ', '_')}.json"), schema.toString())
            val merged = buildJsonObject {
                put("title", enTitle); put("heTitle", "הגהות רבינו פרץ על ספר מצוות קטן"); put("language", "he")
                put("versions", JsonArray(listOf(strings(listOf("Fixture Edition", "")))))
                put("text", JsonArray(simanim.map(::strings)))
            }
            Files.writeString(Files.createDirectories(root.resolve("json/$enTitle")).resolve("merged.json"), merged.toString())
            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, Logger.withTag("AnchorLettersTest"))
            reader.readBooksInParallel(
                root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")),
            ).single()
        } finally {
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    private val simanim = listOf(
        listOf("הגהה ראשונה בסימן א", "הגהה שנייה בסימן א", "הגהה שלישית בסימן א"),
        emptyList(),
        listOf("הגהה יחידה בסימן ג"),
        listOf("הגהה ראשונה בסימן ד", "הגהה שנייה בסימן ד"),
    )

    private fun BookPayload.notes() = lines.filter { "הגהה" in it }

    @Test
    fun eachNoteCarriesItsAnchorLetterRestartingPerSiman() {
        assertEquals(
            listOf(
                "(א) הגהה ראשונה בסימן א",
                "(ב) הגהה שנייה בסימן א",
                "(ג) הגהה שלישית בסימן א",
                "(א) הגהה יחידה בסימן ג",
                "(א) הגהה ראשונה בסימן ד",
                "(ב) הגהה שנייה בסימן ד",
            ),
            readBook("Haggahot Rabbeinu Peretz on Sefer Mitzvot Katan", simanim).notes(),
        )
    }

    @Test
    fun simanHeadingsAndRefsStayAsTheyWere() {
        val book = readBook("Haggahot Rabbeinu Peretz on Sefer Mitzvot Katan", simanim)
        assertEquals(listOf("סימן א", "סימן ג", "סימן ד"), book.headings.map { it.title }.filter { it.startsWith("סימן") })
        assertEquals(
            listOf(1 to 1, 1 to 2, 1 to 3, 3 to 1, 4 to 1, 4 to 2).map { (s, n) -> "Haggahot Rabbeinu Peretz on Sefer Mitzvot Katan $s:$n" },
            book.refEntries.map { it.ref },
        )
    }

    @Test
    fun anAlreadyLetteredLoneNoteKeepsItsPrintedLetter() {
        val alreadyLettered = listOf("(א) הגהה יחידה בסימן א", "(ב) הגהה יחידה בסימן ב")
        val book = readBook(
            "Haggahot Rabbeinu Peretz on Sefer Mitzvot Katan",
            listOf(listOf(alreadyLettered[0]), listOf("", alreadyLettered[1])),
        )
        assertEquals(alreadyLettered, book.notes())
    }

    @Test
    fun anotherBookOfTheSameShapeStaysUnlettered() {
        val notes = readBook("Some Other Glosses on Sefer Mitzvot Katan", simanim).notes()
        assertEquals(simanim.flatten(), notes)
    }
}
