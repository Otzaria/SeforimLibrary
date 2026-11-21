package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Otzaria/otzaria#4488: Sefaria names level 2 of the Shach and the Ketzot "סעיף"; it is the ס"ק.
class SefariaSeifKatanLevelTest {

    private fun strings(values: List<String>) = JsonArray(values.map(::JsonPrimitive))

    private val mainNode = buildJsonObject {
        put("nodeType", "JaggedArrayNode"); put("depth", 3); put("key", "default"); put("default", true)
        put("title", ""); put("heTitle", "")
        put("sectionNames", strings(listOf("Siman", "Seif", "Paragraph")))
        put("heSectionNames", strings(listOf("סימן", "סעיף", "פסקה")))
        put("addressTypes", strings(listOf("Siman", "Seif", "Integer")))
    }

    // Two simanim × two ס"ק × one paragraph.
    private val mainText = JsonArray(
        (1..2).map { siman -> JsonArray((1..2).map { sk -> strings(listOf("סימן $siman ס\"ק $sk")) }) },
    )

    private fun readBook(
        enTitle: String,
        heTitle: String,
        schemaBody: JsonElement,
        text: JsonElement,
    ): BookPayload = runBlocking {
        val root = Files.createTempDirectory("sefaria-seif-katan")
        try {
            Files.createDirectories(root.resolve("schemas"))
            val schema = buildJsonObject {
                put("title", enTitle); put("heTitle", heTitle)
                put("schema", schemaBody)
                put("categories", strings(listOf("Halakhah", "Shulchan Arukh", "Commentary")))
            }
            Files.writeString(root.resolve("schemas/${enTitle.replace(' ', '_')}.json"), schema.toString())
            val merged = buildJsonObject {
                put("title", enTitle); put("heTitle", heTitle); put("language", "he")
                put("versions", JsonArray(listOf(strings(listOf("Fixture Edition", "")))))
                put("text", text)
            }
            Files.writeString(Files.createDirectories(root.resolve("json/$enTitle")).resolve("merged.json"), merged.toString())
            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, Logger.withTag("SeifKatanTest"))
            reader.readBooksInParallel(
                root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")),
            ).single()
        } finally {
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    /** Shach YD's shape: the main default node plus a named one-level node ("S'fek S'feka Summary"). */
    private fun shach(enTitle: String = "Siftei Kohen on Shulchan Arukh, Yoreh De'ah", heTitle: String = "שפתי כהן על שולחן ערוך יורה דעה") =
        readBook(
            enTitle, heTitle,
            buildJsonObject {
                put("title", enTitle); put("heTitle", heTitle); put("key", enTitle)
                put("nodes", JsonArray(listOf(
                    mainNode,
                    buildJsonObject {
                        put("nodeType", "JaggedArrayNode"); put("depth", 1)
                        put("key", "S'fek S'feka Summary"); put("title", "S'fek S'feka Summary"); put("heTitle", "דיני ספק ספקא בקצרה")
                        put("sectionNames", strings(listOf("Seif"))); put("heSectionNames", strings(listOf("סעיף")))
                        put("addressTypes", strings(listOf("Seif")))
                    },
                )))
            },
            buildJsonObject {
                put("", mainText)
                put("S'fek S'feka Summary", strings(listOf("ספק ראשון", "ספק שני")))
            },
        )

    private fun ketzot(enTitle: String = "Ketzot HaChoshen on Shulchan Arukh, Choshen Mishpat", heTitle: String = "קצות החושן על שולחן ערוך חושן משפט") =
        readBook(
            enTitle, heTitle,
            buildJsonObject {
                put("title", enTitle); put("heTitle", heTitle); put("key", enTitle)
                put("nodeType", "JaggedArrayNode"); put("depth", 3)
                put("sectionNames", strings(listOf("Siman", "Seif", "Paragraph")))
                put("heSectionNames", strings(listOf("סימן", "סעיף", "פסקה")))
                put("addressTypes", strings(listOf("Siman", "Seif", "Integer")))
            },
            mainText,
        )

    private fun BookPayload.levelTwo() = headings.filter { it.title.startsWith("סעיף") }.map { it.title }

    @Test
    fun shachLevelTwoIsSeifKatan() {
        val book = shach()
        assertEquals(listOf("סימן א", "סימן ב"), book.headings.filter { it.title.startsWith("סימן") }.map { it.title })
        assertEquals(List(2) { listOf("סעיף קטן א", "סעיף קטן ב") }.flatten(), book.levelTwo())
        assertTrue(book.lines.any { it.endsWith(">סעיף קטן א</h3>") || it.contains(">סעיף קטן א<") })
        assertTrue(book.lines.none { Regex(">סעיף [א-ת]+<").containsMatchIn(it) })
    }

    @Test
    fun ketzotLevelTwoIsSeifKatan() {
        assertEquals(List(2) { listOf("סעיף קטן א", "סעיף קטן ב") }.flatten(), ketzot().levelTwo())
    }

    @Test
    fun choshenMishpatShachIsListedToo() {
        val book = shach("Siftei Kohen on Shulchan Arukh, Choshen Mishpat", "שפתי כהן על שולחן ערוך חושן משפט")
        assertEquals(List(2) { listOf("סעיף קטן א", "סעיף קטן ב") }.flatten(), book.levelTwo())
    }

    @Test
    fun namedNodesOfTheBookKeepTheirOwnNames() {
        // "דיני ספק ספקא בקצרה" numbers real se'ifim and is not the main node.
        val lines = shach().lines
        assertTrue(lines.any { it.startsWith("(א) ") && it.contains("ספק ראשון") })
        assertTrue(lines.none { it.contains("קטן") && it.contains("ספק") })
    }

    @Test
    fun anUnlistedBookKeepsSefariasName() {
        val book = ketzot("Taz on Shulchan Arukh, Choshen Mishpat", "טורי זהב על שולחן ערוך חושן משפט")
        assertEquals(List(2) { listOf("סעיף א", "סעיף ב") }.flatten(), book.levelTwo())
    }

    @Test
    fun relabelledHeadingsKeepTheirLineKeys() {
        val book = ketzot()
        val keys = book.precomputed!!.lineKeyHashes!!
        val index = book.lines.indexOfFirst { it.contains(">סעיף קטן ב<") }
        val oldHeading = book.lines[index].replace("סעיף קטן ב", "סעיף ב")
        assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(oldHeading), keys[index])
    }

    @Test
    fun aReshapedSchemaFailsLoudly() {
        val enTitle = "Ketzot HaChoshen on Shulchan Arukh, Choshen Mishpat"
        val error = assertFailsWith<Exception> {
            readBook(
                enTitle, "קצות החושן על שולחן ערוך חושן משפט",
                buildJsonObject {
                    put("title", enTitle); put("heTitle", "קצות החושן"); put("key", enTitle)
                    put("nodeType", "JaggedArrayNode"); put("depth", 2)
                    put("sectionNames", strings(listOf("Siman", "Paragraph")))
                    put("heSectionNames", strings(listOf("סימן", "פסקה")))
                    put("addressTypes", strings(listOf("Siman", "Integer")))
                },
                JsonArray(listOf(strings(listOf("א")))),
            )
        }
        val messages = generateSequence<Throwable>(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
        assertTrue("סעיף קטן" in messages, messages)
    }
}
