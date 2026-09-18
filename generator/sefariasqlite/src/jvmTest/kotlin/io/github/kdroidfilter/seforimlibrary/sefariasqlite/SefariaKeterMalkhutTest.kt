package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/392 (Keter Malkhut):
 * the book's schema declares `depth=2` (Chapter × Paragraph) but the merged
 * `text` is effectively 1-D — most chapters are a single string rather than a
 * paragraph array. Before the fix, the importer hit the `text !is JsonArray`
 * guard at depth=1 and silently dropped every stanza, leaving only the
 * `<h2>פרק N</h2>` headings.
 */
class SefariaKeterMalkhutTest {
    @Test
    fun chapterStanzaStoredAsStringStillBecomesContentLine() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-keter")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Keter Malkhut"))

        Files.writeString(schemaDir.resolve("Keter_Malkhut.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val perekA = payload.headings.firstOrNull { it.title == "פרק א" }
        val perekB = payload.headings.firstOrNull { it.title == "פרק ב" }
        assertTrue(perekA != null, "expected <h2>פרק א</h2> heading")
        assertTrue(perekB != null, "expected <h2>פרק ב</h2> heading")

        // Stanza content must survive the schema-vs-data mismatch
        assertTrue(
            payload.lines.any { it.contains("נִפְלָאִים") },
            "stanza from chapter 2 should be present as a content line"
        )
        assertTrue(
            payload.lines.any { it.contains("בִּתְפִלָּתִי") },
            "stanza from chapter 1 should be present as a content line"
        )

        // Chapter 1 stanza is referenced as 'כתר מלכות, א'
        val refA = payload.refEntries.firstOrNull { it.heRef == "כתר מלכות, א" }
        assertTrue(refA != null, "chapter 1 should have heRef 'כתר מלכות, א'")
    }

    @Test
    fun legitimate2dChapterStillExpandsToParagraphs() = runBlocking {
        // Guard that the primitive-fallback doesn't short-circuit books where
        // the data actually matches the schema.
        val tempDir = Files.createTempDirectory("seforim-2d")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("FakeBook"))

        Files.writeString(schemaDir.resolve("FakeBook.json"), schema2dJson)
        Files.writeString(bookDir.resolve("merged.json"), merged2dJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // 2 chapters × 2 paragraphs → 4 paragraph refs
        assertEquals(4, payload.refEntries.size)
    }


    @Test
    fun declaredVersionIsReadInsteadOfMerged() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-keter-version")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Keter Malkhut"))

        Files.writeString(schemaDir.resolve("Keter_Malkhut.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), keterMergedJson)
        Files.writeString(bookDir.resolve(MAHBERET_FILE), keterMahberetJson)

        val payload = readSingleBook(jsonDir, schemaDir, preferredVersions())

        // 40 chapters, numbering not shifted by the title/author/preface entries
        assertEquals(40, payload.headings.count { it.title.startsWith("פרק ") })
        assertTrue(
            payload.lines.none { it.contains("שלמה אבן גבירול") },
            "merged.json preface entries must not reach the payload"
        )
        assertEquals("כתר מלכות, א, א", payload.refEntries.first().heRef)
        assertTrue(
            payload.lines.first { it.isNotBlank() && !it.startsWith("<h") }
                .contains("בִּתְפִלָּתִי יִסְכָּן גָּבֶר"),
            "chapter 1 must be the first stanza of the piyut"
        )
        // The closing chapter is no longer duplicated as both ל"ט and מ'
        assertEquals(
            1,
            payload.lines.count { it.contains("אֱלֹהַי יָדַעְתִּי") },
            "closing chapter should appear exactly once"
        )
    }

    @Test
    fun bookWithoutMappingStillReadsMerged() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-no-mapping")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("FakeBook"))

        Files.writeString(schemaDir.resolve("FakeBook.json"), schema2dJson)
        Files.writeString(bookDir.resolve("merged.json"), merged2dJson)

        val payload = readSingleBook(jsonDir, schemaDir, preferredVersions())

        assertEquals(4, payload.refEntries.size)
        assertTrue(payload.lines.any { it.contains("ch1 p1") }, "merged.json content expected")
    }

    @Test
    fun missingDeclaredVersionFails() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-keter-missing")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Keter Malkhut"))

        Files.writeString(schemaDir.resolve("Keter_Malkhut.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), keterMergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest"),
            preferredVersions = preferredVersions(),
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val error = assertFailsWith<MissingPreferredVersionException> {
            reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
        }
        assertTrue(error.message!!.contains(MAHBERET_FILE), "error should name the missing file")
    }

    private suspend fun readSingleBook(
        jsonDir: java.nio.file.Path,
        schemaDir: java.nio.file.Path,
        preferredVersions: SefariaPreferredVersions,
    ): BookPayload {
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest"),
            preferredVersions = preferredVersions,
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        return reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()
    }

    private fun preferredVersions(): SefariaPreferredVersions = parsePreferredVersions(
        listOf("כתר מלכות|$MAHBERET_FILE"),
        Logger.withTag("SefariaKeterMalkhutTest"),
    )

    companion object {
        private const val MAHBERET_FILE =
            "Mahberet miShire Kodesh, I. Davidson. JPS, Philadelphia, 1923.json"

        // מיזוג ספריא: כותרת/מחבר/הקדמה נספרים כפרקים, ופרק הסיום כפול
        private val keterMergedJson = buildString {
            append("{\n  \"title\": \"Keter Malkhut\",\n  \"heTitle\": \"כתר מלכות\",\n  \"text\": [")
            val chapters = listOf("כתר מלכות", "שלמה אבן גבירול", "הקדמה", "בִּתְפִלָּתִי יִסְכָּן גָּבֶר") +
                (5..39).map { "פסקה $it" } +
                listOf("אֱלֹהַי יָדַעְתִּי כִּי הַמִּתְחַנְּנִים לְפָנֶיךָ", "אֱלֹהַי יָדַעְתִּי כִּי הַמִּתְחַנְּנִים לְפָנֶיךָ")
            append(chapters.joinToString(", ") { "[\"$it\"]" })
            append("]\n}")
        }

        // הגרסה המוצהרת: 40 פרקים, הפיוט מתחיל בפרק א, הסיום פעם אחת
        private val keterMahberetJson = buildString {
            append("{\n  \"title\": \"Keter Malkhut\",\n  \"heTitle\": \"כתר מלכות\",\n  \"text\": [")
            val chapters = listOf("בִּתְפִלָּתִי יִסְכָּן גָּבֶר") +
                (2..39).map { "פסקה $it" } +
                listOf("אֱלֹהַי יָדַעְתִּי כִּי הַמִּתְחַנְּנִים לְפָנֶיךָ")
            append(chapters.joinToString(", ") { "[\"$it\"]" })
            append("]\n}")
        }

        private val schemaJson = """
            {
              "title": "Keter Malkhut",
              "heTitle": "כתר מלכות",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Perek", "Integer"],
                "sectionNames": ["Chapter", "Paragraph"],
                "heSectionNames": ["פרק", "פסקה"],
                "title": "Keter Malkhut",
                "heTitle": "כתר מלכות"
              }
            }
        """.trimIndent()

        // Real shape: outer array is chapters, most are a primitive string
        private val mergedJson = """
            {
              "title": "Keter Malkhut",
              "heTitle": "כתר מלכות",
              "text": [
                "בִּתְפִלָּתִי יִסְכָּן גָּבֶר",
                "נִפְלָאִים מַעֲשֶׂיךָ",
                "אַתָּה אֶחָד"
              ]
            }
        """.trimIndent()

        private val schema2dJson = """
            {
              "title": "FakeBook",
              "heTitle": "ספר",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Perek", "Integer"],
                "sectionNames": ["Chapter", "Verse"],
                "heSectionNames": ["פרק", "פסוק"],
                "title": "FakeBook",
                "heTitle": "ספר"
              }
            }
        """.trimIndent()

        private val merged2dJson = """
            {
              "title": "FakeBook",
              "heTitle": "ספר",
              "text": [
                ["ch1 p1", "ch1 p2"],
                ["ch2 p1", "ch2 p2"]
              ]
            }
        """.trimIndent()
    }
}
