package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SefariaBookPayloadReaderTest {
    @Test
    fun defaultNodeWithoutTitleKeepsSimanimAtSameLevelAsIntroduction() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Tur"))

        Files.writeString(schemaDir.resolve("Tur.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaBookPayloadReaderTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val intro = payload.headings.firstOrNull { it.title == "הקדמה" }
        val siman = payload.headings.firstOrNull { it.title == "סימן א" }

        assertNotNull(intro)
        assertNotNull(siman)
        assertEquals(2, intro.level)
        assertEquals(2, siman.level)
        assertTrue(payload.lines.any { it == "<h3>סימן א</h3>" })
        assertTrue(payload.lines.none { it == "<h4>סימן א</h4>" })
    }

    @Test
    fun separatesShortDescFromLongDesc() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Tur"))

        Files.writeString(schemaDir.resolve("Tur.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaBookPayloadReaderTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // heShortDesc must hold Sefaria's real one-line summary; the long heDesc
        // text must land in `description` (→ book.heDesc), not under heShortDesc.
        assertEquals("תקציר קצר של הספר", payload.heShortDesc)
        assertEquals("תיאור ארוך ומפורט של הספר וכל ענייניו", payload.description)
    }

    @Test
    fun declaredBaseAndCollectiveTitlesFromSchema() = runBlocking {
        val payload = readSingle("Rashi_on_Genesis", "Rashi on Genesis", rashiSchemaJson, rashiMergedJson)

        // rawDependence is trim + lowercase; collective titles come from schemas.
        assertEquals("commentary", payload.rawDependence)
        assertEquals("Rashi", payload.collectiveTitleEn)
        assertEquals("רש\"י", payload.collectiveTitleHe)
        // base_text_titles → declared set; the title-pattern set stays empty.
        assertTrue(payload.declaredBaseTextTitleKeys.isNotEmpty())
        assertTrue(payload.inferredBaseTextTitleKeys.isEmpty())
    }

    @Test
    fun inferredBaseFromTitleWhenNoDeclaredBase() = runBlocking {
        val payload = readSingle("Bartenura_on_Genesis", "Bartenura on Genesis", bartenuraSchemaJson, rashiMergedJson)

        // No base_text_titles in schema → declared empty, inferred recovered
        // from the "X on Y" title pattern.
        assertTrue(payload.declaredBaseTextTitleKeys.isEmpty())
        assertTrue(payload.inferredBaseTextTitleKeys.isNotEmpty())
    }

    @Test
    fun readsHebrewPubPlace() = runBlocking {
        val payload = readSingle("Tur", "Tur", schemaJson, mergedJson)

        assertEquals(listOf("ליבורנו, איטליה"), payload.pubPlaces)
    }

    @Test
    fun knownHebrewPubYearReplacesGregorian() {
        assertEquals("ה׳תקמ״ח", hebrewPubDate("נועם אלימלך", "1787"))
        assertEquals("1700", hebrewPubDate("נועם אלימלך", "1700"))
    }

    companion object {
        private fun readSingle(folder: String, title: String, schema: String, merged: String) = runBlocking {
            val tempDir = Files.createTempDirectory("seforim-test")
            val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
            val jsonDir = Files.createDirectories(tempDir.resolve("json"))
            val bookDir = Files.createDirectories(jsonDir.resolve(folder))
            Files.writeString(schemaDir.resolve("$title.json"), schema)
            Files.writeString(bookDir.resolve("merged.json"), merged)
            val reader = SefariaBookPayloadReader(
                Json { ignoreUnknownKeys = true; coerceInputValues = true },
                Logger.withTag("SefariaBookPayloadReaderTest")
            )
            val schemaLookup = reader.buildSchemaLookup(schemaDir)
            reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()
        }

        private val rashiSchemaJson = """
            {
              "title": "Rashi on Genesis",
              "heTitle": "רש\"י על בראשית",
              "dependence": "Commentary",
              "collective_title": { "en": "Rashi", "he": "רש\"י" },
              "base_text_titles": ["Genesis"],
              "schema": {
                "title": "Rashi on Genesis",
                "heTitle": "רש\"י על בראשית",
                "depth": 1,
                "addressTypes": ["Integer"],
                "sectionNames": ["Paragraph"],
                "heSectionNames": ["פסקה"]
              }
            }
        """.trimIndent()

        private val bartenuraSchemaJson = """
            {
              "title": "Bartenura on Genesis",
              "heTitle": "ברטנורא על בראשית",
              "dependence": "Commentary",
              "schema": {
                "title": "Bartenura on Genesis",
                "heTitle": "ברטנורא על בראשית",
                "depth": 1,
                "addressTypes": ["Integer"],
                "sectionNames": ["Paragraph"],
                "heSectionNames": ["פסקה"]
              }
            }
        """.trimIndent()

        private val rashiMergedJson = """
            {
              "title": "Rashi on Genesis",
              "heTitle": "רש\"י על בראשית",
              "text": ["comment one", "comment two"]
            }
        """.trimIndent()

        private val schemaJson = """
            {
              "title": "Tur",
              "heTitle": "טור",
              "pubPlace": "Livorno, Italy",
              "pubPlaceString": {
                "en": "Livorno, Italy",
                "he": "ליבורנו, איטליה"
              },
              "schema": {
                "title": "Tur",
                "heTitle": "טור",
                "heShortDesc": "תקציר קצר של הספר",
                "heDesc": "תיאור ארוך ומפורט של הספר וכל ענייניו",
                "nodes": [
                  {
                    "nodeType": "SchemaNode",
                    "title": "Orach Chayim",
                    "heTitle": "אורח חיים",
                    "key": "Orach Chaim",
                    "nodes": [
                      {
                        "nodeType": "JaggedArrayNode",
                        "depth": 1,
                        "addressTypes": [
                          "Integer"
                        ],
                        "sectionNames": [
                          "Paragraph"
                        ],
                        "title": "Introduction",
                        "heTitle": "הקדמה",
                        "heSectionNames": [
                          "פסקה"
                        ],
                        "key": "Introduction"
                      },
                      {
                        "nodeType": "JaggedArrayNode",
                        "depth": 2,
                        "addressTypes": [
                          "Siman",
                          "Seif"
                        ],
                        "sectionNames": [
                          "Siman",
                          "Seif"
                        ],
                        "title": "",
                        "heTitle": "",
                        "heSectionNames": [
                          "סימן",
                          "סעיף"
                        ],
                        "key": "default",
                        "default": true
                      }
                    ]
                  }
                ]
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Tur",
              "heTitle": "טור",
              "text": {
                "Orach Chayim": {
                  "Introduction": [
                    "intro paragraph"
                  ],
                  "": [
                    [
                      "siman text"
                    ]
                  ]
                }
              }
            }
        """.trimIndent()
    }
}
