package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * "Rosh on Yoma" and "Rosh on Niddah" mark their Halakhah level as not referenceable
 * (`referenceableSections=[true,false,true]`), yet the book is divided by it like every other Rosh tractate.
 * Other non-referenceable levels (Segment, Line...) must still stay out of the TOC.
 */
class SefariaNonReferenceableHalakhahTocTest {
    @Test
    fun nonReferenceableHalakhahGetsHeadingUnderChapter() = runBlocking {
        // Yoma leaves the Hebrew name blank ("Halacha" falls back to "הלכה"); Niddah spells it.
        for ((heName, enName) in listOf("" to "Halacha", "הלכה" to "Halakhah")) {
            val payload = readPayload("Rosh on Yoma", roshSchema(heName, enName), roshText)

            val titles = payload.headings.map { it.title to it.level }
            assertTrue("פרק א" to 1 in titles, "chapter heading missing: $titles")
            assertTrue("הלכה א" to 2 in titles, "halakhah heading missing: $titles")
            assertTrue("הלכה ב" to 2 in titles, "halakhah heading missing: $titles")
            val second = payload.headings.single { it.title == "הלכה ב" }
            assertTrue("siman 2 paragraph 1" in payload.lines[second.lineIndex + 1], "heading must precede its text")
            assertTrue(payload.refEntries.any { it.heRef == "פסקי הראש על יומא, א, ב, א" })
            assertEquals(3, payload.refEntries.size)
        }
    }

    @Test
    fun otherNonReferenceableLevelsStayOutOfToc() = runBlocking {
        val payload = readPayload("Segment Commentary", segmentSchema, segmentText)

        val titles = payload.headings.map { it.title }
        assertTrue("פרק א" in titles, "chapter heading missing: $titles")
        assertTrue(titles.none { it.startsWith("קטע") }, "segment level must not become a heading: $titles")
    }

    private suspend fun readPayload(title: String, schema: String, merged: String): BookPayload {
        val tempDir = Files.createTempDirectory("seforim-nonref")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve(title))
        Files.writeString(schemaDir.resolve(title.replace(' ', '_') + ".json"), schema)
        Files.writeString(bookDir.resolve("merged.json"), merged)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaNonReferenceableHalakhahTocTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        return reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()
    }

    companion object {
        private fun roshSchema(heHalakhah: String, enHalakhah: String) = """
            {
              "title": "Rosh on Yoma",
              "heTitle": "פסקי הראש על יומא",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 3,
                "addressTypes": ["Integer", "Integer", "Integer"],
                "sectionNames": ["Chapter", "$enHalakhah", "Paragraph"],
                "heSectionNames": ["פרק", "$heHalakhah", "פסקה"],
                "referenceableSections": [true, false, true],
                "title": "Rosh on Yoma",
                "heTitle": "פסקי הראש על יומא"
              }
            }
        """.trimIndent()

        private val roshText = """
            {
              "title": "Rosh on Yoma",
              "heTitle": "פסקי הראש על יומא",
              "text": [
                [["siman 1 paragraph 1", "siman 1 paragraph 2"], ["siman 2 paragraph 1"]]
              ]
            }
        """.trimIndent()

        private val segmentSchema = """
            {
              "title": "Segment Commentary",
              "heTitle": "פירוש קטעים",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 3,
                "addressTypes": ["Integer", "Integer", "Integer"],
                "sectionNames": ["Chapter", "Segment", "Comment"],
                "heSectionNames": ["פרק", "", "פירוש"],
                "referenceableSections": [true, false, true],
                "title": "Segment Commentary",
                "heTitle": "פירוש קטעים"
              }
            }
        """.trimIndent()

        private val segmentText = """
            {
              "title": "Segment Commentary",
              "heTitle": "פירוש קטעים",
              "text": [
                [["comment 1"], ["comment 2"]]
              ]
            }
        """.trimIndent()
    }
}
