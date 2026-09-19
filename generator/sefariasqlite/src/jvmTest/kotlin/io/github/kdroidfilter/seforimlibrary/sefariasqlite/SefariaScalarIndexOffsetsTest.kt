package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Talmud Eser HaSefirot: `index_offsets_by_depth: {"1": 54}` (a scalar) crashed
 * the list-only reader and the whole book was skipped with a warning.
 */
class SefariaScalarIndexOffsetsTest {
    @Test
    fun scalarOffsetShiftsTopLevelOfDepth1And2Nodes() = runBlocking {
        val payload = readSingleBook(schemaWith("""{ "1": 54 }""", """{ "1": 54 }"""), mergedJson)

        val questions = payload.refEntries.filter { it.ref.contains("Questions") }.map { it.ref }
        assertEquals(listOf("TES, Questions,  55", "TES, Questions,  56"), questions)
        val answers = payload.refEntries.filter { it.ref.contains("Answers") }.map { it.ref }
        assertEquals(listOf("TES, Answers,  55:1", "TES, Answers,  56:1"), answers)
        // Hebrew labels stay local, as with the Zohar offsets
        assertTrue(payload.refEntries.any { it.heRef == "תעס, תשובות,  א, א" })
    }

    @Test
    fun malformedOffsetFailsLoudly() {
        assertFailsWith<SefariaSchemaException> {
            runBlocking { readSingleBook(schemaWith("""{ "1": "x" }""", """{ "1": 54 }"""), mergedJson) }
        }
        assertFailsWith<SefariaSchemaException> {
            runBlocking { readSingleBook(schemaWith("""{ "2": [0] }""", """{ "1": 54 }"""), mergedJson) }
        }
    }

    @Test
    fun childOffsetsMustCoverEverySection() {
        assertFailsWith<SefariaSchemaException> {
            runBlocking { readSingleBook(schemaWith("""{ "1": 54 }""", """{ "2": [0] }"""), mergedJson) }
        }
    }

    @Test
    fun partialVersionWalksWithSameOffsets() {
        // Zohar "Hebrew Translation" / TES Persian: sections shorter than the offsets list
        val schema = Json.parseToJsonElement(schemaWith("""{ "1": 54 }""", """{ "1": 54, "2": [0, 1] }"""))
            .jsonObject["schema"]!!.jsonObject
        val text = Json.parseToJsonElement("""{ "Questions": [], "Answers": [["a55"]] }""")
        val walk = SefariaBookPayloadReader(Json, Logger.withTag("SefariaScalarIndexOffsetsTest"))
            .walkTextWithSchema(schema, text, "תעס", "TES")
        assertEquals(listOf("TES, Answers,  55:1"), walk.refs.map { it.ref })
    }

    @Test
    fun unknownOffsetKeyFailsLoudly() {
        assertFailsWith<SefariaSchemaException> {
            runBlocking { readSingleBook(schemaWith("""{ "1": 54 }""", """{ "3": [[0]] }"""), mergedJson) }
        }
    }

    private suspend fun readSingleBook(schema: String, merged: String): BookPayload {
        val tempDir = Files.createTempDirectory("seforim-tes")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir: Path = Files.createDirectories(jsonDir.resolve("TES"))
        Files.writeString(schemaDir.resolve("TES.json"), schema)
        Files.writeString(bookDir.resolve("merged.json"), merged)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaScalarIndexOffsetsTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        return reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()
    }

    private fun schemaWith(questionsOffsets: String, answersOffsets: String) = """
        {
          "title": "TES",
          "heTitle": "תעס",
          "schema": {
            "nodeType": "SchemaNode",
            "title": "TES",
            "heTitle": "תעס",
            "nodes": [
              {
                "nodeType": "JaggedArrayNode",
                "depth": 1,
                "addressTypes": ["Integer"],
                "sectionNames": ["Paragraph"],
                "heSectionNames": ["פסקה"],
                "title": "Questions",
                "heTitle": "שאלות",
                "key": "Questions",
                "index_offsets_by_depth": $questionsOffsets
              },
              {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Integer", "Integer"],
                "sectionNames": ["Answer", "Paragraph"],
                "heSectionNames": ["תשובה", "פסקה"],
                "title": "Answers",
                "heTitle": "תשובות",
                "key": "Answers",
                "index_offsets_by_depth": $answersOffsets
              }
            ]
          }
        }
    """.trimIndent()

    private val mergedJson = """
        {
          "title": "TES",
          "heTitle": "תעס",
          "text": {
            "Questions": ["q55", "q56"],
            "Answers": [["a55"], ["a56"]]
          }
        }
    """.trimIndent()
}
