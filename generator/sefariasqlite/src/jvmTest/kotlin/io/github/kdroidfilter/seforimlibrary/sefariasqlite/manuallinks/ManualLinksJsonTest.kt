package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ManualLinksJsonTest {
    @Test
    fun losslessPatchPreservesUnknownFieldsWhitespaceAndFinalLf() {
        val input = """
            [
              {
                "line_index_1": 1.0,
                "heRef_2": "יעד א",
                "path_2": "יעד.txt",
                "line_index_2": 9,
                "unknown": { "kept": true }
              }
            ]
        """.trimIndent()
        val document = ManualLinksDocument.parse(input)
        document.setString(0, "ref_1", "Source 1")
        document.setString(0, "anchor_src_hash", "sha256:${"0".repeat(64)}")
        document.setInt(0, "line_index_2", 2)

        val output = document.render()

        assertTrue(output.contains("\"line_index_1\": 1.0,\n    \"ref_1\": \"Source 1\",\n    \"anchor_src_hash\""))
        assertTrue(output.contains("\"unknown\": { \"kept\": true }"))
        assertTrue(output.endsWith("}") || output.endsWith("]"))
        assertFalse(output.endsWith("\n"))
        assertEquals(2, ManualLinksDocument.parse(output).record(0).get("line_index_2").intValue())
    }

    @Test
    fun anchorContextIsInsertedRightAfterTheHashAndReplacedInPlaceAfterwards() {
        val input = """
            [
              {
                "line_index_1": 1,
                "ref_1": "Source 1",
                "anchor_src_hash": "sha256:${"0".repeat(64)}",
                "heRef_2": "יעד א",
                "path_2": "יעד.txt",
                "line_index_2": 9,
                "start": 4,
                "unknown": { "kept": true }
              }
            ]
        """.trimIndent()
        val document = ManualLinksDocument.parse(input)
        document.setObject(0, "anchor_context", ManualLinksAnchor.contextNode("אבגד הוזח טי", 4))

        val output = document.render()

        assertEquals(
            input.replace(
                "\"anchor_src_hash\": \"sha256:${"0".repeat(64)}\",",
                "\"anchor_src_hash\": \"sha256:${"0".repeat(64)}\",\n" +
                    "    \"anchor_context\": {\"before\":\"אבגד\",\"after\":\" הוזח טי\"},",
            ),
            output,
        )
        assertEquals(
            ManualLinksJson.canonicalString(document.records),
            ManualLinksJson.canonicalString(ManualLinksDocument.parse(output).records),
        )

        // A second pass re-anchors the same record: the object is replaced in place, nothing is added.
        val second = ManualLinksDocument.parse(output)
        second.setInt(0, "start", 5)
        second.setObject(0, "anchor_context", ManualLinksAnchor.contextNode("אבגד הוזח טי", 5))
        val replaced = second.render()
        assertEquals(1, Regex("anchor_context").findAll(replaced).count())
        assertTrue(replaced.contains("\"anchor_context\": {\"before\":\"אבגד \",\"after\":\"הוזח טי\"}"))
        assertTrue(replaced.contains("\"start\": 5"))
        assertTrue(replaced.contains("\"unknown\": { \"kept\": true }"))
    }

    @Test
    fun insertingAnUnsupportedFieldIsRefused() {
        val input = """[{"line_index_1":1,"heRef_2":"x","path_2":"x.txt","line_index_2":1}]"""
        val document = ManualLinksDocument.parse(input)
        assertFailsWith<IllegalStateException> { document.setInt(0, "start", 3) }
    }

    @Test
    fun duplicateKeysAndInvalidNumbersFailStrictly() {
        val duplicate = """[{"line_index_1":1,"line_index_1":2,"heRef_2":"x","path_2":"x.txt","line_index_2":1}]"""
        assertFailsWith<Exception> { ManualLinksDocument.parse(duplicate) }

        listOf("1.5", "0", "-1", "2147483648").forEach { invalid ->
            val json = """[{"line_index_1":$invalid,"heRef_2":"x","path_2":"x.txt","line_index_2":1}]"""
            assertFailsWith<IllegalArgumentException>(invalid) { ManualLinksDocument.parse(json) }
        }
        ManualLinksDocument.parse("""[{"line_index_1":1.0,"heRef_2":"x","path_2":"x.txt","line_index_2":1}]""")
    }

    @Test
    fun canonicalJsonNormalizesNumbersAndSortsByCodePoint() {
        val node = ManualLinksJson.mapper.readTree("""{"z":1.0,"א":"שלום","a":[2.50,true]}""")
        assertEquals("""{"a":[2.5,true],"z":1,"א":"שלום"}""", ManualLinksJson.canonicalString(node))
        assertEquals(
            ManualLinksJson.sha256("""{"a":[2.5,true],"z":1,"א":"שלום"}""".toByteArray()),
            ManualLinksJson.stableHash(node),
        )
    }

    @Test
    fun bomCrLfAndMultipleTrailingLinesAreRejected() {
        val dir = Files.createTempDirectory("manual-links-json")
        val base = """[{"line_index_1":1,"heRef_2":"x","path_2":"x.txt","line_index_2":1}]"""
        val crlf = dir.resolve("crlf.json")
        Files.writeString(crlf, base + "\r\n")
        assertFailsWith<IllegalArgumentException> { ManualLinksDocument.read(crlf) }
        val twoLines = dir.resolve("two.json")
        Files.writeString(twoLines, base + "\n\n")
        assertFailsWith<IllegalArgumentException> { ManualLinksDocument.read(twoLines) }
    }

    @Test
    fun anchorInsertionKeepsSingleLineRecordStyleBeforeANewline() {
        val first = """{"line_index_1":1,"ref_1":"Book 1","heRef_2":"x","path_2":"x.txt","line_index_2":1}"""
        val second = """{"line_index_1":2,"ref_1":"Book 2","heRef_2":"y","path_2":"y.txt","line_index_2":2}"""
        val input = "[$first,\n$second]"
        val document = ManualLinksDocument.parse(input)
        document.setString(0, "anchor_src_hash", "sha256:${"0".repeat(64)}")

        val output = document.render()

        val expectedFirst = first.replace(
            "\"ref_1\":\"Book 1\"",
            "\"ref_1\":\"Book 1\", \"anchor_src_hash\": \"sha256:${"0".repeat(64)}\"",
        )
        assertEquals("[$expectedFirst,\n$second]", output)
    }
}
