package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SefariaParagraphSchemaTest {
    private val reader = SefariaBookPayloadReader(Json, Logger.withTag("ParagraphSchemaTest"))

    @Test
    fun paragraphMetadataBelongsToEachLeafAndSurvivesEmptyNodeCompaction() {
        val schema = Json.parseToJsonElement(
            """{"nodes":[
              {"key":"Empty","title":"Empty","heTitle":"ריק","depth":1,"sectionNames":["Comment"]},
              {"key":"default","depth":3,"sectionNames":["Siman","Seif","Paragraph"]},
              {"key":"Notes","title":"Notes","heTitle":"הגהות","depth":3,
               "sectionNames":["Chapter","Halakhah","Comment"]},
              {"key":"Part","title":"Part","heTitle":"חלק","nodes":[
                {"key":"Text","title":"Text","heTitle":"תוכן","depth":2,
                 "sectionNames":["Comment","Paragraph"]}]}]}""",
        ).jsonObject
        val text = Json.parseToJsonElement(
            """{"Empty":[],"":[[["פסקה ראשונה","פסקה שנייה"]]],
                "Notes":[[["הערה ראשונה","הערה שנייה"]]],"Part":{"Text":[["עוד פסקה"]]}}""",
        )
        val built = reader.walkTextWithSchema(schema, text, "מפרש", "Commentary")

        assertEquals(5, built.refs.size)
        for (ref in built.refs) {
            val isParagraph = "Notes" !in ref.ref
            assertEquals(isParagraph, built.paragraphLineIndexes[ref.lineIndex - 1], ref.ref)
        }
        assertEquals(3, built.paragraphLineIndexes.cardinality())
        assertTrue(built.lines.none { it.contains("ריק") })
    }

    @Test
    fun foldedSectionTitleRetainsItsParagraphMetadata() {
        val schema = Json.parseToJsonElement(
            """{"depth":2,"sectionNames":["Comment","Paragraph"],"heSectionNames":["פירוש","פסקה"],"addressTypes":["Integer","Integer"]}""",
        ).jsonObject
        val built = reader.walkTextWithSchema(
            schema, Json.parseToJsonElement("""[["פירוש א","המשך"]]"""), "מפרש", "Commentary",
        )
        assertEquals(2, built.refs.size)
        assertTrue(built.titleSegmentHeadingLines.isNotEmpty())
        assertEquals(2, built.paragraphLineIndexes.cardinality())
        assertTrue(built.refs.all { built.paragraphLineIndexes[it.lineIndex - 1] })
    }

    @Test
    fun absentOrConflictingSectionNamesDoNotEnableParagraphFallback() {
        val cases = listOf(
            """"depth":3""" to false,
            """"depth":3,"sectionNames":["Chapter","Verse","Paragraph"]""" to false,
            """"depth":2,"sectionNames":["Chapter","Paragraph"]""" to false,
            """"depth":3,"sectionNames":["Chapter","Halakhah","Comment"],"heSectionNames":["פרק","הלכה","פסקה"]""" to false,
            """"depth":3,"heSectionNames":["סימן","סעיף","פסקה"]""" to true,
        )
        for ((fields, expected) in cases) {
            val built = reader.walkTextWithSchema(
                Json.parseToJsonElement("{$fields}").jsonObject,
                Json.parseToJsonElement(if (fields.contains("\"depth\":2")) """[["תוכן"]]""" else """[[["תוכן"]]]"""),
                "מפרש", "Commentary",
            )
            assertEquals(expected, built.paragraphLineIndexes[built.refs.single().lineIndex - 1], fields)
        }
    }
}
