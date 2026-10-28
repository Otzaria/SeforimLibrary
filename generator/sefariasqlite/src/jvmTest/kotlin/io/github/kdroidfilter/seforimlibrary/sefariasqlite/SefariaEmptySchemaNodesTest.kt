package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Arukh HaShulchan HeAtid: Sefaria has text only for Zeraim, but its schema also
 * declares Kodashim, Tahorot and Assorted Laws. Their bare headings made the book
 * look cut off halfway (Otzaria/otzaria#3024).
 */
class SefariaEmptySchemaNodesTest {
    @Test
    fun nodesWithoutTextGetNoHeading() {
        val text = Json.parseToJsonElement(
            """
            {
              "Zeraim": [["a1"]],
              "Kodashim": [],
              "Tahorot": [[], [""]],
              "Assorted": { "Part A": [], "Part B": [[" "]] },
              "Mixed": { "Part A": [], "Part B": [["b1"]] }
            }
            """
        )
        val walk = SefariaBookPayloadReader(Json, Logger.withTag("SefariaEmptySchemaNodesTest"))
            .walkTextWithSchema(schema, text, "ספר", "Book")

        assertEquals(
            listOf("ספר", "זרעים", "סימן א", "מעורב", "חלק ב", "סימן א"),
            walk.headings.map { it.title },
        )
        assertEquals(listOf("Book, Zeraim,  1:1", "Book, Mixed, Part B,  1:1"), walk.refs.map { it.ref })
    }

    private fun leaf(title: String, heTitle: String) = """
        {
          "nodeType": "JaggedArrayNode", "depth": 2, "key": "$title",
          "title": "$title", "heTitle": "$heTitle",
          "sectionNames": ["Siman", "Seif"], "heSectionNames": ["סימן", "סעיף"],
          "addressTypes": ["Integer", "Integer"]
        }
    """

    private fun parent(title: String, heTitle: String) = """
        {
          "key": "$title", "title": "$title", "heTitle": "$heTitle",
          "nodes": [${leaf("Part A", "חלק א")}, ${leaf("Part B", "חלק ב")}]
        }
    """

    private val schema = Json.parseToJsonElement(
        """
        {
          "key": "Book", "title": "Book", "heTitle": "ספר",
          "nodes": [
            ${leaf("Zeraim", "זרעים")},
            ${leaf("Kodashim", "קדשים")},
            ${leaf("Tahorot", "טהרות")},
            ${parent("Assorted", "ליקוטים")},
            ${parent("Mixed", "מעורב")}
          ]
        }
        """
    ).jsonObject
}
