package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Issue otzaria-library#45: the siman's topic line stands above se'if א instead of inside it.
class SefariaSimanTopicLinesTest {

    private val json = Json { ignoreUnknownKeys = true }
    private val reader = SefariaBookPayloadReader(json, Logger.withTag("SefariaSimanTopicLinesTest"))
    private val schema = json.parseToJsonElement(
        """{"depth":2,"sectionNames":["Siman","Seif"],"heSectionNames":["סימן","סעיף"],"addressTypes":["Integer","Integer"]}""",
    ).jsonObject

    private fun build(heTitle: String, vararg simanim: List<String>): SefariaBookPayloadReader.BuiltBookContent =
        reader.walkTextWithSchema(
            schemaObj = schema,
            textElement = JsonArray(simanim.map { seifim -> JsonArray(seifim.map(::JsonPrimitive)) }),
            bookHeTitle = heTitle,
            bookEnTitle = "Shulchan Arukh, Orach Chayim",
        )

    private fun keyHash(built: SefariaBookPayloadReader.BuiltBookContent, lineIndex: Int): ByteArray {
        val payload = BookPayload(
            heTitle = "שולחן ערוך, אורח חיים", enTitle = "Shulchan Arukh, Orach Chayim", categoriesHe = listOf("הלכה"),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null,
            pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = built.cleanShifts,
            lineKeyHashOverrides = built.lineKeyHashOverrides,
        ).precomputeLineData()
        return requireNotNull(payload.precomputed).lineKeyHashes!![lineIndex]
    }

    @Test
    fun `topic moves to a bold line between the siman and its first seif`() {
        val first = """<b>דין השכמת הבוקר. ובו ט סעיפים:</b> <i data-commentator="Taz" data-order="1"></i>יתגבר כארי"""
        val built = build("שולחן ערוך, אורח חיים", listOf(first, "שני"))

        assertEquals(
            listOf(
                "<h1>שולחן ערוך, אורח חיים</h1>",
                "<h2>סימן א</h2>",
                "<b>דין השכמת הבוקר. ובו ט סעיפים:</b>",
                """(א) <i data-commentator="Taz" data-order="1"></i>יתגבר כארי""",
                "(ב) שני",
            ),
            built.lines,
        )
        // Not a navigation entry: the topic line adds no heading.
        assertEquals(listOf("שולחן ערוך, אורח חיים", "סימן א"), built.headings.map { it.title })
        // The se'if keeps its ref and its line id.
        assertEquals(4, built.refs.first().lineIndex)
        assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(first), keyHash(built, 3))
        assertTrue(lineWasModifiedByCleaning(requireNotNull(built.cleanShifts[3])))
    }

    @Test
    fun `commentator markers inside the topic stay on the seif line`() {
        val marker = """<i data-commentator="Peleti" data-order="1"></i>"""
        val built = build("שולחן ערוך, אורח חיים", listOf("<b>בריה אפילו באלף לא בטיל. ובו ${marker}ד' סעיפים:</b> גוף", "שני"))

        assertEquals("<b>בריה אפילו באלף לא בטיל. ובו ד' סעיפים:</b>", built.lines[2])
        assertEquals("(א) ${marker}גוף", built.lines[3])
    }

    @Test
    fun `single seif siman gets its topic line too`() {
        val built = build("שולחן ערוך, אורח חיים", listOf("<b>כוונת הברכות. ובו סעיף אחד:</b> יכוין"))

        assertEquals(listOf("<b>כוונת הברכות. ובו סעיף אחד:</b>", "יכוין"), built.lines.drop(2))
    }

    @Test
    fun `lines without a topic are left as they are`() {
        val countOnly = "<b>ובו סעיף אחד:</b> טקסט"
        val laterSeif = "<b>דין אחר. ובו ב סעיפים:</b> טקסט"
        assertEquals(countOnly, build("שולחן ערוך, אורח חיים", listOf(countOnly)).lines.last())
        assertEquals("(ב) $laterSeif", build("שולחן ערוך, אורח חיים", listOf("ראשון", laterSeif)).lines.last())
        assertEquals("(א) $laterSeif", build("ספר אחר", listOf(laterSeif, "שני")).lines[2])
    }
}
