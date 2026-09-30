package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
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
    fun `a bare seif count gets its own line like every other topic`() {
        val built = build("שולחן ערוך, אורח חיים", listOf("<b>ובו סעיף אחד:</b> טקסט"))

        assertEquals(listOf("<b>ובו סעיף אחד:</b>", "טקסט"), built.lines.drop(2))
    }

    @Test
    fun `only the first seif of a siman in the Shulchan Aruch is split`() {
        val laterSeif = "<b>דין אחר. ובו ב סעיפים:</b> טקסט"
        assertEquals("(ב) $laterSeif", build("שולחן ערוך, אורח חיים", listOf("ראשון", laterSeif)).lines.last())
        assertEquals("(א) $laterSeif", build("ספר אחר", listOf(laterSeif, "שני")).lines[2])
    }

    @Test
    fun `a Topic alt-toc entry opening a siman points at its topic line`() = runBlocking {
        val heTitle = "שולחן ערוך, אורח חיים"
        val built = build(heTitle, listOf("<b>דין השכמת הבוקר. ובו ט סעיפים:</b> יתגבר", "שני"))
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)
        val bookId = repository.insertBook(
            Book(
                id = 0, categoryId = repository.insertCategory(Category(id = 0, parentId = null, title = "שולחן ערוך", level = 0, order = 0)),
                sourceId = repository.insertSource("Sefaria"), title = heTitle, heShortDesc = null, notesContent = null,
                order = 0f, totalLines = built.lines.size, isBaseBook = true, hasAltStructures = true,
            ),
        )
        val lineKeyToId = built.lines.indices.associate { i ->
            (heTitle to i) to repository.insertLine(Line(id = 0, bookId = bookId, lineIndex = i, content = built.lines[i], heRef = null))
        }
        val node = AltNodePayload(
            title = "Laws of Morning Conduct", heTitle = "הלכות הנהגת האדם בבוקר",
            wholeRef = "Shulchan Arukh, Orach Chayim 1", refs = listOf("Shulchan Arukh, Orach Chayim 1:1"),
            addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(), skippedAddresses = emptyList(),
            startingAddress = null, offset = null, children = emptyList(),
        )
        val payload = BookPayload(
            heTitle = heTitle, enTitle = "Shulchan Arukh, Orach Chayim", categoriesHe = listOf("שולחן ערוך"),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null, pubDates = emptyList(),
            altStructures = listOf(AltStructurePayload(key = "Topic", title = "Topic", heTitle = heTitle, nodes = listOf(node))),
        )

        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repository)
        SefariaAltTocBuilder(repository, bindings).buildAltTocStructuresForBook(payload, bookId, heTitle, lineKeyToId, built.lines.size)

        val structureId = repository.getAltTocStructuresForBook(bookId).single().id
        val entry = repository.getAltTocEntriesForStructure(structureId).single { it.text == "הלכות הנהגת האדם בבוקר" }
        assertEquals(lineKeyToId[heTitle to 2], entry.lineId)
        driver.close()
    }
}
