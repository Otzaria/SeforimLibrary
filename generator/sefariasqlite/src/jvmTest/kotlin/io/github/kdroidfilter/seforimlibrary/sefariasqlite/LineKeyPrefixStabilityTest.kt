package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.common.ids.LineOccurrenceCounter
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Issue #1211, second half: the importer injects "(א) ", "(ב) "… into
 * `line.content`, so hashing the stored text would renumber a whole chapter
 * whenever one verse is inserted at its top. The key hashes the raw segment.
 */
class LineKeyPrefixStabilityTest {

    /** "(א) " — the generated prefix the reader records in cleanShiftByLineIndex. */
    private val prefixLen = 4

    private fun chapter(verses: List<String>, withRefs: Boolean = true): BookPayload {
        val lines = verses.mapIndexed { idx, v -> "(${'א' + idx}) $v" }
        return BookPayload(
            heTitle = "ספר בדיקה", enTitle = "Test Book", categoriesHe = listOf("תנך"),
            lines = lines,
            refEntries = if (!withRefs) emptyList() else lines.indices.map {
                RefEntry(ref = "Test 1:${it + 1}", heRef = "בדיקה א׳:${'א' + it}", path = "p", lineIndex = it + 1)
            },
            headings = emptyList(), authors = emptyList(),
            description = null, heShortDesc = null, pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = lines.indices.associateWith { prefixLen },
        ).precomputeLineData()
    }

    /** Allocates the book's line ids exactly the way the insert loop does. */
    private fun allocate(allocator: InMemoryIdAllocator, bookId: Long, payload: BookPayload): List<Long> {
        val pre = requireNotNull(payload.precomputed)
        val occ = LineOccurrenceCounter()
        val legacyOcc = LineOccurrenceCounter()
        return payload.lines.indices.map { idx ->
            val hash = pre.lineKeyHashes!![idx]
            val legacyHash = pre.legacyLineKeyHashes!![idx]
            allocator.lineId(
                bookId, hash, occ.next(bookId, hash),
                LegacyLineKey(legacyHash, legacyOcc.next(bookId, legacyHash)),
            )
        }
    }

    @Test
    fun `the natural key ignores the generated prefix`() {
        val pre = requireNotNull(chapter(listOf("פסוק אלף")).precomputed)
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash("פסוק אלף"),
            pre.lineKeyHashes!![0],
            "the key must hash the raw segment, not \"(א) פסוק אלף\"",
        )
        // The legacy array is the pre-#1211 shape and still sees the rendered line.
        assertContentEquals(
            LegacyLineKey.hash("(א) פסוק אלף", "בדיקה א׳:א"),
            pre.legacyLineKeyHashes!![0],
        )
    }

    @Test
    fun `inserting a verse at the top keeps the ids of every verse after it`() {
        val allocator = InMemoryIdAllocator.load(null)
        val bookId = allocator.bookId("Sefaria", "ספר בדיקה")
        val before = allocate(allocator, bookId, chapter(listOf("פסוק אלף", "פסוק בית", "פסוק גימל")))

        // Same three verses, now prefixed (ב)(ג)(ד) because one was inserted.
        val after = allocate(
            allocator,
            bookId,
            chapter(listOf("פסוק חדש", "פסוק אלף", "פסוק בית", "פסוק גימל")),
        )
        assertEquals(before, after.drop(1), "a head-insert must not renumber the rest of the chapter")
        assertEquals(4, after.toSet().size)
        assertEquals(0, allocator.legacyLineKeysMigrated(), "no seed, so nothing to migrate")
    }

    @Test
    fun `hashing the rendered line would have renumbered them`() {
        // Pins WHY the shift is stripped: the rendered text really does differ.
        val v1 = chapter(listOf("פסוק אלף", "פסוק בית")).lines[1]
        val v2 = chapter(listOf("פסוק חדש", "פסוק אלף", "פסוק בית")).lines[2]
        assertNotEquals(v1, v2)
        assertEquals("פסוק בית", v1.substring(prefixLen))
        assertEquals("פסוק בית", v2.substring(prefixLen))
    }

    @Test
    fun `cleaned lines still ignore a generated prefix`() {
        val json = Json { ignoreUnknownKeys = true }
        val reader = SefariaBookPayloadReader(json, Logger.withTag("LineKeyPrefixStabilityTest"))
        val schema = json.parseToJsonElement(
            """{"depth":1,"sectionNames":["Paragraph"],"addressTypes":["String"]}""",
        ).jsonObject
        fun cleanedLineHash(segments: List<String>): ByteArray {
            val built = reader.walkTextWithSchema(
                schemaObj = schema,
                textElement = JsonArray(segments.map(::JsonPrimitive)),
                bookHeTitle = "ספר בדיקה",
                bookEnTitle = "Test Book",
            )
            val lineIndex = built.lines.indexOfFirst { "טקסט שנוקה" in it }
            val encodedShift = requireNotNull(built.cleanShifts[lineIndex])
            assertTrue(lineWasModifiedByCleaning(encodedShift))
            assertEquals(4, generatedPrefixLength(encodedShift))
            val payload = BookPayload(
                heTitle = "ספר בדיקה", enTitle = "Test Book", categoriesHe = listOf("תנך"),
                lines = built.lines, refEntries = built.refs, headings = built.headings,
                authors = emptyList(), description = null, heShortDesc = null,
                pubDates = emptyList(), altStructures = emptyList(),
                cleanShiftByLineIndex = built.cleanShifts,
                lineKeyHashOverrides = built.lineKeyHashOverrides,
            ).precomputeLineData()
            return requireNotNull(payload.precomputed).lineKeyHashes!![lineIndex]
        }

        // The Otzar markup forces cleanSefariaLine to modify this segment.
        // Inserting a segment before it changes its generated prefix (א -> ב),
        // but must not change the content key.
        val before = cleanedLineHash(listOf("@04טקסט} שנוקה", "שורה שנייה"))
        val after = cleanedLineHash(listOf("שורה חדשה", "@04טקסט} שנוקה", "שורה שנייה"))
        assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash("טקסט שנוקה"), before)
        assertContentEquals(before, after, "cleaning must not make the generated prefix part of the key")
    }

    @Test
    fun `an inline br is kept in the text but keyed as a space`() {
        val json = Json { ignoreUnknownKeys = true }
        val reader = SefariaBookPayloadReader(json, Logger.withTag("LineKeyPrefixStabilityTest"))
        val schema = json.parseToJsonElement(
            """{"depth":1,"sectionNames":["Paragraph"],"addressTypes":["String"]}""",
        ).jsonObject
        val built = reader.walkTextWithSchema(
            schemaObj = schema,
            textElement = JsonArray(listOf("<b>כותרת</b><br>גוף הדיבור", "שורה שנייה").map(::JsonPrimitive)),
            bookHeTitle = "ספר בדיקה",
            bookEnTitle = "Test Book",
        )
        val lineIndex = built.lines.indexOfFirst { "גוף הדיבור" in it }
        assertTrue(built.lines[lineIndex].endsWith("<b>כותרת</b><br>גוף הדיבור"))
        val payload = BookPayload(
            heTitle = "ספר בדיקה", enTitle = "Test Book", categoriesHe = listOf("תנך"),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null,
            pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = built.cleanShifts,
            lineKeyHashOverrides = built.lineKeyHashOverrides,
        ).precomputeLineData()
        // Books built while inline breaks were collapsed keep their line ids.
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash("<b>כותרת</b> גוף הדיבור"),
            requireNotNull(payload.precomputed).lineKeyHashes!![lineIndex],
        )
    }

    @Test
    fun `preserved breaks do not change keys when dashless repair is suppressed`() {
        assertHistoricDashlessKey("דיבור<br>נוסף. פירוש הדברים")
    }

    @Test
    fun `preserved breaks do not change keys when dashless repair is newly enabled`() {
        assertHistoricDashlessKey("דיבור. פירוש<br>– המשך")
    }

    private fun assertHistoricDashlessKey(raw: String) {
        val json = Json { ignoreUnknownKeys = true }
        val reader = SefariaBookPayloadReader(json, Logger.withTag("LineKeyPrefixStabilityTest"))
        val heTitle = "תוספות על סוכה"
        val built = reader.walkTextWithSchema(
            schemaObj = json.parseToJsonElement(
                """{"depth":1,"sectionNames":["Paragraph"],"addressTypes":["String"]}""",
            ).jsonObject,
            textElement = JsonArray(listOf(raw, "שורה שנייה").map(::JsonPrimitive)),
            bookHeTitle = heTitle,
            bookEnTitle = "Tosafot on Sukkah",
        )
        val idx = built.refs.first().lineIndex - 1
        assertTrue("<br>" in built.lines[idx])
        val payload = BookPayload(
            heTitle = heTitle, enTitle = "Tosafot on Sukkah", categoriesHe = emptyList(),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null,
            pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = built.cleanShifts,
            lineKeyHashOverrides = built.lineKeyHashOverrides,
        ).precomputeLineData()
        // The old importer collapsed breaks BEFORE deciding whether to add a dash.
        val oldContent = SefariaDashlessDibburim.separate(
            heTitle, cleanSefariaLine(raw, collapseInlineBreaks = true),
        )
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash(oldContent),
            requireNotNull(payload.precomputed).lineKeyHashes!![idx],
            "The existing line id must survive both directions of the dashless decision: $raw",
        )
    }

    @Test
    fun `reader preserves persisted ids through breaks duplicates and prefix shifts`() = runBlocking {
        val tempDir = Files.createTempDirectory("inline-br-ids")
        try {
            val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
            val jsonDir = Files.createDirectories(tempDir.resolve("json"))
            val bookDir = Files.createDirectories(jsonDir.resolve("Tosafot_on_Sukkah"))
            val heTitle = "תוספות על סוכה"
            val enTitle = "Tosafot on Sukkah"
            Files.writeString(schemaDir.resolve("$enTitle.json"), """
                {"title":"$enTitle","heTitle":"$heTitle","schema":{
                  "title":"$enTitle","heTitle":"$heTitle","depth":1,
                  "sectionNames":["Paragraph"],"addressTypes":["String"]}}
            """.trimIndent())
            val raw = listOf(
                "דיבור<br>נוסף. פירוש הדברים",
                "דיבור. פירוש<br>– המשך",
                "<b>כותרת</b><BR />גוף הדיבור",
                "דיבור<br>נוסף. פירוש הדברים", // same key, distinct occurrence/id
                "פרשה פתוחה<br>",
                "שורה ללא תג",
            )
            val oldContents = raw.map {
                SefariaDashlessDibburim.separate(heTitle, cleanSefariaLine(it, collapseInlineBreaks = true))
            }
            val oldAllocator = InMemoryIdAllocator.load(null)
            val bookId = oldAllocator.bookId("Sefaria", heTitle)
            val oldOccurrences = LineOccurrenceCounter()
            val oldIds = oldContents.map {
                val hash = IdAllocatorBindings.lineNaturalKeyHash(it)
                oldAllocator.lineId(bookId, hash, oldOccurrences.next(bookId, hash))
            }
            val state = tempDir.resolve("build_state.db")
            oldAllocator.snapshotTo(state)

            val reader = SefariaBookPayloadReader(Json, Logger.withTag("LineKeyPrefixStabilityTest"))
            val schemaLookup = reader.buildSchemaLookup(schemaDir)
            for (insertAtHead in listOf(false, true)) {
                val segments = if (insertAtHead) listOf("מקטע חדש") + raw else raw
                Files.writeString(bookDir.resolve("merged.json"), buildJsonObject {
                    put("title", enTitle)
                    put("heTitle", heTitle)
                    put("text", JsonArray(segments.map(::JsonPrimitive)))
                }.toString())
                val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()
                    .precomputeLineData()
                val allocator = InMemoryIdAllocator.load(state)
                val ids = allocate(allocator, bookId, payload)
                val segmentIds = payload.refEntries.map { ids[it.lineIndex - 1] }
                assertEquals(oldIds, if (insertAtHead) segmentIds.drop(1) else segmentIds)
                assertEquals(segmentIds.size, segmentIds.toSet().size)
                assertEquals(4, payload.lineKeyHashOverrides.size, "store only changed segment keys")
                assertTrue(payload.lines.any { "<b>כותרת</b><br>גוף הדיבור" in it })
            }
        } finally {
            tempDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `a legacy prefixed line without an heRef still migrates`() {
        // Build 1: pre-#1211 keys — no heRef, so the rendered (prefixed) content.
        val build1 = InMemoryIdAllocator.load(null)
        val bookId = build1.bookId("Sefaria", "ספר בדיקה")
        val old = chapter(listOf("פסוק אלף", "פסוק בית"), withRefs = false)
        val legacyOcc = LineOccurrenceCounter()
        val legacyIds = old.lines.indices.map { idx ->
            val h = requireNotNull(old.precomputed).legacyLineKeyHashes!![idx]
            build1.lineId(bookId, h, legacyOcc.next(bookId, h))
        }

        // Build 2 keys on the raw segment; the shim must carry both ids over.
        val statePath = Files.createTempDirectory("legacy-prefix").resolve("build_state.db")
        build1.snapshotTo(statePath)
        val build2 = InMemoryIdAllocator.load(statePath)
        val newIds = allocate(build2, build2.bookId("Sefaria", "ספר בדיקה"), chapter(listOf("פסוק אלף", "פסוק בית"), withRefs = false))
        assertEquals(legacyIds, newIds)
        assertEquals(2, build2.legacyLineKeysMigrated())
        assertEquals(0, build2.legacyLineKeysMissed())
    }
}
