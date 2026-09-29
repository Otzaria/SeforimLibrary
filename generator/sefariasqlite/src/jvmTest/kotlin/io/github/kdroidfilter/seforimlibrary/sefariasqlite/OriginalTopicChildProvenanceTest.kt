package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OriginalTopicChildProvenanceTest {
    @Test
    fun `existing Sefaria siman children are source entries regardless of generated shape resemblance`() {
        // Released Piskei Recanati IDs and Topic shape, with a new valid name signature.
        val simanim = listOf(
            SimanHeadingRow(541477, 541476, "סימן א", 2, 10003, 3, 4, "<b>דיני ציצית. ובו סעיף אחד:</b>", headingLineId = 10002),
            SimanHeadingRow(541478, 541476, "סימן ב", 4, 10005, 5, 6, "טקסט", headingLineId = 10004),
        )
        val snapshot = SimanNamesBookSnapshot(
            3806, "פסקי רקנאטי", 340, simanim,
            listOf(
                TopicEntryRow(38796, null, 0, "דיני ציצית ותפילין", 10003, 3, true),
                TopicEntryRow(38797, 38796, 1, "סימן א", 10003, 3, false),
                TopicEntryRow(38798, 38796, 1, "סימן ב", 10005, 5, false),
            ),
        )
        assertTrue(ownSimanNames(simanim).isNotEmpty())
        assertEquals(SimanNamesMode.RELABEL, classifyTopicStructure(snapshot))
        assertEquals(SimanNamesMode.RELABEL, planSimanNames(listOf(snapshot), emptyMap()).single().mode)
    }
}
