package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import io.github.kdroidfilter.seforimlibrary.sefariasqlite.BookPayload
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.RefEntry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class SefariaCorpusIndexTest {
    @Test
    fun retainsOnlyRequestedAnchorContentUnlessFullProofIsExplicit() {
        val payload = BookPayload(
            heTitle = "ספר",
            enTitle = "Book",
            categoriesHe = emptyList(),
            lines = listOf("one", "two", "three"),
            refEntries = listOf(
                RefEntry("Book 1", "ספר א", "Book", 1),
                RefEntry("Book 2", "ספר ב", "Book", 2),
                RefEntry("Book 3", "ספר ג", "Book", 3),
            ),
            headings = emptyList(),
            authors = emptyList(),
            description = null,
            heShortDesc = null,
            pubDates = emptyList(),
            altStructures = emptyList(),
        )

        val selective = payload.toManualIndex(
            RetainedLineRequirements(refs = setOf("Book 2")),
            retainFullLines = false,
        )
        assertEquals(3, selective.lineCount)
        assertEquals(1, selective.retainedAnchorLineCount)
        assertEquals(0, selective.retainedFullLineCount)
        assertNull(selective.retainedContent(1))
        assertEquals("two", selective.retainedContent(2))
        assertNull(selective.retainedContent(3))

        val proof = payload.toManualIndex(
            RetainedLineRequirements(lineIndexes = setOf(2)),
            retainFullLines = true,
        )
        assertEquals(3, proof.retainedFullLineCount)
        proof.releaseFullProofLines()
        assertEquals(0, proof.retainedFullLineCount)
        assertEquals("two", proof.retainedContent(2))
        assertNull(proof.retainedContent(1))
    }

    @Test
    fun missingHeRefIsNullableButDuplicateHeRefIsNeverAFallbackCandidate() {
        val duplicate = BookPayload(
            heTitle = "ספר",
            enTitle = "Book",
            categoriesHe = emptyList(),
            lines = listOf("one", "two"),
            refEntries = listOf(
                RefEntry("Book 1", "ספר א", "Book", 1),
                RefEntry("Book 2", "ספר א", "Book", 2),
            ),
            headings = emptyList(),
            authors = emptyList(),
            description = null,
            heShortDesc = null,
            pubDates = emptyList(),
            altStructures = emptyList(),
        ).toManualIndex(RetainedLineRequirements(), retainFullLines = false)
        val index = SefariaCorpusIndex.fromBooks(listOf(duplicate))

        assertNull(index.resolveHeRefOrNullIfMissing(duplicate, "ספר ב"))
        assertFailsWith<IllegalArgumentException> {
            index.resolveHeRefOrNullIfMissing(duplicate, "ספר א")
        }
    }
}
