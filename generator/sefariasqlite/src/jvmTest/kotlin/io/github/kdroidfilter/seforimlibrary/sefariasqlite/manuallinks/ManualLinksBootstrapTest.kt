package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class ManualLinksBootstrapTest {
    @Test
    fun nationalAndMoreBooksGrammarsAreExact() {
        assertEquals(
            "משנה תורה, הלכות שבת א, ב",
            ManualLinksBootstrap.nationalLibraryHeRef(
                "ספר המדע, הלכות שבת, פרק א, ב",
                "משנה תורה, הלכות שבת",
            ),
        )
        assertEquals("אבודרהם, סדר ג", ManualLinksBootstrap.moreBooksHeRef("אבודרהם, סדר ג,  "))
        assertFailsWith<IllegalArgumentException> {
            ManualLinksBootstrap.nationalLibraryHeRef("ספר, פרק א, ב", "יעד")
        }
    }

    @Test
    fun dictaGrammarKeepsTheVerbatimHeRefAndProvesItsTitleBoundary() {
        assertEquals(
            "רש\"י על שבת כו:, א, א",
            ManualLinksBootstrap.dictaHeRef("רש\"י על שבת כו:, א, א", "רש\"י על שבת"),
        )
        // The quote-stripped Otzaria basename must never satisfy the Sefaria heTitle boundary.
        assertFailsWith<IllegalArgumentException> {
            ManualLinksBootstrap.dictaHeRef("רשי על שבת כו:, א, א", "רש\"י על שבת")
        }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksBootstrap.dictaHeRef("רש\"י על שבת כו:, א, א,", "רש\"י על שבת")
        }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksBootstrap.dictaHeRef("רש\"י על שבת", "רש\"י על שבת")
        }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksBootstrap.dictaHeRef("רש\"י על שבתא כו:, א, א", "רש\"י על שבת")
        }
    }

    @Test
    fun renameBoundariesAndCrossPlatformPathsAreExact() {
        assertEquals("New 1:2", ManualLinksRefresh.rewriteAtTitleBoundary("Old 1:2", "Old", "New"))
        assertEquals("New, Part 1", ManualLinksRefresh.rewriteAtTitleBoundary("Old, Part 1", "Old", "New"))
        assertFailsWith<IllegalStateException> {
            ManualLinksRefresh.rewriteAtTitleBoundary("Older 1", "Old", "New")
        }
        assertFailsWith<IllegalStateException> {
            ManualLinksRefresh.rewriteAtTitleBoundary("Old:1", "Old", "New")
        }
        assertEquals("a/b/New.txt", ManualLinksRefresh.replaceFinalPathComponent("a/b/Old.txt", "Old.txt", "New.txt"))
        assertEquals("a\\b\\New.txt", ManualLinksRefresh.replaceFinalPathComponent("a\\b\\Old.txt", "Old.txt", "New.txt"))
        assertNull(ManualLinksRefresh.replaceFinalPathComponent("a/Older.txt", "Old.txt", "New.txt"))
    }

    @Test
    fun tashmaMarkerRemovalStripsExactlyOneValidatedPrefix() {
        assertEquals("(א) גוף", ManualLinksRefresh.stripOneLeadingMarker("(א) (א) גוף"))
        assertEquals("גוף", ManualLinksRefresh.stripOneLeadingMarker(" {י״ב} גוף"))
        assertEquals("גוף", ManualLinksRefresh.stripOneLeadingMarker("גוף"))
    }

    @Test
    fun hebrewRenameCannotMakeAMissingRefRecordInvisible() {
        assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh.requireHebrewRenameRef(false, "new_source_ref_required")
        }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh.requireHebrewRenameRef(false, "new_target_ref_required")
        }
        ManualLinksRefresh.requireHebrewRenameRef(true, "unused")
    }

    @Test
    fun identicalOverrideTriplesAreDisambiguatedOnlyByExactPostStateHash() {
        fun override(hash: String) = BootstrapRecordOverride(
            path = "MoreBooks/links/book_links.json",
            recordSha256 = "0".repeat(64),
            postRecordSha256 = hash,
            requireHeRef2 = "אבודרהם, סדר ג,",
            ref2 = "Abudarham, Weekday Prayers 3",
            lineIndex2 = 191,
        )
        val first = override("1".repeat(64))
        val second = override("2".repeat(64))

        assertEquals(
            second,
            ManualLinksRefresh.exactPostStateOverride(
                overrides = listOf(first, second),
                path = second.path,
                stableRecordHash = second.postRecordSha256,
                heRef2 = second.requireHeRef2,
                ref2 = second.ref2,
                lineIndex2 = second.lineIndex2,
            ),
        )
        assertFailsWith<IllegalStateException> {
            ManualLinksRefresh.exactPostStateOverride(
                listOf(first, second),
                second.path,
                "3".repeat(64),
                second.requireHeRef2,
                second.ref2,
                second.lineIndex2,
            )
        }
    }
}
