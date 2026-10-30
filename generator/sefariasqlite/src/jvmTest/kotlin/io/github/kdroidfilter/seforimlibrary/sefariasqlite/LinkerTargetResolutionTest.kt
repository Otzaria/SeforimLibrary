package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.assertTrue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LinkerTargetResolutionTest {
    private val refs = listOf(
        RefEntry("Proverbs 27:1", "", "Proverbs", 1),
        RefEntry("Proverbs 27:27", "", "Proverbs", 27),
        RefEntry("Shulchan Arukh, Yoreh De'ah 216:1", "", "YD", 1),
        RefEntry("Shulchan Arukh, Yoreh De'ah 216:2", "", "YD", 2),
        RefEntry("Mishnah Bava Batra 5:1", "", "MBB", 1),
        RefEntry("Mishnah Bava Batra 5:11", "", "MBB", 11),
        RefEntry("Mishneh Torah, Divorce 4:1", "", "MT", 1),
        RefEntry("Mishneh Torah, Divorce 4:23", "", "MT", 23),
        RefEntry("Ibn Ezra on Genesis 6:1:2", "", "IE", 1),
        RefEntry("Ibn Ezra on Genesis 6:1:3", "", "IE", 2),
        RefEntry("Berakhot 2a:1", "", "Berakhot", 1),
        RefEntry("Berakhot 2a:2", "", "Berakhot", 2),
        RefEntry("Berakhot 2b:1", "", "Berakhot", 3),
        RefEntry("Berakhot 2b:2", "", "Berakhot", 4),
        RefEntry("Hyphen-Title 1:1", "", "HT", 1),
        RefEntry("Book 1-2 Stories 1:1", "", "HT2", 1),
    )
    private fun resolve(citation: String, entries: List<RefEntry> = refs) = resolveLinkerTarget(
        citation, entries.groupBy { canonicalCitation(it.ref) }, SectionRefIndex.build(entries),
    )

    @Test fun all42PublishedAddressMismatchesAreRejected() {
        val fixture = Json.parseToJsonElement(checkNotNull(javaClass.getResource("/linker/v35-invalid-targets.json"))
            .readText()).jsonArray.map { row ->
                row.jsonObject.let { it.getValue("requested").jsonPrimitive.content to it.getValue("previous").jsonPrimitive.content }
            }
        val entries = fixture.map { it.second }.distinct().mapIndexed { i, ref ->
            RefEntry(ref, "", ref.substringBeforeLast(' '), i + 1)
        }
        val byCanonical = entries.groupBy { canonicalCitation(it.ref) }
        val byBase = entries.associateBy { canonicalBase(it.ref) }
        val sections = SectionRefIndex.build(entries)
        assertEquals(42, fixture.size)
        for ((citation, _) in fixture) {
            assertTrue(resolveRefs(citation, byCanonical, byBase).isNotEmpty(), "legacy broadening: $citation")
            assertNull(resolveLinkerTarget(citation, byCanonical, sections), citation)
        }
    }

    @Test fun missingExplicitAddressesNeverBroadenToAChapter() {
        for (citation in listOf(
            "Shulchan Arukh, Yoreh De'ah 216:119", "Mishnah Bava Batra 5:75",
            "Mishneh Torah, Divorce 4:31", "Proverbs 27:31-32",
            "Ibn Ezra on Genesis 6:2", "Berakhot 2a:99", "Proverbs 270",
        )) assertNull(resolve(citation), citation)
    }

    @Test fun aRangeRequiresBothEndpointsAndTheirOrder() {
        for (citation in listOf("Proverbs 27:1-99", "Proverbs 27:99-100",
            "Proverbs 27:27-1", "Berakhot 2a-9b", "Berakhot 2b-2a", "Proverbs 27:1-2:3:4")) {
            assertNull(resolve(citation), citation)
        }
        assertEquals(27, resolve("Proverbs 27:1-27")?.last?.lineIndex)
        assertEquals(4, resolve("Berakhot 2a-2b")?.last?.lineIndex)
    }

    @Test fun exactSegmentsAndProvenSectionsKeepTheirScope() {
        assertEquals(null, resolve("Proverbs 27:1")?.last)
        assertEquals(1, resolve("Proverbs 27:1")?.first?.lineIndex)
        assertEquals(27, resolve("Proverbs 27")?.last?.lineIndex)
        assertEquals(2, resolve("Ibn Ezra on Genesis 6:1")?.last?.lineIndex)
        assertEquals(2, resolve("Ibn Ezra on Genesis 6")?.last?.lineIndex)
        assertEquals(4, resolve("Berakhot 2")?.last?.lineIndex)
        assertEquals("HT", resolve("Hyphen-Title 1:1")?.first?.path)
        assertEquals("HT2", resolve("Book 1-2 Stories 1:1")?.first?.path)
        assertNull(resolve("Proverbs")) // whole work is not a numbered section
    }

    @Test fun ambiguousLeavesAndSectionsAreRejected() {
        val duplicate = refs + RefEntry("Proverbs 27:1", "", "other", 1)
        var ambiguities = 0
        assertNull(resolveLinkerTarget("Proverbs 27:1-27", duplicate.groupBy { canonicalCitation(it.ref) },
            SectionRefIndex.build(duplicate)) { ambiguities++ })
        assertEquals(1, ambiguities)
        assertNull(resolve("Proverbs 27", duplicate))
        // Each endpoint alone exists, but the range crosses different sidecar paths.
        assertNull(resolve("Proverbs 27:1-27", refs.filterNot { it.ref == "Proverbs 27:27" } +
            RefEntry("Proverbs 27:27", "", "other", 27)))
    }
}
