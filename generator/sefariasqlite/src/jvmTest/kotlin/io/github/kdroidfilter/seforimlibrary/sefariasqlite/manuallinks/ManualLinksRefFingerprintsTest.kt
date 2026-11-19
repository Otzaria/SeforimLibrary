package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ManualLinksRefFingerprintsTest {
    private val sample = ManualLinksRefFingerprints(
        sections = mapOf("Rashi on Eruvin 17a:4" to 6, "Target" to 2),
        refs = mapOf(
            "Rashi on Eruvin 17a:4:4" to ManualLinksRefFingerprints.hash("לא פנוי מכלים"),
            "Target 1" to ManualLinksRefFingerprints.hash("תוכן"),
        ),
    )

    @Test
    fun sectionIsTheParentNode() {
        assertEquals("Rashi on Kiddushin 57a:3", ManualLinksRefFingerprints.sectionOf("Rashi on Kiddushin 57a:3:2"))
        assertEquals("Mishneh Torah, Sabbath 5", ManualLinksRefFingerprints.sectionOf("Mishneh Torah, Sabbath 5:3"))
        assertEquals("Target", ManualLinksRefFingerprints.sectionOf("Target 1"))
    }

    @Test
    fun renderIsOneEntryPerLineAndRoundTrips() {
        val path = Files.createTempFile("fingerprints", ".json")
        Files.write(path, sample.render())

        assertEquals(sample, ManualLinksRefFingerprints.read(path))
        val text = Files.readString(path)
        assertEquals(11, text.removeSuffix("\n").lines().size)
        assertEquals("\"Rashi on Eruvin 17a:4\": 6,", text.lines()[3])
    }

    @Test
    fun renameRewritesRefsAndSectionsAtTheTitleBoundary() {
        val renamed = sample.renamed("Target", "Renamed")

        assertEquals(setOf("Rashi on Eruvin 17a:4", "Renamed"), renamed.sections.keys)
        assertEquals(setOf("Rashi on Eruvin 17a:4:4", "Renamed 1"), renamed.refs.keys)
    }

    @Test
    fun aRefWithoutItsSectionCountIsRejected() {
        val path = Files.createTempFile("fingerprints", ".json")
        Files.write(path, sample.copy(sections = mapOf("Target" to 2)).render())

        val error = assertFailsWith<IllegalArgumentException> { ManualLinksRefFingerprints.read(path) }
        assertEquals("manual_links_ref_fingerprints.json ref 'Rashi on Eruvin 17a:4:4' has no section count", error.message)
    }
}
