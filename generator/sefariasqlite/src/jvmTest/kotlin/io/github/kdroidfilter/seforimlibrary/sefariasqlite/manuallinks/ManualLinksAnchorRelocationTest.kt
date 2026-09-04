package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.readText
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The real incident: Sefaria deleted three characters early in one Mishnah Berurah segment and every
 * word-level anchor in it drifted. These tests pin the self-healing path end to end.
 */
class ManualLinksAnchorRelocationTest {
    private val logger = Logger.withTag("ManualLinksAnchorRelocationTest")

    @Test
    fun contextIsFilledOnAnUnchangedSegmentDuringRefresh() {
        val fixture = anchoredFixture(starts = listOf(50, 178))
        val bootstrapped = fixture.bootstrapOutput
        assertTrue(bootstrapped.contains("\"anchor_context\": {\"before\":"))
        val record = ManualLinksDocument.parse(bootstrapped).record(0)
        assertEquals(
            fixture.segment.substring(50, 50 + ManualLinksAnchor.WINDOW),
            record.get("anchor_context").get("after").textValue(),
        )
        assertEquals(
            fixture.segment.substring(50 - ManualLinksAnchor.WINDOW, 50),
            record.get("anchor_context").get("before").textValue(),
        )

        val withoutContext = stripAnchorContext(bootstrapped)
        assertNotEquals(bootstrapped, withoutContext)
        val result = fixture.refresh(withoutContext, fixture.segment, "fill")

        assertEquals("ok", result.status)
        // Byte-preserving: refilling the context must reproduce the bootstrap bytes exactly.
        assertEquals(bootstrapped, fixture.outputOf("fill").readText())
        val records = ManualLinksJson.readStrict(result.reportPath).get("records")
        assertEquals(2, records.get("anchors_context_filled").intValue())
        assertEquals(0, records.get("anchors_relocated").intValue())
    }

    @Test
    fun aDeletionBeforeTheAnchorRelocatesEveryAnchorInTheSegment() {
        val starts = listOf(50, 178, 223, 242, 275)
        val fixture = anchoredFixture(starts)
        val edited = fixture.segment.substring(0, 40) + fixture.segment.substring(43)

        val result = fixture.refresh(fixture.bootstrapOutput, edited, "deletion")

        assertEquals("ok", result.status)
        val document = ManualLinksDocument.read(fixture.outputOf("deletion"))
        starts.forEachIndexed { index, start ->
            assertEquals(start - 3, document.record(index).get("start").intValue(), "record $index")
            assertEquals(
                "sha256:${ManualLinksJson.sha256(edited.toByteArray(Charsets.UTF_8))}",
                document.record(index).get("anchor_src_hash").textValue(),
            )
            assertEquals(
                edited.substring(start - 3, start - 3 + ManualLinksAnchor.WINDOW),
                document.record(index).get("anchor_context").get("after").textValue(),
            )
        }
        val report = ManualLinksJson.readStrict(result.reportPath)
        val records = report.get("records")
        assertEquals(5, records.get("anchors_relocated").intValue())
        assertEquals(0, records.get("anchors_context_filled").intValue())
        assertEquals(0, records.get("anchors_unrelocatable").size())
        assertEquals(5, report.get("anchors").get("checked").intValue())
        assertEquals(0, report.get("anchors").get("drifted").intValue())
        val strategies = records.get("anchors_relocations").map { it.get("strategy").textValue() }
        // The first anchor's stored windows straddle the deletion, so only the one-sided fallback hits.
        assertEquals("after${ManualLinksAnchor.FALLBACK_WINDOW}", strategies.first())
        assertEquals(listOf("full", "full", "full", "full"), strategies.drop(1))
        assertEquals(50, records.get("anchors_relocations").first().get("old_start").intValue())
        assertEquals(47, records.get("anchors_relocations").first().get("new_start").intValue())
    }

    @Test
    fun anInsertionBeforeTheAnchorRelocatesForward() {
        val fixture = anchoredFixture(starts = listOf(178))
        val edited = fixture.segment.substring(0, 12) + "פתיחה " + fixture.segment.substring(12)

        val result = fixture.refresh(fixture.bootstrapOutput, edited, "insertion")

        assertEquals("ok", result.status)
        assertEquals(178 + 6, ManualLinksDocument.read(fixture.outputOf("insertion")).record(0).get("start").intValue())
        val records = ManualLinksJson.readStrict(result.reportPath).get("records")
        assertEquals(1, records.get("anchors_relocated").intValue())
        assertEquals("full", records.get("anchors_relocations").first().get("strategy").textValue())
    }

    @Test
    fun anEditInsideTheAfterWindowFallsBackToTheShorterWindows() {
        val fixture = anchoredFixture(starts = listOf(178))
        val edited = fixture.segment.substring(0, 198) + "ק" + fixture.segment.substring(199)
        assertNotEquals(fixture.segment, edited)

        val result = fixture.refresh(fixture.bootstrapOutput, edited, "after-window")

        assertEquals("ok", result.status)
        val record = ManualLinksDocument.read(fixture.outputOf("after-window")).record(0)
        assertEquals(178, record.get("start").intValue())
        assertEquals(
            "sha256:${ManualLinksJson.sha256(edited.toByteArray(Charsets.UTF_8))}",
            record.get("anchor_src_hash").textValue(),
        )
        val records = ManualLinksJson.readStrict(result.reportPath).get("records")
        assertEquals(1, records.get("anchors_relocated").intValue())
        assertEquals(
            "window${ManualLinksAnchor.FALLBACK_WINDOW}",
            records.get("anchors_relocations").first().get("strategy").textValue(),
        )
    }

    @Test
    fun anAmbiguousContextLeavesTheRecordUntouchedAndIsCapped() {
        val fixture = anchoredFixture(starts = listOf(50))
        val duplicated = fixture.segment + " " + fixture.segment

        val result = fixture.refresh(fixture.bootstrapOutput, duplicated, "ambiguous")

        assertTrue(result.status == "ok" || result.status == "no_op", result.status)
        assertEquals(fixture.bootstrapOutput, fixture.outputOf("ambiguous").readText())
        val report = ManualLinksJson.readStrict(result.reportPath)
        assertEquals(0, report.get("records").get("anchors_relocated").intValue())
        val unrelocatable = report.get("records").get("anchors_unrelocatable").single()
        assertEquals("ambiguous_context", unrelocatable.get("reason").textValue())
        assertEquals(50, unrelocatable.get("start").intValue())
        // `drifted` stays the "the run stopped" signal the corpus-QA gate reads; an absorbed
        // drift is `unrelocatable`, and the anchor still counts as checked for expectedAnchors.
        assertEquals(0, report.get("anchors").get("drifted").intValue())
        assertEquals(1, report.get("anchors").get("unrelocatable").intValue())
        assertEquals(1, report.get("anchors").get("checked").intValue())

        val capped = assertFailsWith<IllegalArgumentException> {
            fixture.refresh(fixture.bootstrapOutput, duplicated, "ambiguous-capped", cap = 0)
        }
        assertTrue(capped.message!!.contains("anchor_content_drift"), capped.message!!)
    }

    @Test
    fun anEditOnTheAnchoredWordItselfIsRefusedWhenTheSidesDisagree() {
        val fixture = anchoredFixture(starts = listOf(178))
        // The two windows survive but are no longer adjacent, so each side alone would place the
        // anchor somewhere else. Relocating onto either would stamp a fresh hash over a wrong offset.
        val edited = fixture.segment.substring(0, 178) + "פסקה חדשה " + fixture.segment.substring(178)

        val result = fixture.refresh(fixture.bootstrapOutput, edited, "sides-disagree")

        assertTrue(result.status == "ok" || result.status == "no_op", result.status)
        assertEquals(fixture.bootstrapOutput, fixture.outputOf("sides-disagree").readText())
        val report = ManualLinksJson.readStrict(result.reportPath)
        assertEquals(0, report.get("records").get("anchors_relocated").intValue())
        val unrelocatable = report.get("records").get("anchors_unrelocatable").single()
        assertEquals("context_sides_disagree", unrelocatable.get("reason").textValue())
        assertEquals(178, unrelocatable.get("start").intValue())
        assertEquals(1, report.get("anchors").get("checked").intValue())
        assertEquals(1, report.get("anchors").get("unrelocatable").intValue())
    }

    @Test
    fun aDriftedRecordWithoutStoredContextStaysUnrelocatable() {
        val fixture = anchoredFixture(starts = listOf(50))
        val withoutContext = stripAnchorContext(fixture.bootstrapOutput)
        val edited = fixture.segment.substring(0, 40) + fixture.segment.substring(43)

        val result = fixture.refresh(withoutContext, edited, "no-context")

        assertEquals(withoutContext, fixture.outputOf("no-context").readText())
        val report = ManualLinksJson.readStrict(result.reportPath)
        val unrelocatable = report.get("records").get("anchors_unrelocatable").single()
        assertEquals("missing_anchor_context", unrelocatable.get("reason").textValue())
        assertEquals(0, report.get("records").get("anchors_context_filled").intValue())
        assertEquals(1, report.get("anchors").get("checked").intValue())
        assertEquals(1, report.get("anchors").get("unrelocatable").intValue())
        assertEquals(0, report.get("anchors").get("drifted").intValue())
    }

    @Test
    fun fiveThousandRecordsRelocateWellUnderTwoSeconds() {
        val segment = segmentText()
        val edited = segment.substring(0, 40) + segment.substring(43)
        val drifted = setOf(17, 2_500, 4_999)
        val source = buildString {
            append("[\n")
            (0 until 5_000).forEach { index ->
                if (index > 0) append(",\n")
                val start = 178 + (index % 40)
                val context = ManualLinksAnchor.contextAt(segment, start)
                append("  {\n")
                append("    \"line_index_1\": 2,\n")
                append("    \"ref_1\": \"Source 1\",\n")
                append("    \"anchor_src_hash\": \"sha256:${ManualLinksJson.sha256(segment.toByteArray(Charsets.UTF_8))}\",\n")
                append("    \"anchor_context\": {\"before\":${jsonText(context.before)},")
                append("\"after\":${jsonText(context.after)}},\n")
                append("    \"heRef_2\": \"מפרש $index\",\n")
                append("    \"path_2\": \"מפרש.txt\",\n")
                append("    \"line_index_2\": ${index + 1},\n")
                append("    \"start\": $start\n")
                append("  }")
            }
            append("\n]")
        }

        val elapsed = measureTimeMillis {
            val document = ManualLinksDocument.parse(source)
            var relocated = 0
            repeat(document.records.size()) { index ->
                if (index !in drifted) return@repeat
                val context = ManualLinksAnchor.contextOrNull(document.record(index).get("anchor_context"))!!
                val relocation = ManualLinksAnchor.relocate(edited, context)
                assertTrue(relocation is ManualLinksAnchor.Relocation.Relocated, "record $index")
                val start = relocation.start
                assertEquals(document.record(index).get("start").intValue() - 3, start)
                document.setInt(index, "start", start)
                document.setString(index, "anchor_src_hash", "sha256:${ManualLinksJson.sha256(edited.toByteArray(Charsets.UTF_8))}")
                document.setObject(index, "anchor_context", ManualLinksAnchor.contextNode(edited, start))
                relocated++
            }
            assertEquals(3, relocated)
            val rendered = document.render()
            assertEquals(5_000, ManualLinksDocument.parse(rendered).records.size())
        }
        assertTrue(elapsed < 2_000, "relocation over 5000 records took ${elapsed}ms")
    }

    private fun stripAnchorContext(text: String): String =
        text.replace(Regex(",\\s*\"anchor_context\": \\{[^{}]*}"), "")

    /** Numbered tokens: every 12-character window is unique, so uniqueness failures are deliberate. */
    private fun segmentText(tokens: Int = 90): String = (1..tokens).joinToString(" ") { "מילה$it" }

    private fun jsonText(value: Any): String = ManualLinksJson.mapper.writeValueAsString(value)

    private inner class Fixture(
        val segment: String,
        val linkFile: Path,
        val export: Path,
        val arguments: ManualLinksArguments,
        val lineage: Path,
        val bootstrapOutput: String,
    ) {
        fun outputOf(name: String): Path =
            arguments.output.parent.resolve("$name/links/מקור_links.json")

        fun refresh(
            linkFileText: String,
            segmentText: String,
            outputName: String,
            cap: Int = ManualLinksAnchor.DEFAULT_UNRELOCATABLE_CAP,
        ): ManualLinksResult {
            Files.writeString(linkFile, linkFileText)
            writeSourceBook(export, segmentText)
            return ManualLinksRefresh(
                arguments.copy(
                    mode = ManualLinksMode.REFRESH,
                    lineagePath = lineage,
                    changelogDir = Files.createDirectories(arguments.output.parent.resolve("$outputName-chain")),
                    output = arguments.output.parent.resolve(outputName),
                    anchorUnrelocatableCap = cap,
                ),
                logger,
            ).run()
        }
    }

    /**
     * Builds a Sefaria source book with one long segment plus a manual-link file anchored into it,
     * runs the bootstrap that stamps `anchor_src_hash` and `anchor_context`, and returns both.
     */
    private fun anchoredFixture(starts: List<Int>): Fixture {
        val segment = segmentText()
        val temp = Files.createTempDirectory("manual-links-anchor")
        val repository = Files.createDirectories(temp.resolve("repo"))
        val links = Files.createDirectories(repository.resolve("links"))
        val linkFile = links.resolve("מקור_links.json")
        Files.writeString(linkFile, linkFileText(starts))
        val config = repository.resolve("manual_links_sync.json")
        Files.writeString(
            config,
            """
                {
                  "schema_version": 1,
                  "seforim_tool_ref": "refs/heads/test",
                  "links_roots": [
                    {"path": "links", "expected_state": "present"}
                  ],
                  "bootstrap_adapters": {},
                  "bootstrap_file_renames": [],
                  "bootstrap_record_overrides": []
                }
            """.trimIndent(),
        )
        val export = Files.createDirectories(temp.resolve("export/database_export"))
        writeSourceBook(export, segment)
        val metadata = temp.resolve("release_metadata.json")
        Files.writeString(metadata, releaseMetadata())
        val output = temp.resolve("output/bootstrap")
        val arguments = ManualLinksArguments(
            mode = ManualLinksMode.BOOTSTRAP,
            repository = repository,
            configPath = config,
            lineagePath = null,
            expectedOldConfigSha256 = null,
            expectedOldToolCommit = null,
            sefariaExport = temp.resolve("export"),
            releaseMetadataPath = metadata,
            releaseMetadataSha256 = ManualLinksJson.rawSha256(metadata),
            changelogDir = null,
            seforimToolCommit = "b".repeat(40),
            output = output,
        )
        val bootstrap = ManualLinksRefresh(arguments, logger).run()
        assertEquals("ok", bootstrap.status)
        val bootstrapped = output.resolve("links/מקור_links.json").readText()
        val lineage = repository.resolve("manual_links_lineage.json")
        Files.copy(output.resolve("manual_links_lineage.json"), lineage, StandardCopyOption.REPLACE_EXISTING)
        return Fixture(segment, linkFile, export, arguments, lineage, bootstrapped)
    }

    private fun linkFileText(starts: List<Int>): String = buildString {
        append("[\n")
        starts.forEachIndexed { index, start ->
            if (index > 0) append(",\n")
            append("  {\n")
            append("    \"line_index_1\": 2,\n")
            append("    \"ref_1\": \"Source 1\",\n")
            append("    \"heRef_2\": ${jsonText("מפרש $index")},\n")
            append("    \"path_2\": ${jsonText("מפרש.txt")},\n")
            append("    \"line_index_2\": ${index + 1},\n")
            append("    \"start\": $start,\n")
            append("    \"Conection Type\": \"commentary\"\n")
            append("  }")
        }
        append("\n]")
    }

    private fun writeSourceBook(export: Path, segment: String) {
        val heTitle = jsonText("מקור")
        Files.writeString(
            Files.createDirectories(export.resolve("schemas")).resolve("Source.json"),
            """
                {
                  "title": "Source",
                  "heTitle": $heTitle,
                  "schema": {
                    "title": "Source",
                    "heTitle": $heTitle,
                    "depth": 1,
                    "addressTypes": ["Integer"],
                    "sectionNames": ["Paragraph"],
                    "heSectionNames": ["פסקה"]
                  }
                }
            """.trimIndent(),
        )
        Files.writeString(
            Files.createDirectories(export.resolve("json/Source")).resolve("merged.json"),
            """{"title":"Source","heTitle":$heTitle,"text":[${jsonText(segment)},"תוכן שני"]}""",
        )
    }

    private fun releaseMetadata(): String = """
        {
          "schema_version": 1,
          "tag": "test-release",
          "run_id": 1,
          "run_attempt": 1,
          "source_commit": "${"a".repeat(40)}",
          "previous": null,
          "archive": {
            "sha256": "${"1".repeat(64)}",
            "size": 0,
            "parts": [{"name":"archive.part-00","size":0,"sha256":"${"2".repeat(64)}"}]
          },
          "manifest": {"name":"manifest.txt","size":0,"sha256":"${"3".repeat(64)}"},
          "titles": {"name":"titles.json","size":0,"sha256":"${"4".repeat(64)}"},
          "changelog": {
            "name":"changelog_diff.json",
            "size":0,
            "sha256":"${"5".repeat(64)}",
            "old_tag":"",
            "new_tag":"test-release"
          }
        }
    """.trimIndent()
}
