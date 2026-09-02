package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ManualLinksRefreshIntegrationTest {
    private val logger = Logger.withTag("ManualLinksRefreshIntegrationTest")

    @Test
    fun bootstrapEnrichesCopyAndLeavesInputCheckoutUntouched() {
        val fixture = fixture("יעד א,")
        val original = fixture.linkFile.readText()

        val result = ManualLinksRefresh(fixture.arguments, Logger.withTag("ManualLinksRefreshIntegrationTest")).run()

        assertEquals("ok", result.status)
        assertEquals(original, fixture.linkFile.readText())
        val outputFile = fixture.output.resolve("MoreBooks/links/מקומי_links.json")
        val output = ManualLinksDocument.read(outputFile).record(0)
        assertEquals("Target 1", output.get("ref_2").textValue())
        assertEquals(2, output.get("line_index_2").intValue())
        assertEquals("preserved", output.get("unknown").textValue())
        assertTrue(result.markerPath.exists())
        assertTrue(fixture.output.resolve("manual_links_lineage.json").exists())

        Files.copy(outputFile, fixture.linkFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        val lineage = fixture.arguments.repository.resolve("manual_links_lineage.json")
        Files.copy(fixture.output.resolve("manual_links_lineage.json"), lineage)
        val lineageWithPriorAudit = ManualLinksLineage.read(lineage).copy(
            appliedChangelogChain = listOf(
                AppliedChangelog(
                    tag = "historic-release",
                    metadataSha256 = "6".repeat(64),
                    previous = PreviousRelease("baseline-release", "7".repeat(64)),
                    changelogName = "changelog_diff.json",
                    changelogSha256 = "8".repeat(64),
                ),
            ),
        )
        Files.write(lineage, lineageWithPriorAudit.canonicalBytes())
        val secondOutput = fixture.output.parent.resolve("second-output")
        val second = ManualLinksRefresh(
            fixture.arguments.copy(
                mode = ManualLinksMode.REFRESH,
                lineagePath = lineage,
                changelogDir = Files.createDirectories(fixture.output.parent.resolve("empty-chain")),
                output = secondOutput,
            ),
            Logger.withTag("ManualLinksRefreshIntegrationTest"),
        ).run()
        assertEquals("no_op", second.status)
        assertEquals(outputFile.readText(), secondOutput.resolve("MoreBooks/links/מקומי_links.json").readText())
        assertEquals(lineage.readText(), secondOutput.resolve("manual_links_lineage.json").readText())
        val thirdOutput = fixture.output.parent.resolve("third-output")
        val third = ManualLinksRefresh(
            fixture.arguments.copy(
                mode = ManualLinksMode.REFRESH,
                lineagePath = lineage,
                changelogDir = fixture.output.parent.resolve("empty-chain"),
                output = thirdOutput,
            ),
            Logger.withTag("ManualLinksRefreshIntegrationTest"),
        ).run()
        assertEquals("no_op", third.status)
        assertEquals(second.reportPath.readText(), third.reportPath.readText())
        assertEquals(second.markerPath.readText(), third.markerPath.readText())
    }

    @Test
    fun unresolvedRefProducesNoCompletionMarkerAndDoesNotTouchInput() {
        val fixture = fixture("יעד ג,")
        val original = fixture.linkFile.readText()

        assertFailsWith<Exception> {
            ManualLinksRefresh(fixture.arguments, Logger.withTag("ManualLinksRefreshIntegrationTest")).run()
        }

        assertEquals(original, fixture.linkFile.readText())
        assertFalse(fixture.output.resolve(".manual-links-refresh-complete").exists())
        val failure = ManualLinksJson.readStrict(fixture.output.resolve("manual_links_refresh_report.json"))
        assertEquals("failed", failure.get("status").textValue())
        val item = failure.get("failures").single()
        assertEquals("MoreBooks/links/מקומי_links.json", item.get("file").textValue())
        assertEquals(0, item.get("record_index").intValue())
        assertTrue(item.get("record_sha256").textValue().matches(Regex("[0-9a-f]{64}")))
    }

    @Test
    fun alreadyEnrichedBootstrapRefMustMatchTheDeterministicHeRefAdapter() {
        val fixture = fixture("יעד א,")
        ManualLinksRefresh(fixture.arguments, Logger.withTag("ManualLinksRefreshIntegrationTest")).run()
        val outputFile = fixture.output.resolve("MoreBooks/links/מקומי_links.json")
        val tampered = outputFile.readText().replace("Target 1", "Target 2")
        assertTrue(tampered != outputFile.readText())
        Files.writeString(fixture.linkFile, tampered)

        assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh(
                fixture.arguments.copy(
                    output = fixture.output.parent.resolve("adapter-output"),
                ),
                Logger.withTag("ManualLinksRefreshIntegrationTest"),
            ).run()
        }
    }

    @Test
    fun bootstrapWithExistingLineageIsGatedAndCanOnlyBeANoOp() {
        val fixture = fixture("יעד א,")
        ManualLinksRefresh(fixture.arguments, Logger.withTag("ManualLinksRefreshIntegrationTest")).run()
        Files.copy(
            fixture.output.resolve("MoreBooks/links/מקומי_links.json"),
            fixture.linkFile,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
        Files.copy(
            fixture.output.resolve("manual_links_lineage.json"),
            fixture.arguments.repository.resolve("manual_links_lineage.json"),
        )

        assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh(
                fixture.arguments.copy(
                    seforimToolCommit = "c".repeat(40),
                    output = fixture.output.parent.resolve("bootstrap-wrong-tool"),
                ),
                Logger.withTag("ManualLinksRefreshIntegrationTest"),
            ).run()
        }
        val noOp = ManualLinksRefresh(
            fixture.arguments.copy(output = fixture.output.parent.resolve("bootstrap-no-op")),
            Logger.withTag("ManualLinksRefreshIntegrationTest"),
        ).run()
        assertEquals("no_op", noOp.status)
    }

    @Test
    fun aQuotedSefariaTargetIsMatchedByItsHeTitleWithNoTitleMapping() {
        val fixture = quotedTargetFixture()

        val enriched = ManualLinksRefresh(fixture.arguments, logger).run()

        assertEquals("ok", enriched.status)
        val records = ManualLinksJson.readStrict(enriched.reportPath).get("records")
        assertEquals(1, records.get("relevant").intValue())
        assertEquals(0, records.get("irrelevant").intValue())
        val record = ManualLinksDocument.read(fixture.output.resolve("MoreBooks/links/מקומי_links.json")).record(0)
        assertEquals("Target 2", record.get("ref_2").textValue())
        assertEquals(3, record.get("line_index_2").intValue())
    }

    @Test
    fun aTargetTitleCarriedByTwoSefariaBooksIsFatal() {
        val fixture = quotedTargetFixture(duplicateTargetBook = true)

        val error = assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh(fixture.arguments, logger).run()
        }
        assertTrue(error.message!!.contains("target book is ambiguous"))
    }

    @Test
    fun existingDictaTargetsAreReprovedThroughTheAdapterOnMigrate() {
        val fixture = quotedTargetFixture()
        ManualLinksRefresh(fixture.arguments, logger).run()
        val lineage = feedOutputBackIntoTheCheckout(fixture)

        val migrated = ManualLinksRefresh(rerun(fixture, lineage, ManualLinksMode.MIGRATE, "migrate"), logger).run()

        assertEquals("no_op", migrated.status)
        val record = ManualLinksDocument.read(
            fixture.output.parent.resolve("migrate/MoreBooks/links/מקומי_links.json"),
        ).record(0)
        assertEquals("Target 2", record.get("ref_2").textValue())
        assertEquals(3, record.get("line_index_2").intValue())

        // A ref that still resolves but no longer matches the deterministic Dicta heRef_2.
        Files.writeString(fixture.linkFile, fixture.linkFile.readText().replace("Target 2", "Target 1"))
        val error = assertFailsWith<IllegalArgumentException> {
            ManualLinksRefresh(rerun(fixture, lineage, ManualLinksMode.MIGRATE, "migrate-tampered"), logger).run()
        }
        assertTrue(error.message!!.contains("does not match the deterministic heRef_2 adapter"))
    }

    @Test
    fun aRecordWithSefariaOnBothSidesIsFatal() {
        val fixture = fixture("יעד א,", sourceIsSefaria = true)

        val error = assertFailsWith<IllegalStateException> {
            ManualLinksRefresh(fixture.arguments, logger).run()
        }
        assertTrue(error.message!!.contains("is Sefaria↔Sefaria"))
    }

    /** Turns a completed bootstrap into the next run's input checkout and returns its lineage. */
    private fun feedOutputBackIntoTheCheckout(fixture: Fixture): Path {
        Files.copy(
            fixture.output.resolve("MoreBooks/links/מקומי_links.json"),
            fixture.linkFile,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
        val lineage = fixture.arguments.repository.resolve("manual_links_lineage.json")
        Files.copy(fixture.output.resolve("manual_links_lineage.json"), lineage)
        return lineage
    }

    private fun rerun(
        fixture: Fixture,
        lineage: Path,
        mode: ManualLinksMode,
        outputName: String,
    ): ManualLinksArguments = fixture.arguments.copy(
        mode = mode,
        lineagePath = lineage,
        expectedOldConfigSha256 = ManualLinksJson.rawSha256(fixture.arguments.configPath)
            .takeIf { mode == ManualLinksMode.MIGRATE },
        expectedOldToolCommit = fixture.arguments.seforimToolCommit.takeIf { mode == ManualLinksMode.MIGRATE },
        changelogDir = Files.createDirectories(fixture.output.parent.resolve("$outputName-chain")),
        output = fixture.output.parent.resolve(outputName),
    )

    /** Mirrors the Dicta records whose path_2 carries Sefaria's own gershayim spelling. */
    private fun quotedTargetFixture(duplicateTargetBook: Boolean = false): Fixture = fixture(
        heRef = "יעד\"ן ב",
        targetHeTitle = "יעד\"ן",
        adapter = "dicta_heref_v1",
        duplicateTargetBook = duplicateTargetBook,
    )

    private fun fixture(
        heRef: String,
        targetHeTitle: String = "יעד",
        adapter: String = "morebooks_heref_v1",
        duplicateTargetBook: Boolean = false,
        sourceIsSefaria: Boolean = false,
    ): Fixture {
        val temp = Files.createTempDirectory("manual-links-integration")
        val repository = Files.createDirectories(temp.resolve("repo"))
        val links = Files.createDirectories(repository.resolve("MoreBooks/links"))
        val linkFile = links.resolve("מקומי_links.json")
        Files.writeString(
            linkFile,
            """
                [
                  {
                    "line_index_1": 1,
                    "heRef_2": ${jsonText(heRef)},
                    "path_2": ${jsonText("folder\\" + targetHeTitle + ".txt")},
                    "line_index_2": 99,
                    "unknown": "preserved"
                  }
                ]
            """.trimIndent(),
        )
        val config = repository.resolve("manual_links_sync.json")
        Files.writeString(
            config,
            """
                {
                  "schema_version": 1,
                  "seforim_tool_ref": "refs/heads/test",
                  "links_roots": [
                    {"path": "MoreBooks/links", "expected_state": "present"}
                  ],
                  "bootstrap_adapters": {"MoreBooks/links": "$adapter"},
                  "bootstrap_file_renames": [],
                  "bootstrap_record_overrides": []
                }
            """.trimIndent(),
        )
        val export = Files.createDirectories(temp.resolve("export/database_export"))
        writeSefariaBook(export, "Target", targetHeTitle)
        if (duplicateTargetBook) writeSefariaBook(export, "Target Two", targetHeTitle)
        if (sourceIsSefaria) writeSefariaBook(export, "Local Source", "מקומי")
        val metadata = temp.resolve("release_metadata.json")
        Files.writeString(
            metadata,
            """
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
            """.trimIndent(),
        )
        val output = temp.resolve("output")
        return Fixture(
            linkFile = linkFile,
            output = output,
            export = export,
            arguments = ManualLinksArguments(
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
            ),
        )
    }

    private fun writeSefariaBook(export: Path, enTitle: String, bookHeTitle: String) {
        val heTitle = jsonText(bookHeTitle)
        Files.writeString(
            Files.createDirectories(export.resolve("schemas")).resolve("$enTitle.json"),
            """
                {
                  "title": "$enTitle",
                  "heTitle": $heTitle,
                  "schema": {
                    "title": "$enTitle",
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
            Files.createDirectories(export.resolve("json/$enTitle")).resolve("merged.json"),
            """{"title":"$enTitle","heTitle":$heTitle,"text":["תוכן ראשון","תוכן שני"]}""",
        )
    }

    private fun jsonText(value: Any): String = ManualLinksJson.mapper.writeValueAsString(value)

    private data class Fixture(
        val linkFile: Path,
        val output: Path,
        val export: Path,
        val arguments: ManualLinksArguments,
    )
}
