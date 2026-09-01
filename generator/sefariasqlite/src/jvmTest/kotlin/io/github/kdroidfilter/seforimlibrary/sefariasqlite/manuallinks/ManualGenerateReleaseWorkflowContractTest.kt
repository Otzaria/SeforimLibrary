package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ManualGenerateReleaseWorkflowContractTest {
    @Test
    fun resultAndRecoveryContractsRemainCompleteAndCanonical() {
        val workflow = repositoryRoot()
            .resolve(".github/workflows/manual-generate-release.yml")
            .readText()
        val requiredResultFields = listOf(
            "schema_version",
            "status",
            "correlation_id",
            "child_run_id",
            "child_run_attempt",
            "source_commit",
            "sefaria_tag",
            "sefaria_release_metadata_sha256",
            "sefaria_archive_sha256",
            "otzaria_tag",
            "otzaria_asset_sha256",
            "fordb_archive_sha256",
            "fordb_tag",
            "expected_links_commit",
            "otzaria_target_commit",
            "release_tag",
            "build_provenance_sha256",
            "lineage_sha256",
            "config_sha256",
            "source_links_tree_sha256",
            "packaged_links_tree_sha256",
            "assets",
        )
        requiredResultFields.forEach { field ->
            assertTrue(workflow.contains("\"$field\""), "pipeline result must contain $field")
        }
        assertTrue(workflow.contains("json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(\",\", \":\")) + \"\\n\""))
        assertTrue(workflow.contains("gh api --paginate \"repos/\$GITHUB_REPOSITORY/releases?per_page=100\""))
        assertTrue(workflow.contains("\"lineage_sha256\""))
        assertTrue(workflow.contains("build_provenance.json --argjson size"))
        assertTrue(workflow.contains("refresh-release-manifest:"))
        assertTrue(workflow.contains("SEFARIA_EXTRACT_ROOT=\"\$INPUTS/sefaria-extract\""))
        assertTrue(workflow.contains("test -d \"\$SEFARIA_EXTRACT_ROOT/json\""))
        assertTrue(workflow.contains("test -d \"\$SEFARIA_EXTRACT_ROOT/schemas\""))
        assertFalse(workflow.contains("SEFARIA_DB_ROOTS"))
        assertTrue(workflow.contains(".sefaria.archive == \$metadata[0].archive"))
        assertTrue(workflow.contains("repos/otzaria/otzaria-library/commits/\$OTZARIA_TAG"))
        assertTrue(
            workflow.contains(
                "GENERATED_AT=\$(jq -r '[.[].publishedAt | select(. != null)] | max // \"1970-01-01T00:00:00Z\"'",
            ),
            "manifest generation must converge from immutable release timestamps",
        )
        assertFalse(workflow.contains("GENERATED_AT=\$(date"))
        assertFalse(workflow.contains("gh release list --limit"), "release discovery must not truncate history")
        assertTrue(workflow.contains("select(.draft|not)"), "drafts must not advance db_version")
        assertFalse(workflow.contains("-name database_export -print -quit"))
        assertFalse(workflow.contains("gh api \"repos/\$GITHUB_REPOSITORY/releases?per_page=100\" >"))
        assertTrue(
            workflow.contains("path: .pipeline-control"),
            "build payload and orchestration scripts must be checked out separately",
        )
        assertTrue(
            workflow.contains("python3 .pipeline-control/.github/scripts/validate_build_provenance.py"),
            "the pinned payload checkout must not downgrade the active provenance validator",
        )
        assertTrue(
            workflow.contains("python3 .pipeline-control/.github/scripts/host_lease.py"),
            "host safety fixes must come from the immutable workflow revision",
        )
        assertTrue(workflow.contains("Bootstrap durable cross-repo host lease"))
        assertTrue(workflow.contains("bootstrap_host_lock.sh"))
        assertTrue(workflow.contains(".github/host/otzaria-pipeline.tmpfiles.conf"))
        assertTrue(workflow.contains("RAW_SNAPSHOT=\"\$RUNNER_TEMP/lines-snapshot-"))
        assertTrue(workflow.contains("-PlinesSnapshot=\"\$RAW_SNAPSHOT\""))
        assertFalse(workflow.contains("-PlinesSnapshot=\$PWD/build/lines_snapshot.db"))
    }

    @Test
    fun forDbRulesArchiveIsPinnedByDigestVerifiedAndRecorded() {
        val workflow = repositoryRoot()
            .resolve(".github/workflows/manual-generate-release.yml")
            .readText()
        // Declared as a required, sha256-shaped pinned input — same discipline as
        // the Sefaria/Otzaria pins, so the build is a pure function of its inputs.
        assertTrue(workflow.contains("fordb_archive_sha256:"), "ForDB archive digest must be a pinned input")
        assertTrue(
            workflow.contains("[[ \"\$FORDB_ARCHIVE_SHA\" =~ ^[0-9a-f]{64}\$ ]]"),
            "the ForDB digest input must be validated as a sha256",
        )
        // Fetched exactly once and verified against the pin (fail closed on drift).
        assertTrue(workflow.contains("fordb_tag:"), "ForDB immutable tag must be a required input")
        assertTrue(workflow.contains("gh release download \"\$FORDB_TAG\" -R otzaria/otzaria-library"))
        assertTrue(workflow.contains("[[ \"\${FORDB_TAG#fordb-sha256-}\" == \"\$FORDB_ARCHIVE_SHA\" ]]"))
        assertTrue(workflow.contains("echo \"\$FORDB_ARCHIVE_SHA  \$FORDB_ARCHIVE\" | sha256sum -c -"))
        // The one verified archive is handed to every ForDB post-process JVM. The
        // digest reaches the command line via env only (never a raw ${{ inputs }}
        // interpolation inside a run body — script-injection hardening).
        assertTrue(workflow.contains("-PforDbArchive=\"\$FORDB_ARCHIVE\""))
        assertTrue(workflow.contains("-PforDbSha256=\"\$FORDB_ARCHIVE_SHA\""))
        // Recorded in provenance and part of the reuse-match, so a ForDB change can
        // never be silently reused as an older build.
        assertTrue(workflow.contains("\"fordb_archive_sha256\": os.environ[\"FORDB_ARCHIVE_SHA\"]"))
        assertTrue(workflow.contains("\"fordb_tag\": os.environ[\"FORDB_TAG\"]"))
        assertTrue(workflow.contains(".fordb_archive_sha256==\$fd"))
    }

    @Test
    fun relinkPayloadIsPinnedVerifiedAgainstItsManifestAndRecorded() {
        val workflow = repositoryRoot()
            .resolve(".github/workflows/manual-generate-release.yml")
            .readText()
        // The relink is dispatched pinned to THIS build's Sefaria vintage + snapshot.
        // The pins reach the dispatch via env only — a raw ${{ inputs }} inside the
        // run body would be a script-injection vector (the job holds a PAT).
        assertTrue(workflow.contains("-f sefaria_tag=\"\$SEFARIA_TAG\""))
        assertFalse(workflow.contains("-f sefaria_tag=\"\${{"), "inputs must not be interpolated into run bodies")
        assertTrue(workflow.contains("-f snapshot_sha256=\"\$SNAPSHOT_ZST_SHA256\""))
        assertTrue(workflow.contains("SNAPSHOT_ZST_SHA256=\$(sha256sum build/lines_snapshot.db.zst"))
        // The pinned Sefaria metadata digest is forwarded so the relink can content-verify
        // its changelog (which rewrites target_ref) — not just match the tag name.
        assertTrue(workflow.contains("-f sefaria_release_metadata_sha256=\"\$SEFARIA_METADATA_SHA\""))
        // Phase-2 verifies the relink manifest fail-closed before injecting links: the
        // payload matches its own digest, the relink used this build's pinned Sefaria +
        // snapshot, and it is the run we dispatched.
        assertTrue(workflow.contains("relink_manifest.json"))
        assertTrue(
            workflow.contains("echo \"\$(jq -r .payload_sha256 \"\$M\")  build/linker/linker_links.zst\" | sha256sum -c -"),
        )
        // The manifest is strictly validated in Python (exact key set) and its self-reported
        // linker_commit / relink_run_attempt are cross-checked against the relink run's REAL
        // head_sha / attempt from the GitHub API — a forged manifest cannot pass.
        assertTrue(workflow.contains("--jq .head_sha"))
        assertTrue(workflow.contains("--jq .run_attempt"))
        assertTrue(
            workflow.contains(
                "if set(m) != {\"schema_version\", \"sefaria_tag\", \"snapshot_zst_sha256\", \"engine_fingerprint\",",
            ),
        )
        // Assert the stable comparison substrings (not the `need(...)` wrapper, which the
        // strictness guards prefix with s(...)/type(...) is int) so this stays robust.
        assertTrue(workflow.contains("m[\"sefaria_tag\"] == os.environ[\"EXP_SEFARIA_TAG\"]"))
        assertTrue(workflow.contains("m[\"snapshot_zst_sha256\"] == os.environ[\"EXP_SNAPSHOT_SHA\"]"))
        assertTrue(workflow.contains("m[\"relink_run_id\"] == int(os.environ[\"EXP_RUN_ID\"])"))
        assertTrue(workflow.contains("m[\"linker_commit\"] == os.environ[\"RUN_HEAD_SHA\"]"))
        assertTrue(workflow.contains("m[\"relink_run_attempt\"] == int(os.environ[\"RUN_ATTEMPT\"])"))
        // Manifest strictness: duplicate JSON keys rejected; fingerprint can't inject env lines.
        assertTrue(workflow.contains("duplicate key"))
        assertTrue(workflow.contains("non-empty single-line"))
        // The linker's identity is recorded in build provenance.
        assertTrue(workflow.contains("\"linker_payload_sha256\": os.environ[\"LINKER_PAYLOAD_SHA256\"]"))
        assertTrue(workflow.contains("\"linker_engine_fingerprint\": os.environ[\"LINKER_ENGINE_FINGERPRINT\"]"))
        assertTrue(workflow.contains("\"linker_relink_run_id\": int(os.environ[\"LINKER_RELINK_RUN_ID\"])"))
        assertTrue(workflow.contains("\"linker_commit\": os.environ[\"LINKER_COMMIT\"]"))
        assertTrue(workflow.contains("\"linker_relink_run_attempt\": int(os.environ[\"LINKER_RELINK_RUN_ATTEMPT\"])"))
    }

    @Test
    fun relinkIdentityIsPerAttemptExactMatchedAndReconcilable() {
        val workflow = repositoryRoot()
            .resolve(".github/workflows/manual-generate-release.yml")
            .readText()
        // The request id is a DETERMINISTIC function of this attempt + saga correlation +
        // pinned inputs + target — no timestamp, no randomness; a rerun gets a new id.
        assertTrue(workflow.contains("--arg schema relink-request-v1"))
        assertTrue(workflow.contains("--argjson parent_run_attempt \"\$GITHUB_RUN_ATTEMPT\""))
        assertTrue(workflow.contains("RELINK_REQUEST_ID=\$(printf '%s' \"\$REQUEST_JSON\" | sha256sum"))
        // The id + parent attempt are dispatched to BOTH LinkerToOtzaria workflows.
        assertTrue(workflow.contains("-f relink_request_id=\"\$RELINK_REQUEST_ID\""))
        assertTrue(workflow.contains("-f parent_run_attempt="))
        // Discovery: FULL-title equality with a uniqueness invariant — never contains,
        // never a positional pick — plus created-after-dispatch and expected-head pins,
        // over FULLY PAGINATED REST (never a "last N runs" window).
        assertTrue(workflow.contains("select(.display_title == env.RELINK_TITLE)"))
        assertTrue(workflow.contains("gh api --paginate -X GET \"repos/Otzaria/LinkerToOtzaria/actions/workflows/relink.yml/runs\""))
        assertTrue(workflow.contains("refusing to guess"))
        assertTrue(workflow.contains("before our dispatch"))
        assertTrue(workflow.contains("main moved mid-dispatch"))
        assertFalse(workflow.contains("contains(\"library_run_id="), "discovery must not use substring matching")
        assertFalse(workflow.contains("gh run list -R Otzaria/LinkerToOtzaria"), "discovery/cleanup must paginate, not window")
        // The manifest (schema 2) must echo OUR id and OUR exact parent coordinates.
        assertTrue(workflow.contains("m[\"schema_version\"] == 2"))
        assertTrue(workflow.contains("m[\"relink_request_id\"] == os.environ[\"EXP_REQUEST_ID\"]"))
        assertTrue(workflow.contains("m[\"parent_run_id\"] == int(os.environ[\"PARENT_RUN_ID\"])"))
        assertTrue(workflow.contains("m[\"parent_run_attempt\"] == int(os.environ[\"PARENT_RUN_ATTEMPT\"])"))
        // The identity is recorded in build provenance.
        assertTrue(workflow.contains("\"linker_relink_request_id\": os.environ[\"RELINK_REQUEST_ID\"]"))
        // Cleanup: exact-title matching, dispatcher first with a bounded terminal wait
        // (no fixed settle sleeps), a structured orphan marker for the reconciler, and
        // query-failure tracking so a broken scan can never report "cleanup done".
        assertTrue(workflow.contains("select(.display_title == env.TITLE)"))
        assertTrue(workflow.contains("wait_terminal"))
        assertTrue(workflow.contains("ORPHAN_INTENT"))
        assertTrue(workflow.contains("query_ok=0"))
        assertFalse(workflow.contains("sleep 20"), "cleanup must wait on state, not on a fixed settle")
        // Cross-repo host lease: an identity-bound holder owns the real flock.
        // Server relink is serialized by releasing and reacquiring; metadata is
        // never treated as delegated lock ownership.
        assertTrue(workflow.contains("LEASE=/run/lock/otzaria/host-heavy.lock"))
        assertTrue(workflow.contains("host_lease.py start"))
        assertTrue(workflow.contains("host_lease.py release"))
        assertFalse(workflow.contains("running under its lease"))
        assertTrue(
            workflow.contains("timeout-minutes: 1440"),
            "self-hosted parent must outlast DB generation and the complete split relink chain",
        )
        assertTrue(workflow.contains("--ttl 90000"), "cross-step lease must cover the 24h job")
    }

    @Test
    fun bothReleaseManifestWritersConvergeOnImmutableState() {
        val root = repositoryRoot()
        val workflows = listOf(
            root.resolve(".github/workflows/manual-generate-release.yml").readText(),
            root.resolve(".github/workflows/update-release-manifest.yml").readText(),
        )
        workflows.forEach { workflow ->
            assertTrue(workflow.contains("del(.assets[].downloadCount)"))
            assertTrue(
                workflow.contains(
                    "GENERATED_AT=\$(jq -r '[.[].publishedAt | select(. != null)] | max // \"1970-01-01T00:00:00Z\"'",
                ),
            )
            assertFalse(workflow.contains("GENERATED_AT=\$(date"))
            assertTrue(workflow.contains("gh api --paginate \"repos/\$GITHUB_REPOSITORY/releases?per_page=100\""))
            assertFalse(workflow.contains("--limit 1000"))
        }
    }

    @Test
    fun pinnedCorpusWorkflowRemainsImmutableAndComplete() {
        val root = repositoryRoot()
        val workflow = root.resolve(".github/workflows/manual-links-corpus-qa.yml").readText()
        val validator = root.resolve(".github/scripts/validate-sefaria-release-metadata.py").readText()

        listOf(
            "mode:",
            "otzaria_commit:",
            "sefaria_tag:",
            "sefaria_release_metadata_sha256:",
            "sefaria_archive_sha256:",
            "seforim_tool_commit:",
            "expected_old_config_sha256:",
            "expected_old_tool_commit:",
        ).forEach { input ->
            assertTrue(workflow.contains(input), "pinned corpus workflow must expose $input")
        }
        assertTrue(workflow.contains("ref: \${{ inputs.seforim_tool_commit }}"))
        assertTrue(workflow.contains("ref: \${{ inputs.otzaria_commit }}"))
        assertTrue(workflow.contains("validate-sefaria-release-metadata.py"))
        assertTrue(workflow.contains(":sefariasqlite:manualLinksCorpusTest"))
        assertTrue(workflow.contains("EXPECTED_TARGET_RECORDS: '81496'"))
        assertTrue(workflow.contains("EXPECTED_SOURCE_RECORDS: '17980'"))
        assertFalse(
            workflow.contains(Regex("excluded", RegexOption.IGNORE_CASE)),
            "the excluded_files mechanism is gone, in any casing",
        )
        assertTrue(workflow.contains("EXPECTED_ANCHORS: '17980'"))
        assertTrue(workflow.contains("options: [refresh, bootstrap, migrate]"), "migrate must be dispatchable")
        assertTrue(workflow.contains("\"\$MODE\" == refresh || \"\$MODE\" == migrate"), "migrate consumes the pinned lineage")
        assertTrue(workflow.contains("-PexpectedOldConfigSha256=\"\$EXPECTED_OLD_CONFIG_SHA\""))
        assertTrue(workflow.contains("-PexpectedOldToolCommit=\"\$EXPECTED_OLD_TOOL_COMMIT\""))
        assertTrue(workflow.contains("test \"\${#EXPORT_ROOTS[@]}\" -eq 1"))
        assertTrue(workflow.contains(".refs.missing == 0"))
        assertTrue(workflow.contains(".refs.duplicate == 0"))
        assertTrue(workflow.contains(".anchors.drifted == 0"))
        assertTrue(workflow.contains(".packaging_collisions == 0"))
        assertTrue(workflow.contains("actions/upload-artifact@v4"))
        assertTrue(workflow.contains("include-hidden-files: true"), "completion marker is a dotfile")
        assertTrue(workflow.contains("DISK_AVAILABLE_KIB < 12582912"), "corpus gate requires 12 GiB free disk")
        assertTrue(workflow.contains("MEM_AVAILABLE_KIB < 6291456"), "corpus gate requires 6 GiB available memory")
        assertTrue(workflow.contains("--max-workers=2"), "full-corpus parallelism must be bounded")
        assertTrue(
            workflow.indexOf("Preflight full-corpus capacity") <
                workflow.indexOf("Download and verify the exact Sefaria export"),
            "capacity preflight must fail before the large export is downloaded or extracted",
        )
        assertFalse(workflow.contains("releases/latest"), "immutable corpus QA must not discover a moving release")

        assertTrue(validator.contains("archive.parts must be non-empty"))
        assertTrue(validator.contains("archive parts must have unique UTF-8-sorted names"))
        assertTrue(validator.contains("archive part sizes do not sum to archive.size"))
    }

    private fun repositoryRoot(): Path = generateSequence(Path.of("").toAbsolutePath()) { it.parent }
        .firstOrNull { Files.isRegularFile(it.resolve(".github/workflows/manual-generate-release.yml")) }
        ?: error("Could not locate repository root")
}
