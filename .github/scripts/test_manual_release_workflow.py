import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[1] / "workflows" / "manual-generate-release.yml"
MANIFEST_WORKFLOW = Path(__file__).parents[1] / "workflows" / "update-release-manifest.yml"
HANDOFF_PUBLISHER = Path(__file__).parent / "publish_release_handoff.sh"
FAILURE_RECONCILER = (
    Path(__file__).parents[1] / "workflows" / "reconcile-linker-after-failure.yml"
)


class ManualReleaseWorkflowContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def step(self, name):
        marker = f"      - name: {name}\n"
        self.assertEqual(self.workflow.count(marker), 1, f"step {name!r} must exist exactly once")
        return self.workflow.split(marker, 1)[1].split("\n      - ", 1)[0]

    def test_linker_reconciliation_is_failure_event_driven(self):
        workflow = FAILURE_RECONCILER.read_text(encoding="utf-8")
        header = workflow.split("jobs:\n", 1)[0]

        self.assertIn("workflow_run:", header)
        self.assertIn(
            "Weekly 5–6/6 · Build, link, validate and publish Seforim DB",
            header,
        )
        self.assertNotIn("schedule:", header)
        self.assertNotIn("cron:", header)
        self.assertIn(
            "if: ${{ github.event.workflow_run.conclusion != 'success' }}",
            workflow,
        )
        self.assertIn(
            "gh workflow run reconcile-pipeline.yml -R Otzaria/LinkerToOtzaria",
            workflow,
        )
        self.assertIn("GH_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", workflow)

    def test_local_host_is_the_default_db_and_linker_target(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        runner_input = self.workflow.split("      runner_selection:\n", 1)[1].split(
            "      prerelease:\n", 1
        )[0]

        self.assertIn("default: 'local'", runner_input)
        self.assertIn("- 'local'", runner_input)
        self.assertIn("otzaria-db", self.workflow)
        self.assertIn("vars.SERIAL_LINKER_TARGET || 'local'", relink)
        self.assertIn('local|kaggle|server)', relink)
        self.assertIn('-f library_run_id="$GITHUB_RUN_ID" -f target=local', relink)

    def test_durable_host_skips_reinstalling_existing_dependencies(self):
        root = Path(__file__).parents[2]
        installer = (root / ".github/scripts/install-db-workflow-deps.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("for tool in gh sqlite3 zstd unzstd jq curl unzip", installer)
        self.assertIn("skipping package-manager work", installer)

    def test_disk_probe_ignores_absent_optional_wsl_drives(self):
        disk = self.step("Disk before build")

        self.assertIn("df -h /", disk)
        self.assertIn("if [ -d /mnt/c ]; then", disk)
        self.assertIn("df -h /mnt/c", disk)
        self.assertNotIn("run: df -h\n", disk)

    def test_durable_host_uses_preinstalled_java_25_without_network_setup(self):
        java = self.step("Verify durable Java 25 toolchain")

        self.assertNotIn("actions/setup-java", self.workflow)
        self.assertNotIn("gradle/actions/setup-gradle", self.workflow)
        self.assertIn("command -v java", java)
        self.assertIn("command -v javac", java)
        self.assertIn('[[ "$java_version" == 25 || "$java_version" == 25.* ]]', java)
        self.assertIn('echo "JAVA_HOME=$java_home" >> "$GITHUB_ENV"', java)

    def test_durable_host_uses_preinstalled_gradle_without_wrapper_download(self):
        gradle = self.step("Verify durable Gradle 9.1.0 toolchain")

        self.assertIn("command -v gradle", gradle)
        self.assertIn('[ "$gradle_version" = 9.1.0 ]', gradle)
        self.assertNotIn("./gradlew", self.workflow)
        self.assertNotIn("gradle/actions/setup-gradle", self.workflow)

    def test_phase2_has_dedicated_heap_and_matching_host_headroom(self):
        mount = self.step("Mount RAM-backed build dir (tmpfs)")
        heaps = self.step("Bridle daemon heaps + generator forks for the 16 GB runner")

        self.assertIn('$((24*1024*1024))', mount)
        self.assertIn('(<24 GiB)', mount)
        self.assertIn('generatorHeap=8g', heaps)
        self.assertIn('linkerHeap=12g', heaps)

    def test_recovery_overlays_exact_phase2_sources_from_workflow_revision(self):
        checkout = self.step("Checkout immutable pipeline control scripts")
        overlay = self.step("Overlay pinned recovery Phase-2 implementation")
        apply_links = self.step("Apply LINKER links (Phase-2)")

        disk_allocator = (
            "generator/common/src/jvmMain/kotlin/io/github/kdroidfilter/"
            "seforimlibrary/common/ids/DiskBackedLinkIdAllocator.kt"
        )
        importer = (
            "generator/sefariasqlite/src/jvmMain/kotlin/io/github/kdroidfilter/"
            "seforimlibrary/sefariasqlite/GenerateLinkerLinks.kt"
        )
        self.assertIn(disk_allocator, checkout)
        self.assertIn(importer, checkout)
        self.assertIn("if: inputs.relink_recovery_run_id != ''", overlay)
        self.assertIn('install -m 0644 ".pipeline-control/$file" "$file"', overlay)
        self.assertIn('old = \'jvmArgs = listOf("-Xmx6g")\'', overlay)
        self.assertIn('new = \'jvmArgs = listOf("-Xmx12g", "-XX:+UseG1GC")\'', overlay)
        self.assertIn("text.count(old) != 1", overlay)
        self.assertIn("DiskBackedLinkIdAllocator", overlay)
        self.assertIn("InMemoryIdAllocator", overlay)
        self.assertIn("grep -Fq", overlay)
        self.assertNotIn("rg -q", overlay)
        self.assertIn("rm -rf generator/common/build generator/sefariasqlite/build", overlay)
        self.assertLess(
            self.workflow.index("      - name: Overlay pinned recovery Phase-2 implementation\n"),
            self.workflow.index("      - name: Apply LINKER links (Phase-2)\n"),
        )
        self.assertIn("gradle :sefariasqlite:generateLinkerLinks", apply_links)

    def test_phase2_implementation_commit_is_part_of_release_identity(self):
        lookup = self.step("Find and verify exact provenance")
        stage = self.step("Stage release assets")

        expression = "${{ inputs.relink_recovery_run_id != '' && github.sha || inputs.source_commit }}"
        self.assertGreaterEqual(self.workflow.count(f"PHASE2_IMPLEMENTATION_COMMIT: {expression}"), 3)
        self.assertIn('--arg phase2 "$PHASE2_IMPLEMENTATION_COMMIT"', lookup)
        self.assertIn(".phase2_implementation_commit==$phase2", lookup)
        self.assertIn('"schema_version": 3', stage)
        self.assertIn(
            '"phase2_implementation_commit": os.environ["PHASE2_IMPLEMENTATION_COMMIT"]',
            stage,
        )

    def test_pinned_sefaria_archive_uses_its_explicit_root_contract(self):
        extract = self.step("Verify pinned lineage and extract exact inputs")

        self.assertIn('SEFARIA_EXTRACT_ROOT="$INPUTS/sefaria-extract"', extract)
        self.assertIn('test -d "$SEFARIA_EXTRACT_ROOT/json"', extract)
        self.assertIn('test -d "$SEFARIA_EXTRACT_ROOT/schemas"', extract)
        self.assertIn("root json/ contains no JSON files", extract)
        self.assertIn("root schemas/ contains no JSON files", extract)
        self.assertNotIn("SEFARIA_DB_ROOTS", extract)

    def test_self_hosted_release_digests_are_read_through_rest(self):
        extract = self.step("Verify pinned lineage and extract exact inputs")
        apply_links = self.step("Apply LINKER links (Phase-2)")
        publish = self.step("Create draft, verify every uploaded asset, then publish")

        self.assertIn(
            'repos/otzaria/otzaria-library/releases/tags/$OTZARIA_TAG', extract
        )
        self.assertIn(
            'repos/Otzaria/LinkerToOtzaria/releases/tags/$LINKER_RELEASE_TAG',
            apply_links,
        )
        self.assertIn(
            'repos/$GITHUB_REPOSITORY/releases/tags/$RELEASE_TAG', publish
        )
        self.assertIn("release_assets_json", publish)
        for step in (extract, apply_links, publish):
            self.assertNotIn("gh release view", step)

    def test_release_write_is_probed_before_the_expensive_build(self):
        probe = self.step("Preflight release write credentials")
        self.assertLess(
            self.workflow.index("      - name: Preflight release write credentials\n"),
            self.workflow.index("      - name: Mount RAM-backed build dir (tmpfs)\n"),
        )
        self.assertIn("[ \"$code\" = 200 ]", probe)
        self.assertIn(".permissions.push == true", probe)
        self.assertNotIn("--request POST", probe)
        self.assertIn("RELEASE_AUTOMATIC_WRITABLE", probe)
        self.assertIn("RELEASE_CROSS_REPO_WRITABLE", probe)
        self.assertIn("RELEASE_TOKEN_KIND=automatic", probe)
        self.assertIn("RELEASE_TOKEN_KIND=cross-repo", probe)
        self.assertLess(
            probe.index('if [ "$cross_repo_writable" = true ]'),
            probe.index('elif [ "$automatic_writable" = true ]'),
        )

    def test_publisher_reconciles_and_falls_back_only_to_preflighted_credentials(self):
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        self.assertIn("AUTOMATIC_TOKEN: ${{ secrets.GITHUB_TOKEN }}", publish)
        self.assertIn("CROSS_REPO_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", publish)
        self.assertIn('use_token "$RELEASE_TOKEN_KIND"', publish)
        self.assertIn('switch_token()', publish)
        self.assertIn('export GH_TOKEN="$AUTOMATIC_TOKEN"', publish)
        self.assertIn('export GH_TOKEN="$CROSS_REPO_TOKEN"', publish)
        self.assertIn('exact-empty-draft', publish)
        self.assertIn('for asset_path in release-staging/*', publish)
        self.assertNotIn('gh release upload "$RELEASE_TAG" "$asset_path" --clobber', publish)

    def test_recovery_sets_both_cleanup_titles_and_cleanup_defaults_them(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        cleanup = self.step("Cancel any in-flight relink for this build (no orphaned linker run)")
        self.assertIn('echo "KAGGLE_TITLE=kaggle-relink request=$RELINK_REQUEST_ID', relink)
        self.assertIn('echo "RELINK_DISPATCH_STARTED=1" >> "$GITHUB_ENV"', relink)
        self.assertIn('[ "${RELINK_DISPATCH_STARTED:-}" = 1 ]', cleanup)
        self.assertIn(
            'actions/runs/$EXPECTED_PARENT_RUN_ID/attempts/$EXPECTED_PARENT_RUN_ATTEMPT',
            relink,
        )
        self.assertIn(': "${RELINK_TITLE:=}"', cleanup)
        self.assertIn(': "${KAGGLE_TITLE:=}"', cleanup)

    def test_large_snapshot_uses_content_addressed_release_not_actions_artifact(self):
        publish = self.step("Publish immutable snapshot release for the relink run")
        self.assertIn('tag="lines-snapshot-sha256-$SNAPSHOT_ZST_SHA256"', publish)
        self.assertIn('gh release create "$tag"', publish)
        self.assertIn('gh release upload "$tag" "$snapshot"', publish)
        self.assertIn('digest=="sha256:"+sys.argv[3]', publish)
        self.assertNotIn("actions/upload-artifact", publish)
        self.assertNotIn("Upload snapshot artifact for the relink run", self.workflow)
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        self.assertIn(
            'SNAPSHOT_RELEASE_TAG="lines-snapshot-sha256-$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256"',
            relink,
        )
        self.assertIn("recovery parent snapshot release is missing or not byte-exact", relink)
        self.assertNotIn(
            "recovery parent must retain exactly one live source snapshot artifact",
            relink,
        )

    def test_recovery_verifies_semantic_snapshot_before_phase2(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        apply_links = self.step("Apply LINKER links (Phase-2)")

        self.assertIn("EXPECTED_LINKER_SNAPSHOT_ZST_SHA256", relink)
        self.assertIn("relink-recovery-manifest.json", relink)
        self.assertIn(
            'EXP_SNAPSHOT_SHA="$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256"',
            apply_links,
        )
        self.assertIn("verify_relink_recovery_snapshot.py", apply_links)
        self.assertIn("--original \"$ORIGINAL_SNAPSHOT_DB\"", apply_links)
        self.assertIn("--rebuilt \"$REBUILT_SNAPSHOT_DB\"", apply_links)
        self.assertLess(
            apply_links.index("verify_relink_recovery_snapshot.py"),
            apply_links.index("gradle :sefariasqlite:generateLinkerLinks"),
        )

    def test_weekly_workflow_has_no_actions_artifact_handoffs(self):
        self.assertNotIn("actions/upload-artifact", self.workflow)
        self.assertNotIn("actions/download-artifact", self.workflow)
        self.assertNotIn("gh run download", self.workflow)
        self.assertIn("pipeline-result-run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}", self.workflow)
        self.assertIn("linker-output-${EXPECTED_RELINK_REQUEST_ID}-${RUN_ATTEMPT}", self.workflow)

    def test_handoff_prereleases_do_not_pollute_database_manifest(self):
        refresh = self.workflow.split("  refresh-release-manifest:\n", 1)[1]
        self.assertIn("(.prerelease|not)", refresh)
        standalone = MANIFEST_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("(.prerelease|not)", standalone)
        self.assertIn("github.event.release.prerelease == false", standalone)

    def test_split_kaggle_child_releases_and_reacquires_host_lease(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        release = (
            'python3 .pipeline-control/.github/scripts/host_lease.py release '
            '--state "$HOST_LEASE_STATE"'
        )
        reacquire = (
            "python3 .pipeline-control/.github/scripts/host_lease.py start \\\n"
            "            --lock /run/lock/otzaria/host-heavy.lock"
        )
        dispatch_case = 'case "$SERIAL_LINKER_TARGET" in\n            local)'
        terminal = "completed:success) break"

        self.assertEqual(relink.count(release), 1)
        self.assertEqual(relink.count(reacquire), 1)
        self.assertLess(relink.index(release), relink.index(dispatch_case))
        self.assertLess(relink.index(terminal), relink.index(reacquire))
        self.assertNotIn(
            'if [ "$SERIAL_LINKER_TARGET" = server ]; then',
            relink,
            "the split Kaggle child also needs the Oracle host lease",
        )

    def test_parent_timeout_covers_db_build_and_complete_split_child(self):
        self.assertIn(
            "    timeout-minutes: 1440\n",
            self.workflow,
            "the self-hosted parent must outlive DB generation plus the legal split child chain",
        )
        self.assertIn("90m GPU NER + 480m CPU resolution", self.workflow)
        self.assertEqual(
            self.workflow.count('--ttl 90000'),
            2,
            "both lease lives must exceed the 24-hour parent ceiling",
        )

    def test_weekly_database_releases_default_to_final(self):
        prerelease_input = self.workflow.split("      prerelease:\n", 1)[1].split(
            "      source_commit:\n", 1
        )[0]
        self.assertIn("default: false", prerelease_input)
        self.assertIn("Weekly database builds are final releases", prerelease_input)

    def test_reuse_skips_invalid_legacy_provenance_but_not_the_requested_source(self):
        lookup = self.step("Find and verify exact provenance")
        validation = 'if ! python3 .github/scripts/validate_build_provenance.py "$file"; then'
        requested_source_guard = 'if [ "$target" = "$SOURCE_COMMIT" ]; then'
        legacy_skip = '::warning::Skipping legacy release $tag with invalid build provenance'

        self.assertIn(validation, lookup)
        self.assertIn(requested_source_guard, lookup)
        self.assertIn(legacy_skip, lookup)
        self.assertLess(lookup.index(validation), lookup.index(requested_source_guard))
        self.assertLess(lookup.index(requested_source_guard), lookup.index(legacy_skip))

    def test_release_publisher_rejects_asset_names_github_would_normalize(self):
        helper = HANDOFF_PUBLISHER.read_text(encoding="utf-8")
        self.assertIn("release asset basename is unsafe or would be normalized by GitHub", helper)
        self.assertIn("^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$", helper)
        self.assertIn('repos/$GITHUB_REPOSITORY/releases/tags/$tag', helper)
        self.assertIn("targetCommitish:.target_commitish", helper)
        self.assertNotIn('gh release view "$tag" --json', helper)


if __name__ == "__main__":
    unittest.main()
