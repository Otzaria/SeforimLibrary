import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[1] / "workflows" / "manual-generate-release.yml"
MANIFEST_WORKFLOW = Path(__file__).parents[1] / "workflows" / "update-release-manifest.yml"
HANDOFF_PUBLISHER = Path(__file__).parent / "publish_release_handoff.sh"
ZSTD_WORKERS_HELPER = Path(__file__).parent / "zstd_workers.sh"
FAILURE_RECONCILER = (
    Path(__file__).parents[1] / "workflows" / "reconcile-linker-after-failure.yml"
)
RELEASE_DRAFT = Path(__file__).parent / "release_draft.sh"
EARLY_UPLOAD = Path(__file__).parent / "upload_early_release_assets.sh"
ANCHOR_PREFETCH = Path(__file__).parent / "prefetch_patch_anchors.sh"
ANCHOR_DERIVATION = Path(__file__).parent / "patch_fan_anchors.sh"


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
        self.assertIn('legacy = \'jvmArgs = listOf("-Xmx6g")\'', overlay)
        self.assertIn(
            'configured = \'jvmArgs = listOf("-Xmx$linkerHeap", "-XX:+UseG1GC")\'',
            overlay,
        )
        self.assertIn("if legacy_count == 1 and configured_count == 0", overlay)
        self.assertIn("elif legacy_count == 0 and configured_count == 1", overlay)
        self.assertIn("text = text.replace(legacy, configured)", overlay)
        self.assertIn("grep -Fxq 'linkerHeap=12g'", overlay)
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
        self.assertIn('"schema_version": 4', stage)
        self.assertIn(
            '"phase2_implementation_commit": os.environ["PHASE2_IMPLEMENTATION_COMMIT"]',
            stage,
        )

    def test_patch_fan_skips_only_anchors_the_producer_declares_unpatchable(self):
        # A column added without a db_schema_version bump (category.heShortDesc,
        # 2026-07-16) is now handled inside PatchDbProducer, which emits an
        # ALTER TABLE … ADD COLUMN migration. The old shell pre-check that
        # compared PRAGMA table_info and dropped the anchor must be gone: the
        # producer is the single authority on what is patchable, and it says so
        # by exiting 3 and writing "<out>.unpatchable" next to the patch.
        patch_fan = self.step("Produce + verify patch fan")
        self.assertNotIn("MISSING_COLUMNS", patch_fan)
        self.assertNotIn('PRAGMA table_info("{t}")', patch_fan)
        self.assertNotIn("patches cannot add columns", patch_fan)

        gradle_at = patch_fan.index("gradle :generator-common:producePatchAndVerify")
        # The marker is cleared before the producer runs, so a stale file from
        # an earlier anchor can never skip a good one.
        clear_at = patch_fan.index('rm -f "$PATCH_OUT.unpatchable"')
        self.assertLess(clear_at, gradle_at)
        self.assertIn("-Pout=$PATCH_OUT", patch_fan)
        skip_at = patch_fan.index("producer declared the anchor unpatchable; skip anchor")
        self.assertLess(gradle_at, skip_at)
        self.assertIn('if [ -f "$PATCH_OUT.unpatchable" ]; then', patch_fan)
        # A skipped anchor must leave patches/ clean: the marker, the producer's
        # half-built .tmp and any stale .db all go.
        self.assertIn(
            'rm -f "$PATCH_OUT.unpatchable" "$PATCH_OUT" "$PATCH_OUT.tmp"',
            patch_fan[skip_at:],
        )
        self.assertIn("rm -rf prev-dbs\n              continue", patch_fan[skip_at:])
        # Exit 3 is the only tolerated failure mode; everything else still
        # aborts the job through set -e.
        self.assertIn("exits 3", patch_fan)
        self.assertIn("Every other non-zero exit still fails the release", patch_fan)
        # Patch compression at L19 parallelises across cores; L22 ran single-core.
        self.assertIn('ZSTD_LEVEL: "19"', patch_fan)
        # Skips are warnings, but zero patches with prior releases must fail.
        self.assertIn("patch fan produced no patch although prior releases exist", patch_fan)
        self.assertLess(gradle_at, patch_fan.index("patch fan produced no patch although prior releases exist"))

    def test_patch_fan_decides_unpatchable_anchors_before_downloading_them(self):
        # An anchor the producer will reject costs 110–135 s of download plus a
        # decompress before anyone learns that (run 33865604251, anchor v10).
        # The cheap verdict must therefore come BEFORE `gh release download` of
        # seforim.db.zst, and must never be able to fail the release.
        fingerprint = self.step("Fingerprint published DB schema")
        self.assertIn(
            "patch_anchor_schema.py \\\n            dump build/seforim.db > build/db_schema.json",
            fingerprint,
        )
        # It reads build/seforim.db, so it has to sit before the fan (which
        # deletes nothing) and before the compress step that supersedes it.
        self.assertLess(
            self.workflow.index("      - name: Fingerprint published DB schema\n"),
            self.workflow.index("      - name: Produce + verify patch fan\n"),
        )
        self.assertLess(
            self.workflow.index("      - name: Fingerprint published DB schema\n"),
            self.workflow.index("      - name: Compress Seforim Database (zstd)\n"),
        )

        patch_fan = self.step("Produce + verify patch fan")
        precheck_at = patch_fan.index("patch_anchor_schema.py check")
        db_download_at = patch_fan.index("--pattern 'seforim.db.zst'")
        self.assertLess(precheck_at, db_download_at)
        # Only the tiny provenance asset is fetched to decide.
        self.assertLess(
            patch_fan.index("--pattern 'build_provenance.json'"), db_download_at
        )
        self.assertIn("--anchor-version \"$TARGET_VER\"", patch_fan)
        self.assertIn("--this-schema build/db_schema.json", patch_fan)
        # The column comparison is scoped to the producer's own table list, and
        # that list comes from the PAYLOAD checkout — the same commit whose
        # PatchDbProducer runs — not from .pipeline-control. Comparing more
        # tables than the producer does would skip anchors it would have patched.
        contract = "generator/common/src/jvmTest/resources/patch_tables_contract.json"
        self.assertIn(f"--contract-tables {contract}", patch_fan)
        self.assertNotIn(f"--contract-tables .pipeline-control/{contract}", patch_fan)
        self.assertTrue((Path(__file__).parents[2] / contract).is_file())
        # Advisory-safe: a missing asset or a crashing pre-check degrades to
        # PROCEED instead of aborting the job under `set -e`.
        self.assertIn("--dir prev-meta || true", patch_fan)
        self.assertIn(
            '|| PRECHECK="PROCEED pre-check did not run', patch_fan
        )
        # A pre-check skip is announced with the same ::warning::anchor …
        # skip anchor line shape as the producer's marker path, and leaves the
        # loop exactly as that path does — nothing downloaded, nothing staged.
        skip_at = patch_fan.index("pre-download schema check declared the anchor unpatchable; skip anchor")
        self.assertIn(
            '::warning::anchor v${TARGET_VER} ($TAG): ${PRECHECK#* } —', patch_fan
        )
        self.assertLess(precheck_at, skip_at)
        self.assertLess(skip_at, db_download_at)
        # The producer's own marker path is untouched and still authoritative.
        self.assertLess(
            db_download_at, patch_fan.index("producer declared the anchor unpatchable; skip anchor")
        )
        # prev-meta is run-scoped like prev-dbs.
        self.assertIn(
            "rm -rf prev-dbs prev-meta patches release-staging prior-versions.tsv",
            self.workflow,
        )

    def test_provenance_publishes_the_schema_a_future_anchor_check_needs(self):
        stage = self.step("Stage release assets")
        self.assertIn(
            'python3 - "$STAGE" "$MANUAL_LINKS_LINEAGE" build/db_schema.json', stage
        )
        self.assertIn(
            'db_schema = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))',
            stage,
        )
        self.assertIn('"db_schema": db_schema,', stage)
        # Published and compared blocks share one producer, so they cannot drift.
        self.assertEqual(
            self.workflow.count("patch_anchor_schema.py \\\n            dump build/seforim.db"), 1
        )

    def test_patch_fan_allows_only_the_supported_schema_transitions(self):
        patch_fan = self.step("Produce + verify patch fan")

        supported = (
            '[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 4 ]',
            '[ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 3 ]',
            '[ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 4 ]',
            '[ "$PREV_SCHEMA" = 3 ] && [ "$THIS_SCHEMA" = 4 ]',
        )
        for transition in supported:
            self.assertIn(transition, patch_fan)
            self.assertLess(
                patch_fan.index(transition),
                patch_fan.index("gradle :generator-common:producePatchAndVerify"),
            )
        self.assertIn("producing the supported cross-schema delta", patch_fan)
        self.assertIn("is unsupported — skip anchor", patch_fan)
        self.assertNotIn("cross-schema delta unsupported", patch_fan)
        self.assertNotIn('[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 2 ]', patch_fan)
        self.assertNotIn('[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 3 ]', patch_fan)

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
        # The create/reconcile/upload contract lives in ONE sourced script,
        # because the early uploads (during the relink wait and the patch fan)
        # drive the very same draft release; two copies could drift apart.
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        machinery = RELEASE_DRAFT.read_text(encoding="utf-8")
        self.assertIn("AUTOMATIC_TOKEN: ${{ secrets.GITHUB_TOKEN }}", publish)
        self.assertIn("CROSS_REPO_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", publish)
        self.assertIn(
            "source .pipeline-control/.github/scripts/release_draft.sh", publish
        )
        self.assertIn('use_token "$RELEASE_TOKEN_KIND"', publish)
        self.assertIn('switch_token()', machinery)
        self.assertIn('export GH_TOKEN="${AUTOMATIC_TOKEN:-}"', machinery)
        self.assertIn('export GH_TOKEN="${CROSS_REPO_TOKEN:-}"', machinery)
        self.assertIn('exact-draft', machinery)
        self.assertIn('for asset_path in release-staging/*', publish)
        self.assertNotIn('gh release upload "$RELEASE_TAG" "$asset_path" --clobber', machinery)
        self.assertNotIn('gh release upload "$RELEASE_TAG" "$asset_path" --clobber', publish)
        self.assertIn("Never use --clobber.", machinery)
        self.assertIn(
            'gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100"', machinery
        )
        self.assertIn("Draft releases are not", machinery)
        self.assertIn('RELEASE_ID="$(resolve_release_id)"', publish)
        self.assertIn('repos/$GITHUB_REPOSITORY/releases/$RELEASE_ID', machinery)
        self.assertNotIn('releases/tags/$RELEASE_TAG" > "$output"', machinery)
        # A draft is adopted only when its identity is exactly this build's and
        # it carries nothing but the assets this build uploads early.
        self.assertIn('EARLY_RELEASE_ASSETS="lines_snapshot.db.zst seforim.db.buildstate"', machinery)
        self.assertIn("set(names) <= allowed", machinery)
        self.assertIn("len(names)==len(set(names))", machinery)

    def test_patch_fan_anchors_are_prefetched_while_the_db_is_generated(self):
        # The fan paid 110-135 s per anchor to download a 1.3 GB seforim.db.zst
        # with the CPU idle (run 33865604251). The tags are known before the DB
        # build, which spends ~36 minutes without touching the network.
        prefetch = self.step("Prefetch patch-fan anchor DBs (background)")
        fan = self.step("Produce + verify patch fan")
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )
        script = ANCHOR_PREFETCH.read_text(encoding="utf-8")

        self.assertLess(
            self.workflow.index("      - name: Auto-discover prior releases\n"),
            self.workflow.index(
                "      - name: Prefetch patch-fan anchor DBs (background)\n"
            ),
        )
        self.assertLess(
            self.workflow.index(
                "      - name: Prefetch patch-fan anchor DBs (background)\n"
            ),
            self.workflow.index("      - name: Generate Seforim Database\n"),
        )
        self.assertIn("if: steps.discover.outputs.has_prev == 'true'", prefetch)

        # ONE derivation of offset -> tag, used by the prefetch and by the fan:
        # a prefetch that resolved an offset differently would fetch the wrong DB.
        derivation = (
            'patch_fan_anchors.sh \\\n'
            '            "$THIS_VER" prior-versions.tsv $PATCH_OFFSETS > "$ANCHORS"'
        )
        self.assertIn(derivation, prefetch)
        self.assertIn(derivation, fan)
        self.assertTrue(ANCHOR_DERIVATION.is_file())
        self.assertNotIn("awk -F'\\t' -v v=\"$TARGET_VER\"", fan)
        for status in ("ANCHOR", "BELOW-ONE", "NO-RELEASE"):
            self.assertIn(status, ANCHOR_DERIVATION.read_text(encoding="utf-8"))
            self.assertIn(status, fan)

        # Onto the workspace disk, never onto the 16 GiB tmpfs build/.
        self.assertIn('start "$ANCHORS" prefetch', prefetch)
        self.assertNotIn("build/prefetch", self.workflow)

        # Task C's verdict still gates the download: the prefetch runs the same
        # check with the same arguments and never fetches a rejected anchor.
        self.assertIn("patch_anchor_schema.py", script)
        self.assertIn('--this-schema "$THIS_SCHEMA"', script)
        self.assertIn('--anchor-version "$version"', script)
        self.assertIn('--contract-tables "$CONTRACT_TABLES"', script)
        self.assertIn(
            "generator/common/src/jvmTest/resources/patch_tables_contract.json", script
        )
        self.assertLess(
            script.index('[ "${verdict%% *}" = UNPATCHABLE ]'),
            script.index("--pattern 'seforim.db.zst'"),
        )
        # Verified against the release asset's own published size and digest.
        self.assertIn("size mismatch", script)
        self.assertIn("digest mismatch", script)
        self.assertIn('sha256:$(sha256sum "$file"', script)
        self.assertNotIn("gh release view", script)

        # The fan waits for THIS tag's marker, then falls back unchanged.
        self.assertIn('while [ ! -f "$PREFETCH_DIR/$TAG/.done" ]', fan)
        self.assertIn('"$PREFETCH_WAITED" -lt "$PREFETCH_WAIT_SECONDS"', fan)
        self.assertIn(
            'if [ "$PREFETCH_STATE" = ok ] && [ -s "$PREFETCH_DIR/$TAG/seforim.db.zst" ]; then',
            fan,
        )
        self.assertIn('mv "$PREFETCH_DIR/$TAG/seforim.db.zst" prev-dbs/seforim.db.zst', fan)
        self.assertIn("falling back to the serial download", fan)
        self.assertIn("prefetch_patch_anchors.sh abort", fan)
        self.assertLess(
            fan.index("PREFETCH_STATE=absent"), fan.index("--pattern 'seforim.db.zst'")
        )
        # A prefetch timing line per anchor, like the fan's own.
        self.assertIn("prefetch anchor v%s (%s) timings:", script)

        # Nothing background outlives the job; prefetch/ goes with prev-dbs.
        self.assertIn("prefetch_patch_anchors.sh abort prefetch", cleanup)
        self.assertIn(
            "rm -rf prev-dbs prev-meta patches release-staging prior-versions.tsv prefetch",
            cleanup,
        )
        # A stale pid on this weeks-old runner must never be signalled blindly.
        self.assertIn("ps -o args= -p \"$pid\"", script)

    def test_final_assets_upload_while_the_job_waits_instead_of_at_publish(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        fan = self.step("Produce + verify patch fan")
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        uploader = EARLY_UPLOAD.read_text(encoding="utf-8")
        machinery = RELEASE_DRAFT.read_text(encoding="utf-8")

        # The draft is created before the relink wait, with the SHARED create.
        start_snapshot = (
            "upload_early_release_assets.sh \\\n"
            "            start snapshot build/lines_snapshot.db.zst"
        )
        self.assertIn(start_snapshot, relink)
        self.assertIn('source "$(dirname "$self")/release_draft.sh"', uploader)
        self.assertIn("ensure_draft || return 1", uploader)
        self.assertEqual(
            machinery.count(
                'gh release create "$RELEASE_TAG" --target "$SOURCE_COMMIT" '
                '--title "$RELEASE_TAG" --draft'
            ),
            2,
            "the draft create + retry pair may exist only in the shared script",
        )
        self.assertNotIn('gh release create "$RELEASE_TAG"', self.workflow)
        # It starts before the polling wait and is reaped inside the same step,
        # so no upload is ever in flight when the publish step runs.
        self.assertLess(relink.index(start_snapshot), relink.index("completed:success) break"))
        wait_snapshot = (
            "upload_early_release_assets.sh \\\n            wait snapshot 3600"
        )
        self.assertIn(wait_snapshot, relink)
        self.assertLess(relink.index("completed:success) break"), relink.index(wait_snapshot))

        # seforim.db.buildstate is NOT final before the relink wait: Phase-2
        # allocates this build's fresh stable link ids straight into it, so it
        # rides the patch fan instead — after "Apply LINKER links (Phase-2)".
        self.assertNotIn("build/seforim.db.buildstate", relink)
        self.assertIn("DiskBackedLinkIdAllocator", machinery)
        self.assertIn(
            "upload_early_release_assets.sh \\\n"
            "            start buildstate build/seforim.db.buildstate",
            fan,
        )
        self.assertIn(
            "upload_early_release_assets.sh \\\n            wait buildstate 3600", fan
        )
        self.assertLess(
            self.workflow.index("      - name: Apply LINKER links (Phase-2)\n"),
            self.workflow.index("      - name: Produce + verify patch fan\n"),
        )

        # A failed early upload is a lost optimisation, never a failed build.
        self.assertIn("never a failed build", uploader)
        self.assertIn("exit 0", uploader)

        # The publish step still uploads the rest and re-verifies everything by
        # name+size+digest against the staged bytes.
        self.assertIn(
            'for asset_path in release-staging/*; do upload_asset "$asset_path"; done',
            publish,
        )
        self.assertIn("verify_remote", publish)
        self.assertIn(
            "remote release descriptors do not exactly match staged bytes", publish
        )

    def test_a_failed_build_deletes_the_draft_release_it_created(self):
        delete = self.step("Delete this build's unpublished draft release on failure")
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        publish = self.step("Create draft, verify every uploaded asset, then publish")

        self.assertIn("if: failure() || cancelled()", delete)
        self.assertIn('[ "${DRAFT_RELEASE_CREATED:-}" = 1 ]', delete)
        self.assertIn('echo "DRAFT_RELEASE_CREATED=1" >> "$GITHUB_ENV"', relink)
        self.assertIn('echo "DRAFT_RELEASE_CREATED=1" >> "$GITHUB_ENV"', publish)
        # Only ever a DRAFT of exactly this build's identity, and never the tag.
        self.assertIn(".draft == true", delete)
        self.assertIn(".target_commitish == $target", delete)
        self.assertIn(
            'gh api -X DELETE "repos/$GITHUB_REPOSITORY/releases/$release_id"', delete
        )
        self.assertNotIn("--cleanup-tag", self.workflow)
        self.assertNotIn("gh release delete", self.workflow)
        self.assertLess(
            self.workflow.index(
                "      - name: Create draft, verify every uploaded asset, then publish\n"
            ),
            self.workflow.index(
                "      - name: Delete this build's unpublished draft release on failure\n"
            ),
        )
        # The relink orphan cleanup is untouched and still always runs.
        self.assertIn(
            "      - name: Cancel any in-flight relink for this build "
            "(no orphaned linker run)\n        if: always()\n",
            self.workflow,
        )

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

    def test_zstd_steps_saturate_the_runner_without_changing_published_bytes(self):
        helper = ZSTD_WORKERS_HELPER.read_text(encoding="utf-8")
        source = ". .pipeline-control/.github/scripts/zstd_workers.sh"
        snapshot = self.step("Dump lines snapshot for the linker")
        compress = self.step("Compress Seforim Database (zstd)")

        # `-T0` resolves to PHYSICAL cores (8 of the runner's 16 vCPUs), so it
        # must not survive anywhere in the workflow.
        self.assertNotIn("zstd -T0", self.workflow)
        zstd_calls = [
            line.strip()
            for line in self.workflow.splitlines()
            if "zstd " in line and not line.lstrip().startswith("#")
        ]
        self.assertTrue(zstd_calls, "the workflow must still compress with zstd")
        for call in zstd_calls:
            self.assertNotIn("-T0", call)
            # Levels above 19 need --ultra; without it `-22` was silently
            # clamped, so neither may appear in an actual invocation.
            self.assertNotIn("--ultra", call)
            self.assertNotIn("-22", call)

        # The helper is the single worker-count policy, and it must degrade to
        # zstd's own detection (0) rather than emitting an empty `-T`.
        self.assertIn("zstd_workers() {", helper)
        self.assertIn('n="$(nproc 2>/dev/null || echo 0)"', helper)
        self.assertIn("''|*[!0-9]*) n=0 ;;", helper)
        self.assertIn('if [ "$n" -gt 32 ]; then', helper)

        # Both steps source it from the pipeline-control checkout: the payload
        # checkout is pinned to source_commit and may predate the helper.
        self.assertIn(source, snapshot)
        self.assertIn(source, compress)

        # Transient snapshot: level drops to 12 (bytes change once, and only
        # this build's own sha256 — recorded here — gates the consumer).
        self.assertIn(
            'zstd -T"$(zstd_workers)" -12 -f -o build/lines_snapshot.db.zst "$RAW_SNAPSHOT"',
            snapshot,
        )
        self.assertIn(
            'SNAPSHOT_ZST_SHA256=$(sha256sum build/lines_snapshot.db.zst | cut -d\' \' -f1)',
            snapshot,
        )

        # Published DB: explicit -19 is byte-identical to the clamped -22.
        self.assertIn(
            'zstd -T"$(zstd_workers)" -19 -f -o build/seforim.db.zst build/seforim.db',
            compress,
        )

        # The in-JVM patch compressor keeps its own level and is untouched.
        self.assertIn('ZSTD_LEVEL: "19"', self.workflow)

    def test_release_publisher_rejects_asset_names_github_would_normalize(self):
        helper = HANDOFF_PUBLISHER.read_text(encoding="utf-8")
        self.assertIn("release asset basename is unsafe or would be normalized by GitHub", helper)
        self.assertIn("^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$", helper)
        self.assertIn('repos/$GITHUB_REPOSITORY/releases/tags/$tag', helper)
        self.assertIn("targetCommitish:.target_commitish", helper)
        self.assertNotIn('gh release view "$tag" --json', helper)


if __name__ == "__main__":
    unittest.main()
