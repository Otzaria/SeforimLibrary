"""Contract tests for build-library-index.yml.

The library search index used to be rebuilt inside every Otzaria/otzaria
release build — 118 minutes of the 130-minute `Linux full x86_64` job of run
34779834547, with two further jobs waiting on it — although it is a pure
function of seforim.db and of the search engine. It is now built once per
database release, here, and stored on that release.

Two properties are load-bearing and invisible in a green run, so they are
pinned here:

  * this must never sit on the database release's critical path, and must never
    start on one of the per-build releases the pipeline publishes while the
    database build is still running;
  * the stored index must stay platform-neutral, which holds only while the
    bundled PDFs are kept out of it (a PDF's index key is its own absolute
    path, so indexing one on a runner would bake a runner path into an archive
    that Windows, macOS and Android clients consume).

Assertions read the parsed workflow, not its comment text.
"""

import re
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image these jobs run on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

SCRIPTS = Path(__file__).parent
WORKFLOWS = SCRIPTS.parent / "workflows"
INDEX = WORKFLOWS / "build-library-index.yml"
RELEASE = WORKFLOWS / "manual-generate-release.yml"
SPLIT = SCRIPTS / "split_release_asset.sh"

# manual-generate-release.yml composes the tag as v<dbVersion>-<utcTimestamp>
# ("Compute release tag"); the snapshot and hand-off releases it also publishes
# are named lines-snapshot-sha256-* and pipeline-result-*.
DATABASE_TAG = re.compile(r"^v[0-9]+-[0-9]{14}$")


def steps_of(job):
    return job.get("steps") or []


def body(job):
    return "\n".join(step["run"] for step in steps_of(job) if "run" in step)


def executed(job):
    """The job's shell, with comment lines removed."""
    return "\n".join(
        line for line in body(job).splitlines()
        if not line.lstrip().startswith("#")
    )


def commands(job):
    """One entry per logical shell command: `\\` continuations joined."""
    joined, pending = [], ""
    for line in executed(job).splitlines():
        stripped = line.strip()
        if stripped.endswith("\\"):
            pending += stripped[:-1].rstrip() + " "
            continue
        joined.append((pending + stripped).strip())
        pending = ""
    if pending:
        joined.append(pending.strip())
    return [c for c in joined if c]


@unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
class LibraryIndexWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = INDEX.read_text(encoding="utf-8")
        cls.doc = yaml.safe_load(cls.text)
        cls.gate = cls.doc["jobs"]["gate"]
        cls.index = cls.doc["jobs"]["index"]
        cls.release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))

    # ─── when it runs ──────────────────────────────────────────────────────

    def test_it_starts_only_after_a_release_is_already_public(self):
        triggers = self.doc[True]  # YAML 1.1 reads a bare `on:` key as True
        self.assertEqual(triggers["release"], {"types": ["published"]})
        # `created` fires on a draft and `prereleased` would double up with
        # `published`; either would put this on the release's critical path.
        self.assertEqual(set(triggers), {"release", "workflow_dispatch"})

    def test_the_gate_admits_database_releases_and_nothing_else(self):
        gate = body(self.gate)
        pattern = re.search(r"grep -Eq '(\^v.*\$)'", gate)
        self.assertIsNotNone(pattern, "the gate no longer matches a tag pattern")
        self.assertEqual(pattern.group(1), DATABASE_TAG.pattern)
        for accepted in ("v1-20260910220310", "v28-20260910220310"):
            self.assertRegex(accepted, DATABASE_TAG)
        for refused in (
            "lines-snapshot-sha256-" + "a" * 40,
            "pipeline-result-run-34535234412-1",
            "v28",
            "fordb-latest",
        ):
            self.assertNotRegex(refused, DATABASE_TAG)

    def test_the_heavy_job_runs_only_behind_the_gate(self):
        self.assertEqual(self.index["needs"], "gate")
        self.assertEqual(self.index["if"], "needs.gate.outputs.proceed == 'true'")

    def test_a_mistyped_manual_tag_fails_instead_of_skipping(self):
        # A dispatch that silently decided "not a database release" would
        # report a green check over an index that was never built.
        self.assertIn(
            "::error::release_tag '$TAG' is not a database release tag",
            body(self.gate),
        )

    def test_a_queued_run_is_never_cancelled_or_superseded(self):
        concurrency = self.doc["concurrency"]
        self.assertEqual(concurrency["cancel-in-progress"], False)
        # GitHub keeps ONE pending run per group by default: a third release
        # would supersede the second, leaving that release with no index and
        # every later application build failing on it. Same durable queue the
        # release build uses.
        self.assertEqual(concurrency["queue"], "max")
        self.assertEqual(
            concurrency["queue"], self.release["concurrency"]["queue"]
        )

    # ─── where it runs ─────────────────────────────────────────────────────

    def test_it_offers_the_same_three_servers_as_the_database_build(self):
        choice = self.doc[True]["workflow_dispatch"]["inputs"]["runner_selection"]
        self.assertEqual(choice["default"], "local")
        self.assertEqual(choice["options"], ["local", "ARM64", "server-2"])
        # Same label sets, so "the machine the database was built on" means the
        # same machine in both workflows.
        release_runs_on = self.release["jobs"]["build-and-release"]["runs-on"]
        for labels in ("otzaria-db", "self-hosted", "ARM64", "server-2"):
            self.assertIn(labels, release_runs_on)
            self.assertIn(labels, body(self.gate))

    def test_the_index_job_resolves_its_runner_from_the_gate(self):
        self.assertEqual(
            self.index["runs-on"], "${{ fromJSON(needs.gate.outputs.runner_labels) }}"
        )

    def test_it_refuses_to_start_without_docker_or_disk(self):
        preflight = body(self.index)
        self.assertIn("::error::docker is required on this runner", preflight)
        self.assertIn("Refusing to start.", preflight)

    # ─── what it produces ──────────────────────────────────────────────────

    def test_the_catalogue_tree_matches_what_the_application_will_compute(self):
        run = body(self.index)
        # catalogueOrder is a POSITION in the whole tree and is baked into every
        # document id, while the application's staleness check compares only
        # kCatalogueOrderRevision — so a tree missing the bundled volumes ships
        # a silently mis-ordered index. They are laid down but never indexed
        # (IndexingRepository.isIndexableBook excludes them since 9b67127).
        self.assertIn("talmud_bavli_latest.tar.zst", run)
        self.assertIn("::error::books/תלמוד בבלי is missing", run)
        self.assertIn("::error::books/seforim.db is missing", run)
        # What is pinned is the volume NAME SET, not the archive bytes: names
        # decide the order, so a re-scan of the same tractates must not fail a
        # build and a changed set must. The two sides compare digests of the
        # same pipeline, so it has to appear here verbatim.
        self.assertIn("talmudVolumesDigest: $talmudVolumesDigest", run)
        pipeline = r"tar -tf - | sed 's#.*/##' | grep -i '\.pdf$'"
        self.assertIn(pipeline, run)
        self.assertIn("LC_ALL=C sort -u", run)
        # ...and the names in the archive are proved against what landed on
        # disk, which is the only thing that catches an extraction the
        # application would never see.
        self.assertIn('cmp -s "$WORK/volumes.txt" "$WORK/volumes-on-disk.txt"', run)
        # isTalmudBavliInstallInProgress hides the whole directory when .version
        # reads 'installing'; writing the digest makes that unreachable.
        self.assertIn('> "$WORK/books/תלמוד בבלי/.version"', run)
        self.assertIn("talmudBavliSha256: $talmudBavliSha256", run)

    def test_the_display_is_brought_up_on_an_observable_condition(self):
        # Comments are stripped: this asserts on what runs, and the step's
        # comments name the wrapper they exist to explain the absence of.
        run = executed(self.index)
        # xvfb-run blocks in `wait` for a SIGUSR1 from Xvfb and has no ceiling
        # of its own, so when that signal does not arrive it is silent forever:
        # runs 34845248197 (2h06m), 34871116986 (2h07m) and 34886226177 (killed
        # at 150s) all died that way, while the identical command exited 0 in a
        # second in probe run 34885339600. Intermittent and unobservable is the
        # one thing this step may not be, so the wrapper is gone and the
        # display is waited for on its socket.
        self.assertNotIn("xvfb-run", run)
        self.assertIn("Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp", run)
        self.assertIn("[ -e /tmp/.X11-unix/X99 ] && break", run)
        self.assertIn("export DISPLAY=:99", run)
        # A dead Xvfb must end the wait at once rather than burn the full 20s,
        # and either way its own log is what the failure prints.
        self.assertIn('kill -0 "$xvfb"', run)
        self.assertIn("cat /tmp/xvfb.log", run)
        # Nothing may be retried or guessed around it.
        self.assertNotIn("SERVERNUM", run)

    def test_every_command_that_can_stall_carries_a_bound(self):
        # The hang this job had was in a container, but it is not the only
        # place one can happen: `docker build` resolves an image and installs
        # ~40 packages over the network, and the downloads and the upload move
        # gigabytes. A socket that stays open without transferring hangs any of
        # them for as long as the job's own ceiling allows, silently.
        run = executed(self.index)
        stallers = (
            "docker build",
            "docker run",
            "gh run download",
            "gh release download",
            "gh release upload",
        )
        for command in commands(self.index):
            for staller in stallers:
                if command.startswith(staller):
                    self.fail(f"unbounded: {command}")
        for staller in stallers:
            self.assertIn(staller, run, f"{staller} vanished from the job")
        # curl's --retry does not fire for a connection that stays open and
        # stops transferring; only a speed floor ends that.
        self.assertIn("--speed-limit", run)
        self.assertIn("--speed-time", run)
        # zstd -19 over the whole index, and the packing it feeds.
        self.assertIn("timeout --kill-after=60 180m bash -c", run)

    def test_a_degraded_index_cannot_be_published(self):
        # Run 34898795644 published an index in 188 segments where run
        # 34888351248 had produced 8 from the same inputs, and every check in
        # the job stayed green: the count is not a correctness property, so
        # nothing else looks at it. The engine names 8 as its own target.
        run = executed(self.index)
        self.assertIn("segments in", run)
        self.assertIn('[ "$SEGMENTS" -le 8 ]', run)
        # An absent optimize line must fail loudly rather than pass as zero.
        self.assertIn('[ -n "$SEGMENTS" ]', run)
        # grep would exit 1 on no match and, under pipefail, kill the step
        # before either message could be printed.
        self.assertNotIn("SEGMENTS=$(grep", run)
        self.assertIn("indexSegments: $indexSegments", body(self.index))

    def test_a_long_transfer_reports_what_it_is_doing(self):
        # Step 9 of run 34893800787 took 40 minutes against 6 in the green run
        # and printed nothing at all until it was over, so a legitimately slow
        # phase could not be told from a hung one and the run was cancelled on
        # a wrong reading. gh and zstd are both silent; the growth report is
        # the only evidence either is still moving bytes.
        run = executed(self.index)
        self.assertIn("watch_growth downloading", run)
        self.assertIn("watch_growth expanding", run)
        self.assertIn("while sleep 30; do", run)
        # A reporter that outlived its step would keep writing into the next.
        self.assertIn(
            """trap 'kill "${WATCH:-}" 2>/dev/null || true' EXIT""", run
        )

    def test_a_silent_hang_is_impossible(self):
        # A bound without --kill-after is not a bound here: plain `timeout`
        # SIGTERMs the docker client, which forwards it and waits on a
        # container that is not listening. `timeout 120` on the preflight of
        # run 34871116986 did exactly that and never fired; with --kill-after
        # the same preflight failed loudly at 150s in run 34886226177.
        run = executed(self.index)
        bounds = re.findall(r"^\s*timeout([^\n]*?) (?:docker|gh|bash|grep)", run, re.M)
        self.assertTrue(bounds, "nothing is bounded any more")
        for bound in bounds:
            self.assertIn("--kill-after", bound, f"unbounded: timeout{bound}")
        # And the bound must reach the container, not only the client that
        # spoke to it: SIGTERM is swallowed by PID 1 and the SIGKILL that
        # follows cannot be proxied, so the container outlives the step.
        self.assertIn("--cidfile", run)
        self.assertIn("docker rm -f", run)
        self.assertIn("build-release-index --help", run)
        self.assertLessEqual(self.index["timeout-minutes"], 180)

    def test_the_container_runs_the_way_the_application_build_runs_it(self):
        run = body(self.index)
        # build_linux runs this binary in this image as root. An unmapped uid
        # with no /etc/passwd entry is an unproven deviation, so the index is
        # produced as root and the tree handed back afterwards.
        self.assertNotIn("--user", run)
        self.assertIn('sudo chown -R "$(id -u):$(id -g)" "$WORK"', run)
        # A tag plus docker's layer cache would otherwise freeze the image on
        # whatever the first build produced, differently on each runner.
        self.assertIn("docker build --pull", run)
        # The external catalogue is a separate screen and no part of the tree.
        self.assertNotIn("otzar-HB_catalog", run)

    def test_the_bundle_matches_the_runner_architecture(self):
        gate = body(self.gate)
        # An x86_64 bundle on the ARM host is an exec format error two hours in.
        self.assertIn("raw_artifact=$RAW", gate)
        self.assertIn("RAW=otzaria-linux-raw-arm64", gate)
        self.assertEqual(
            self.index["env"]["RAW_ARTIFACT"],
            "${{ needs.gate.outputs.raw_artifact }}",
        )
        self.assertIn('-n "$RAW_ARTIFACT"', body(self.index))

    def test_the_bootstrap_path_is_written_down(self):
        # The first application build after this change finds no stored index
        # and fails; its own run is what must then be pinned here. Nothing in
        # the code says a failed run is admissible unless the input does.
        description = self.doc[True]["workflow_dispatch"]["inputs"][
            "otzaria_run_id"
        ]["description"]
        self.assertIn("conclusion is not checked", description)
        self.assertIn("bootstrap", description)

    def test_the_index_is_built_from_the_exact_published_database(self):
        run = body(self.index)
        self.assertIn(
            "::error::seforim.db.zst downloaded as $ACTUAL but the release publishes $EXPECTED",
            run,
        )
        self.assertIn("seforimDbZstSha256: $seforimDbZstSha256", run)

    def test_the_provenance_pins_the_engine_that_built_the_index(self):
        run = body(self.index)
        # The index schema is the search engine's; an index built by another
        # engine reaches the user as one the application rejects and rebuilds.
        self.assertIn("otzaria_search_engine:", run)
        self.assertIn("searchEngineVersion: $searchEngineVersion", run)
        self.assertIn("otzaria-index-inputs", run)

    def test_nothing_caps_the_memory_the_build_may_use(self):
        # Comments are stripped first: this asserts on what is executed, and
        # the step's comments name the very flags it must not carry.
        run = executed(self.index)
        # EVERY container invocation, not the slice before the first
        # `build-release-index` — that slice ends at the --help preflight and
        # leaves the invocation that actually builds the index unchecked, so
        # --memory=32g on it used to pass this test green.
        invocations = [
            c for c in commands(self.index)
            if c.startswith("bounded_run ") or "docker run" in c or "docker build" in c
        ]
        self.assertGreaterEqual(
            len(invocations), 3, "the index is no longer built in a container"
        )
        # Owner decision: the job takes whatever the host gives and must keep
        # doing so after the host's RAM is increased — so no fixed ceiling of
        # any kind. --ipc=host is NOT the way to honour that: run 34845248197
        # carried it and hung for two hours with no output at all, and the
        # environment this command is proven in does not use it.
        #
        # Scoped to the invocations because `zstd --memory=2048MB` on the
        # database download is the opposite of a cap: it RAISES zstd's refusal
        # threshold for the 2 GiB window the asset declares.
        for invocation in invocations:
            self.assertNotIn("--ipc=host", invocation)
            for capped in ("--memory", "--shm-size", "--cpus", "--cpuset"):
                self.assertNotIn(capped, invocation, f"{capped} caps: {invocation}")
        # No RAM precondition either: the release build has one because of its
        # 20G tmpfs; this job writes to disk and must not refuse a smaller box.
        self.assertNotIn("MemAvailable", run)

    def test_it_records_that_pdfs_are_not_in_the_index(self):
        self.assertIn("includesPdfBooks: false", body(self.index))

    def test_assets_are_split_below_the_github_limit_and_reuploadable(self):
        run = body(self.index)
        self.assertIn("split_release_asset.sh", run)
        self.assertIn("--clobber", run)
        self.assertIn("1992294400", self.text)
        self.assertTrue(SPLIT.exists(), "the split helper is missing")
        split = SPLIT.read_text(encoding="utf-8")
        # Otzaria/otzaria reassembles these parts with its own
        # tool/release/assemble_split_asset.sh, which reads exactly these keys.
        for field in ("schemaVersion: 1", "archive: $archive", "parts: $parts"):
            self.assertIn(field, split)
        self.assertIn("github_limit=2147483648", split)

    def test_the_work_directory_is_released_even_when_the_build_fails(self):
        cleanup = [s for s in steps_of(self.index) if s.get("if") == "always()"]
        self.assertEqual(len(cleanup), 1)
        # sudo, or this step cannot succeed at all after a failed build: the
        # container runs as root and `sudo chown` sits on the success path, so
        # a failure leaves root-owned files in the workspace. An unprivileged
        # rm -rf then fails, the files survive, and the NEXT run of any
        # workflow on this runner dies in actions/checkout's `git clean -ffdx`.
        self.assertIn("sudo rm -rf", cleanup[0]["run"])
        prepare = [
            s for s in steps_of(self.index)
            if s.get("name", "").startswith("Prepare a clean work directory")
        ]
        self.assertEqual(len(prepare), 1)
        self.assertIn("sudo rm -rf", prepare[0]["run"])

    def test_the_portability_of_the_index_is_proved_not_declared(self):
        run = executed(self.index)
        # includesPdfBooks: false is a claim written into the provenance. What
        # makes it TRUE is IndexingRepository.isIndexableBook — in another
        # repository, on another branch, at run time. If that regresses, this
        # job would publish an index with the runner's own paths baked into the
        # document keys, and every Windows, macOS and Android client would
        # consume it while both sides reported green.
        self.assertIn('grep -rlF --binary-files=text -- "$WORK/books"', run)
        # ...and a grep that was killed or broke must never read as "found
        # nothing": its three outcomes are kept apart.
        self.assertIn("GREP_RC", run)
        self.assertIn("refusing to publish an index whose portability is unproven", run)

    def test_the_book_count_cannot_silently_become_null(self):
        run = executed(self.index)
        # The only end-to-end signal of how much of the library actually
        # reached the index. ${CATALOGUE:-null} let one reworded line in
        # ReleaseIndexBuilderCli erase it with no red check anywhere.
        self.assertIn('[ -n "$CATALOGUE" ]', run)
        self.assertIn("CATALOGUE_BOOKS=$CATALOGUE", run)
        self.assertNotIn("CATALOGUE_BOOKS=${CATALOGUE:-null}", run)

    def test_untrusted_values_never_reach_the_shell_as_expressions(self):
        # head_branch comes from the Otzaria/otzaria API and git permits `"`
        # and `$` in a ref name. Interpolated into a command line on a runner
        # whose environment holds PIPELINE_TOKEN, that is a shell injection.
        packing = [
            s for s in steps_of(self.index)
            if s.get("name", "").startswith("Pack, describe and split")
        ][0]
        self.assertNotIn("${{", packing["run"])
        self.assertIn("OTZARIA_HEAD_BRANCH", packing["env"])

    def test_the_cross_repository_read_uses_the_pipeline_token(self):
        self.assertEqual(
            self.index["env"]["GH_TOKEN"], "${{ secrets.PIPELINE_TOKEN }}"
        )


if __name__ == "__main__":
    unittest.main()
