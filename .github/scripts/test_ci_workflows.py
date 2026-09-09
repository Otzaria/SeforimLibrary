"""Contract tests for the hosted CI workflows: ci.yml and contract.yml.

Both jobs used to report a green check that carried no evidence — no test count
appears anywhere in either CI log of cycle 33987355439 — while `contract`
re-ran on the machine-made refs the pipeline itself pushes and `CI - Tests` ran
on a deprecated Gradle action that reused nothing (67 of 67 tasks executed).
These tests pin the fixes.

Assertions read the parsed workflow, not its comment text, so a comment that
quotes a warning cannot satisfy or break them.
"""

import re
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image these jobs run on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

WORKFLOWS = Path(__file__).parents[1] / "workflows"
RELEASE = WORKFLOWS / "manual-generate-release.yml"
CI = WORKFLOWS / "ci.yml"
CONTRACT = WORKFLOWS / "contract.yml"
TRUNK = "otzaria"

# GitHub Actions evaluates a `run:` script as ONE template expression and
# refuses to LOAD the whole workflow file when it exceeds 21,000 characters.
# The margin below is what a step body may reach here.
EXPRESSION_CEILING = 21000
RUN_CEILING = 19000

# The lowest major of each action that runs on Node 24. Every "Node.js 20 is
# deprecated … forced to run on Node.js 24" warning in cycle 33987355439 named
# an action below one of these. Read off each action's own action.yml
# (`runs.using`) at the major tag on 2026-09-08, not from memory.
NODE24_MAJOR = {
    "actions/checkout": 5,
    "actions/setup-java": 5,
    "actions/setup-python": 6,
    "actions/upload-artifact": 6,
    "gradle/actions/setup-gradle": 5,
}

# Self-hosted jobs are deliberately NOT bumped: their runner version is not
# proven in this repository and a broken action on the weekly build costs a
# release. Naming them keeps that a decision rather than an oversight, and
# stops a new HOSTED job from quietly joining the exemption.
SELF_HOSTED_JOBS = {
    ("manual-generate-release.yml", "build-and-release"),
    ("delta-pipeline-dryrun.yml", "dryrun"),
    ("delta-real-diff-arm.yml", "real-diff"),
    ("delta-real-diff-test.yml", "real-diff"),
}


def parsed_workflows():
    for path in sorted(WORKFLOWS.glob("*.yml")):
        yield path.name, yaml.safe_load(path.read_text(encoding="utf-8"))


def steps_of(doc):
    for job_name, job in (doc.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            yield job_name, job, step


def action_major(uses):
    match = re.fullmatch(r"([^@]+)@v(\d+)", uses or "")
    return (match.group(1), int(match.group(2))) if match else (None, None)


def contract_suites(body):
    """The suite names of a `for suite in \\ … ; do` loop, in their own order."""
    inner = body.split("for suite in \\\n", 1)[1].split("; do", 1)[0]
    return [
        line.strip().rstrip("\\").strip()
        for line in inner.splitlines()
        if line.strip()
    ]


def named_step(doc, step_name):
    for _, _, step in steps_of(doc):
        if step.get("name") == step_name:
            return step
    raise AssertionError(f"no step named {step_name!r}")


@unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
class CiWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci_text = CI.read_text(encoding="utf-8")
        cls.contract_text = CONTRACT.read_text(encoding="utf-8")
        cls.ci = yaml.safe_load(cls.ci_text)
        cls.contract = yaml.safe_load(cls.contract_text)

    def run_bodies(self, doc):
        return [step["run"] for _, _, step in steps_of(doc) if "run" in step]

    def uses(self, doc):
        return [step["uses"] for _, _, step in steps_of(doc) if "uses" in step]

    # ─── ci.yml ────────────────────────────────────────────────────────────

    def test_no_workflow_still_uses_the_deprecated_gradle_action(self):
        # "##[warning]This job uses deprecated functionality from the
        # 'gradle/gradle-build-action' action" on both CI runs of the cycle.
        for name, doc in parsed_workflows():
            for uses in self.uses(doc):
                self.assertFalse(
                    uses.startswith("gradle/gradle-build-action"),
                    f"{name} still pins {uses}",
                )

    def test_ci_setup_gradle_caches_writably_only_on_the_trunk(self):
        # CI got `67 actionable tasks: 67 executed` — nothing from cache —
        # while contract's setup-gradle got 7 of 16 FROM-CACHE.
        setup = [
            step
            for _, _, step in steps_of(self.ci)
            if step.get("uses", "").startswith("gradle/actions/setup-gradle@")
        ]
        self.assertEqual(len(setup), 1)
        self.assertEqual(
            setup[0]["with"]["cache-read-only"],
            "${{ github.ref != 'refs/heads/%s' }}" % TRUNK,
        )

    def test_ci_and_contract_run_one_generation_of_the_gradle_action(self):
        # Two action generations in one repo is what split the cache.
        def majors(doc):
            return {
                uses
                for uses in self.uses(doc)
                if uses.startswith("gradle/actions/setup-gradle@")
            }

        self.assertEqual(majors(self.ci), majors(self.contract))

    def test_ci_ends_with_an_explicit_test_count(self):
        # No "tests run" line exists anywhere in either CI log of the cycle,
        # and 4 of the 11 modules are `allTests NO-SOURCE`.
        body = "\n".join(self.run_bodies(self.ci))
        self.assertIn(
            'f"tests: allTests {tests - failures - errors - skipped} passed, "', body
        )
        self.assertIn("**/build/test-results/**/*.xml", body)
        self.assertIn("::error::allTests wrote no test-result XML — no test ran", body)
        # Gradle's exit status still decides the step, count or no count.
        self.assertIn(
            "./gradlew allTests --no-daemon --continue --warning-mode all || rc=$?", body
        )
        self.assertIn('exit "$rc"', body)

    def test_exactly_one_invocation_in_the_repo_asks_for_every_deprecation(self):
        # "Deprecated Gradle features were used in this build" fired 4× per
        # release run with no way to see which. This is the one invocation that
        # shows them; the self-hosted release build keeps the quiet default.
        hits = {}
        for name, doc in parsed_workflows():
            count = sum(body.count("--warning-mode all") for body in self.run_bodies(doc))
            if count:
                hits[name] = count
        self.assertEqual(hits, {"ci.yml": 1})

    def test_ci_still_only_runs_on_the_trunk(self):
        triggers = self.ci[True]  # YAML 1.1 reads a bare `on:` key as True
        self.assertEqual(triggers["push"], {"branches": [TRUNK]})
        self.assertEqual(triggers["pull_request"], {"branches": [TRUNK]})

    # ─── contract.yml ──────────────────────────────────────────────────────

    def test_contract_ignores_the_refs_the_pipeline_pushes(self):
        # Runs 34026451036 (branch lines-snapshot-sha256-2486e499…) and
        # 34030778729 (branch v27-20260906092829) were both `push` on the same
        # already-green SHA 2aadf7a7 — 4 runs for 2 distinct trees.
        triggers = self.contract[True]
        self.assertEqual(triggers["push"], {"branches": [TRUNK]})
        self.assertIn("pull_request", triggers)
        self.assertIsNone(triggers["pull_request"], "PRs stay unfiltered on purpose")
        self.assertNotIn("release", triggers)
        self.assertNotIn("tags", triggers["push"])

    def test_contract_ends_with_an_explicit_test_count(self):
        body = "\n".join(self.run_bodies(self.contract))
        self.assertIn(
            'f"tests: PatchTablesContractTest+LogicalHashContractTest {tests - failures - errors - skipped} passed, "',
            body,
        )
        self.assertIn("generator/common/build/test-results/jvmTest/*.xml", body)
        self.assertIn(
            "::error::the contract test wrote no test-result XML — nothing ran", body
        )
        self.assertIn('exit "$rc"', body)

    def test_contract_fixture_steps_say_what_they_checked(self):
        body = "\n".join(self.run_bodies(self.contract))
        self.assertIn('echo "fixtures: 6 present"', body)
        self.assertIn('echo "fixtures: 3 pairs byte-identical"', body)

    def test_pull_requests_run_the_local_orchestration_contract_suites(self):
        # Those suites used to run in exactly one place: the release workflow's
        # `reconcile` job, i.e. only once the weekly build had already started.
        # Run 34195296928 never started a job — GitHub refused to load
        # manual-generate-release.yml — so a merged change went unchecked. The
        # PR job runs the same loop, and the two suite lists are pinned to each
        # other so neither can drift.
        release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
        step_name = "Validate local orchestration boundary contracts"
        ci_loop = named_step(self.ci, step_name)["run"]
        release_loop = named_step(release, step_name)["run"]

        self.assertEqual(contract_suites(ci_loop), contract_suites(release_loop))
        self.assertGreater(len(contract_suites(ci_loop)), 0)
        # Same fail-closed shape: unittest's own count, and a suite that
        # collected nothing fails instead of reporting a green check.
        for pinned in (
            "set -euo pipefail",
            "ran=$(sed -n 's/^Ran \\([0-9][0-9]*\\) test.*/\\1/p'",
            "::error::$suite failed — see its output above",
            "::error::$suite reported no tests — it collected nothing",
            'echo "tests: local orchestration boundary contracts $total passed"',
        ):
            self.assertIn(pinned, ci_loop)
            self.assertIn(pinned, release_loop)

        # Cheap and hosted: no Gradle, no JVM, a bounded ceiling and read-only
        # credentials. The heavy `test` job already owns the JVM side.
        job = self.ci["jobs"]["contracts"]
        self.assertEqual(job["runs-on"], "ubuntu-latest")
        self.assertEqual(job["timeout-minutes"], 10)
        self.assertEqual(job["permissions"], {"contents": "read"})
        for step in job["steps"]:
            self.assertNotIn("gradle", (step.get("run") or "").lower())
            self.assertNotIn("gradle", step.get("uses", "").lower())
        # A missing PyYAML would skip most of those suites and still report a
        # green check, so the job says which interpreter it proved.
        self.assertIn("import sys, yaml;", "\n".join(self.run_bodies(self.ci)))

    # ─── every workflow in the repository ──────────────────────────────────

    def test_no_run_script_approaches_the_max_expression_length(self):
        """Every `run:` script stays well under GitHub's 21,000-char ceiling.

        A `run:` script is one template expression, and GitHub refuses to LOAD
        a workflow whose expression exceeds 21,000 characters. Run 34195296928
        (commit 15ef349) never started a job: "Invalid workflow file:
        .github/workflows/manual-generate-release.yml — (Line: 1974, Col: 14):
        Exceeded max expression length 21000". "Produce + verify patch fan" had
        reached 22,592 characters and the relink wait 21,230; both moved into
        .github/scripts/. Nothing in the workflow file itself reports this — the
        error only appears when GitHub tries to load the file — so it is pinned
        here, with ~2,000 characters of headroom so one more paragraph of
        comment cannot walk a step over the cliff.
        """
        # parsed_workflows() globs *.yml; a *.yaml would escape the sweep.
        self.assertEqual(sorted(WORKFLOWS.glob("*.yaml")), [])
        self.assertLess(RUN_CEILING, EXPRESSION_CEILING)
        longest = ("", 0)
        checked = 0
        for name, doc in parsed_workflows():
            for job_name, _job, step in steps_of(doc):
                run = step.get("run")
                if not isinstance(run, str):
                    continue
                checked += 1
                where = (
                    f"{name}:{job_name} step "
                    f"{step.get('name') or step.get('id') or '<unnamed>'!r}"
                )
                if len(run) > longest[1]:
                    longest = (where, len(run))
                self.assertLess(
                    len(run),
                    RUN_CEILING,
                    f"{where} has a {len(run)}-character run: script; GitHub "
                    f"refuses the whole workflow at {EXPRESSION_CEILING}. Move a "
                    "cohesive block into .github/scripts/ and call or source it.",
                )
        self.assertGreater(checked, 0, f"no run: script found (longest {longest})")

    # ─── every hosted job in the repository ────────────────────────────────

    def test_hosted_jobs_use_action_majors_that_run_on_node_24(self):
        checked = 0
        for name, doc in parsed_workflows():
            for job_name, job, step in steps_of(doc):
                self_hosted = (name, job_name) in SELF_HOSTED_JOBS
                self.assertEqual(
                    self_hosted,
                    job.get("runs-on") != "ubuntu-latest",
                    f"{name}:{job_name} changed runner class",
                )
                if self_hosted:
                    continue
                action, major = action_major(step.get("uses"))
                if action not in NODE24_MAJOR:
                    continue
                checked += 1
                self.assertGreaterEqual(
                    major,
                    NODE24_MAJOR[action],
                    f"{name}:{job_name} pins {action}@v{major}, still Node 20",
                )
        self.assertGreater(checked, 0)


if __name__ == "__main__":
    unittest.main()
