"""Write out, verbatim, the build_linux steps of Otzaria/otzaria that the index build replays.

Refuses a step that is no longer a plain bash step, and a toolchain pin that no longer matches.

    extract_app_build_steps.py <workflow> <out-dir> <arch>
"""

import os
import sys

import yaml

# file stem -> exact step name in the build_linux job.
REPLAYED = {
    "wpe-script": "Prepare WPE bundling script (shared)",
    "pub-deps": "Prepare Flutter dependencies for Linux release",
    "cargokit-options": "Build the search engine inside the compatibility container",
    "patch-inappwebview": "Patch inappwebview_linux theme_color for WPE 2.48",
    "verify-wpe-platform": "Verify WPE SDK (WPEPlatform) was used, not FDO/system",
    "verify-glibc": "Verify search engine glibc compatibility",
    "bundle-wpe": "Bundle WPE runtime into main bundle (raw + FULL)",
    "verify-wpe-runtime": "Verify WPE runtime in packaged artifacts",
}


def fail(message):
    print(f"::error::{message}", file=sys.stderr)
    sys.exit(1)


def one_step(steps, name):
    found = [s for s in steps if s.get("name") == name]
    if len(found) != 1:
        fail(f"build_linux has {len(found)} steps named '{name}', expected exactly 1")
    return found[0]


def pin(env_name):
    value = os.environ.get(env_name, "")
    if not value:
        fail(f"{env_name} is not set")
    return value


def check_pins(steps, arch):
    flutter = pin("FLUTTER_VERSION")
    if arch == "x86_64":
        found = (one_step(steps, "Set up Flutter").get("with") or {}).get("flutter-version")
        if str(found) != flutter:
            fail(f"the application builds with Flutter {found}; the builder image pins {flutter}")
    else:
        run = one_step(steps, "Set up Flutter (aarch64, from git)").get("run") or ""
        if f"--branch {flutter} " not in run:
            fail(f"the application no longer clones Flutter {flutter} for aarch64")

    wpe = one_step(steps, "Download prebuilt WPE runtime SDK")
    wpe_env = wpe.get("env") or {}
    if str(wpe_env.get("WPE_RUNTIME_VERSION")) != pin("WPE_VERSION"):
        fail(f"the application builds with WPE {wpe_env.get('WPE_RUNTIME_VERSION')}; "
             f"the builder image pins {pin('WPE_VERSION')}")
    if f'tag="{pin("WPE_TAG")}"' not in (wpe.get("run") or ""):
        fail(f"the application no longer downloads WPE from tag {pin('WPE_TAG')}")
    if pin("WPE_SHA256") not in str(wpe_env.get("WPE_RUNTIME_SHA256")):
        fail(f"the application pins another WPE runtime digest for {arch}")


def main():
    if len(sys.argv) != 4:
        fail(__doc__.strip().splitlines()[-1].strip())
    workflow, out_dir, arch = sys.argv[1:]
    if arch not in ("x86_64", "aarch64"):
        fail(f"unsupported arch '{arch}'")
    with open(workflow, encoding="utf-8") as handle:
        doc = yaml.safe_load(handle)
    steps = ((doc.get("jobs") or {}).get("build_linux") or {}).get("steps")
    if not steps:
        fail(f"{workflow} has no build_linux job")

    check_pins(steps, arch)

    os.makedirs(out_dir, exist_ok=False)
    for stem, name in REPLAYED.items():
        step = one_step(steps, name)
        extra = sorted(set(step) - {"name", "shell", "run", "id"})
        if extra:
            fail(f"step '{name}' now carries {extra}; it can no longer be replayed verbatim")
        if step.get("shell") != "bash":
            fail(f"step '{name}' is no longer a bash step")
        run = step.get("run") or ""
        if not run.strip() or "${{" in run:
            fail(f"step '{name}' is empty or uses a workflow expression")
        with open(os.path.join(out_dir, f"{stem}.sh"), "w", encoding="utf-8") as handle:
            handle.write(run)
        print(f"extracted '{name}' -> {stem}.sh")


if __name__ == "__main__":
    main()
