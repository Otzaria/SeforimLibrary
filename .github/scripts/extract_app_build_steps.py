"""Write out, verbatim, the build_linux steps of Otzaria/otzaria that the index build replays.

Every build_linux step must be one this script knows as replayed, mirrored or skipped; any drift fails.

    extract_app_build_steps.py <workflow> <out-dir> <arch>
    extract_app_build_steps.py --fingerprints <workflow>
"""

import hashlib
import os
import re
import sys

import yaml

# file stem -> exact step name; run verbatim, in this order.
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

FLUTTER_BUILD = "Build Flutter Linux app (with retry)"
WPE_DOWNLOAD = "Download prebuilt WPE runtime SDK"

# Reproduced by build-library-index.yml itself; pinned by fingerprint of the comment-free run.
MIRRORED_RUN = {
    "Prepare container base tools": "b996259b07b684be56e7cc529cfb4f1712c3381f19e732bc41e383b2ab63b67b",
    "Set up Flutter (aarch64, from git)": "a285bfe7c1b4f2924efbb7563e398a9dd3f6ff85c4bdd56f169c3fb7dbc43c49",
    "Add cargo to PATH and verify toolchain": "ccd2ca002cdeb025b85eace91b76b9c430c8459f11d07fc991221d30f6a4f879",
    "Install Linux build dependencies": "afad8660067a7909e955667c2cabd3782c2a5f889664f1bdc1604fa5daaa11cc",
    WPE_DOWNLOAD: "291e7657cf03dd9b570975bf30f0bc9158bec15ed796b2eaf0887e7c30ede838",
    "Find and prepare Linux packages": "21a48f84f5ae62a1db74bf93c6d9f81ab22f3cbcb80a5fd993ba76586c50c7b1",
}
MIRRORED_USES = {
    "Clone repository": {
        "uses": "actions/checkout@v7.0.1",
        "with": {"ref": "${{ needs.bump_version.outputs.sha }}"},
    },
    "Set up Flutter": {
        "if": "matrix.arch != 'aarch64'",
        "uses": "subosito/flutter-action@v2",
        "with": {"channel": "stable", "flutter-version": "3.47.2", "cache": True},
    },
    "Install Rust (stable) with rustup": {"uses": "dtolnay/rust-toolchain@stable"},
}

# The comment-free run of FLUTTER_BUILD; the index build runs it once, without the secret.
FLUTTER_BUILD_RUN = """set -eo pipefail
tries=0
: > "$GITHUB_WORKSPACE/build_linux.log"
until [ $tries -ge 3 ]
do
if flutter build linux --verbose --dart-define=BIO_OBF_KEY=${{ secrets.BIO_OBF_KEY }} 2>&1 | tee -a "$GITHUB_WORKSPACE/build_linux.log"; then
break
fi
tries=$((tries+1))
echo "flutter build linux failed. retry $tries/3 after short sleep..."
sleep 15
done
if [ $tries -ge 3 ]; then
echo "::error::flutter build linux failed after 3 attempts"
exit 1
fi"""
FLUTTER_BUILD_ENV = {
    "CC": "clang",
    "CXX": "clang++",
    "CFLAGS": "-Wno-error=unknown-warning-option",
    "CXXFLAGS": "-Wno-error=unknown-warning-option",
}

# The job environment; the index build passes the /opt/wpe-sdk and arch-derived values itself.
JOB_ENV = {
    "RUSTUP_HOME": "${{ github.workspace }}/.rustup",
    "CARGO_HOME": "${{ github.workspace }}/.cargo",
    "DEBIAN_FRONTEND": "noninteractive",
    "FLUTTER_BUILD_DIR": "build/linux/${{ matrix.arch == 'aarch64' && 'arm64' || 'x64' }}/release",
    "PKG_CONFIG_PATH": "/opt/wpe-sdk/lib/pkgconfig",
    "LD_LIBRARY_PATH": "/opt/wpe-sdk/lib",
    "LIBRARY_PATH": "/opt/wpe-sdk/lib",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-${{ matrix.arch == 'aarch64' && 'arm64' || 'amd64' }}",
}
CONTAINER = {"image": "debian:bookworm-slim"}

# Every build_linux step, in order. R = replayed, M = mirrored, S = skipped (packaging,
# uploads, secrets, caches and FULL-only work the raw bundle does not contain).
STEPS = [
    ("Prepare container base tools", "M"),
    ("Clone repository", "M"),
    ("Check if commit message is a version tag", "S"),
    ("Set up Flutter", "M"),
    ("Set up Flutter (aarch64, from git)", "M"),
    ("Create Google Calendar Credentials", "S"),
    ("Install Rust (stable) with rustup", "M"),
    ("Add cargo to PATH and verify toolchain", "M"),
    ("Cache cargo registry and git", "S"),
    ("Install Linux build dependencies", "M"),
    (WPE_DOWNLOAD, "M"),
    ("Prepare WPE bundling script (shared)", "R"),
    ("Download library assets for Linux FULL bundle", "S"),
    ("Download bundled plugins for Linux packages", "S"),
    ("Install flutter_distributor (with retry)", "S"),
    ("Fetch biographies data", "S"),
    ("Prepare Flutter dependencies for Linux release", "R"),
    ("Build the search engine inside the compatibility container", "R"),
    ("Patch inappwebview_linux theme_color for WPE 2.48", "R"),
    (FLUTTER_BUILD, "M"),
    ("Verify WPE SDK (WPEPlatform) was used, not FDO/system", "R"),
    ("Verify search engine glibc compatibility", "R"),
    ("Dump CMake error log on failure", "S"),
    ("Build and Patch Linux DEB package", "S"),
    ("Build and Patch Linux RPM package", "S"),
    ("Bundle WPE runtime into main bundle (raw + FULL)", "R"),
    ("Verify WPE runtime in packaged artifacts", "R"),
    ("Find and prepare Linux packages", "M"),
    ("Upload Linux DEB package", "S"),
    ("Upload Linux RPM package", "S"),
    ("Upload linux build (raw)", "S"),
    ("Upload index build inputs", "S"),
    ("Build bundled zstd (Linux FULL)", "S"),
    ("Build atomic updater (Linux FULL)", "S"),
    ("Stamp installed release (Linux FULL)", "S"),
    ("Create Linux FULL portable bundle", "S"),
    ("Generate application file manifest (Linux)", "S"),
    ("Upload application file manifest (Linux)", "S"),
    ("Upload Linux FULL bundle", "S"),
    ("Upload indexed FULL library parts", "S"),
    ("Upload indexed FULL manifest for installer build", "S"),
    ("Build Linux Download Assistant (non-fatal helper tool)", "S"),
    ("Upload Linux Download Assistant", "S"),
]

WPE_SHA_EXPR = re.compile(
    r"^\$\{\{ matrix\.arch == 'aarch64' && '([0-9a-f]{64})' \|\| '([0-9a-f]{64})' \}\}$"
)


def fail(message):
    print(f"::error::{message}", file=sys.stderr)
    sys.exit(1)


def normalized(run):
    lines = (line.strip() for line in (run or "").splitlines())
    return "\n".join(line for line in lines if line and not line.startswith("#"))


def fingerprint(run):
    return hashlib.sha256(normalized(run).encode("utf-8")).hexdigest()


def pin(env_name):
    value = os.environ.get(env_name, "")
    if not value:
        fail(f"{env_name} is not set")
    return value


def build_linux(workflow):
    with open(workflow, encoding="utf-8") as handle:
        doc = yaml.safe_load(handle)
    job = (doc.get("jobs") or {}).get("build_linux")
    if not job or not job.get("steps"):
        fail(f"{workflow} has no build_linux job")
    return job


def check_structure(job):
    names = [step.get("name") for step in job["steps"]]
    expected = [name for name, _ in STEPS]
    if names != expected:
        added = [n for n in names if n not in expected]
        removed = [n for n in expected if n not in names]
        fail(f"build_linux steps changed (added {added}, removed {removed}, or reordered); "
             "classify them in extract_app_build_steps.py")
    if job.get("env") != JOB_ENV:
        fail(f"build_linux job environment changed: {job.get('env')}")
    if job.get("container") != CONTAINER:
        fail(f"build_linux container changed: {job.get('container')}")


def check_mirrored(steps, arch):
    for name, expected in MIRRORED_USES.items():
        found = {k: v for k, v in steps[name].items() if k != "name"}
        if found != expected:
            fail(f"step '{name}' changed: {found}")
    for name, expected in MIRRORED_RUN.items():
        if fingerprint(steps[name].get("run")) != expected:
            fail(f"step '{name}' changed (fingerprint {fingerprint(steps[name].get('run'))})")

    build = steps[FLUTTER_BUILD]
    if normalized(build.get("run")) != FLUTTER_BUILD_RUN or build.get("env") != FLUTTER_BUILD_ENV:
        fail(f"step '{FLUTTER_BUILD}' changed: the index build's flutter build no longer mirrors it")
    if set(build) != {"name", "shell", "env", "run"} or build.get("shell") != "bash":
        fail(f"step '{FLUTTER_BUILD}' changed shape: {sorted(build)}")

    flutter = pin("FLUTTER_VERSION")
    if str(MIRRORED_USES["Set up Flutter"]["with"]["flutter-version"]) != flutter:
        fail(f"the application builds with Flutter "
             f"{MIRRORED_USES['Set up Flutter']['with']['flutter-version']}; the builder image pins {flutter}")
    if f"--branch {flutter} " not in steps["Set up Flutter (aarch64, from git)"].get("run", ""):
        fail(f"the application no longer clones Flutter {flutter} for aarch64")

    wpe = steps[WPE_DOWNLOAD]
    wpe_env = wpe.get("env") or {}
    if set(wpe_env) != {"WPE_RUNTIME_VERSION", "WPE_RUNTIME_SHA256"}:
        fail(f"step '{WPE_DOWNLOAD}' environment changed: {sorted(wpe_env)}")
    if str(wpe_env["WPE_RUNTIME_VERSION"]) != pin("WPE_VERSION"):
        fail(f"the application builds with WPE {wpe_env['WPE_RUNTIME_VERSION']}; "
             f"the builder image pins {pin('WPE_VERSION')}")
    if f'tag="{pin("WPE_TAG")}"' not in (wpe.get("run") or ""):
        fail(f"the application no longer downloads WPE from tag {pin('WPE_TAG')}")
    digests = WPE_SHA_EXPR.match(str(wpe_env["WPE_RUNTIME_SHA256"]))
    if not digests:
        fail(f"WPE_RUNTIME_SHA256 is no longer a per-arch pair: {wpe_env['WPE_RUNTIME_SHA256']}")
    app_digest = digests.group(1) if arch == "aarch64" else digests.group(2)
    if app_digest != pin("WPE_SHA256"):
        fail(f"the application pins WPE runtime {app_digest} for {arch}; the builder image pins {pin('WPE_SHA256')}")


def extract(steps, out_dir):
    os.makedirs(out_dir, exist_ok=False)
    for stem, name in REPLAYED.items():
        step = steps[name]
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


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--fingerprints":
        steps = {s.get("name"): s for s in build_linux(sys.argv[2])["steps"]}
        for name in MIRRORED_RUN:
            print(f"{fingerprint(steps[name].get('run'))}  {name}")
        return
    if len(sys.argv) != 4:
        fail("usage: extract_app_build_steps.py <workflow> <out-dir> <arch>")
    workflow, out_dir, arch = sys.argv[1:]
    if arch not in ("x86_64", "aarch64"):
        fail(f"unsupported arch '{arch}'")
    job = build_linux(workflow)
    check_structure(job)
    steps = {step["name"]: step for step in job["steps"]}
    check_mirrored(steps, arch)
    extract(steps, out_dir)


if __name__ == "__main__":
    main()
