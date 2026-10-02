"""The ONNX model package a worker embeds with: its checksum, and a verified fetch into a cache.

The package checksum is the sidecar's (src/semantic/model_package.rs; tools/onnx_package_checksum.py):

    manifest       = "otzaria-onnx-package-v1\\n"
                   + per file, ordered by relpath bytewise: relpath "\\t" size "\\t" sha256 "\\n"
    model_checksum = sha256(manifest)

over the graph, the tokenizer.json beside it and every external-data file the graph names.
This module covers the files it is told about — the graph and tokenizer.json for a package
without external data, which the Meivin packages are. A package with external data has a
different checksum than this computes, so it is refused, never silently accepted.

The fetch reads the access token from an environment variable and never prints it: it goes
only into the Authorization header of requests to the hub's own host, and is dropped when a
download is redirected to another host (the hub's CDN, whose URLs are already signed). By
default it reads the repository at the commit pins.env pins (MODEL_REPO, MODEL_REVISION),
never at a branch; what it fetches is held to the package checksum either way.

Standard library only.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

MANIFEST_VERSION = "otzaria-onnx-package-v1"
TOKENIZER_FILE = "tokenizer.json"
DEFAULT_TOKEN_ENV = "OTZARIA_HF_TOKEN"


def _pins() -> Dict[str, str]:
    """pins.env beside this file, KEY=value lines."""
    pins = {}
    path = Path(__file__).resolve().parent / "pins.env"
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            pins[key] = value.strip().strip('"')
    return pins


_PINS = _pins()
DEFAULT_REPO = _PINS["MODEL_REPO"]
DEFAULT_REVISION = _PINS["MODEL_REVISION"]
HUB = "https://huggingface.co"


class PackageError(Exception):
    pass


def file_entry(root: Path, relpath: str) -> Tuple[str, int, str]:
    path = Path(root) / relpath
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 24), b""):
            h.update(block)
            size += len(block)
    return relpath, size, h.hexdigest()


def checksum_of_entries(entries: Iterable[Tuple[str, int, str]]) -> str:
    lines = [MANIFEST_VERSION + "\n"]
    for relpath, size, digest in sorted(entries, key=lambda e: e[0].encode("utf-8")):
        lines.append(f"{relpath}\t{size}\t{digest}\n")
    return hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()


def package_checksum(root: Path, graph: str, extra: Iterable[str] = ()) -> str:
    """The checksum of the package rooted at `root`: `graph`, tokenizer.json, and `extra`."""
    names = [graph, TOKENIZER_FILE] + list(extra)
    return checksum_of_entries(file_entry(root, n) for n in names)


def tokenizer_checksum(root: Path) -> str:
    return file_entry(root, TOKENIZER_FILE)[2]


class _DropAuthOnForeignRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc:
            new.remove_header("Authorization")
        return new


def _download(url: str, dest: Path, token: Optional[str], timeout: int = 120) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "seforim-vectors-worker"})
    if token:
        request.add_header("Authorization", "Bearer " + token)
    opener = urllib.request.build_opener(_DropAuthOnForeignRedirect())
    tmp = Path(str(dest) + ".partial")
    try:
        with opener.open(request, timeout=timeout) as response, open(tmp, "wb") as out:
            shutil.copyfileobj(response, out, 1 << 22)
            out.flush()
            os.fsync(out.fileno())
    except urllib.error.HTTPError as error:
        tmp.unlink(missing_ok=True)
        hint = " (is the access token set and allowed to read the repository?)" if error.code in (401, 403, 404) else ""
        raise PackageError(f"HTTP {error.code} fetching {url}{hint}") from None
    except (urllib.error.URLError, OSError) as error:
        tmp.unlink(missing_ok=True)
        raise PackageError(f"could not fetch {url}: {getattr(error, 'reason', error)}") from None
    os.replace(tmp, dest)


def fetch_package(cache: Path, checksum: str, graph: str, repo: str = DEFAULT_REPO,
                  revision: str = DEFAULT_REVISION, token_env: str = DEFAULT_TOKEN_ENV,
                  hub: str = HUB, log=None) -> Path:
    """The package with `checksum`, in `cache/<checksum>/`: verified if present, else fetched.

    A cached copy that does not hash to `checksum` is removed and fetched again; a fetched
    copy that does not is removed and refused. Concurrent callers serialize on a lock file.
    """
    if not (len(checksum) == 64 and all(c in "0123456789abcdef" for c in checksum)):
        raise PackageError("the package checksum must be 64 lowercase hex digits")
    if not revision:
        raise PackageError("no revision to fetch the package at")
    root = Path(cache) / checksum
    root.mkdir(parents=True, exist_ok=True)
    with open(Path(cache) / f".{checksum}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        files = [graph, TOKENIZER_FILE]
        if all((root / f).exists() for f in files) and package_checksum(root, graph) == checksum:
            if log:
                log(f"package {checksum[:12]} found in the cache, checksum verified")
            return root
        for f in files:
            (root / f).unlink(missing_ok=True)
        token = os.environ.get(token_env) or None
        for f in files:
            url = f"{hub.rstrip('/')}/{repo}/resolve/{urllib.parse.quote(revision, safe='')}/{urllib.parse.quote(f)}"
            if log:
                log(f"fetching {url}" + (" (authenticated)" if token else " (anonymous)"))
            _download(url, root / f, token)
        got = package_checksum(root, graph)
        if got != checksum:
            for f in files:
                (root / f).unlink(missing_ok=True)
            raise PackageError(f"the fetched package hashes to {got}, not the plan's {checksum}; refused")
        if log:
            log(f"package {checksum[:12]} fetched and verified")
        return root


def main(argv=None) -> int:
    """model_package.py fetch --cache DIR --checksum HEX --graph NAME [--repo R] [--revision REV]:
    the verified package's directory on stdout, the log on stderr (for the driver's CPU path)."""
    import argparse
    ap = argparse.ArgumentParser(description="Fetch a model package into a cache, verified, and print its directory.")
    sub = ap.add_subparsers(dest="command", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--cache", required=True, type=Path)
    f.add_argument("--checksum", required=True)
    f.add_argument("--graph", required=True)
    f.add_argument("--repo", default=DEFAULT_REPO)
    f.add_argument("--revision", default=DEFAULT_REVISION)
    f.add_argument("--token-env", default=DEFAULT_TOKEN_ENV)
    f.add_argument("--hub", default=HUB, help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    try:
        root = fetch_package(a.cache, a.checksum, a.graph, repo=a.repo, revision=a.revision, token_env=a.token_env,
                             hub=a.hub, log=lambda message: print(message, file=sys.stderr, flush=True))
    except PackageError as error:
        print(f"model_package: {error}", file=sys.stderr)
        return 2
    print(root)
    return 0


def check_family(root: Path, graph: str, model: dict, package: dict, worker_dim: int,
                 worker_pooling: str, worker_max_tokens: int) -> None:
    """check_runtime: the package is the plan's passage package, and the tokenizer, width,
    pooling and token cap are the family's."""
    got = package_checksum(root, graph)
    if got != package["checksum"]:
        raise PackageError(f"passage_package: the plan declares {package['quantization']} "
                           f"{package['checksum']} and the loaded package is {got}")
    for field, declared, loaded in (
        ("tokenizer_checksum", model["tokenizer_checksum"], tokenizer_checksum(root)),
        ("embedding_dim", str(model["embedding_dim"]), str(worker_dim)),
        ("pooling", model["pooling"], worker_pooling),
        ("max_tokens", str(model["max_tokens"]), str(worker_max_tokens)),
    ):
        if declared != loaded:
            raise PackageError(f"{field}: the family declares {declared} and the worker loaded {loaded}")


if __name__ == "__main__":
    sys.exit(main())
