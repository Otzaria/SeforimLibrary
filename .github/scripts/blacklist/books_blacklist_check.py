#!/usr/bin/env python3
"""Check books_blacklist.txt against the titles the importer will actually see.

The importer drops a Sefaria book when normalizeTitleKey(heTitle or enTitle)
equals the key of a blacklist line (SefariaBlacklists.kt). Nothing complains
when a line stops matching: Sefaria renames a book, or a line was typed with
different punctuation ("תלמוד מהו" vs "תלמוד מהו?"), and the book silently
comes back unless the author blacklist happens to catch it too. Ten lines had
drifted like that by October 2026.

Two title sources are compared:

* export: titles.json of the latest Otzaria/SefariaExport release. That is
  the dump the next build imports, so a line must match it.
* live: sefaria.org/api/index. A rename shows up here first, and the export
  follows days later. In October 2026 "Learning to Read Midrash" was
  "לפשוטו של מדרש" live and still "ללמוד לקרוא מדרש" in the export.

books_blacklist_titles.tsv (next to this script) maps every line to the
English title it blocked when it last matched. The English title is the
book's key in Sefaria and survives Hebrew renames, so a renamed book is found
for certain even after both sources moved on.

With --fix, only certain changes are written:

* add the book's current Hebrew title (export or live) under a line that
  blocks it by an old one. The old line stays: it is harmless, and it still
  holds if Sefaria reverts.
* replace a line that matches nothing but differs from exactly one title only
  in punctuation or spacing (these never matched anything).
* drop a later line whose key duplicates an earlier one.

Anything else is reported with close-match suggestions and fails the run.
Exit codes: 0 clean (possibly after fixes), 1 lines need a decision,
2 a title source could not be fetched (nothing is written).
"""

import argparse
import difflib
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BLACKLIST = ROOT / "generator/sefariasqlite/src/jvmMain/resources/books_blacklist.txt"
LOCK = Path(__file__).resolve().parent / "books_blacklist_titles.tsv"
EXPORT_TITLES_URL = "https://github.com/Otzaria/SefariaExport/releases/latest/download/titles.json"
LIVE_INDEX_URL = "https://www.sefaria.org/api/index/"
# Both sources list ~6,600 books. A response far below that is truncated, and
# checking against it would call half the blacklist stale.
MIN_TITLES = 5000

LOCK_HEADER = (
    "# Maintained by books_blacklist_check.py: blacklist line <TAB> the Sefaria English\n"
    "# title it blocked when it last matched. Used to follow Hebrew renames.\n"
)


def normalize_key(value):
    """normalizeTitleKey from SefariaImportText.kt, character for character."""
    if value is None or not value.strip():
        return None
    for quote in ('"', "'", "׳", "״"):
        value = value.replace(quote, "")
    return re.sub(r"\s+", " ", value.lower()).replace("_", " ").strip()


def loose_key(value):
    """Letters and digits only: what a punctuation-only typo cannot change."""
    key = normalize_key(value) or ""
    return re.sub(r"[\W_]+", "", key)


def unescape(value):
    return value.replace('\\"', '"').replace("\\'", "'")


@dataclass
class Entry:
    index: int  # position in the file's line list
    title: str  # as the importer reads it (trimmed, unescaped)

    @property
    def key(self):
        return normalize_key(self.title)


def parse_blacklist(text):
    entries = []
    for i, raw in enumerate(text.split("\n")):
        line = raw.removeprefix("﻿").strip()
        if line and not line.startswith("#"):
            entries.append(Entry(i, unescape(line)))
    return entries


@dataclass
class Result:
    adds: list = field(default_factory=list)  # (entry, new title, source)
    replaces: list = field(default_factory=list)  # (entry, new title)
    duplicates: list = field(default_factory=list)  # (entry, earlier entry)
    unresolved: list = field(default_factory=list)  # (entry, [suggestions])
    obsolete: list = field(default_factory=list)  # (entry, en title)
    skipped_paths: list = field(default_factory=list)
    lock: dict = field(default_factory=dict)

    @property
    def changed(self):
        return bool(self.adds or self.replaces or self.duplicates)


def analyze(entries, export_titles, live_titles, lock):
    """export_titles / live_titles: {English title: Hebrew title}."""
    by_key = {}
    for source in (export_titles, live_titles):
        for en, he in source.items():
            for title in (en, he):
                k = normalize_key(title)
                if k:
                    by_key.setdefault(k, en)
    hebrew_titles = sorted({he for s in (export_titles, live_titles) for he in s.values() if he})
    by_loose = {}
    for he in hebrew_titles:
        by_loose.setdefault(loose_key(he), set()).add(he)

    result = Result()
    seen = {}
    covered = {e.key for e in entries if e.key}
    for entry in entries:
        if "/" in entry.title or "\\" in entry.title:
            result.skipped_paths.append(entry)
            continue
        if entry.key in seen:
            result.duplicates.append((entry, seen[entry.key]))
            continue
        seen[entry.key] = entry

        en = by_key.get(entry.key)
        if en is None and entry.title in lock and (lock[entry.title] in export_titles or lock[entry.title] in live_titles):
            en = lock[entry.title]
        if en is not None:
            result.lock[entry.title] = en
            if normalize_key(en) in covered:
                continue  # the English line blocks it under any Hebrew name
            for source_name, source in (("export", export_titles), ("live", live_titles)):
                he = source.get(en)
                if he and normalize_key(he) not in covered:
                    result.adds.append((entry, he, source_name))
                    covered.add(normalize_key(he))
            if entry.key not in by_key:
                result.obsolete.append((entry, en))
            continue

        candidates = by_loose.get(loose_key(entry.title), set())
        if len(candidates) == 1:
            new = next(iter(candidates))
            if normalize_key(new) in covered:
                result.duplicates.append((entry, seen.get(normalize_key(new), entry)))
            else:
                result.replaces.append((entry, new))
                covered.add(normalize_key(new))
                en = by_key.get(normalize_key(new))
                if en:
                    result.lock[new] = en
            continue

        result.unresolved.append((entry, suggest(entry.title, hebrew_titles)))
    return result


def suggest(title, hebrew_titles, limit=3):
    target = loose_key(title)
    loose = {}
    for he in hebrew_titles:
        loose.setdefault(loose_key(he), he)
    picks = [loose[k] for k in difflib.get_close_matches(target, list(loose), n=limit, cutoff=0.6)]
    if len(target) >= 6:
        for k, he in loose.items():
            if len(k) >= 6 and (k in target or target in k) and he not in picks:
                picks.append(he)
    return picks[:limit]


def apply_fixes(text, result):
    lines = text.split("\n")
    drop = {entry.index for entry, _ in result.duplicates}
    replace = {entry.index: new for entry, new in result.replaces}
    after = {}
    for entry, new, _ in result.adds:
        after.setdefault(entry.index, []).append(new)
    out = []
    for i, line in enumerate(lines):
        if i in drop:
            continue
        out.append(replace.get(i, line))
        out.extend(after.get(i, []))
    return "\n".join(out)


def render_lock(entries_text, lock):
    rows = [f"{e.title}\t{lock[e.title]}" for e in parse_blacklist(entries_text) if e.title in lock]
    return LOCK_HEADER + "".join(row + "\n" for row in rows)


def read_lock(path):
    lock = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line and not line.startswith("#") and "\t" in line:
                he, en = line.split("\t", 1)
                lock[he] = en
    return lock


def fetch_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "seforim-library-ci/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_live_titles():
    titles = {}

    def walk(node):
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            if "contents" in node:
                walk(node["contents"])
            elif node.get("title"):
                titles[node["title"]] = node.get("heTitle") or ""

    walk(fetch_json(LIVE_INDEX_URL))
    return titles


def render_report(result, export_count, live_count):
    out = [f"Checked against {export_count} export titles and {live_count} live Sefaria titles.", ""]
    if result.adds or result.replaces or result.duplicates:
        out += ["### Fixed", ""]
        for entry, new, source in result.adds:
            out.append(f"- added `{new}` under `{entry.title}` (the book's current title, {source})")
        for entry, new in result.replaces:
            out.append(f"- `{entry.title}` → `{new}` (punctuation/spacing only)")
        for entry, earlier in result.duplicates:
            out.append(f"- removed duplicate `{entry.title}` (same key as `{earlier.title}`)")
        out.append("")
    if result.unresolved:
        out += ["### Needs a decision", "",
                "These lines match no book in the export or on Sefaria, so they block nothing.", "",
                "| line | suggestions |", "| --- | --- |"]
        for entry, picks in result.unresolved:
            shown = "<br>".join(f"`{p}`" for p in picks) or "no close title"
            out.append(f"| `{entry.title}` (line {entry.index + 1}) | {shown} |")
        out.append("")
    if result.obsolete:
        out += ["### Old names kept", ""]
        for entry, en in result.obsolete:
            out.append(f"- `{entry.title}` no longer matches; the book ({en}) is blocked by its current title")
        out.append("")
    if result.skipped_paths:
        out.append(f"{len(result.skipped_paths)} path line(s) not checked.")
    if not (result.changed or result.unresolved):
        out.append("All lines match a book.")
    return "\n".join(out).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fix", action="store_true", help="write the certain fixes and the lock file")
    parser.add_argument("--report", type=Path, help="write the Markdown report here")
    args = parser.parse_args()

    try:
        export_titles = fetch_json(EXPORT_TITLES_URL)
        live_titles = fetch_live_titles()
    except Exception as e:  # noqa: BLE001 - any network/decoding failure means: do not judge
        print(f"::error::could not fetch a title source: {e}")
        return 2
    if not isinstance(export_titles, dict) or len(export_titles) < MIN_TITLES or len(live_titles) < MIN_TITLES:
        print(f"::error::title source looks truncated (export {len(export_titles)}, live {len(live_titles)})")
        return 2

    text = BLACKLIST.read_text(encoding="utf-8")
    old_lock = read_lock(LOCK)
    result = analyze(parse_blacklist(text), export_titles, live_titles, old_lock)
    report = render_report(result, len(export_titles), len(live_titles))
    print(report)
    if args.report:
        args.report.write_text(report, encoding="utf-8")

    changed = False
    if args.fix:
        new_text = apply_fixes(text, result) if result.changed else text
        lock = dict(old_lock)
        lock.update(result.lock)
        new_lock = render_lock(new_text, lock)
        if new_text != text:
            BLACKLIST.write_text(new_text, encoding="utf-8")
            changed = True
        if not LOCK.exists() or LOCK.read_text(encoding="utf-8") != new_lock:
            LOCK.write_text(new_lock, encoding="utf-8")
            changed = True

    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"changed={'true' if changed else 'false'}\n")
            f.write(f"unresolved={len(result.unresolved)}\n")
    return 1 if result.unresolved else 0


if __name__ == "__main__":
    sys.exit(main())
