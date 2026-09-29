#!/usr/bin/env python3
"""Announce what a published Seforim DB release changed, book by book.

The announcement compares the new release with the previous *published* one
(pre-releases never reach users, so they are skipped), and describes the two
databases themselves — Otzaria and Sefaria books alike — rather than any
source repository's file tree.

  previous-tag --current TAG     tag names on stdin -> the previous published tag
  catalog DB OUT                 seforim.db -> a small per-book JSON catalog
  announce --old A --new B ...   diff two catalogs, post to forum / Chat / Yemot
"""
import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Jerusalem")
TAG_RE = re.compile(r"^v([1-9][0-9]*)-[0-9]{14}$")
CATALOG_SCHEMA = 1
FORUM_URL = "https://otzaria.org/forum"
FORUM_TOPIC_ID = 20
# NodeBB's default maximumPostLength is 32767; keep headroom for the header.
FORUM_MAX_CHARS = 30000
CHANNELS = ("forum", "chat", "yemot")
YEMOT_PATH = "ivr2:/1"
TZINTUK_LIST = "books update"

SECTIONS = (
    ("added", "התווספו הספרים הבאים:"),
    ("removed", "נמחקו הספרים הבאים:"),
    ("moved", "שונה מיקום/שם של הספרים הבאים:"),
    ("changed", "השתנו הספרים הבאים:"),
)


def db_version(tag):
    match = TAG_RE.fullmatch(tag)
    return int(match.group(1)) if match else None


def previous_published_tag(current, tags):
    """The newest published tag strictly older than `current`, or None."""
    current_version = db_version(current)
    if current_version is None:
        raise ValueError(f"{current!r} is not a v<N>-<timestamp> release tag")
    older = [(v, t) for t in tags if (v := db_version(t)) is not None and v < current_version]
    return max(older)[1] if older else None


def category_paths(conn):
    rows = {cid: (parent, title) for cid, parent, title in conn.execute("SELECT id, parentId, title FROM category")}
    paths = {}
    for cid in rows:
        parts, seen, node = [], set(), cid
        # The library root is its own parent, so a plain parent walk never ends.
        while node in rows and node not in seen:
            seen.add(node)
            parent, title = rows[node]
            parts.append(title)
            node = parent
        paths[cid] = "/".join(reversed(parts))
    return paths


def build_catalog(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        paths = category_paths(conn)
        sources = dict(conn.execute("SELECT id, name FROM source"))
        books = {}
        for book_id, title, category_id, source_id in conn.execute(
                "SELECT id, title, categoryId, sourceId FROM book"):
            books[book_id] = {"id": book_id, "title": title, "path": paths.get(category_id, ""),
                              "source": sources.get(source_id, ""), "lines": 0, "hash": None}
        digest, current = None, None

        def flush():
            if current in books:
                books[current]["hash"] = digest.hexdigest()

        for book_id, content in conn.execute("SELECT bookId, content FROM line ORDER BY bookId, lineIndex"):
            if book_id != current:
                if current is not None:
                    flush()
                current, digest = book_id, hashlib.sha256()
            digest.update(content.encode("utf-8"))
            digest.update(b"\0")
            if book_id in books:
                books[book_id]["lines"] += 1
        if current is not None:
            flush()
        return {"schema": CATALOG_SCHEMA, "books": sorted(books.values(), key=lambda b: b["id"])}
    finally:
        conn.close()


def display_name(book):
    return f"{book['path']}/{book['title']}" if book["path"] else book["title"]


def diff_catalogs(old, new):
    old_books = {b["id"]: b for b in old["books"]}
    new_books = {b["id"]: b for b in new["books"]}
    added = [new_books[i] for i in new_books.keys() - old_books.keys()]
    removed = [old_books[i] for i in old_books.keys() - new_books.keys()]

    # A book's id is keyed by its title, so a rename surfaces as remove + add of identical text.
    removed_by_hash = {}
    for book in removed:
        if book["lines"]:
            removed_by_hash.setdefault(book["hash"], []).append(book)
    moved, still_added = [], []
    for book in sorted(added, key=lambda b: b["id"]):
        twins = removed_by_hash.get(book["hash"]) if book["lines"] else None
        if twins:
            moved.append((twins.pop(0), book))
        else:
            still_added.append(book)
    paired = {id(old_book) for old_book, _ in moved}
    still_removed = [b for b in removed if id(b) not in paired]

    changed = []
    for book_id in old_books.keys() & new_books.keys():
        before, after = old_books[book_id], new_books[book_id]
        if display_name(before) != display_name(after):
            moved.append((before, after))
        if before["hash"] != after["hash"]:
            changed.append(after)

    def names(books):
        return sorted(display_name(b) for b in books)

    return {
        "added": names(still_added),
        "removed": names(still_removed),
        "moved": sorted(f"{display_name(a)} (לשעבר: {display_name(b)})" for b, a in moved),
        "changed": names(changed),
    }


def heb_date(day=None):
    day = day or datetime.now(tz=TZ).date()
    try:
        from pyluach import dates
        return dates.HebrewDate.from_pydate(day).hebrew_date_string()
    except Exception:
        return day.isoformat()


def build_forum_post(changes, version, date_text, limit=FORUM_MAX_CHARS):
    text = f"# גירסת ספרייה {version}\n\n**עדכון {date_text}**\n"
    for key, heading in SECTIONS:
        items = changes[key]
        if not items:
            continue
        text += f"\n## {heading}\n"
        for index, item in enumerate(items):
            line = f"* {item}\n"
            if len(text) + len(line) > limit:
                text += f"* ועוד {len(items) - index} ספרים\n"
                break
            text += line
    return text


def build_yemot_content(changes):
    return {heading: "\n".join(item.rsplit("/", 1)[-1] for item in changes[key])
            for key, heading in SECTIONS if changes[key]}


class ForumRefused(RuntimeError):
    # NodeBB's own error keys stay untranslated, so a locale change cannot hide the rate limit.
    RATE_LIMIT_MARKERS = ("ניתן לפרסם פוסט רק פעם ב", "too-many-posts", "still-posting")

    @property
    def retryable(self):
        return any(marker in str(self) for marker in self.RATE_LIMIT_MARKERS)


def post_to_forum(text, token, session=None):
    """One reply through NodeBB's Write API; raises ForumRefused if the forum refused it."""
    import requests
    response = (session or requests).post(
        f"{FORUM_URL}/api/v3/topics/{FORUM_TOPIC_ID}",
        json={"content": text},
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    try:
        status = response.json().get("status") or {}
    except ValueError:
        raise ForumRefused(f"http-{response.status_code}: {response.text[:200]!r}") from None
    if status.get("code") != "ok":
        raise ForumRefused(f"{status.get('code') or response.status_code}: {status.get('message', '')}")


def send_forum(text, token, attempts=4, delay=5.0, post=post_to_forum):
    for attempt in range(1, attempts + 1):
        try:
            post(text, token)
            return
        except ForumRefused as exc:
            # Only the post-rate refusal is retried: after a transport error the
            # post may already exist, and a duplicate is worse than a miss.
            if not exc.retryable or attempt == attempts:
                raise
            time.sleep(delay * attempt)


def send_chat(text, url):
    import requests
    response = requests.post(url, json={"text": text}, timeout=30)
    response.raise_for_status()


def send_yemot(content, date_text, token):
    from yemot import split_and_send
    split_and_send(content, f"עדכון {date_text}\n", token, YEMOT_PATH, TZINTUK_LIST)


def announce(args):
    old = json.loads(Path(args.old).read_text(encoding="utf-8"))
    new = json.loads(Path(args.new).read_text(encoding="utf-8"))
    for catalog in (old, new):
        if catalog.get("schema") != CATALOG_SCHEMA:
            raise SystemExit(f"::error::unsupported catalog schema {catalog.get('schema')!r}")
    version = db_version(args.tag)
    if version is None:
        raise SystemExit(f"::error::{args.tag!r} is not a v<N>-<timestamp> release tag")

    changes = diff_catalogs(old, new)
    print("changes: " + " ".join(f"{k}={len(v)}" for k, v in changes.items()))
    if not any(changes.values()):
        print("⏭️  No book changed since the previous published release — nothing to announce.")
        return 0

    day = date.fromisoformat(args.as_of) if args.as_of else None
    date_text = heb_date(day)
    post = build_forum_post(changes, version, date_text)
    print("----- announcement -----")
    print(post)
    print("------------------------")

    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    unknown = set(channels) - set(CHANNELS)
    if unknown:
        raise SystemExit(f"::error::unknown channel(s): {', '.join(sorted(unknown))}")
    if os.getenv("ANNOUNCE", "false").lower() != "true":
        print(f"⏭️  ANNOUNCE is not 'true' — dry run, nothing sent to {', '.join(channels)}.")
        return 0

    senders = {
        "forum": lambda: send_forum(post, os.environ["FORUM_TOKEN"]),
        "chat": lambda: send_chat(post, os.environ["GOOGLE_CHAT_URL"]),
        "yemot": lambda: send_yemot(build_yemot_content(changes), date_text, os.environ["TOKEN_YEMOT"]),
    }
    failed = []
    for channel in channels:
        try:
            senders[channel]()
        except Exception as exc:
            failed.append(channel)
            print(f"::error::{channel} announcement failed: {exc!r}")
        else:
            print(f"✅ {channel} announced")
    return 1 if failed else 0


def main(argv=None):
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    previous = commands.add_parser("previous-tag")
    previous.add_argument("--current", required=True)
    catalog = commands.add_parser("catalog")
    catalog.add_argument("db")
    catalog.add_argument("out")
    post = commands.add_parser("announce")
    post.add_argument("--old", required=True)
    post.add_argument("--new", required=True)
    post.add_argument("--tag", required=True)
    post.add_argument("--as-of", default="")
    post.add_argument("--channels", default=",".join(CHANNELS))
    args = parser.parse_args(argv)

    if args.command == "previous-tag":
        tag = previous_published_tag(args.current, sys.stdin.read().split())
        print(tag or "")
        return 0
    if args.command == "catalog":
        value = build_catalog(args.db)
        with open(args.out, "w", encoding="utf-8") as out:
            json.dump(value, out, ensure_ascii=False, separators=(",", ":"))
        print(f"catalog: {len(value['books'])} books -> {args.out}")
        return 0
    return announce(args)


if __name__ == "__main__":
    sys.exit(main())
