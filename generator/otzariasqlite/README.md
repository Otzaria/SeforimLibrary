# Otzaria → SQLite (append)

This module appends **Otzaria** content into an existing `seforim.db` that was generated from **Sefaria** (see `:sefariasqlite`).

Scope:
- ✅ Insert/append categories, books, TOC entries, lines, links.
- ✅ Enrich the DB with book acronyms (table `book_acronym`) using the **SeforimAcronymizer** database.
- ❌ Does **not** build `catalog.pb` (see `:catalog:buildCatalog`).
- ❌ Does **not** build Lucene indexes (see `:searchindex:buildLuceneIndexDefault`).

## Key tasks

### Download helpers
- `./gradlew :otzariasqlite:downloadOtzaria`: download Otzaria source into `generator/otzariasqlite/build/otzaria/source`.
- `./gradlew :otzariasqlite:downloadAcronymizer` (alias: `:otzariasqlite:downloadAcronymizerDb`): download the Acronymizer DB into `generator/otzariasqlite/build/acronymizer/acronymizer.db`.

The Acronymizer DB is used during import to populate `book_acronym` (it’s not related to Lucene indexing).

### Recommended pipeline (append into existing DB)
- `./gradlew :otzariasqlite:appendOtzaria`: wrapper that runs:
  - `:otzariasqlite:appendOtzariaLines`
  - `:otzariasqlite:appendOtzariaLinks`

Default input/output DB is `build/seforim.db` at the repo root (override with `-PseforimDb=/path/to/seforim.db`).

### Advanced / manual phases
- `./gradlew :otzariasqlite:appendOtzariaLines`: append categories/books/TOCs/lines into an existing DB.
- `./gradlew :otzariasqlite:appendOtzariaLinks`: append links (requires that lines/books exist).
- `./gradlew :otzariasqlite:generateLines`: phase 1 generation (creates/persists `build/seforim.db` by default).
- `./gradlew :otzariasqlite:generateLinks`: phase 2 link processing.

## Common properties / env vars

- DB path:
  - `-PseforimDb=/path/to/seforim.db` or env `SEFORIM_DB`
- Otzaria source dir:
  - `-PsourceDir=/path/to/otzaria` or env `OTZARIA_SOURCE_DIR` (must contain `metadata.json` and `אוצריא/`, not the `אוצריא/` folder itself)
- Acronymizer DB:
  - `-PacronymDb=/path/to/acronymizer.db` or env `ACRONYM_DB`
- In-memory mode:
  - default is in-memory for speed; set `-PinMemoryDb=false` to work directly on disk.

## Publication safety

`generateLines` and `generateLinks` publish `seforim.db` together with its
`seforim.db.buildstate` allocator snapshot. Both files are first written as
same-directory `.candidate` files and verified against each other. Their final
replacement is a journaled pair commit: if a process dies between the two
renames, the next phase restores the complete preceding pair before it reads
any allocator IDs. A commit that reached its durable marker is retained and
only its temporary backups are cleaned up.

Before the pair journal is written, SQLite seals both old and candidate members:
it verifies a WAL checkpoint, switches to `journal_mode=DELETE`, confirms no
sidecar remains, and fsyncs the main file. That can normalize SQLite's physical
bytes, but preserves the same committed logical contents and allocator identity;
the publisher never deletes a target WAL to make a rename possible. A WAL that
cannot be checkpointed causes a failure before the pair moves begin.

The publisher never falls back to delete-and-write when there is insufficient
room for a DB candidate. It fails before modifying the existing DB; free space
and retry. This can temporarily require approximately one additional DB copy
plus 12.5% and 64 MiB of headroom while a release is being replaced.
