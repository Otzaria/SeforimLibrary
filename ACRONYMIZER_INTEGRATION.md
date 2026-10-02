# Acronymizer Integration

This document describes how SeforimLibrary integrates with the SeforimAcronymizer database.

## Overview

The SeforimLibrary generator pipeline downloads and uses the SeforimAcronymizer database to enrich books with alternative names (acronyms) for better searchability.

## Database Structure

The SeforimAcronymizer database uses a normalized relational structure:

- **Books**: Stores unique book titles
- **Acronyms**: Stores unique acronym terms (deduplicated)
- **BookAcronyms**: Junction table linking books to their acronyms

This structure allows efficient storage (no duplication) and fast lookups.

## Integration Points

### 1. Download Phase

The `AcronymizerFetcher` automatically downloads the latest acronymizer database from GitHub releases:

```kotlin
val dbPath = AcronymizerFetcher.ensureLocalDb(logger)
// Downloads to: build/acronymizer/acronymizer.db
```

### 2. Generation Phase

During book insertion in `DatabaseGenerator.insertBookOrCategory()`, acronyms are fetched and inserted:

```kotlin
val terms = fetchAcronymsForTitle(bookTitle)
repository.bulkInsertBookAcronyms(bookId, terms)
```

### 3. Query Implementation

The `fetchAcronymsForTitle()` function queries the relational structure:

```sql
SELECT a.acronym
FROM Books b
JOIN BookAcronyms ba ON b.id = ba.book_id
JOIN Acronyms a ON ba.acronym_id = a.id
WHERE b.title = ?
ORDER BY a.acronym
```

This replaces the old CSV-based approach that used the `AcronymResults` view.

### 4. Storage in SeforimLibrary

Acronyms are stored in SeforimLibrary's own `book_acronym` table:

```sql
CREATE TABLE book_acronym (
    bookId INTEGER NOT NULL,
    term TEXT NOT NULL,
    PRIMARY KEY (bookId, term),
    FOREIGN KEY (bookId) REFERENCES book(id) ON DELETE CASCADE
);
```

### 5. Renamed Books

The Acronymizer is keyed by exact title, so a renamed book used to lose every
acronym until someone added its new title there. Once every Otzaria book is in,
`carryAcronymsAcrossRenames()` gives a book that has no acronyms, and no
Acronymizer entry under its current title, the terms filed under its old title,
plus the old title itself. An entry for the new title always wins.

The old title comes from the build state, which never drops a book key
(`RenamedBookTitles`):

- **same id** — renamed in place after import (ForDB/book_renames.csv).
- **same content** — a renamed file: a dead key whose last line hashes overlap one
  live book of the same source by Jaccard >= 0.8, and no other.

A rename is followed only when it added or dropped words, or respelled them
(honorifics and full/defective spelling ignored). One that replaces words, such as a
corrected attribution or edition, is refused, and so is an old title that is now
another book's title or that two books would inherit. The build log lists the
refused renames whose old title had acronyms. If the book really is the same, add
its new title to Otzaria/SeforimAcronymizer.

## Usage

### Generate Database with Acronyms

```bash
./gradlew :otzariasqlite:generateLines \
  -PseforimDb=/path/to/seforim.db \
  -PsourceDir=/path/to/otzaria
  # Acronymizer DB is auto-downloaded if not present
```

### Use Custom Acronymizer DB

```bash
./gradlew :otzariasqlite:generateLines \
  -PseforimDb=/path/to/seforim.db \
  -PsourceDir=/path/to/otzaria \
  -PacronymDb=/path/to/custom/acronymizer.db
```

### Download Acronymizer DB Only

```bash
./gradlew :otzariasqlite:downloadAcronymizer
```

## Benefits

1. **Automatic Updates**: Always uses the latest acronymizer database from releases
2. **Efficient Storage**: Relational structure eliminates duplication
3. **Fast Lookups**: Indexed columns enable quick book → acronyms queries
4. **Fallback-Safe**: Pipeline continues if acronymizer DB is unavailable

## Statistics

After integration (typical dataset):
- ~6,878 books
- ~27,475 unique acronyms
- Average ~4 acronyms per book
- Acronymizer DB size: ~5.9 MB

## Migration Notes

The integration was updated to use the new relational structure (Books/Acronyms/BookAcronyms) instead of the deprecated CSV-based `AcronymResults` view. The old view is maintained for backward compatibility but is no longer used by SeforimLibrary.
