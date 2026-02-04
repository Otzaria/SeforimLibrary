# SefariaDiff Generator

This module compares Sefaria books between old and new database versions and generates a detailed difference report.

## Purpose

The SefariaDiff generator serves as an intermediate step in the database generation pipeline, running after the Sefaria database is created but before Otzaria books are imported. It provides users with visibility into what has changed between database versions.

## Features

- Compares books from the "Sefaria" source between old backup database and newly generated database
- Identifies newly added books
- Identifies removed books  
- Lists unchanged books with line count comparisons
- Generates a detailed markdown report in `SeferiaDiff.txt`
- Waits for user confirmation before continuing with Otzaria import

## Usage

### As part of the main pipeline:
```bash
./gradlew generateSeforimDb
```

### Standalone execution:
```bash
./gradlew :sefariadiff:generateSefariaDiff
```

### With custom paths:
```bash
./gradlew :sefariadiff:generateSefariaDiff \
  -PnewSeforimDb=/path/to/new/seforim.db \
  -PoldSeforimDb=/path/to/old/seforim.db.bak \
  -PdiffFile=/path/to/custom/diff.txt
```

## Parameters

- `newSeforimDb`: Path to the newly generated database (default: `build/seforim.db`)
- `oldSeforimDb`: Path to the backup database (default: `build/seforim.db.bak`)
- `diffFile`: Output path for the diff report (default: `build/SeferiaDiff.txt`)

## Report Format

The generated report includes:

1. **Summary**: Total counts of books in both databases and change statistics
2. **Added Books**: List of new books with Hebrew descriptions and line counts
3. **Removed Books**: List of books no longer present
4. **Unchanged Books**: List of books present in both databases with line count changes

## Integration

The SefariaDiff step is integrated into the main pipeline and runs automatically:

1. After `:sefariasqlite:generateSefariaSqlite` completes
2. Before `:otzariasqlite:appendOtzaria` begins
3. Pauses execution for user review before continuing

## Fresh Installation

If no backup database is found, the tool recognizes this as a fresh installation and generates a simple report noting that all books are new.