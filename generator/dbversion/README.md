# DB Version Module

This module writes version metadata to the SQLite database and performs integrity checks after DB generation completes.

## Overview

The **DB Versioning Agent** is responsible for:
- ✅ Writing `PRAGMA user_version` (schema version)
- ✅ Writing `PRAGMA application_id` (optional)
- ✅ Creating/updating `db_meta` table with version information
- ✅ Verifying all written values within a transaction
- ✅ Running integrity checks (`PRAGMA integrity_check`, `PRAGMA foreign_key_check`)
- ✅ Generating JSON report with results

## Usage

### Standalone execution
```bash
# Auto-increment from backup or start at 1
./gradlew :dbversion:writeDbVersion -PschemaVersion=1

# Manually specify version
./gradlew :dbversion:writeDbVersion -PschemaVersion=1 -PcontentVersion=5
```

### With optional parameters
```bash
./gradlew :dbversion:writeDbVersion \
  -PschemaVersion=1 \
  -PcontentVersion=10 \
  -PbuildId=abc123def456 \
  -PappId=0x5346524D
```

### Integrated in pipeline (auto-increment)
```bash
# Content version will auto-increment from backup
./gradlew generateSeforimDb -PschemaVersion=1
```

## Required Properties

- **`-PschemaVersion`**: Schema version (integer >= 1)

## Optional Properties

- **`-PcontentVersion`**: Content version counter (integer >= 1). If not provided, automatically increments from backup (`build/seforim.db.bak`) or starts at 1 if no backup exists.
- **`-PbuildId`**: Build identifier (defaults to `GITHUB_SHA`, `BUILD_ID` env vars, or `manual-{timestamp}`)
- **`-PappId`**: Application ID in hex format (e.g., `0x5346524D`). Leave unset to not write application_id.
- **`-PmanifestPath`**: Override the manifest output path (defaults next to the DB file).

## Database Path

The module always operates on `build/seforim.db` at the project root.

## Versioning Format

### Schema Version
- Written to `PRAGMA user_version`
- Integer value representing the database schema version
- Must be >= 1
- Also stored in `db_meta` table as `schema_version_int`

### Content Version
- Stored as two values:
  - `content_version_int`: Integer counter (1, 2, 3, ...)
  - `content_version_str`: Display string in format `v{counter}-{date}` (e.g., `v1-2026-02-05`)
- **Auto-increment logic**:
  - If `-PcontentVersion` is provided: use that value
  - If `build/seforim.db.bak` exists: read its version and increment by 1
  - Otherwise: start at version 1

### Build ID
- Unique identifier for this build
- Stored in `db_meta` table as `build_id`
- Sources (in order of preference):
  1. `-PbuildId` property
  2. `GITHUB_SHA` environment variable
  3. `BUILD_ID` environment variable
  4. `manual-{timestamp}` (format: `manual-20260205143022`)

## db_meta Table

The module creates and populates the following keys in the `db_meta` table:

### Mandatory keys
- `content_version_int`: Content version counter (e.g., "1", "2")
- `content_version_str`: Display version string (e.g., "v1-2026-02-05")
- `build_id`: Build identifier

### Optional/Generated keys
- `created_at_utc`: UTC timestamp of metadata write (e.g., "2026-02-05 14:30:22")
- `generator_agent`: Always "DB Versioning Agent"
- `schema_version_int`: Copy of schema version for convenience
- `db_uuid`: Unique database UUID (generated once on first write, persisted thereafter)

## Output

The module outputs a JSON report to stdout with the following structure:

```json
{
  "dbPath": "/path/to/build/seforim.db",
  "schemaVersionInt": 1,
  "contentVersionInt": 1,
  "contentVersionStr": "v1-2026-02-05",
  "buildId": "manual-20260205143022",
  "applicationIdHex": null,
  "dbUuid": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "integrityCheck": "ok",
  "foreignKeyCheckRows": 0,
  "status": "ok"
}
```

Additionally, a **manifest file** (`build/seforim-manifest.json`) is created/updated with version tracking:

```json
{
  "latest_schema": 1,
  "latest_content": 20260205,
  "patches": []
}
```

### Manifest File

The manifest file tracks the latest versions and provides a foundation for future patch management:

- **`latest_schema`**: Current schema version (from `PRAGMA user_version`)
- **`latest_content`**: Content version in YYYYMMDD format (e.g., 20260205)
- **`patches`**: Array of patch entries for incremental updates (auto-appended when content advances)

When a manifest already exists and `latest_content` changes, a new patch entry is appended with
`file` set to `patch_{from}_to_{to}.bin` and `sha256` set to `pending` as placeholders.

The manifest is updated automatically each time the versioning agent runs successfully.

## Integrity Checks

After committing the transaction, the module runs:

1. **`PRAGMA integrity_check`**: Verifies database structure integrity
   - Expected result: `"ok"`
   - Any other result indicates corruption

2. **`PRAGMA foreign_key_check`**: Checks for foreign key violations
   - Expected result: 0 rows
   - Non-zero count indicates referential integrity violations

If either check fails, the module:
- Prints a warning message
- Exits with status code 1
- Does NOT rollback the transaction (metadata is already committed)

## Transaction Behavior

All metadata writes are performed in a single transaction:
1. Begin transaction
2. Write PRAGMA statements
3. Create/update `db_meta` table
4. Verify all written values
5. Commit transaction
6. Run integrity checks (outside transaction)

If any error occurs during steps 1-4, the transaction is automatically rolled back and no changes are persisted.

## Error Handling

The module fails with exit code 1 and detailed error messages in the following cases:

- Missing required properties (`-PschemaVersion` or `-PcontentVersion`)
- Invalid property values (non-integer or < 1)
- Database file not found or cannot be opened
- Transaction failure (automatic rollback)
- Verification mismatch (written vs read values)
- Integrity check failure
- Foreign key violations detected

All errors include:
- Error type and description
- SQL statement that failed (if applicable)
- Full stack trace (logged but not in JSON report)

## Examples

### Example 1: First version (no backup)
```bash
./gradlew :dbversion:writeDbVersion -PschemaVersion=1
```

Result in `db_meta`:
```
content_version_int  = "1"   # No backup found, starting at 1
content_version_str  = "v1-2026-02-05"
build_id             = "manual-20260205143022"
schema_version_int   = "1"
created_at_utc       = "2026-02-05 14:30:22"
generator_agent      = "DB Versioning Agent"
db_uuid              = "a1b2c3d4..."
```

### Example 2: Incremental update (backup exists with version 3)
```bash
# Backup has content_version_int = "3"
./gradlew :dbversion:writeDbVersion -PschemaVersion=1
```

Result:
```
content_version_int  = "4"   # Auto-incremented from backup (3 + 1)
content_version_str  = "v4-2026-02-05"
build_id             = "manual-20260205150000"
db_uuid              = "a1b2c3d4..."  # Same UUID (preserved from backup if DB copied)
```

### Example 3: Manual version override
```bash
./gradlew :dbversion:writeDbVersion \
  -PschemaVersion=1 \
  -PcontentVersion=10 \
  -PbuildId=gh-actions-123
```

Result:
```
content_version_int  = "10"  # Manually specified (ignores backup)
content_version_str  = "v10-2026-02-05"
build_id             = "gh-actions-123"
```

## Integration with Pipeline

In [build.gradle.kts](../../build.gradle.kts), the versioning task is integrated as:

```kotlin
tasks.register("generateSeforimDb") {
    // ...
    dependsOn(":dbversion:writeDbVersion")
}

project(":dbversion").tasks.matching { it.name == "writeDbVersion" }.configureEach {
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
}

project(":catalog").tasks.matching { it.name == "buildCatalog" }.configureEach {
    mustRunAfter(":dbversion:writeDbVersion")
}
```

This ensures:
1. DB is fully populated (Sefaria + Otzaria + links)
2. Version metadata is written
3. Catalog and indexes are built from versioned DB

## Querying Version Information

### From command line (sqlite3)
```bash
# Schema version
sqlite3 build/seforim.db "PRAGMA user_version;"

# Application ID
sqlite3 build/seforim.db "PRAGMA application_id;"

# All metadata
sqlite3 build/seforim.db "SELECT * FROM db_meta ORDER BY key;"

# Specific version
sqlite3 build/seforim.db "SELECT value FROM db_meta WHERE key='content_version_str';"
```

### From application code
```kotlin
val driver = JdbcSqliteDriver("jdbc:sqlite:build/seforim.db")

// Schema version
val schemaVersion = driver.executeQuery(null, "PRAGMA user_version", { cursor ->
    if (cursor.next().value) QueryResult.Value(cursor.getLong(0)?.toInt())
    else QueryResult.Value(null)
}, 0).value

// Content version string
val contentVersion = driver.executeQuery(null,
    "SELECT value FROM db_meta WHERE key='content_version_str'",
    { cursor ->
        if (cursor.next().value) QueryResult.Value(cursor.getString(0))
        else QueryResult.Value(null)
    }, 0).value
```

## Troubleshooting

### Task not found
If you see `Task 'writeDbVersion' not found`, ensure:
1. Module is registered in [settings.gradle.kts](../../settings.gradle.kts)
2. Run `./gradlew tasks --all` to verify

### Missing properties error
```
Missing required property: -PschemaVersion
```
Solution: Always provide both `-PschemaVersion` and `-PcontentVersion`

### Integrity check failed
```
❌ DB integrity check failed!
Integrity check result: database disk image is malformed
```
This indicates database corruption. Common causes:
- Disk full during write
- Interrupted transaction
- Concurrent access without WAL mode
- Hardware/storage issues

Solution: Regenerate database from scratch

### Foreign key violations
```
❌ DB integrity check failed!
Foreign key violations: 3
```
This indicates referential integrity issues. Enable verbose logging to see details:
```bash
SEFORIMAPP_REPOSITORY_LOGGING=true ./gradlew :dbversion:writeDbVersion -PschemaVersion=1 -PcontentVersion=1
```
