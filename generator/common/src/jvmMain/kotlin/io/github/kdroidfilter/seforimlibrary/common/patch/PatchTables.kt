package io.github.kdroidfilter.seforimlibrary.common.patch

/**
 * Per-table patch configuration. One entry per seforim.db table the
 * producer/applier knows about.
 *
 *  - [name]       : table name (also used as the suffix for upsert_<name>
 *                   and delete_<name>).
 *  - [primaryKey] : ordered column list. Forms the SQLite ON CONFLICT(...)
 *                   target. For composite keys, ALL columns are part of the
 *                   delete_* table's PK.
 *  - [updatable]  : `true` if the table has non-PK columns to update on
 *                   conflict; `false` for pure junctions (PK == all cols).
 */
internal data class PatchTable(
    val name: String,
    val primaryKey: List<String>,
    val updatable: Boolean,
)

/** Current `seforim.db` schema produced by this revision. */
internal const val CURRENT_DB_SCHEMA_VERSION: Int = 5

/**
 * Canonical table order — parents (referenced) come before children
 * (referencing) for upserts. The applier runs upserts in this order and
 * deletes in reverse. The "depends on" comment on each line tracks why
 * the ordering matters.
 *
 * The schema_meta table is special-cased: keyed by a TEXT `key` column,
 * still tracked here for completeness.
 */
internal val PATCH_TABLES_IN_FK_ORDER: List<PatchTable> = listOf(
    // Lookup / atomic tables — no FK in.
    PatchTable("source",             listOf("id"),       updatable = true),
    PatchTable("author",             listOf("id"),       updatable = true),
    PatchTable("topic",              listOf("id"),       updatable = true),
    PatchTable("pub_place",          listOf("id"),       updatable = true),
    PatchTable("pub_date",           listOf("id"),       updatable = true),
    PatchTable("connection_type",    listOf("id"),       updatable = true),
    PatchTable("tocText",            listOf("id"),       updatable = true),
    PatchTable("generation",         listOf("id"),       updatable = true),

    // Self-ref tree — categories. parentId FK is self → same table, OK.
    PatchTable("category",           listOf("id"),       updatable = true),
    PatchTable("category_closure",   listOf("ancestorId", "descendantId"), updatable = false),

    // Book — depends on category + source.
    PatchTable("book",               listOf("id"),       updatable = true),

    // Book-attribute junctions — depend on book + author/topic/pubPlace/pubDate/generation.
    PatchTable("book_author",        listOf("bookId", "authorId"),       updatable = false),
    PatchTable("book_base_text",     listOf("bookId", "baseBookId"),     updatable = false),
    PatchTable("book_topic",         listOf("bookId", "topicId"),        updatable = false),
    PatchTable("book_pub_place",     listOf("bookId", "pubPlaceId"),     updatable = false),
    PatchTable("book_pub_date",      listOf("bookId", "pubDateId"),      updatable = false),
    PatchTable("book_acronym",       listOf("bookId", "term"),           updatable = false),
    PatchTable("book_generation",    listOf("bookId", "generationId"),   updatable = false),

    // TOC. tocEntry FK to line (lineId) and line FK to tocEntry (tocEntryId)
    // form a cycle, broken at apply time with PRAGMA defer_foreign_keys = ON.
    PatchTable("tocEntry",           listOf("id"),       updatable = true),
    PatchTable("line",               listOf("id"),       updatable = true),
    PatchTable("line_toc",           listOf("lineId"),   updatable = true),
    // Schema 4. Canonical line-reference index — pure key table (PK == all
    // columns), so there is nothing to update on conflict.
    PatchTable("line_ref",           listOf("bookId", "refKeyHash", "lineIndex"), updatable = false),
    // Schema 5. Dibbur-hamatchil index — dhDisplay rides along the key.
    PatchTable("line_dh",            listOf("bookId", "dhText", "lineIndex"), updatable = true),

    // Links.
    PatchTable("link",               listOf("id"),       updatable = true),
    PatchTable("link_anchor",        listOf("linkId", "side", "charStart"), updatable = true),
    PatchTable("link_range",         listOf("linkId", "side"), updatable = true),
    PatchTable("link_coverage",      listOf("lineId", "linkId", "side"), updatable = false),
    // Schema 3. reasonMask can change while the composite key stays stable.
    PatchTable("link_suppressed_side", listOf("linkId", "side"), updatable = true),
    PatchTable("book_has_links",     listOf("bookId"),   updatable = true),

    // Book editions — book_version depends on book, version_line on it + line.
    PatchTable("book_version",       listOf("id"),       updatable = true),
    PatchTable("version_line",       listOf("versionId", "lineId"), updatable = true),

    // Alternative TOCs.
    PatchTable("alt_toc_structure",  listOf("id"),       updatable = true),
    PatchTable("alt_toc_entry",      listOf("id"),       updatable = true),
    PatchTable("line_alt_toc",       listOf("lineId", "structureId"), updatable = true),

    // Defaults (book → book references).
    PatchTable("default_commentator", listOf("bookId", "commentatorBookId"), updatable = true),
    PatchTable("default_targum",      listOf("bookId", "targumBookId"),      updatable = true),

    // Versioning. Keyed by a string `key` column.
    PatchTable("schema_meta",        listOf("key"),      updatable = true),
)

/** Schema-4 contract shipped in v26, before line_dh gained dhDisplay. */
internal val PATCH_TABLES_SCHEMA_4: List<PatchTable> =
    PATCH_TABLES_IN_FK_ORDER.map { table ->
        if (table.name == "line_dh") table.copy(updatable = false) else table
    }

/** Schema-3 contract retained for updater compatibility tests. */
internal val PATCH_TABLES_SCHEMA_3: List<PatchTable> =
    PATCH_TABLES_SCHEMA_4.filterNot { it.name in setOf("line_ref", "line_dh") }

/** Schema-2 contract retained for updater compatibility tests. */
internal val PATCH_TABLES_SCHEMA_2: List<PatchTable> =
    PATCH_TABLES_SCHEMA_3.filterNot { it.name == "link_suppressed_side" }

/** Schema-1 contract predates the book_base_text junction. */
internal val PATCH_TABLES_SCHEMA_1: List<PatchTable> =
    PATCH_TABLES_SCHEMA_2.filterNot { it.name == "book_base_text" }

/** Exact patch-table contract used to produce a database schema version. */
internal fun patchTablesForSchemaVersion(schemaVersion: Int): List<PatchTable> = when (schemaVersion) {
    1 -> PATCH_TABLES_SCHEMA_1
    2 -> PATCH_TABLES_SCHEMA_2
    3 -> PATCH_TABLES_SCHEMA_3
    4 -> PATCH_TABLES_SCHEMA_4
    5 -> PATCH_TABLES_IN_FK_ORDER
    else -> error("Unsupported patch-table schema version $schemaVersion")
}
