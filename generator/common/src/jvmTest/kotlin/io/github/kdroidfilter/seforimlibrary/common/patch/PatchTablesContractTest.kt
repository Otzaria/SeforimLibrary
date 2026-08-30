package io.github.kdroidfilter.seforimlibrary.common.patch

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Contract guard against the Dart `seforim_library_updater` package.
 *
 * Serializes [PATCH_TABLES_IN_FK_ORDER], [LogicalContentHasher.DEFAULT_TABLES]
 * and [PatchDbSchema.CURRENT_VERSION] to a canonical JSON form and compares it
 * to a committed fixture. The identical fixture lives in the updater repo
 * (`test/patch_tables_contract.json`), where its own test asserts the Dart
 * lists produce byte-identical output — so the two table specs cannot drift.
 *
 * Canonical rules: fixed key order, FK/hash order preserved (no sorting),
 * UTF-8, trailing newline.
 */
class PatchTablesContractTest {

    @Test
    fun `schema 2 contracts remain frozen when schema 3 adds visibility`() {
        assertEquals(
            PATCH_TABLES_IN_FK_ORDER.filterNot { it.name == "link_suppressed_side" },
            PATCH_TABLES_SCHEMA_2,
        )
        assertEquals(
            listOf(
                "source", "author", "topic", "pub_place", "pub_date", "connection_type", "generation",
                "category", "category_closure", "tocText", "book", "book_topic", "book_author",
                "book_base_text", "book_pub_place", "book_pub_date", "book_generation", "tocEntry", "line",
                "line_toc", "link", "link_anchor", "link_range", "link_coverage", "book_has_links",
                "book_version", "version_line", "book_acronym", "alt_toc_structure", "alt_toc_entry",
                "line_alt_toc", "default_commentator", "default_targum", "schema_meta",
            ),
            LogicalContentHasher.TABLES_SCHEMA_2,
        )
        assertEquals(
            LogicalContentHasher.TABLES_SCHEMA_2.toMutableList().apply {
                add(indexOf("link_coverage") + 1, "link_suppressed_side")
            },
            LogicalContentHasher.TABLES_SCHEMA_3,
        )
    }

    private fun canonicalContract(
        fkOrder: List<PatchTable>,
        hashOrder: List<String>,
        schemaVersion: Int,
    ): String {
        val b = StringBuilder()
        b.append("{\n")
        b.append("  \"schemaVersion\": ").append(schemaVersion).append(",\n")
        b.append("  \"fkOrder\": [\n")
        for ((i, t) in fkOrder.withIndex()) {
            val pk = t.primaryKey.joinToString(", ") { "\"$it\"" }
            b.append("    { \"table\": \"").append(t.name)
                .append("\", \"pk\": [").append(pk)
                .append("], \"updatable\": ").append(t.updatable).append(" }")
            if (i != fkOrder.lastIndex) b.append(",")
            b.append("\n")
        }
        b.append("  ],\n")
        b.append("  \"hashOrder\": [\n")
        for ((i, name) in hashOrder.withIndex()) {
            b.append("    \"").append(name).append("\"")
            if (i != hashOrder.lastIndex) b.append(",")
            b.append("\n")
        }
        b.append("  ]\n")
        b.append("}\n")
        return b.toString()
    }

    @Test
    fun `canonical serialization matches committed fixture`() {
        val expected = javaClass.getResourceAsStream("/patch_tables_contract.json")
            ?.readBytes()?.toString(Charsets.UTF_8)
            ?: error("fixture patch_tables_contract.json missing from test resources")
        val actual = canonicalContract(
            PATCH_TABLES_IN_FK_ORDER,
            LogicalContentHasher.DEFAULT_TABLES,
            PatchDbSchema.CURRENT_VERSION,
        )
        assertEquals(expected, actual)
    }
}
