package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ManualLinksConfigTest {
    @Test
    fun heTitleAliasesAreAnExplicitOneToOneBridge() {
        val cases = listOf(
            // The field itself is mandatory.
            "",
            """{"root/links":{"רשי על שבת":"רשי על שבת"}}""",
            """{"root/links":{"רשי על שבת":"רש\"י על שבת","רשי על עירובין":"רש\"י על שבת"}}""",
            """{"root/links":{"רשי על שבת":"רש\"י על שבת","רש\"י על שבת":"רשבם"}}""",
            """{"root/links":{"רשי על שבת":" רש\"י על שבת "}}""",
            """{"root/links":{"a/b":"רש\"י על שבת"}}""",
            """{"root/links":[]}""",
            """{"other/links":{"רשי על שבת":"רש\"י על שבת"}}""",
            // A non-string Sefaria side is rejected exactly as in manual_links_packaging.py.
            """{"root/links":{"רשי על שבת":7}}""",
            """{"root/links":{"רשי על שבת":null}}""",
            """{"root/links":{"רשי על שבת":["רשבם"]}}""",
        )

        cases.forEachIndexed { index, aliases ->
            val path = Files.createTempFile("invalid-aliases-$index", ".json")
            Files.writeString(path, config(if (aliases.isEmpty()) "" else ""","he_title_aliases":$aliases"""))
            assertFailsWith<Exception>("invalid alias case $index must fail") { ManualLinksConfig.read(path) }
        }

        val valid = Files.createTempFile("valid-aliases", ".json")
        Files.writeString(valid, config(""","he_title_aliases":{"root/links":{"רשי על שבת":"רש\"י על שבת"}}"""))
        assertEquals(
            mapOf("root/links" to mapOf("רשי על שבת" to "רש\"י על שבת")),
            ManualLinksConfig.read(valid).heTitleAliases,
        )
    }

    private fun config(aliases: String): String =
        """{"schema_version":1,"seforim_tool_ref":"refs/heads/test","links_roots":[{"path":"root/links","expected_state":"present"}],"bootstrap_adapters":{}$aliases,"bootstrap_file_renames":[],"bootstrap_record_overrides":[]}"""
}
