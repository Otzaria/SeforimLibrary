package io.github.kdroidfilter.seforimlibrary.common.ids

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals

class LineNaturalKeyHashTest {

    @Test
    fun `hash ignores heRef entirely`() {
        // The v27 regression (issue #1211): a heRef-only edit must not move the key.
        val content = "בְּרֵאשִׁית בָּרָא אֱלֹהִים"
        assertEquals(
            IdAllocatorBindings.lineNaturalKeyHash(content).toList(),
            IdAllocatorBindings.lineNaturalKeyHash(content).toList(),
        )
        assertNotEquals(
            LegacyLineKey.hash(content, "בראשית א׳:א׳").toList(),
            LegacyLineKey.hash(content, "בראשית, א׳:א׳").toList(),
            "the legacy key was heRef-sensitive — that is what this change removes",
        )
    }

    @Test
    fun `distinct content produces distinct hash`() {
        assertNotEquals(
            IdAllocatorBindings.lineNaturalKeyHash("<h1>בראשית</h1>").toList(),
            IdAllocatorBindings.lineNaturalKeyHash("<h2>פרק א</h2>").toList(),
        )
    }

    @Test
    fun `hash is always 20 bytes`() {
        assertEquals(20, IdAllocatorBindings.lineNaturalKeyHash("x").size)
        assertEquals(20, IdAllocatorBindings.lineNaturalKeyHash("").size)
        assertEquals(20, LegacyLineKey.hash("x", "y").size)
    }

    @Test
    fun `heading lines keep the pre-change key`() {
        // Lines without an heRef were already keyed on "CT:<content>"; keeping that
        // shape means the change churns only the ref-bearing lines.
        assertEquals(
            LegacyLineKey.hash("<h1>בראשית</h1>", null).toList(),
            IdAllocatorBindings.lineNaturalKeyHash("<h1>בראשית</h1>").toList(),
        )
    }

    @Test
    fun `namespaced apart from the Otzaria raw content hash`() {
        val a = IdAllocatorBindings.lineNaturalKeyHash("<p>שלום</p>")
        val raw = IdAllocatorBindings.normalisedContentHash("<p>שלום</p>")
        assertFalse(a.toList() == raw.toList(), "new hash must stay namespaced (CT: prefix)")
    }
}
