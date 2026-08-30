package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SefariaLinkVisibilityContractTest {
    @Test
    fun `Kotlin mask names match the cross-repo fixture`() {
        val body = javaClass.getResourceAsStream("/link_visibility_contract_v1.json")
            ?.readBytes()?.toString(Charsets.UTF_8)
            ?: error("missing link_visibility_contract_v1.json")
        val root = Json.parseToJsonElement(body).jsonObject
        assertEquals(1, root.getValue("schemaVersion").jsonPrimitive.int)
        assertEquals(
            LINK_VISIBILITY_MASK_NAMES,
            root.getValue("maskBits").jsonObject.mapValues { it.value.jsonPrimitive.content },
        )
    }

    @Test
    fun `metadata counts must match all observed CSV masks`() {
        val expected = LinkVisibilityMetadataContract(
            suppressedBySide = mapOf(1 to 0L, 2 to 0L),
            reasonCounts = (1..2).flatMap { side ->
                LINK_VISIBILITY_MASK_NAMES.keys.map { bit -> (side to bit.toInt()) to 0L }
            }.toMap(),
        )
        val observed = LinkVisibilityObservedCounts().also { it.record(mask1 = 1, mask2 = 0) }

        assertFailsWith<IllegalArgumentException> {
            expected.requireMatches(observed.snapshot())
        }
    }
}
