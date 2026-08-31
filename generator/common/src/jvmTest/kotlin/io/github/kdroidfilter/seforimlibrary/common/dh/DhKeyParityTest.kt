package io.github.kdroidfilter.seforimlibrary.common.dh

import io.github.kdroidfilter.seforimlibrary.core.dh.DhKey
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Pins [DhKey] to the fixtures the Otzaria client is pinned to. A failure
 * here means the two normalisations drifted apart — which would show up in
 * production as dibburim that silently stop matching what the user types.
 */
class DhKeyParityTest {

    private val fixtures = Json.parseToJsonElement(
        requireNotNull(javaClass.getResourceAsStream("/dh_key_fixtures.json")) {
            "dh_key_fixtures.json missing from test resources"
        }.bufferedReader().readText(),
    ).jsonObject

    @Test
    fun `normalised dibburim match the shared fixtures`() {
        for (entry in fixtures.getValue("normalize").jsonArray) {
            val o = entry.jsonObject
            val input = o.getValue("input").jsonPrimitive.content
            val expected = o.getValue("key").jsonPrimitive.contentOrNull
            assertEquals(expected, DhKey.normalize(input), "key for '$input'")
        }
    }
}
