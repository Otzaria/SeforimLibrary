package io.github.kdroidfilter.seforimlibrary.common.refs

import io.github.kdroidfilter.seforimlibrary.core.refs.RefKey
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Pins [RefKey] to the fixture that the companion Otzaria implementation must
 * copy and pass. A failure here means the database normalisation changed and
 * the client fixture/implementation must be updated before release.
 */
class RefKeyParityTest {

    private val fixtures = Json.parseToJsonElement(
        requireNotNull(javaClass.getResourceAsStream("/ref_key_fixtures.json")) {
            "ref_key_fixtures.json missing from test resources"
        }.bufferedReader().readText(),
    ).jsonObject

    @Test
    fun `refKeys match the shared fixtures`() {
        for (entry in fixtures.getValue("refKeys").jsonArray) {
            val o = entry.jsonObject
            val input = o.getValue("input").jsonPrimitive.content
            val expected = o.getValue("key").jsonPrimitive.contentOrNull
            val key = RefKey.of(input)
            assertEquals(expected, key, "key for '$input'")
            if (key != null) {
                assertEquals(o.getValue("hash").jsonPrimitive.long, RefKey.hash(key), "hash for '$input'")
            }
        }
    }

    @Test
    fun `line keys match the shared fixtures`() {
        for (entry in fixtures.getValue("lineKeys").jsonArray) {
            val o = entry.jsonObject
            val heRef = o.getValue("heRef").jsonPrimitive.content
            val aliases = o.getValue("aliases").jsonArray.map { it.jsonPrimitive.content }
            val expected = o.getValue("key").jsonPrimitive.contentOrNull
            val key = RefKey.ofLine(heRef, aliases)
            assertEquals(expected, key, "line key for '$heRef'")
            if (key != null) {
                assertEquals(o.getValue("hash").jsonPrimitive.long, RefKey.hash(key), "hash for '$heRef'")
            }
        }
    }

    @Test
    fun `partial line keys match the shared fixtures`() {
        for (entry in fixtures.getValue("partialLineKeys").jsonArray) {
            val o = entry.jsonObject
            val heRef = o.getValue("heRef").jsonPrimitive.content
            val aliases = o.getValue("aliases").jsonArray.map { it.jsonPrimitive.content }
            val expected = o.getValue("keys").jsonArray.map { it.jsonObject }
            val keys = RefKey.partialLineKeys(heRef, aliases)
            assertEquals(expected.map { it.getValue("key").jsonPrimitive.content }, keys, "partial keys for '$heRef'")
            assertEquals(
                expected.map { it.getValue("hash").jsonPrimitive.long },
                keys.map(RefKey::hash),
                "partial hashes for '$heRef'",
            )
        }
    }

    @Test
    fun `partial ref keys match the shared fixtures`() {
        for (entry in fixtures.getValue("partialRefKeys").jsonArray) {
            val o = entry.jsonObject
            val input = o.getValue("input").jsonPrimitive.content
            val key = RefKey.partialOf(input)
            assertEquals(o.getValue("key").jsonPrimitive.content, key, "partial key for '$input'")
            assertEquals(o.getValue("hash").jsonPrimitive.long, RefKey.hash(key!!), "hash for '$input'")
        }
    }

    @Test
    fun `a heading line gets no key`() {
        assertNull(RefKey.ofLine("ישעיהו", listOf("ישעיהו")))
    }

    @Test
    fun `line token matching prefers the longest alias regardless of order`() {
        val heRef = RefKey.tokens("בית חדש ג")
        val short = RefKey.tokens("בית")
        val long = RefKey.tokens("בית חדש")

        assertEquals("ג", RefKey.ofLineTokens(heRef, listOf(short, long)))
        assertEquals("ג", RefKey.ofLineTokens(heRef, listOf(long, short)))
    }
}
