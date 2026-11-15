package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaTurSeifMarkersReviewedMappingsTest {
    @Test
    fun `resource follows the reviewed Shulchan Arukh topics including relevant Rema clauses`() {
        val loader = SefariaTurSeifMarkers::class.java.classLoader
        val cases = requireNotNull(loader.getResourceAsStream("tur_seifim_review_cases.json")).use { stream ->
            Json.parseToJsonElement(stream.bufferedReader(Charsets.UTF_8).readText())
                .jsonObject.getValue("cases").jsonArray
        }
        val points = SefariaTurSeifMarkers.loadPoints(loader)
        for (element in cases) {
            val case = element.jsonObject
            val prefix = case.getValue("turRefPrefix").jsonPrimitive.content
            val seif = case.getValue("seif").jsonPrimitive.content
            val actual = points.filterKeys { it.startsWith(prefix) }.flatMap { (ref, entries) ->
                entries.filter { it.seif == seif }.map { ref to it.context }
            }
            val expected = case.getValue("expected").let { value ->
                if (value == JsonNull) emptyList() else {
                    val point = value.jsonObject
                    listOf(point.getValue("ref").jsonPrimitive.content to point.getValue("context").jsonPrimitive.content)
                }
            }
            assertEquals(
                expected,
                actual,
                "${case.getValue("sourceRef").jsonPrimitive.content}: ${case.getValue("reason").jsonPrimitive.content}",
            )
        }
    }
}
