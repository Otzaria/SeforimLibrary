package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.SuppressionReason
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.atomic.LongAdder

/** Validates the authoritative sidecar before schema-3 link visibility is imported. */
internal fun validateLinkVisibilityMetadata(path: Path): LinkVisibilityMetadataContract {
    require(Files.isRegularFile(path)) {
        "Missing required Sefaria link visibility metadata at $path"
    }
    val root = runCatching {
        Json.parseToJsonElement(Files.readString(path)).jsonObject
    }.getOrElse { error("Invalid Sefaria link visibility metadata at $path: ${it.message}") }

    require(root.requiredInt("schema_version") == 1) {
        "Unsupported link visibility metadata schema at $path"
    }
    require(root.requiredString("sefaria_project_sha").matches(Regex("[0-9a-f]{40}"))) {
        "Invalid or missing sefaria_project_sha in $path"
    }

    val expectedBits = LINK_VISIBILITY_MASK_NAMES
    val actualBits = root["mask_bits"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content }
    require(actualBits == expectedBits) { "Unexpected suppression mask contract in $path: $actualBits" }
    require(SuppressionReason.ALL == expectedBits.keys.sumOf { it.toInt() }) {
        "Kotlin suppression bits drifted from link visibility metadata contract"
    }

    val counts = root["counts"]?.jsonObject ?: error("Missing counts in $path")
    validateRefSet(root, counts, path, "perek_refs")
    validateRefSet(root, counts, path, "parasha_refs")
    val suppressedBySide = (1..2).associateWith { side ->
        counts.requiredInt("suppressed_side_$side").also {
            require(it >= 0) {
                "Negative suppressed_side_$side in $path"
            }
        }
    }
    val bySideAndBit = counts["suppressed_by_side_and_bit"]?.jsonObject
        ?: error("Missing suppressed_by_side_and_bit in $path")
    val reasonCounts = LinkedHashMap<Pair<Int, Int>, Long>()
    for (side in 1..2) {
        val byBit = bySideAndBit[side.toString()]?.jsonObject
            ?: error("Missing visibility counts for side $side in $path")
        require(byBit.keys == expectedBits.values.toSet()) {
            "Unexpected visibility reason counts for side $side in $path"
        }
        require(byBit.values.all { it.jsonPrimitive.intOrNull?.let { count -> count >= 0 } == true }) {
            "Invalid visibility reason count for side $side in $path"
        }
        for ((bit, name) in expectedBits) {
            reasonCounts[side to bit.toInt()] = byBit.getValue(name).jsonPrimitive.intOrNull!!.toLong()
        }
    }
    return LinkVisibilityMetadataContract(
        suppressedBySide = suppressedBySide.mapValues { it.value.toLong() },
        reasonCounts = reasonCounts,
    )
}

internal val LINK_VISIBILITY_MASK_NAMES = mapOf(
    "1" to "anchor_not_segment_level",
    "2" to "other_side_too_coarse",
    "4" to "whole_talmud_perek",
    "8" to "whole_parasha",
)
private val LINK_VISIBILITY_BITS = LINK_VISIBILITY_MASK_NAMES.keys.map(String::toInt).toIntArray()

internal data class LinkVisibilityMetadataContract(
    val suppressedBySide: Map<Int, Long>,
    val reasonCounts: Map<Pair<Int, Int>, Long>,
) {
    fun requireMatches(observed: LinkVisibilityObservedCounts.Snapshot) {
        require(suppressedBySide == observed.suppressedBySide) {
            "Link visibility suppressed-side counts differ: metadata=$suppressedBySide CSV=${observed.suppressedBySide}"
        }
        require(reasonCounts == observed.reasonCounts) {
            "Link visibility reason counts differ: metadata=$reasonCounts CSV=${observed.reasonCounts}"
        }
    }
}

internal class LinkVisibilityObservedCounts {
    private val suppressed = Array(2) { LongAdder() }
    private val byReason = Array(2) { Array(LINK_VISIBILITY_BITS.size) { LongAdder() } }

    fun record(mask1: Int, mask2: Int) {
        recordSide(0, mask1)
        recordSide(1, mask2)
    }

    private fun recordSide(sideIndex: Int, mask: Int) {
        if (mask != 0) suppressed[sideIndex].increment()
        for (bitIndex in LINK_VISIBILITY_BITS.indices) {
            if (mask and LINK_VISIBILITY_BITS[bitIndex] != 0) {
                byReason[sideIndex][bitIndex].increment()
            }
        }
    }

    fun snapshot() = Snapshot(
        suppressedBySide = (1..2).associateWith { suppressed[it - 1].sum() },
        reasonCounts = buildMap {
            for (sideIndex in 0..1) {
                for (bitIndex in LINK_VISIBILITY_BITS.indices) {
                    put(
                        (sideIndex + 1) to LINK_VISIBILITY_BITS[bitIndex],
                        byReason[sideIndex][bitIndex].sum(),
                    )
                }
            }
        },
    )

    data class Snapshot(
        val suppressedBySide: Map<Int, Long>,
        val reasonCounts: Map<Pair<Int, Int>, Long>,
    )
}

private fun validateRefSet(root: JsonObject, counts: JsonObject, path: Path, key: String) {
    val array: JsonArray = root[key]?.jsonArray ?: error("Missing $key in $path")
    val refs = array.map { it.jsonPrimitive.content }
    require(refs.isNotEmpty()) { "$key is empty in $path" }
    require(refs == refs.sorted() && refs.size == refs.distinct().size) {
        "$key must be sorted and unique in $path"
    }
    require(counts.requiredInt(key) == refs.size) {
        "$key count does not match its array in $path"
    }
    val expectedDigest = root.requiredString("${key}_sha256")
    val actualDigest = MessageDigest.getInstance("SHA-256")
        .digest(refs.joinToString("\n").toByteArray())
        .joinToString("") { "%02x".format(it) }
    require(expectedDigest == actualDigest) { "$key digest mismatch in $path" }
}

private fun JsonObject.requiredString(key: String): String =
    this[key]?.jsonPrimitive?.content ?: error("Missing $key")

private fun JsonObject.requiredInt(key: String): Int =
    this[key]?.jsonPrimitive?.intOrNull ?: error("Missing or invalid integer $key")
