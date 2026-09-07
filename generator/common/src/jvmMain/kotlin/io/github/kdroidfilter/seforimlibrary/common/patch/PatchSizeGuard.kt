package io.github.kdroidfilter.seforimlibrary.common.patch

/**
 * Hard safety net against shipping a pathological delta (Otzaria issue #1211):
 * clients that cannot offer the user a choice would otherwise spend far longer
 * applying indexed upserts than downloading the full DB.
 *
 * Deliberately looser than the Otzaria updater's own 0.25 "heavy delta" mark —
 * that one only labels the route so the user can still prefer the smaller
 * download on a slow link; this one removes the route altogether.
 */
object PatchSizeGuard {

    const val DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO: Double = 0.5

    /** First token of the `<out>.unpatchable` marker written when this guard fires. */
    const val MARKER_REASON_TOKEN: String = "oversized-delta"

    /** System property / env var that overrides [DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO]. */
    const val RATIO_PROPERTY: String = "maxDeltaUncompressedRatio"
    const val RATIO_ENV: String = "MAX_DELTA_UNCOMPRESSED_RATIO"

    data class Decision(
        val publish: Boolean,
        val patchUncompressedSize: Long,
        val newDbSize: Long,
        val ratio: Double,
        val maxRatio: Double,
    ) {
        fun describe(): String =
            "patch.db ${patchUncompressedSize} B is ${"%.1f".format(ratio * 100)}% of the new seforim.db " +
                "($newDbSize B); limit is ${"%.1f".format(maxRatio * 100)}%"
    }

    fun decide(
        patchUncompressedSize: Long,
        newDbSize: Long,
        maxRatio: Double = DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO,
    ): Decision {
        require(maxRatio > 0.0) { "maxDeltaUncompressedRatio must be positive, got $maxRatio" }
        require(newDbSize > 0L) { "new seforim.db size must be positive, got $newDbSize" }
        require(patchUncompressedSize >= 0L) { "patch size must not be negative, got $patchUncompressedSize" }
        val ratio = patchUncompressedSize.toDouble() / newDbSize.toDouble()
        return Decision(
            publish = ratio <= maxRatio,
            patchUncompressedSize = patchUncompressedSize,
            newDbSize = newDbSize,
            ratio = ratio,
            maxRatio = maxRatio,
        )
    }

    /** Reads the configured limit: `-D`/`-P` first, then env, else the default. */
    fun configuredMaxRatio(): Double {
        val raw = System.getProperty(RATIO_PROPERTY) ?: System.getenv(RATIO_ENV)
        if (raw == null) return DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO
        return raw.toDoubleOrNull() ?: error("$RATIO_PROPERTY='$raw' is not a number")
    }
}
