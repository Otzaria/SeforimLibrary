package io.github.kdroidfilter.seforimlibrary.common.patch

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PatchSizeGuardTest {

    private val dbSize = 4_000_000_000L

    @Test
    fun `small delta publishes`() {
        val d = PatchSizeGuard.decide(patchUncompressedSize = 40_000_000L, newDbSize = dbSize)
        assertTrue(d.publish)
        assertEquals(0.01, d.ratio, 1e-9)
    }

    @Test
    fun `the v27 shape is rejected`() {
        // 3 GB of patch against a ~4 GB seforim.db — issue #1211.
        val d = PatchSizeGuard.decide(patchUncompressedSize = 3_000_000_000L, newDbSize = dbSize)
        assertFalse(d.publish)
        assertTrue(d.describe().contains("limit is 50.0%"))
    }

    @Test
    fun `exactly at the limit still publishes`() {
        val d = PatchSizeGuard.decide(patchUncompressedSize = 2_000_000_000L, newDbSize = dbSize)
        assertTrue(d.publish)
        assertEquals(PatchSizeGuard.DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO, d.maxRatio)
    }

    @Test
    fun `one byte over the limit is rejected`() {
        assertFalse(PatchSizeGuard.decide(2_000_000_001L, dbSize).publish)
    }

    @Test
    fun `the default is the server-side safety net, looser than the updater's heavy mark`() {
        assertEquals(0.5, PatchSizeGuard.DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO)
        // The updater calls 0.25 "heavy" and lets the user choose; the guard
        // only removes deltas no client should ever be handed.
        assertTrue(PatchSizeGuard.decide(1_200_000_000L, dbSize).publish)
    }

    @Test
    fun `an explicit ratio overrides the default`() {
        assertTrue(PatchSizeGuard.decide(3_000_000_000L, dbSize, maxRatio = 0.9).publish)
        assertFalse(PatchSizeGuard.decide(40_000_000L, dbSize, maxRatio = 0.001).publish)
    }

    @Test
    fun `an empty patch publishes`() {
        assertTrue(PatchSizeGuard.decide(0L, dbSize).publish)
    }

    @Test
    fun `nonsensical inputs fail loudly`() {
        assertFailsWith<IllegalArgumentException> { PatchSizeGuard.decide(1L, 0L) }
        assertFailsWith<IllegalArgumentException> { PatchSizeGuard.decide(-1L, dbSize) }
        assertFailsWith<IllegalArgumentException> { PatchSizeGuard.decide(1L, dbSize, maxRatio = 0.0) }
    }

    @Test
    fun `configured ratio falls back to the default`() {
        val previous = System.getProperty(PatchSizeGuard.RATIO_PROPERTY)
        try {
            System.clearProperty(PatchSizeGuard.RATIO_PROPERTY)
            if (System.getenv(PatchSizeGuard.RATIO_ENV) == null) {
                assertEquals(
                    PatchSizeGuard.DEFAULT_MAX_DELTA_UNCOMPRESSED_RATIO,
                    PatchSizeGuard.configuredMaxRatio(),
                )
            }
            System.setProperty(PatchSizeGuard.RATIO_PROPERTY, "0.25")
            assertEquals(0.25, PatchSizeGuard.configuredMaxRatio())
            System.setProperty(PatchSizeGuard.RATIO_PROPERTY, "not-a-number")
            assertFailsWith<IllegalStateException> { PatchSizeGuard.configuredMaxRatio() }
        } finally {
            if (previous == null) System.clearProperty(PatchSizeGuard.RATIO_PROPERTY)
            else System.setProperty(PatchSizeGuard.RATIO_PROPERTY, previous)
        }
    }
}
