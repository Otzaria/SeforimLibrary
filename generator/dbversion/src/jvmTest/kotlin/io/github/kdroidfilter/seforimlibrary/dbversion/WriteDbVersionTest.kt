package io.github.kdroidfilter.seforimlibrary.dbversion

import co.touchlab.kermit.Logger
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class WriteDbVersionTest {

    private val logger = Logger.withTag("WriteDbVersionTest")

    @Test
    fun `manifest uses exact content version without date conversion`() {
        val tempDir = createTempDirectory("dbversion-test").toFile()
        val manifestPath = tempDir.resolve("seforim-manifest.json").absolutePath

        updateManifest(
            manifestPath = manifestPath,
            schemaVersion = 1,
            contentVersion = 1,
            logger = logger
        )

        val manifest = readManifest(manifestPath, logger)
        assertNotNull(manifest)
        assertEquals(1, manifest.latest_content)
        assertEquals(1, manifest.latest_schema)
        assertEquals(0, manifest.patches.size)
    }

    @Test
    fun `manifest appends patch for sequential same-day style releases`() {
        val tempDir = createTempDirectory("dbversion-test").toFile()
        val manifestPath = tempDir.resolve("seforim-manifest.json").absolutePath

        updateManifest(
            manifestPath = manifestPath,
            schemaVersion = 1,
            contentVersion = 1,
            logger = logger
        )
        updateManifest(
            manifestPath = manifestPath,
            schemaVersion = 1,
            contentVersion = 2,
            logger = logger
        )

        val manifest = readManifest(manifestPath, logger)
        assertNotNull(manifest)
        assertEquals(2, manifest.latest_content)
        assertEquals(1, manifest.patches.size)
        assertEquals(1, manifest.patches[0].from)
        assertEquals(2, manifest.patches[0].to)
        assertEquals("patch_1_to_2.bin", manifest.patches[0].file)
    }
}

