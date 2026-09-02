package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ManualLinksTreeHashTest {
    @Test
    fun framingMatchesIndependentGoldenDigest() {
        val base = Files.createTempDirectory("manual-links-tree")
        val root = Files.createDirectories(base.resolve("r"))
        Files.writeString(root.resolve("x.json"), "{}")
        val config = config(LinksRoot("a", ExpectedState.ABSENT), LinksRoot("r", ExpectedState.PRESENT))

        val scan = ManualLinksTreeHash.scan(base, config)

        assertEquals("a1c8de6378768a32171c5ab13895bae224a95ede6f1092b5448e142ecdc8dba1", scan.sourceTreeSha256)
        assertEquals("1906874d85febfbb60fd0167cf17da50e6fdcb572ebf109a2c4842c0664b6c5a", scan.packagedTreeSha256)
    }

    @Test
    fun packagedCollisionAndUnexpectedStateFail() {
        val base = Files.createTempDirectory("manual-links-collision")
        Files.createDirectories(base.resolve("one")).also { Files.writeString(it.resolve("same.json"), "1") }
        Files.createDirectories(base.resolve("two")).also { Files.writeString(it.resolve("same.json"), "2") }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksTreeHash.scan(
                base,
                config(LinksRoot("one", ExpectedState.PRESENT), LinksRoot("two", ExpectedState.PRESENT)),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            ManualLinksTreeHash.scan(base, config(LinksRoot("one", ExpectedState.ABSENT)))
        }
    }

    private fun config(vararg roots: LinksRoot) = ManualLinksConfig(
        seforimToolRef = "refs/heads/test",
        linksRoots = roots.toList(),
        bootstrapAdapters = emptyMap(),
        bootstrapFileRenames = emptyList(),
        bootstrapRecordOverrides = emptyList(),
    )
}
