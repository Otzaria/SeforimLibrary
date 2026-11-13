package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SefariaImageCompactorTest {
    private val bound = SefariaImageCompactor.MAX_SIDE

    /** Seeded noise: incompressible, so a downscale really is smaller. */
    private fun noise(width: Int, height: Int, type: Int, alphaAt: (Int) -> Int = { 0xFF }): BufferedImage {
        val random = Random(42)
        return BufferedImage(width, height, type).apply {
            for (y in 0 until height) for (x in 0 until width) {
                setRGB(x, y, (alphaAt(x) shl 24) or (random.nextInt() and 0xFFFFFF))
            }
        }
    }

    private fun encode(image: BufferedImage, format: String): ByteArray =
        ByteArrayOutputStream().use {
            check(ImageIO.write(image, format, it))
            it.toByteArray()
        }

    private fun decode(bytes: ByteArray): BufferedImage = ImageIO.read(ByteArrayInputStream(bytes))

    @Test
    fun largeImageIsDownscaledWithinTheBoundKeepingItsAspectRatio() {
        val original = encode(noise(bound * 2, bound, BufferedImage.TYPE_INT_RGB), "png")

        val result = SefariaImageCompactor.compact("image/png", original)

        val image = decode(result.bytes)
        assertEquals(bound, image.width)
        assertEquals(bound / 2, image.height)
        assertTrue(result.bytes.size < original.size)
    }

    @Test
    fun portraitImageIsBoundedByItsHeight() {
        val original = encode(noise(1001, 2002, BufferedImage.TYPE_BYTE_GRAY), "png")

        val image = decode(SefariaImageCompactor.compact("image/png", original).bytes)

        assertEquals(bound, image.height)
        assertEquals(800, image.width, "1001 × 1600 / 2002, rounded")
    }

    @Test
    fun imageWithinTheBoundIsKeptAsIs() {
        val original = encode(noise(bound, bound / 2, BufferedImage.TYPE_INT_RGB), "png")

        val result = SefariaImageCompactor.compact("image/png", original)

        assertSame(original, result.bytes)
        assertEquals("image/png", result.mime)
    }

    @Test
    fun transparencyIsKeptAsPng() {
        val original = encode(
            noise(bound * 2, bound, BufferedImage.TYPE_INT_ARGB) { x -> if (x < bound) 0 else 0x80 },
            "png",
        )

        val result = SefariaImageCompactor.compact("image/png", original)

        assertEquals("image/png", result.mime)
        val image = decode(result.bytes)
        assertTrue(image.colorModel.hasAlpha())
        assertEquals(0, image.getRGB(10, 10) ushr 24, "transparent half")
        assertEquals(0x80, image.getRGB(bound - 10, 10) ushr 24, "translucent half")
    }

    @Test
    fun theSameInputAlwaysGivesTheSameBytes() {
        val original = encode(noise(bound + 401, bound + 7, BufferedImage.TYPE_INT_RGB), "png")

        val first = SefariaImageCompactor.compact("image/png", original)
        val second = SefariaImageCompactor.compact("image/png", original.copyOf())

        assertEquals(first.mime, second.mime)
        assertContentEquals(first.bytes, second.bytes)
    }

    @Test
    fun originalIsKeptWhenTheReencodeIsNotSmaller() {
        // 1-bit noise barely downscaled turns into 8-bit gray noise, which no encoder packs as tightly.
        val original = encode(noise(bound + 100, bound / 2, BufferedImage.TYPE_BYTE_BINARY), "png")

        val result = SefariaImageCompactor.compact("image/png", original)

        assertSame(original, result.bytes)
        assertEquals("image/png", result.mime)
    }

    @Test
    fun gifSvgAndUndecodableBytesAreKept() {
        val gif = encode(noise(bound * 2, bound, BufferedImage.TYPE_BYTE_INDEXED), "gif")
        val svg = """<svg xmlns="http://www.w3.org/2000/svg" width="5000" height="5000"/>""".toByteArray()
        val garbage = byteArrayOf(1, 2, 3)

        assertSame(gif, SefariaImageCompactor.compact("image/gif", gif).bytes)
        assertSame(svg, SefariaImageCompactor.compact("image/svg+xml", svg).bytes)
        assertSame(garbage, SefariaImageCompactor.compact("image/png", garbage).bytes)
    }
}
