package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.awt.Transparency
import java.awt.color.ColorSpace
import java.awt.image.BufferedImage
import java.awt.image.ComponentColorModel
import java.awt.image.DataBuffer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
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

    /** Gray noise around [level] (0..255), written as raw samples so no color conversion is involved. */
    private fun grayNoise(width: Int, height: Int, level: Int, bits: Int = 8, alpha: Int? = null): BufferedImage {
        val image = if (alpha == null) {
            BufferedImage(width, height, if (bits == 16) BufferedImage.TYPE_USHORT_GRAY else BufferedImage.TYPE_BYTE_GRAY)
        } else {
            val model = ComponentColorModel(
                ColorSpace.getInstance(ColorSpace.CS_GRAY), true, false, Transparency.TRANSLUCENT, DataBuffer.TYPE_BYTE,
            )
            BufferedImage(model, model.createCompatibleWritableRaster(width, height), false, null)
        }
        val max = (1 shl bits) - 1
        val random = Random(3)
        for (y in 0 until height) for (x in 0 until width) {
            image.raster.setSample(x, y, 0, (level + random.nextInt(-20, 21)) * max / 255)
            if (alpha != null) image.raster.setSample(x, y, 1, alpha)
        }
        return image
    }

    /** Mean displayed level (0..255) of the first band, read raw for gray so it matches what a browser shows. */
    private fun meanLevel(image: BufferedImage): Double {
        val gray = image.colorModel.colorSpace.type == ColorSpace.TYPE_GRAY
        val max = ((1 shl image.colorModel.getComponentSize(0)) - 1).toDouble()
        var sum = 0.0
        for (y in 0 until image.height) for (x in 0 until image.width) {
            sum += if (gray) image.raster.getSample(x, y, 0) * 255 / max else ((image.getRGB(x, y) shr 8) and 0xFF).toDouble()
        }
        return sum / (image.width * image.height)
    }

    private fun assertLevelKept(level: Int, mime: String, original: ByteArray) {
        val result = SefariaImageCompactor.compact(mime, original)
        assertNotSame(original, result.bytes, "the image must actually be re-encoded")
        val mean = meanLevel(decode(result.bytes))
        assertTrue(kotlin.math.abs(mean - level) <= 2, "mean $mean drifted from $level ($mime -> ${result.mime})")
    }

    @Test
    fun grayLevelsAreKeptForEightBitPng() =
        assertLevelKept(80, "image/png", encode(grayNoise(bound * 2, bound / 4, 80), "png"))

    @Test
    fun grayLevelsAreKeptForSixteenBitPng() =
        assertLevelKept(80, "image/png", encode(grayNoise(bound * 2, bound / 4, 80, bits = 16), "png"))

    @Test
    fun grayLevelsAreKeptForGrayJpeg() =
        assertLevelKept(80, "image/jpeg", encode(grayNoise(bound * 3, bound / 4, 80), "jpeg"))

    @Test
    fun grayLevelsAndAlphaAreKeptForGrayAlphaPng() {
        val original = encode(grayNoise(bound * 2, bound / 4, 80, alpha = 0xC0), "png")
        assertLevelKept(80, "image/png", original)
        val image = decode(SefariaImageCompactor.compact("image/png", original).bytes)
        assertEquals(0xC0, image.getRGB(5, 5) ushr 24)
    }

    /** [jpeg] with an Exif APP1 segment carrying [orientation] right after SOI. */
    private fun withExifOrientation(jpeg: ByteArray, orientation: Int): ByteArray {
        val tiff = byteArrayOf(
            'M'.code.toByte(), 'M'.code.toByte(), 0, 42, 0, 0, 0, 8, // big-endian header, IFD0 at 8
            0, 1, // one entry
            0x01, 0x12, 0, 3, 0, 0, 0, 1, 0, orientation.toByte(), 0, 0, // Orientation, SHORT, 1
            0, 0, 0, 0, // no next IFD
        )
        val payload = "Exif".toByteArray() + byteArrayOf(0, 0) + tiff
        val length = payload.size + 2
        val app1 = byteArrayOf(0xFF.toByte(), 0xE1.toByte(), (length shr 8).toByte(), length.toByte()) + payload
        return jpeg.copyOfRange(0, 2) + app1 + jpeg.copyOfRange(2, jpeg.size)
    }

    @Test
    fun rotatedExifJpegIsKeptAsIs() {
        val jpeg = encode(noise(bound * 2, bound, BufferedImage.TYPE_INT_RGB), "jpeg")
        val upright = withExifOrientation(jpeg, 1)
        val rotated = withExifOrientation(jpeg, 6)

        assertNotSame(upright, SefariaImageCompactor.compact("image/jpeg", upright).bytes, "orientation 1 is compacted")
        assertSame(rotated, SefariaImageCompactor.compact("image/jpeg", rotated).bytes)
    }
}
