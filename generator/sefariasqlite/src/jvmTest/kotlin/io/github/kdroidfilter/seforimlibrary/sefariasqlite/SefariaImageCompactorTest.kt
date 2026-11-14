package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.awt.Transparency
import java.awt.color.ColorSpace
import java.awt.color.ICC_Profile
import java.awt.image.BufferedImage
import java.awt.image.ComponentColorModel
import java.awt.image.DataBuffer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Locale
import java.util.zip.CRC32
import java.util.zip.DeflaterOutputStream
import javax.imageio.ImageReadParam
import javax.imageio.ImageReader
import javax.imageio.ImageIO
import javax.imageio.ImageTypeSpecifier
import javax.imageio.metadata.IIOMetadata
import javax.imageio.spi.IIORegistry
import javax.imageio.spi.ImageReaderSpi
import javax.imageio.stream.ImageInputStream
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

    private fun pngChunk(type: String, data: ByteArray): ByteArray = ByteArrayOutputStream().use { out ->
        val tag = type.toByteArray(Charsets.US_ASCII)
        DataOutputStream(out).apply {
            writeInt(data.size)
            write(tag)
            write(data)
            writeInt(CRC32().apply { update(tag); update(data) }.value.toInt())
        }
        out.toByteArray()
    }

    /** Insert complete chunks after IHDR, preserving the encoded pixels and all CRCs. */
    private fun withPngChunks(png: ByteArray, vararg chunks: ByteArray): ByteArray =
        png.copyOfRange(0, 33) + chunks.fold(byteArrayOf()) { out, chunk -> out + chunk } +
            png.copyOfRange(33, png.size)

    private fun tiffOrientation(orientation: Int, order: ByteOrder): ByteArray =
        ByteBuffer.allocate(26).order(order).apply {
            putShort(if (order == ByteOrder.BIG_ENDIAN) 0x4D4D.toShort() else 0x4949.toShort())
            putShort(42)
            putInt(8)
            putShort(1)
            putShort(0x0112)
            putShort(3) // SHORT
            putInt(1)
            putShort(orientation.toShort())
            putShort(0)
            putInt(0)
        }.array()

    private fun assertPngKept(original: ByteArray) {
        assertEquals(SefariaImageValidation.Valid("image/png"), SefariaImageValidation.validate(original))
        val result = SefariaImageCompactor.compact("image/png", original)
        assertSame(original, result.bytes, "pixels and display metadata must stay together")
        assertEquals("image/png", result.mime)
    }

    @Test
    fun pngWithExplicitGammaKeepsItsOriginalDisplayLevels() {
        // These raw levels display near 152 with gAMA=1.0, but near 80 if the
        // metadata is stripped. ImageIO's raw-sample gray tests cannot detect that.
        val png = encode(grayNoise(bound * 2, bound / 8, 80), "png")
        val gamma = pngChunk("gAMA", ByteBuffer.allocate(4).putInt(100_000).array())
        assertPngKept(withPngChunks(png, gamma))
    }

    @Test
    fun pngWithColorProfileOrChromaticitiesIsKept() {
        val png = encode(noise(bound * 2, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        val profile = ByteArrayOutputStream().use { out ->
            DeflaterOutputStream(out).use { it.write(ICC_Profile.getInstance(ColorSpace.CS_LINEAR_RGB).data) }
            out.toByteArray()
        }
        val iccp = pngChunk("iCCP", "linear RGB\u0000\u0000".toByteArray(Charsets.US_ASCII) + profile)
        val chromaticities = ByteBuffer.allocate(32).apply {
            listOf(31270, 32900, 64000, 33000, 30000, 60000, 15000, 6000).forEach { putInt(it) }
        }.array()
        assertPngKept(withPngChunks(png, iccp))
        assertPngKept(withPngChunks(png, pngChunk("cHRM", chromaticities)))
    }

    @Test
    fun rotatedAndMirroredExifPngIsKeptInEitherByteOrder() {
        val png = encode(noise(bound * 2, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        for (order in listOf(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)) {
            for (orientation in listOf(2, 6, 8)) {
                assertPngKept(withPngChunks(png, pngChunk("eXIf", tiffOrientation(orientation, order))))
            }
            val upright = withPngChunks(png, pngChunk("eXIf", tiffOrientation(1, order)))
            assertNotSame(upright, SefariaImageCompactor.compact("image/png", upright).bytes)
        }
    }

    @Test
    fun unreadablePngExifIsNotDiscarded() {
        val png = encode(noise(bound * 2, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        val tiff = tiffOrientation(6, ByteOrder.BIG_ENDIAN)
        assertPngKept(withPngChunks(png, pngChunk("eXIf", tiff.copyOf(18))))
        val invalidOffset = tiff.copyOf().apply { ByteBuffer.wrap(this).putInt(4, Int.MAX_VALUE) }
        assertPngKept(withPngChunks(png, pngChunk("eXIf", invalidOffset)))
    }

    private fun pngImageData(png: ByteArray): ByteArray = ByteArrayOutputStream().use { out ->
        val input = ByteBuffer.wrap(png).position(8)
        while (input.remaining() >= 12) {
            val length = input.int
            if (input.int == 0x49444154) out.write(png, input.position(), length)
            input.position(input.position() + length + 4)
        }
        out.toByteArray()
    }

    private fun frameControl(sequence: Int, width: Int, height: Int): ByteArray =
        pngChunk("fcTL", ByteBuffer.allocate(26).apply {
            putInt(sequence)
            putInt(width)
            putInt(height)
            putInt(0)
            putInt(0)
            putShort(1) // delay numerator
            putShort(10) // delay denominator
            put(0) // dispose: none
            put(0) // blend: source
        }.array())

    @Test
    fun animatedPngWithIdatInTextBeforeActlKeepsBothFrames() {
        val first = grayNoise(bound * 2, bound / 8, 80)
        val second = grayNoise(first.width, first.height, 180)
        val png = encode(first, "png")
        val text = pngChunk("tEXt", "Description\u0000IDAT is the image data chunk".toByteArray())
        val actl = pngChunk("acTL", ByteBuffer.allocate(8).putInt(2).putInt(0).array())
        val secondData = ByteBuffer.allocate(4).putInt(2).array() + pngImageData(encode(second, "png"))
        val animated = png.copyOfRange(0, 33) + text + actl +
            frameControl(0, first.width, first.height) + pngChunk("IDAT", pngImageData(png)) +
            frameControl(1, first.width, first.height) + pngChunk("fdAT", secondData) +
            pngChunk("IEND", byteArrayOf())

        // The old substring search misses acTL and re-encodes just the first frame.
        assertPngKept(animated)
    }

    @Test
    fun chunkNamesInsidePngTextDoNotPreventCompaction() {
        val png = encode(noise(bound * 2, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        val text = pngChunk("tEXt", "Description\u0000acTL precedes IDAT; eXIf gAMA iCCP are optional".toByteArray())
        val original = withPngChunks(png, text)
        assertEquals(SefariaImageValidation.Valid("image/png"), SefariaImageValidation.validate(original))
        assertNotSame(original, SefariaImageCompactor.compact("image/png", original).bytes)
    }

    @Test
    fun smallImageOnlyReadsDimensionsWhileLargeImageIsDecodedAndReadersAreDisposed() {
        val small = encode(noise(bound, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        val large = encode(noise(bound * 2, bound / 8, BufferedImage.TYPE_INT_RGB), "png")
        val registry = IIORegistry.getDefaultInstance()
        val providers = registry.getServiceProviders(ImageReaderSpi::class.java, true).asSequence().toList()
        val delegate = ImageIO.getImageReadersByFormatName("png").next().let { reader ->
            reader.originatingProvider.also { reader.dispose() }
        }
        val probe = CountingPngReaderSpi(delegate)
        registry.registerServiceProvider(probe)
        try {
            providers.forEach { registry.setOrdering(ImageReaderSpi::class.java, probe, it) }

            assertSame(small, SefariaImageCompactor.compact("image/png", small).bytes)
            assertEquals(0, probe.reads, "an unchanged small image must not allocate or decode a raster")
            assertEquals(1, probe.disposals)

            assertNotSame(large, SefariaImageCompactor.compact("image/png", large).bytes)
            assertEquals(1, probe.reads, "large images still need one compactor decode")
            assertEquals(2, probe.disposals)
        } finally {
            registry.deregisterServiceProvider(probe)
        }
    }

    private class CountingPngReaderSpi(private val delegate: ImageReaderSpi) : ImageReaderSpi(
        "SeforimLibrary", "1", arrayOf("png"), arrayOf("png"), arrayOf("image/png"),
        CountingPngReader::class.java.name, arrayOf(ImageInputStream::class.java), null,
        false, null, null, null, null, false, null, null, null, null,
    ) {
        var reads = 0
        var disposals = 0

        override fun canDecodeInput(source: Any?): Boolean = delegate.canDecodeInput(source)
        override fun createReaderInstance(extension: Any?): ImageReader = CountingPngReader(delegate.createReaderInstance(), this)
        override fun getDescription(locale: Locale?): String = "PNG reader that counts raster decodes"
    }

    private class CountingPngReader(private val delegate: ImageReader, private val probe: CountingPngReaderSpi) : ImageReader(probe) {
        override fun setInput(input: Any?, seekForwardOnly: Boolean, ignoreMetadata: Boolean) {
            super.setInput(input, seekForwardOnly, ignoreMetadata)
            delegate.setInput(input, seekForwardOnly, ignoreMetadata)
        }

        override fun getNumImages(allowSearch: Boolean): Int = delegate.getNumImages(allowSearch)
        override fun getWidth(imageIndex: Int): Int = delegate.getWidth(imageIndex)
        override fun getHeight(imageIndex: Int): Int = delegate.getHeight(imageIndex)
        override fun getImageTypes(imageIndex: Int): MutableIterator<ImageTypeSpecifier> = delegate.getImageTypes(imageIndex)
        override fun getStreamMetadata(): IIOMetadata? = delegate.streamMetadata
        override fun getImageMetadata(imageIndex: Int): IIOMetadata? = delegate.getImageMetadata(imageIndex)

        override fun read(imageIndex: Int, param: ImageReadParam?): BufferedImage {
            probe.reads++
            return delegate.read(imageIndex, param)
        }

        override fun dispose() {
            probe.disposals++
            delegate.dispose()
            super.dispose()
        }
    }
}
