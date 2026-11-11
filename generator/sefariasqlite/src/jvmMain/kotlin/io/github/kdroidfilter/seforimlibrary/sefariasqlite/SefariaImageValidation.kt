package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.Semaphore
import java.util.zip.CRC32
import javax.imageio.ImageIO
import javax.imageio.stream.MemoryCacheImageInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamException

internal sealed interface SefariaImageValidation {
    data class Valid(val mime: String) : SefariaImageValidation
    data class Invalid(val reason: String) : SefariaImageValidation

    companion object {
        private const val MAX_IMAGE_PIXELS = 25_000_000L
        private const val NETFREE_PLACEHOLDER = "content-filter-placeholder (NetFree image)"
        private const val NETFREE_PIXEL_SHA256 = "d558a2c923ff1d58e205a5c4724eccce5b02796b4bb7e2e9f50ebe0e24638622"

        // Bound decompression concurrency independently of network I/O.
        private val rasterDecoders = Semaphore(4)
        private val hex = HexFormat.of()

        fun validate(bytes: ByteArray): SefariaImageValidation {
            if (bytes.isEmpty()) return Invalid("empty")
            if (bytes.size > SefariaImageEmbedder.MAX_IMAGE_BYTES) {
                return Invalid("oversized (> ${SefariaImageEmbedder.MAX_IMAGE_BYTES} bytes)")
            }

            val head = String(bytes, 0, minOf(bytes.size, 512), Charsets.UTF_8).removePrefix("\uFEFF").trimStart()
            if (head.startsWith("<!doctype html", ignoreCase = true) || head.startsWith("<html", ignoreCase = true)) {
                return Invalid("html-instead-of-image")
            }
            if (head.startsWith('<') || bytes.startsWith(0xFF, 0xFE) || bytes.startsWith(0xFE, 0xFF)) {
                return validateSvg(bytes)
            }

            val format = when {
                bytes.startsWith(0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A) -> Raster.PNG
                bytes.startsWith(0xFF, 0xD8) -> Raster.JPEG
                head.startsWith("GIF87a") || head.startsWith("GIF89a") -> Raster.GIF
                bytes.size >= 12 && head.startsWith("RIFF") && String(bytes, 8, 4, Charsets.US_ASCII) == "WEBP" ->
                    Raster.WEBP
                else -> return Invalid("invalid-or-unsupported-image")
            }

            if (format == Raster.PNG && sha256(bytes) == SefariaImageEmbedder.FILTER_PLACEHOLDER_SHA256) {
                return Invalid(NETFREE_PLACEHOLDER)
            }
            val complete = when (format) {
                Raster.PNG -> hasCompletePngChunks(bytes)
                Raster.JPEG -> bytes.size >= 4 && bytes[bytes.lastIndex - 1] == 0xFF.toByte() && bytes.last() == 0xD9.toByte()
                Raster.GIF -> bytes.size >= 14 && bytes.last() == ';'.code.toByte()
                Raster.WEBP -> ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(4) == bytes.size - 8
            }
            if (!complete) return Invalid("incomplete-or-corrupt-image")
            if (canvasPixels(bytes, format) > MAX_IMAGE_PIXELS) return Invalid("too-many-image-pixels")

            rasterDecoders.acquire()
            return try {
                validateRaster(bytes, format)
            } catch (_: IOException) {
                Invalid("invalid-or-unsupported-image")
            } catch (_: RuntimeException) {
                Invalid("invalid-or-unsupported-image")
            } finally {
                rasterDecoders.release()
            }
        }

        private enum class Raster(val readerName: String, val mime: String) {
            PNG("PNG", "image/png"),
            JPEG("JPEG", "image/jpeg"),
            GIF("GIF", "image/gif"),
            WEBP("WebP", "image/webp"),
        }

        // Animated GIF and WebP readers report frame dimensions. The logical
        // canvas may be much larger than any of the individual frames.
        private fun canvasPixels(bytes: ByteArray, format: Raster): Long = when {
            format == Raster.GIF -> {
                val header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
                header.getShort(6).toUShort().toLong() * header.getShort(8).toUShort().toLong()
            }
            format == Raster.WEBP && bytes.size >= 30 && String(bytes, 12, 4, Charsets.US_ASCII) == "VP8X" ->
                (1L + bytes.readLittleEndian24(24)) * (1L + bytes.readLittleEndian24(27))
            else -> 0L
        }

        private fun ByteArray.readLittleEndian24(offset: Int): Int =
            (this[offset].toInt() and 0xFF) or
                ((this[offset + 1].toInt() and 0xFF) shl 8) or
                ((this[offset + 2].toInt() and 0xFF) shl 16)

        private fun validateRaster(bytes: ByteArray, format: Raster): SefariaImageValidation =
            MemoryCacheImageInputStream(ByteArrayInputStream(bytes)).use { input ->
                val readers = ImageIO.getImageReadersByFormatName(format.readerName)
                if (!readers.hasNext()) return Invalid("missing decoder for ${format.readerName}")
                val reader = readers.next()
                try {
                    reader.setInput(input, false, true)
                    var warning = false
                    reader.addIIOReadWarningListener { _, _ -> warning = true }
                    val frameCount = reader.getNumImages(true)
                    if (frameCount <= 0) return Invalid("invalid-or-unsupported-image")
                    var pixels = 0L
                    for (index in 0 until frameCount) {
                        val width = reader.getWidth(index)
                        val height = reader.getHeight(index)
                        if (width <= 0 || height <= 0) return Invalid("invalid-or-unsupported-image")
                        pixels += width.toLong() * height
                        if (pixels > MAX_IMAGE_PIXELS) return Invalid("too-many-image-pixels")
                        val image = reader.read(index)
                        if (warning) return Invalid("incomplete-or-corrupt-image")
                        if (index == 0 && isNetfreeImage(image)) return Invalid(NETFREE_PLACEHOLDER)
                    }
                    Valid(format.mime)
                } finally {
                    reader.dispose()
                }
            }

        // A PNG decoder can return pixels from a file missing its final chunks.
        // Require all chunk CRCs and a terminal IEND before decoding.
        private fun hasCompletePngChunks(bytes: ByteArray): Boolean {
            val input = ByteBuffer.wrap(bytes).position(8)
            val crc = CRC32()
            while (input.remaining() >= 12) {
                val size = input.int
                if (size < 0 || size > input.remaining() - 8) return false
                val start = input.position()
                val type = input.int
                crc.reset()
                crc.update(bytes, start, 4 + size)
                input.position(input.position() + size)
                if (crc.value.toInt() != input.int) return false
                if (type == 0x49454E44) return size == 0 && !input.hasRemaining() // IEND
            }
            return false
        }

        private fun isNetfreeImage(image: BufferedImage): Boolean {
            if (image.width != 666 || image.height != 367) return false
            // ARGB pixels in row order. Lossless re-encoding and metadata edits
            // can change the file digest while keeping the displayed image.
            val row = IntArray(image.width)
            val argb = ByteBuffer.allocate(image.width * Int.SIZE_BYTES)
            val digest = MessageDigest.getInstance("SHA-256")
            for (y in 0 until image.height) {
                image.getRGB(0, y, image.width, 1, row, 0, image.width)
                argb.asIntBuffer().put(row)
                digest.update(argb.array())
            }
            return hex.formatHex(digest.digest()) == NETFREE_PIXEL_SHA256
        }

        private fun sha256(bytes: ByteArray): String =
            hex.formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

        private fun validateSvg(bytes: ByteArray): SefariaImageValidation {
            val factory = XMLInputFactory.newDefaultFactory().apply {
                setProperty(XMLInputFactory.SUPPORT_DTD, false)
                setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
            }
            return try {
                val reader = factory.createXMLStreamReader(ByteArrayInputStream(bytes))
                try {
                    var rootSeen = false
                    while (reader.hasNext()) {
                        when (reader.next()) {
                            XMLStreamConstants.START_ELEMENT -> if (!rootSeen) {
                                if (reader.localName != "svg" ||
                                    !reader.namespaceURI.isNullOrEmpty() && reader.namespaceURI != "http://www.w3.org/2000/svg"
                                ) return Invalid("not-an-image")
                                rootSeen = true
                            }
                            XMLStreamConstants.ENTITY_REFERENCE -> return Invalid("invalid-svg")
                        }
                    }
                    if (rootSeen) Valid("image/svg+xml") else Invalid("invalid-svg")
                } finally {
                    reader.close()
                }
            } catch (_: XMLStreamException) {
                Invalid("invalid-svg")
            }
        }

        private fun ByteArray.startsWith(vararg signature: Int): Boolean =
            size >= signature.size && signature.indices.all { this[it] == signature[it].toByte() }
    }
}
