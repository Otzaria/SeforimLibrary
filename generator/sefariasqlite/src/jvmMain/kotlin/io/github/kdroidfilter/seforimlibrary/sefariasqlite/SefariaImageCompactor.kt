package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.awt.color.ColorSpace
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.concurrent.Semaphore
import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.stream.MemoryCacheImageInputStream
import javax.imageio.stream.MemoryCacheImageOutputStream
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/** The reader holds a whole book in memory, so full-size page scans (Otzaria issue 5033) are shrunk
 * before inlining. Output is a pure function of the input: relink recovery compares lines across builds. */
internal object SefariaImageCompactor {
    // A portrait page scan becomes ~1130 px wide: a reader column on a HiDPI screen.
    // Issue 5033's 78 images: 27 MB of base64 instead of 236 MB.
    internal const val MAX_SIDE = 1600
    private const val JPEG_QUALITY = 0.85f

    private val decoders = Semaphore(4)

    data class Embeddable(val mime: String, val bytes: ByteArray)

    /** [bytes] unchanged unless a downscaled re-encode is strictly smaller. */
    fun compact(mime: String, bytes: ByteArray): Embeddable {
        val original = Embeddable(mime, bytes)
        if (mime != "image/png" && mime != "image/jpeg" && mime != "image/webp") return original
        if (mime == "image/png" && isAnimatedPng(bytes)) return original
        if (mime == "image/jpeg" && exifOrientation(bytes) != 1) return original
        decoders.acquire()
        return try {
            val decoded = readSingleFrame(bytes) ?: return original
            if (max(decoded.width, decoded.height) <= MAX_SIDE) return original
            val image = grayToArgb(decoded)
            val smaller = encodeSmallest(downscale(image)) ?: return original
            if (smaller.bytes.size < bytes.size) smaller else original
        } catch (_: Exception) {
            original
        } finally {
            decoders.release()
        }
    }

    private fun readSingleFrame(bytes: ByteArray): BufferedImage? =
        MemoryCacheImageInputStream(ByteArrayInputStream(bytes)).use { input ->
            val reader = ImageIO.getImageReaders(input).asSequence().firstOrNull() ?: return null
            try {
                reader.setInput(input, false, true)
                if (reader.getNumImages(true) != 1) null else reader.read(0)
            } finally {
                reader.dispose()
            }
        }

    // ImageIO ignores APNG's acTL chunk and would flatten the animation to its first frame.
    private fun isAnimatedPng(bytes: ByteArray): Boolean {
        val chunks = String(bytes, Charsets.ISO_8859_1)
        val actl = chunks.indexOf("acTL")
        return actl >= 0 && actl < chunks.indexOf("IDAT")
    }

    // ImageIO decodes gray as linear CS_GRAY, and getRGB would brighten it on the way to sRGB.
    // Browsers show gray samples as-is, so copy the raw levels instead.
    private fun grayToArgb(image: BufferedImage): BufferedImage {
        val model = image.colorModel
        if (model.colorSpace.type != ColorSpace.TYPE_GRAY) return image
        val raster = image.raster
        val grayMax = (1 shl model.getComponentSize(0)) - 1
        val alphaMax = if (model.hasAlpha()) (1 shl model.getComponentSize(1)) - 1 else 0
        val out = BufferedImage(image.width, image.height, BufferedImage.TYPE_INT_ARGB)
        for (y in 0 until image.height) for (x in 0 until image.width) {
            val v = ((raster.getSample(x, y, 0).toLong() * 255 + grayMax / 2) / grayMax).toInt()
            val a = if (alphaMax == 0) 255 else ((raster.getSample(x, y, 1).toLong() * 255 + alphaMax / 2) / alphaMax).toInt()
            out.setRGB(x, y, (a shl 24) or (v shl 16) or (v shl 8) or v)
        }
        return out
    }

    /** The Exif orientation tag of a JPEG, or 1 (upright) when there is none. ImageIO ignores it. */
    private fun exifOrientation(jpeg: ByteArray): Int {
        fun u16(at: Int, bigEndian: Boolean = true): Int {
            val hi = jpeg[if (bigEndian) at else at + 1].toInt() and 0xFF
            val lo = jpeg[if (bigEndian) at + 1 else at].toInt() and 0xFF
            return (hi shl 8) or lo
        }
        fun u32(at: Int, bigEndian: Boolean) =
            if (bigEndian) (u16(at) shl 16) or u16(at + 2) else (u16(at + 2, false) shl 16) or u16(at, false)
        return try {
            var pos = 2
            while (pos + 4 <= jpeg.size && jpeg[pos] == 0xFF.toByte()) {
                val marker = jpeg[pos + 1].toInt() and 0xFF
                if (marker == 0xDA || marker == 0xD9) break
                val length = u16(pos + 2)
                val tiff = pos + 10
                if (marker == 0xE1 && String(jpeg, pos + 4, 6, Charsets.ISO_8859_1) == "Exif\u0000\u0000") {
                    val big = jpeg[tiff] == 'M'.code.toByte()
                    val ifd = tiff + u32(tiff + 4, big)
                    for (i in 0 until u16(ifd, big)) {
                        val entry = ifd + 2 + i * 12
                        if (u16(entry, big) == 0x0112) return u16(entry + 8, big)
                    }
                }
                pos += 2 + length
            }
            1
        } catch (_: IndexOutOfBoundsException) {
            1
        }
    }

    /**
     * Area-average resample in premultiplied alpha. Plain arithmetic instead of
     * Java2D, whose rendering pipeline may differ between machines.
     */
    internal fun downscale(image: BufferedImage): BufferedImage {
        val srcW = image.width
        val srcH = image.height
        val longest = max(srcW, srcH)
        val outW = max(1, ((srcW.toLong() * MAX_SIDE + longest / 2) / longest).toInt())
        val outH = max(1, ((srcH.toLong() * MAX_SIDE + longest / 2) / longest).toInt())
        val xWeights = coverage(srcW, outW)
        val yWeights = coverage(srcH, outH)
        val out = BufferedImage(outW, outH, BufferedImage.TYPE_INT_ARGB)
        val srcRow = IntArray(srcW)
        val rowSum = DoubleArray(outW * 4)
        val acc = DoubleArray(outW * 4)
        val outRow = IntArray(outW)
        val area = (srcW.toDouble() / outW) * (srcH.toDouble() / outH)
        for (oy in 0 until outH) {
            acc.fill(0.0)
            val (yStart, yW) = yWeights[oy]
            for (k in yW.indices) {
                image.getRGB(0, yStart + k, srcW, 1, srcRow, 0, srcW)
                rowSum.fill(0.0)
                for (ox in 0 until outW) {
                    val (xStart, xW) = xWeights[ox]
                    var a = 0.0; var r = 0.0; var g = 0.0; var b = 0.0
                    for (j in xW.indices) {
                        val p = srcRow[xStart + j]
                        val pa = (p ushr 24) * xW[j]
                        a += pa
                        r += ((p shr 16) and 0xFF) * pa
                        g += ((p shr 8) and 0xFF) * pa
                        b += (p and 0xFF) * pa
                    }
                    val i = ox * 4
                    rowSum[i] = a; rowSum[i + 1] = r; rowSum[i + 2] = g; rowSum[i + 3] = b
                }
                for (i in acc.indices) acc[i] += rowSum[i] * yW[k]
            }
            for (ox in 0 until outW) {
                val i = ox * 4
                val a = acc[i] / area
                val alpha = channel(a)
                outRow[ox] = if (alpha == 0) 0 else
                    (alpha shl 24) or (channel(acc[i + 1] / acc[i]) shl 16) or
                        (channel(acc[i + 2] / acc[i]) shl 8) or channel(acc[i + 3] / acc[i])
            }
            out.setRGB(0, oy, outW, 1, outRow, 0, outW)
        }
        return out
    }

    private fun channel(value: Double): Int = value.roundToInt().coerceIn(0, 255)

    /** For each output pixel: first source index and the overlap of each source pixel. */
    private fun coverage(src: Int, out: Int): Array<Pair<Int, DoubleArray>> {
        val scale = src.toDouble() / out
        return Array(out) { o ->
            val start = o * scale
            val end = min(src.toDouble(), (o + 1) * scale)
            val first = start.toInt()
            val last = min(src - 1, kotlin.math.ceil(end).toInt() - 1)
            first to DoubleArray(last - first + 1) { k ->
                min(end, first + k + 1.0) - max(start, (first + k).toDouble())
            }
        }
    }

    private fun encodeSmallest(image: BufferedImage): Embeddable? {
        val pixels = image.getRGB(0, 0, image.width, image.height, null, 0, image.width)
        if (pixels.any { it ushr 24 != 0xFF }) return Embeddable("image/png", write(image, "png") ?: return null)
        val gray = pixels.all { ((it shr 16) and 0xFF) == (it and 0xFF) && ((it shr 8) and 0xFF) == (it and 0xFF) }
        val opaque = BufferedImage(
            image.width, image.height,
            if (gray) BufferedImage.TYPE_BYTE_GRAY else BufferedImage.TYPE_3BYTE_BGR,
        )
        opaque.raster.let { raster ->
            val row = IntArray(if (gray) image.width else image.width * 3)
            for (y in 0 until image.height) {
                for (x in 0 until image.width) {
                    val p = pixels[y * image.width + x]
                    if (gray) row[x] = p and 0xFF
                    else { row[x * 3] = (p shr 16) and 0xFF; row[x * 3 + 1] = (p shr 8) and 0xFF; row[x * 3 + 2] = p and 0xFF }
                }
                raster.setPixels(0, y, image.width, 1, row)
            }
        }
        val png = write(opaque, "png")?.let { Embeddable("image/png", it) }
        val jpeg = write(opaque, "jpeg")?.let { Embeddable("image/jpeg", it) }
        return listOfNotNull(png, jpeg).minByOrNull { it.bytes.size }
    }

    /** No metadata is passed, so neither writer emits a timestamp or other per-run chunk. */
    private fun write(image: BufferedImage, format: String): ByteArray? {
        val writer = ImageIO.getImageWritersByFormatName(format).asSequence().firstOrNull() ?: return null
        val out = ByteArrayOutputStream()
        try {
            MemoryCacheImageOutputStream(out).use { stream ->
                writer.output = stream
                val param = writer.defaultWriteParam.apply {
                    compressionMode = ImageWriteParam.MODE_EXPLICIT
                    compressionQuality = if (format == "jpeg") JPEG_QUALITY else 0f
                }
                writer.write(null, IIOImage(image, null, null), param)
            }
        } finally {
            writer.dispose()
        }
        return out.toByteArray()
    }
}
