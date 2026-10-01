package io.github.kdroidfilter.seforimlibrary.common.db

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDecompressCtx
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictDecompress
import java.security.MessageDigest
import java.sql.Connection

/**
 * Line text stored as one zstd frame per row, compressed with a single dictionary
 * that the DB carries in [DICT_TABLE].
 *
 * The dictionary, the zstd-jni version and [LEVEL] are frozen: changing any of them
 * changes the bytes of every row, so every patch would carry the whole library.
 * Replace the dictionary only together with a full-rebase schema bump.
 */
object LineContentCompression {
    const val DICT_TABLE: String = "zstd_dict"
    const val LEVEL: Int = 19

    /** Same cap as the app's LineContentCodec.maxLineBytes; a longer line fails the build. */
    const val MAX_LINE_BYTES: Int = 16 * 1024 * 1024
    private const val DICT_RESOURCE = "/zstd/line_content.zdict"

    /** SHA-256 of the bundled dictionary; a mismatch means the resource changed. */
    const val DICT_SHA256: String = "1e5c9d66ab76f72640e2f18f2c81a0c5869a03c7218dc57e37384897a82c3f6c"

    val bundledDictionary: ByteArray by lazy {
        val bytes = checkNotNull(LineContentCompression::class.java.getResourceAsStream(DICT_RESOURCE)) {
            "missing resource $DICT_RESOURCE"
        }.use { it.readBytes() }
        check(sha256(bytes) == DICT_SHA256) { "$DICT_RESOURCE does not match DICT_SHA256" }
        bytes
    }

    val bundledDictionaryId: Long by lazy { Zstd.getDictIdFromDict(bundledDictionary) }

    /** 1 when the DB keeps line text compressed (it has [DICT_TABLE]). */
    fun isCompressed(conn: Connection, schema: String = "main"): Boolean =
        conn.prepareStatement("SELECT 1 FROM $schema.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, DICT_TABLE)
            ps.executeQuery().use { it.next() }
        }

    /** The dictionaries stored in [conn], by zstd dictionary id. */
    fun storedDictionaries(conn: Connection, schema: String = "main"): Map<Long, ByteArray> {
        if (!isCompressed(conn, schema)) return emptyMap()
        return conn.createStatement().use { st ->
            st.executeQuery("SELECT id, dict FROM $schema.$DICT_TABLE ORDER BY id").use { rs ->
                buildMap { while (rs.next()) put(rs.getLong(1), rs.getBytes(2)) }
            }
        }
    }

    /** Not thread-safe; use one per thread. */
    class Compressor(dictionary: ByteArray = bundledDictionary) : AutoCloseable {
        private val dict = ZstdDictCompress(dictionary, LEVEL)
        private val ctx = ZstdCompressCtx().apply {
            setLevel(LEVEL)
            setChecksum(false)
            setContentSize(true)
            setDictID(true)
            loadDict(dict)
        }

        fun compress(utf8: ByteArray): ByteArray = ctx.compress(utf8)

        override fun close() {
            ctx.close()
            dict.close()
        }
    }

    /** Not thread-safe; use one per thread; threads may share one [ZstdDictDecompress]. */
    class Decompressor private constructor(
        private val dict: ZstdDictDecompress,
        private val dictId: Long,
        private val ownsDict: Boolean,
    ) : AutoCloseable {
        constructor(dictionary: ByteArray = bundledDictionary) :
            this(ZstdDictDecompress(dictionary), Zstd.getDictIdFromDict(dictionary), true)

        /** Borrows [dict] (whose id is [dictId]); the caller closes it after every Decompressor. */
        constructor(dict: ZstdDictDecompress, dictId: Long) : this(dict, dictId, false)

        private val ctx = ZstdDecompressCtx().apply { loadDict(dict) }

        /** Decodes [frame] only if the app would: exactly one frame, this dictionary's id, a known size within the cap. */
        fun decompress(frame: ByteArray): ByteArray {
            val size = Zstd.getFrameContentSize(frame)
            check(size in 0..MAX_LINE_BYTES.toLong()) {
                "zstd frame content size $size is unknown, invalid or over the reader cap $MAX_LINE_BYTES"
            }
            val frameDictId = Zstd.getDictIdFromFrame(frame)
            check(frameDictId == dictId) { "zstd frame names dictionary $frameDictId, not $dictId" }
            val frameSize = Zstd.findFrameCompressedSize(frame)
            check(frameSize == frame.size.toLong()) { "zstd frame is $frameSize of ${frame.size} bytes" }
            val bytes = ctx.decompress(frame, size.toInt())
            check(bytes.size.toLong() == size) { "zstd frame decoded to ${bytes.size} bytes, not $size" }
            return bytes
        }

        override fun close() {
            ctx.close()
            if (ownsDict) dict.close()
        }
    }

    internal fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
