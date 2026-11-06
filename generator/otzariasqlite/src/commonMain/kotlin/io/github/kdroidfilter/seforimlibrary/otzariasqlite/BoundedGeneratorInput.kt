package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedReader
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FilterReader
import java.io.InputStream
import java.io.PushbackInputStream
import java.io.Reader
import java.nio.file.Files
import java.nio.file.Path

/**
 * Budget for retained input strings, not an upper bound on the JVM heap. The
 * allocator, SQLite, companion-note plans and one oversized record per active
 * stage have their own lifetimes. The two prepared inputs and the SQL line
 * batch each use at most a third of this budget.
 */
internal fun configuredGeneratorInputBytes(): Long {
    val configured = System.getProperty("seforim.generator.inputBufferMiB")
        ?: System.getenv("SEFORIM_GENERATOR_INPUT_BUFFER_MIB")
        ?: "48"
    val mib = configured.toLongOrNull()
    require(mib != null && mib in 1..16_384) {
        "seforim.generator.inputBufferMiB must be between 1 and 16384"
    }
    return mib * 1024L * 1024L
}

/** Conservative retained-string estimate, including the list slot and object headers. */
internal fun retainedStringBytes(value: String): Long = 64L + value.length.toLong() * 2L

/**
 * A complete, validated input held either in a bounded list or in a temporary
 * length-prefixed UTF-8 file. Disk iteration is sequential and its stream is
 * closed before the file is deleted (also required on Windows).
 */
internal class PreparedStrings private constructor(
    private var memory: MutableList<String>?,
    private var spillPath: Path?,
    val size: Int,
    val retainedBytes: Long,
    val firstLineHasBom: Boolean,
) : Closeable {
    private var closed = false
    val spilled: Boolean get() = spillPath != null

    /** The single-use sequence must be consumed inside [block], before its stream closes. */
    suspend fun <T> useLines(block: suspend (Sequence<String>) -> T): T {
        check(!closed) { "Prepared input already closed" }
        memory?.let { return block(it.asSequence()) }
        val path = checkNotNull(spillPath)
        val context = currentCoroutineContext()
        return DataInputStream(Files.newInputStream(path).buffered()).use { input ->
            block(sequence {
                repeat(size) {
                    context.ensureActive()
                    val length = input.readInt()
                    check(length >= 0) { "Invalid prepared input record length" }
                    val bytes = ByteArray(length)
                    input.readFully(bytes)
                    yield(bytes.toString(Charsets.UTF_8))
                }
            })
        }
    }

    override fun close() {
        if (closed && spillPath == null) return
        closed = true
        memory?.clear()
        memory = null
        spillPath?.let { Files.deleteIfExists(it) }
        spillPath = null
    }

    class Builder(
        private val maxMemoryBytes: Long,
        private val spillDirectory: Path? = null,
    ) : Closeable {
        private var memory: MutableList<String>? = ArrayList()
        private var spillPath: Path? = null
        private var output: DataOutputStream? = null
        private var retainedBytes = 0L
        private var count = 0
        private var firstLineHasBom = false
        private var finished = false

        init { require(maxMemoryBytes > 0) }

        fun add(value: String) {
            check(!finished) { "Prepared input builder already finished" }
            check(count < Int.MAX_VALUE) { "Too many records in one input" }
            if (count == 0) firstLineHasBom = value.startsWith(Utf8Bom.CHAR)
            val bytes = retainedStringBytes(value)
            if (output == null && bytes <= maxMemoryBytes - retainedBytes) {
                checkNotNull(memory).add(value)
                retainedBytes += bytes
            } else {
                if (output == null) startSpill()
                writeRecord(value)
            }
            count++
        }

        private fun startSpill() {
            val path = if (spillDirectory == null) {
                Files.createTempFile("seforim-input-", ".records")
            } else {
                Files.createDirectories(spillDirectory)
                Files.createTempFile(spillDirectory, "seforim-input-", ".records")
            }
            spillPath = path
            output = DataOutputStream(Files.newOutputStream(path).buffered())
            checkNotNull(memory).forEach(::writeRecord)
            memory?.clear()
            memory = null
            retainedBytes = 0
        }

        private fun writeRecord(value: String) {
            val bytes = value.toByteArray(Charsets.UTF_8)
            checkNotNull(output).apply {
                writeInt(bytes.size)
                write(bytes)
            }
        }

        fun finish(): PreparedStrings {
            check(!finished) { "Prepared input builder already finished" }
            output?.close()
            output = null
            finished = true
            return PreparedStrings(memory, spillPath, count, retainedBytes, firstLineHasBom).also {
                memory = null
                spillPath = null
            }
        }

        override fun close() {
            try {
                output?.close()
            } finally {
                output = null
                memory?.clear()
                memory = null
                spillPath?.let { Files.deleteIfExists(it) }
                spillPath = null
                finished = true
            }
        }
    }
}

/**
 * Matches Kotlin String.lines(), including CR, LF, CRLF, empty input and the
 * trailing empty record. BufferedReader.lineSequence() loses that final record
 * and would silently change line indices and stable IDs.
 */
internal fun Reader.exactLineSequence(checkCancelled: () -> Unit = {}): Sequence<String> = sequence {
    // Delegate scanning to the JDK's buffered reader. Tracking the final code
    // unit restores precisely the one empty record readLine() omits at EOF.
    // This wrapper does not own its receiver; the caller closes that reader.
    val tracking = object : FilterReader(this@exactLineSequence) {
        var lastCharacter: Char? = null
        override fun read(buffer: CharArray, offset: Int, length: Int): Int {
            checkCancelled()
            val read = super.read(buffer, offset, length)
            if (read > 0) lastCharacter = buffer[offset + read - 1]
            return read
        }
    }
    val buffered = BufferedReader(tracking)
    var hadLines = false
    while (true) {
        val line = buffered.readLine() ?: break
        hadLines = true
        yield(line)
    }
    if (!hadLines || tracking.lastCharacter == '\r' || tracking.lastCharacter == '\n') yield("")
}

internal suspend fun prepareBookInput(path: Path, maxBytes: Long, spillDirectory: Path? = null): PreparedStrings =
    PreparedStrings.Builder(maxBytes, spillDirectory).use { builder ->
        val context = currentCoroutineContext()
        // InputStreamReader replaces malformed UTF-8, just as the old readText().
        Files.newInputStream(path).reader(Charsets.UTF_8).use { reader ->
            for (line in reader.exactLineSequence { context.ensureActive() }) {
                builder.add(line)
            }
        }
        builder.finish()
    }

/** Strips only the leading UTF-8 BOM, leaving every subsequent byte untouched. */
internal fun InputStream.withoutUtf8Bom(): InputStream {
    val input = PushbackInputStream(this, 3)
    val head = ByteArray(3)
    var count = 0
    while (count < head.size) {
        val read = input.read(head, count, head.size - count)
        if (read < 0) break
        count += read
    }
    if (count != 3 || head[0] != 0xEF.toByte() || head[1] != 0xBB.toByte() || head[2] != 0xBF.toByte()) {
        input.unread(head, 0, count)
    }
    return input
}

/**
 * One producer and one consumer, with no per-file async/deferred collection.
 * A rendezvous channel allows exactly one prepared lookahead alongside the
 * consumer's input. Skipped files are discarded in discovery order; database
 * writes and ID allocation remain entirely in the serial consumer.
 */
internal class OrderedInputPipeline(
    scope: CoroutineScope,
    paths: List<Path>,
    prepare: suspend (Path) -> PreparedStrings,
) {
    private data class Entry(val path: Path, val input: PreparedStrings)
    private val channel = Channel<Entry>(Channel.RENDEZVOUS, onUndeliveredElement = { it.input.close() })
    private val producer = scope.launch(Dispatchers.IO) {
        try {
            for (path in paths) {
                currentCoroutineContext().ensureActive()
                channel.send(Entry(path, prepare(path)))
            }
        } catch (error: Throwable) {
            channel.close(error)
            throw error
        } finally {
            channel.close()
        }
    }

    suspend fun take(path: Path): PreparedStrings {
        while (true) {
            val received = channel.receiveCatching()
            received.exceptionOrNull()?.let { throw it }
            val entry = received.getOrNull()
                ?: error("No prepared input for $path; the source tree changed during generation")
            if (entry.path == path) return entry.input
            entry.input.close()
        }
    }

    suspend fun close() = withContext(NonCancellable) {
        channel.cancel()
        producer.cancelAndJoin()
    }
}
