package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.IOException
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BoundedGeneratorInputTest {
    @Test
    fun exactLinesMatchStringLinesIncludingTrailingRecordsAndBufferBoundaries() {
        val samples = listOf(
            "", "\n", "\r", "\r\n", "a", "a\n", "a\r", "a\r\nb\nc\rd\n\n",
            "\uFEFF<h1>בדיקה</h1>\r\nטקסט\uFEFF\n",
            "a".repeat(8191) + "\r\nb\r\nc\n",
            "a".repeat(100_000) + "\r\n" + "b".repeat(8192) + "\r",
        )
        for (sample in samples) {
            assertEquals(sample.lines(), StringReader(sample).exactLineSequence().toList())
        }
    }

    @Test
    fun preparedInputsStayWithinTheirByteBudgetOrSpillWithoutChangingText() = runBlocking {
        withDirectory { root ->
            val records = List(200) { "שורה $it — " + "טקסט ".repeat(it % 10) }
            for (budget in listOf(128L, 1024L * 1024)) {
                PreparedStrings.Builder(budget, root).use { builder ->
                    records.forEach(builder::add)
                    builder.finish().use { prepared ->
                        assertEquals(records.size, prepared.size)
                        assertTrue(prepared.retainedBytes <= budget)
                        assertEquals(budget == 128L, prepared.spilled)
                        assertEquals(records, prepared.useLines { it.toList() })
                        // An early consumer exit must close its stream too (Windows deletion).
                        assertEquals(records.first(), prepared.useLines { it.first() })
                    }
                }
                assertDirectoryEmpty(root)
            }
        }
    }

    @Test
    fun bookPreparationPreservesEmptyInputBomAndMalformedUtf8Replacement() = runBlocking {
        withDirectory { root ->
            val spill = Files.createDirectory(root.resolve("spill"))
            val source = root.resolve("source.txt")
            val samples = listOf(
                byteArrayOf(),
                "\uFEFFa\r\nb\r\n".toByteArray(),
                byteArrayOf(0x61, 0xC3.toByte(), 0x28, 0x0A),
            )
            for (bytes in samples) {
                Files.write(source, bytes)
                prepareBookInput(source, 64, spill).use { prepared ->
                    assertEquals(bytes.toString(Charsets.UTF_8).lines(), prepared.useLines { it.toList() })
                    assertEquals(bytes.toString(Charsets.UTF_8).startsWith(Utf8Bom.CHAR), prepared.firstLineHasBom)
                }
                assertDirectoryEmpty(spill)
            }
        }
    }

    @Test
    fun partiallyWrittenSpillIsDeletedWhenPreparationFails() {
        withDirectory { root ->
            assertFailsWith<IOException> {
                PreparedStrings.Builder(64, root).use { builder ->
                    builder.add("large input".repeat(200))
                    throw IOException("injected preparation failure")
                }
            }
            assertDirectoryEmpty(root)
        }
    }

    @Test
    fun pipelineAllowsOneLookaheadAndCleansSpillsOnCancellation() = runBlocking {
        withDirectory { root ->
            val spill = Files.createDirectory(root.resolve("spill"))
            val paths = (0..3).map { number ->
                root.resolve("$number.txt").also { Files.writeString(it, "input $number\n".repeat(100)) }
            }
            val count = AtomicInteger()
            val secondPrepared = CompletableDeferred<Unit>()
            val failure = assertFailsWith<CancellationException> {
                withTimeout(60_000) {
                    coroutineScope {
                        val pipeline = OrderedInputPipeline(this, paths) { path ->
                            prepareBookInput(path, 64, spill).also {
                                if (count.incrementAndGet() == 2) secondPrepared.complete(Unit)
                            }
                        }
                        try {
                            pipeline.take(paths.first()).use { first ->
                                secondPrepared.await()
                                assertEquals(2, count.get(), "the blocked lookahead cannot prepare a third file")
                                assertTrue(first.spilled)
                                throw CancellationException("injected consumer cancellation")
                            }
                        } finally {
                            pipeline.close()
                        }
                    }
                }
            }
            assertEquals("injected consumer cancellation", failure.message)
            assertDirectoryEmpty(spill)
        }
    }

    @Test
    fun producerFailurePropagatesAndClosesPreviouslyPreparedInput() = runBlocking {
        withDirectory { root ->
            val spill = Files.createDirectory(root.resolve("spill"))
            val first = root.resolve("first.txt").also { Files.writeString(it, "input\n".repeat(100)) }
            val missing = root.resolve("missing.txt")
            assertFailsWith<IOException> {
                coroutineScope {
                    val pipeline = OrderedInputPipeline(this, listOf(first, missing)) { path ->
                        prepareBookInput(path, 64, spill)
                    }
                    try {
                        pipeline.take(first).use { input ->
                            assertFalse(input.retainedBytes > 64)
                            pipeline.take(missing).close()
                        }
                    } finally {
                        pipeline.close()
                    }
                }
            }
            assertDirectoryEmpty(spill)
        }
    }

    @Test
    fun skippedInputsAreReleasedBeforeTheNextFileIsReturned() = runBlocking {
        withDirectory { root ->
            val spill = Files.createDirectory(root.resolve("spill"))
            val paths = (0..2).map { number ->
                root.resolve("$number.txt").also { Files.writeString(it, "input $number\n".repeat(100)) }
            }
            coroutineScope {
                val pipeline = OrderedInputPipeline(this, paths) { path -> prepareBookInput(path, 64, spill) }
                try {
                    pipeline.take(paths.last()).use { input ->
                        assertEquals("input 2", input.useLines { it.first() })
                        assertEquals(1L, Files.list(spill).use { it.count() })
                    }
                } finally {
                    pipeline.close()
                }
            }
            assertDirectoryEmpty(spill)
        }
    }

    private inline fun <T> withDirectory(block: (Path) -> T): T {
        val root = Files.createTempDirectory("bounded-generator-test-")
        return try { block(root) } finally { root.toFile().deleteRecursively() }
    }

    private fun assertDirectoryEmpty(path: Path) {
        assertEquals(0L, Files.list(path).use { it.count() }, "temporary inputs must be deleted: $path")
    }
}
