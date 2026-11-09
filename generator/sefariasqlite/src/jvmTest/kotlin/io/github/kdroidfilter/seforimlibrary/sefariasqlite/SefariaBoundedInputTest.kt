package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SefariaBoundedInputTest {
    private fun payload(n: Int) = BookPayload(
        heTitle = "ספר $n",
        enTitle = "Book $n",
        categoriesHe = listOf("ספרים"),
        lines = listOf("Text $n"),
        refEntries = emptyList(),
        headings = emptyList(),
        authors = emptyList(),
        description = null,
        heShortDesc = null,
        pubDates = emptyList(),
        altStructures = emptyList(),
    )

    private fun fixture(count: Int, bytesPerFile: Int = 1): Pair<Path, List<SefariaBookDescriptor>> {
        val dir = Files.createTempDirectory("bounded-sefaria-")
        val descriptors = (0 until count).map { n ->
            val path = dir.resolve("$n-merged.json")
            Files.writeString(path, "x".repeat(bytesPerFile))
            SefariaBookDescriptor(path, "ספר $n", "Book $n", listOf("ספרים"))
        }
        return dir to descriptors
    }

    @Test
    fun preservesOrderAndProducerDoesNotRetainEntireCorpus() = runBlocking {
        val (dir, descriptors) = fixture(12)
        try {
            val release = CompletableDeferred<Unit>()
            val loadCount = AtomicInteger()
            val seen = mutableListOf<String>()
            val writer = launch {
                SefariaBoundedInput.consumeInOrder(
                    descriptors = descriptors,
                    load = { path ->
                        loadCount.incrementAndGet()
                        payload(path.fileName.toString().substringBefore('-').toInt())
                    },
                    logger = Logger.withTag("SefariaBoundedInputTest"),
                    budgetBytes = 1024L,
                ) { descriptor, book ->
                    descriptor.verify(book)
                    seen += book.heTitle
                    if (seen.size == 1) release.await()
                }
            }
            withTimeout(5_000) {
                while (seen.isEmpty()) delay(10)
                delay(100)
            }
            // One consumed and at most one producer-held pending send; no buffered third book.
            assertTrue(loadCount.get() <= 2, "read-ahead was not bounded: ${loadCount.get()}")
            release.complete(Unit)
            withTimeout(5_000) { writer.join() }
            assertEquals((0 until 12).map { "ספר $it" }, seen)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun oversizedSourceIsLoadedOnlyByConsumer() = runBlocking {
        val (dir, descriptors) = fixture(3, bytesPerFile = 128)
        try {
            val loads = AtomicInteger()
            val stats = SefariaBoundedInput.consumeInOrder(
                descriptors,
                load = { path ->
                    loads.incrementAndGet()
                    payload(path.fileName.toString().substringBefore('-').toInt())
                },
                logger = Logger.withTag("SefariaBoundedInputTest"),
                budgetBytes = 16L,
            ) { descriptor, book -> descriptor.verify(book) }
            assertEquals(3, loads.get())
            assertEquals(3, stats.readOnConsumer)
            assertEquals(3, stats.booksWritten)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun decodedBookExceedingBudgetIsReparsedOnTheConsumer() = runBlocking {
        val (dir, descriptors) = fixture(1)
        try {
            val calls = AtomicInteger()
            val text = "אבג".repeat(1000)
            val stats = SefariaBoundedInput.consumeInOrder(
                descriptors = descriptors,
                load = {
                    calls.incrementAndGet()
                    payload(0).copy(lines = listOf(text))
                },
                logger = Logger.withTag("SefariaBoundedInputTest"),
                budgetBytes = 1024L,
            ) { descriptor, book ->
                descriptor.verify(book)
                assertEquals(listOf(text), book.lines)
            }
            assertEquals(2, calls.get(), "large decoded content must not be retained in a queued item")
            assertEquals(1, stats.readOnConsumer)
            assertEquals(1, stats.booksWritten)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun failedConsumerNeverContinuesToWriteFollowingBooks() {
        val (dir, descriptors) = fixture(8)
        try {
            val written = mutableListOf<String>()
            assertFailsWith<IllegalStateException> {
                runBlocking {
                    withTimeout(5_000) {
                        SefariaBoundedInput.consumeInOrder(
                            descriptors = descriptors,
                            load = { path ->
                                payload(path.fileName.toString().substringBefore('-').toInt())
                            },
                            logger = Logger.withTag("SefariaBoundedInputTest"),
                            budgetBytes = 1024L,
                        ) { _, book ->
                            written += book.heTitle
                            error("injected serial writer failure")
                        }
                    }
                }
            }
            assertEquals(listOf("ספר 0"), written)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun refusesIdentityChangesBetweenDiscoveryAndInsert() {
        val (dir, descriptors) = fixture(1)
        try {
            assertFailsWith<IllegalStateException> {
                runBlocking {
                    SefariaBoundedInput.consumeInOrder(
                        descriptors,
                        load = { payload(99) },
                        logger = Logger.withTag("SefariaBoundedInputTest"),
                        budgetBytes = 1024L,
                    ) { _, _ -> }
                }
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun metadataPriorityOrderingMatchesPayloadOrderingAndDuplicateRules() {
        val input = listOf(payload(2), payload(0), payload(1))
        val priority = listOf("ספרים/ספר 1", "ספרים/ספר 1", "missing")
        val (oldBooks, oldMissing) = applyPriorityOrdering(input, priority)
        val descriptors = input.map {
            SefariaBookDescriptor(Path.of(it.enTitle), it.heTitle, it.enTitle, it.categoriesHe)
        }
        val (newBooks, newMissing) = applyPriorityOrderingByPath(descriptors, priority) { it.normalizedPath }
        assertEquals(oldBooks.map { it.heTitle }, newBooks.map { it.heTitle })
        assertEquals(oldMissing, newMissing)
    }
}
