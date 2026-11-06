package io.github.kdroidfilter.seforimlibrary.search

import com.sun.management.ThreadMXBean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.Term
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.KnnFloatVectorQuery
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.math.ceil

/**
 * Opt-in benchmark; never executed as a unit test. Run each mode in a fresh JVM:
 * :search:benchmarkLuceneSearchers --args="legacy 10000 1000 1"
 * :search:benchmarkLuceneSearchers --args="managed 10000 1000 1"
 * Arguments: mode, line count, measured requests per worker, worker count.
 */
object LuceneSearcherBenchmark {
    @JvmStatic
    fun main(args: Array<String>) {
        val mode = args.getOrNull(0) ?: "managed"
        val documents = args.getOrNull(1)?.toInt() ?: 10_000
        val iterations = args.getOrNull(2)?.toInt() ?: 1_000
        val workers = args.getOrNull(3)?.toInt() ?: 1
        require(mode == "legacy" || mode == "managed")
        require(documents > 0 && iterations > 0 && workers in 1..64)

        val index = Files.createTempDirectory("searcher-benchmark")
        try {
            LuceneLifecycleFixture.create(index, documents)
            val opens = AtomicInteger()
            val factory = object : SearcherFactory() {
                override fun newSearcher(reader: IndexReader, previousReader: IndexReader?): IndexSearcher {
                    opens.incrementAndGet()
                    return super.newSearcher(reader, previousReader)
                }
            }
            FSDirectory.open(index).use { directory ->
                ManagedIndexSearchers(index, searcherFactory = factory).use { managed ->
                    val queries = listOf<Query>(
                        TermQuery(Term("text", "token7")),
                        PrefixQuery(Term("title", "book")),
                        KnnFloatVectorQuery("vec", LuceneLifecycleFixture.vector(9), 20),
                    )
                    fun request(sequence: Int): Long {
                        val query = queries[sequence % queries.size]
                        return if (mode == "legacy") {
                            DirectoryReader.open(directory).use { reader ->
                                opens.incrementAndGet()
                                runQuery(IndexSearcher(reader), query)
                            }
                        } else {
                            managed.acquire().use { lease -> runQuery(lease.searcher, query) }
                        }
                    }

                    val allocated = (ManagementFactory.getThreadMXBean() as? ThreadMXBean)
                        ?.takeIf { it.isThreadAllocatedMemorySupported }
                        ?.also { it.isThreadAllocatedMemoryEnabled = true }
                    val pool = Executors.newFixedThreadPool(workers)
                    val ready = CountDownLatch(workers)
                    val start = CountDownLatch(1)
                    try {
                        val futures = (0 until workers).map {
                            pool.submit<Sample> {
                                repeat(100) { request(it) }
                                ready.countDown()
                                check(start.await(60, TimeUnit.SECONDS))
                                val threadId = Thread.currentThread().threadId()
                                val bytesBefore = allocated?.getThreadAllocatedBytes(threadId) ?: 0L
                                val latency = LongArray(iterations)
                                var checksum = 0L
                                repeat(iterations) { i ->
                                    val begin = System.nanoTime()
                                    checksum += request(i)
                                    latency[i] = System.nanoTime() - begin
                                }
                                Sample(latency, checksum, (allocated?.getThreadAllocatedBytes(threadId) ?: 0L) - bytesBefore)
                            }
                        }
                        check(ready.await(60, TimeUnit.SECONDS))
                        System.gc()
                        val openedBefore = opens.get()
                        val gcBefore = gcTime()
                        val peakHeap = AtomicLong(0)
                        val peakRss = AtomicLong(0)
                        val peakMapped = AtomicLong(0)
                        val monitoring = java.util.concurrent.atomic.AtomicBoolean(true)
                        val monitor = thread(isDaemon = true, name = "search-benchmark-sampler") {
                            while (monitoring.get()) {
                                peakHeap.accumulateAndGet(ManagementFactory.getMemoryMXBean().heapMemoryUsage.used, ::maxOf)
                                peakRss.accumulateAndGet(residentBytes(), ::maxOf)
                                peakMapped.accumulateAndGet(
                                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean::class.java)
                                        .filter { it.name.startsWith("mapped") }.sumOf { it.memoryUsed }, ::maxOf,
                                )
                                Thread.sleep(10)
                            }
                        }
                        val begin = System.nanoTime()
                        start.countDown()
                        var elapsed = 0L
                        val samples = try {
                            futures.map { it.get(120, TimeUnit.SECONDS) }.also {
                                elapsed = System.nanoTime() - begin
                            }
                        } finally {
                            monitoring.set(false)
                            monitor.join()
                        }
                        val sorted = samples.flatMap { it.latency.toList() }.sorted()
                        val requests = iterations.toLong() * workers
                        fun percentile(q: Double) = sorted[(ceil(q * sorted.size).toInt() - 1).coerceAtLeast(0)] / 1_000.0
                        println(buildJsonObject {
                            put("mode", mode)
                            put("java", System.getProperty("java.version"))
                            put("os", System.getProperty("os.name"))
                            put("documents", documents)
                            put("workers", workers)
                            put("requests", requests)
                            put("elapsedMs", elapsed / 1_000_000.0)
                            put("requestsPerSecond", requests * 1_000_000_000.0 / elapsed)
                            put("p50Micros", percentile(0.5))
                            put("p95Micros", percentile(0.95))
                            put("p99Micros", percentile(0.99))
                            put("allocationMeasurementAvailable", allocated != null)
                            put("allocatedBytesPerRequest", samples.sumOf { it.allocatedBytes } / requests)
                            put("sampledPeakHeapBytes", peakHeap.get())
                            put("rssMeasurementAvailable", peakRss.get() > 0)
                            put("sampledPeakRssBytes", peakRss.get())
                            put("sampledPeakMappedBytes", peakMapped.get())
                            put("gcMillis", gcTime() - gcBefore)
                            put("readerOpensMeasured", opens.get() - openedBefore)
                            put("readerOpensIncludingWarmup", opens.get())
                            put("checksum", samples.sumOf { it.checksum })
                        })
                    } finally {
                        start.countDown()
                        pool.shutdown()
                        check(pool.awaitTermination(60, TimeUnit.SECONDS))
                    }
                }
            }
        } finally {
            LuceneLifecycleFixture.delete(index)
        }
    }

    private fun runQuery(searcher: IndexSearcher, query: Query): Long {
        val stored = searcher.storedFields()
        var checksum = 1L
        for (hit in searcher.search(query, 20).scoreDocs) {
            val doc = stored.document(hit.doc)
            val id = doc.getField("line_id")?.numericValue()?.toLong()
                ?: doc.getField("book_id").numericValue().toLong()
            checksum = checksum * 31 + id
            checksum = checksum * 31 + hit.score.toRawBits()
        }
        return checksum
    }

    private fun gcTime(): Long = ManagementFactory.getGarbageCollectorMXBeans().sumOf { it.collectionTime.coerceAtLeast(0) }

    private fun residentBytes(): Long = try {
        Files.readAllLines(java.nio.file.Path.of("/proc/self/status"))
            .first { it.startsWith("VmRSS:") }.trim().split(Regex("\\s+"))[1].toLong() * 1024
    } catch (_: Exception) {
        // RSS is unavailable on non-Linux hosts; report zero rather than invent an estimate.
        0L
    }

    private data class Sample(val latency: LongArray, val checksum: Long, val allocatedBytes: Long)
}
