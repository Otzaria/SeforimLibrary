package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.readText

/**
 * Opt-in generator benchmark. The same source runs against the unmodified baseline.
 * Outputs a real SQLite database and sampled memory/timing evidence, without timing gates.
 * Reuse one fixture directory for both revisions so filesystem link-file order is identical.
 */
object GeneratorPipelineProbe {
    private val json = Json { prettyPrint = true }

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        require(args.size == 4) { "Usage: <fixture-dir> <output-dir> <books> <lines-per-book>" }
        val fixture = Path.of(args[0]).toAbsolutePath()
        val output = Files.createDirectories(Path.of(args[1]).toAbsolutePath())
        val books = args[2].toInt().also { require(it > 0) }
        val lines = args[3].toInt().also { require(it >= 4) }
        if (!Files.exists(fixture)) createFixture(fixture, books, lines)
        val spec = json.parseToJsonElement(fixture.resolve("probe-spec.json").readText()).jsonObject
        require(spec.getValue("books").jsonPrimitive.int == books)
        require(spec.getValue("linesPerBook").jsonPrimitive.int == lines)
        val db = output.resolve("seforim.db")
        require(!Files.exists(db)) { "Refusing to overwrite $db" }
        System.setProperty("generatorReportDir", output.resolve("reports").toString())
        val driver = JdbcSqliteDriver("jdbc:sqlite:$db")
        val repo = try {
            SeforimDb.Schema.create(driver)
            SeforimRepository(db.toString(), driver)
        } catch (failure: Throwable) {
            driver.close()
            throw failure
        }
        Logger.setMinSeverity(Severity.Warn)
        val allocator = InMemoryIdAllocator.load(path = null)
        val sampler = MemorySampler()
        try {
            System.gc()
            sampler.start()
            val start = System.nanoTime()
            DatabaseGenerator(fixture, repo, allocator = allocator).generateLinesOnly()
            val linesEnd = System.nanoTime()
            // Production starts phase two in a new process; do not depend on phase-one caches.
            DatabaseGenerator(fixture, repo, allocator = allocator).generateLinksOnly()
            val linksEnd = System.nanoTime()
            sampler.close()
            val metrics = buildJsonObject {
                put("booksRequested", books)
                put("linesPerBookRequested", lines)
                put("sourceBytes", Files.walk(fixture).use { paths ->
                    paths.filter { Files.isRegularFile(it) }.mapToLong { Files.size(it) }.sum()
                })
                put("phaseLinesMs", (linesEnd - start) / 1_000_000.0)
                put("phaseLinksMs", (linksEnd - linesEnd) / 1_000_000.0)
                put("totalMs", (linksEnd - start) / 1_000_000.0)
                put("sampleIntervalMs", 10)
                put("sampledPeakHeapBytes", sampler.peakHeap.get())
                put("sampledPeakRssBytes", sampler.peakRss.get())
                put("processPeakRssBytes", rss("VmHWM:"))
                put("javaVersion", System.getProperty("java.version"))
                put("os", System.getProperty("os.name"))
                put("availableProcessors", Runtime.getRuntime().availableProcessors())
                put("maxHeapBytes", Runtime.getRuntime().maxMemory())
                put("jvmArguments", JsonArray(ManagementFactory.getRuntimeMXBean().inputArguments.map(::JsonPrimitive)))
            }
            val encoded = json.encodeToString(JsonObject.serializer(), metrics)
            Files.writeString(output.resolve("metrics.json"), encoded + "\n")
            println(encoded)
        } finally {
            sampler.close()
            repo.close()
        }
    }

    private fun createFixture(root: Path, books: Int, lines: Int) {
        Files.createDirectories(root)
        val library = Files.createDirectories(root.resolve("אוצריא"))
        val links = Files.createDirectories(root.resolve("links"))
        for (book in 0 until books) {
            val category = Files.createDirectories(library.resolve("בדיקה ${book % 3}").resolve("ענף ${book % 2}"))
            val title = "מקור ${book.toString().padStart(5, '0')}"
            Files.newBufferedWriter(category.resolve("$title.txt")).use { writer ->
                writer.write("\uFEFF<h1>$title</h1>\r\n")
                for (line in 1 until lines) {
                    writer.write(when {
                        line == 2 -> "<h3>כותרת משנה</h3>"
                        line % 13 == 0 -> "שורה זהה חוזרת"
                        else -> "תוכן בדיקה $book $line " + "טקסט סינתטי למדידה ".repeat(12)
                    })
                    writer.write(if (line % 3 == 0) "\r" else if (line % 3 == 1) "\n" else "\r\n")
                }
                // String.lines preserves a final empty record; this catches readLine regressions.
                writer.write("\n")
            }
            val next = "מקור ${((book + 1) % books).toString().padStart(5, '0')}"
            val entries = buildJsonArray {
                // Duplicate range keys intentionally exercise widest-range semantics across batches.
                for (n in 0 until maxOf(4, lines / 10)) add(buildJsonObject {
                    put("heRef_2", next)
                    put("line_index_1", 2)
                    put("line_index_2", 4)
                    put("path_2", "$next.txt")
                    put("Conection Type", "commentary")
                    if (n % 2 == 0) {
                        put("line_index_1_end", 4)
                        put("line_index_2_end", minOf(lines, 6))
                    }
                    put("start", 0)
                    put("end", 4)
                })
            }
            Files.writeString(links.resolve("${title}_links.json"), json.encodeToString(JsonArray.serializer(), entries))
        }
        val notesDir = Files.createDirectories(library.resolve("בדיקת הערות"))
        Files.writeString(notesDir.resolve("מקור עם הערה.txt"), "<h1>מקור עם הערה</h1>\nטקסט<sup>1</sup> המשך")
        Files.writeString(notesDir.resolve("הערות על מקור עם הערה.txt"), "<h1>הערות על מקור עם הערה</h1>\n<sup>1</sup> הערה סינתטית")
        Files.writeString(links.resolve("מקור עם הערה_links.json"), """[{"line_index_1":2,"heRef_2":"הערה","path_2":"הערות על מקור עם הערה.txt","line_index_2":2,"Conection Type":"commentary"}]""")
        Files.writeString(links.resolve("ignored_headings.json"), """{"heading":1}""")
        // A separate malformed file must contribute no partial links.
        Files.writeString(links.resolve("missing_links.json"), "[{\"line_index_1\":2},")
        val manifest = buildJsonObject {
            Files.walk(library).use { paths ->
                paths.filter { Files.isRegularFile(it) }.sorted().forEach { file ->
                    val hash = MessageDigest.getInstance("SHA-256")
                    Files.newInputStream(file).use { input ->
                        val buffer = ByteArray(8192)
                        while (true) {
                            val count = input.read(buffer)
                            if (count < 0) break
                            hash.update(buffer, 0, count)
                        }
                    }
                    put(root.relativize(file).toString().replace('\\', '/'), buildJsonObject {
                        put("hash", hash.digest().joinToString("") { "%02x".format(it) })
                    })
                }
            }
        }
        Files.writeString(root.resolve("files_manifest.json"), json.encodeToString(JsonObject.serializer(), manifest))
        Files.writeString(root.resolve("probe-spec.json"), "{\"books\":$books,\"linesPerBook\":$lines}")
    }

    private fun rss(field: String): Long = runCatching {
        Files.readAllLines(Path.of("/proc/self/status")).firstOrNull { it.startsWith(field) }
            ?.substringAfter(':')?.trim()?.substringBefore(' ')?.toLong()?.times(1024) ?: -1L
    }.getOrDefault(-1)

    private class MemorySampler : AutoCloseable {
        val peakHeap = AtomicLong()
        val peakRss = AtomicLong(-1)
        private val running = AtomicBoolean(false)
        private var thread: Thread? = null
        fun start() {
            check(running.compareAndSet(false, true))
            thread = Thread({
                while (running.get()) {
                    val runtime = Runtime.getRuntime()
                    peakHeap.accumulateAndGet(runtime.totalMemory() - runtime.freeMemory(), ::maxOf)
                    peakRss.accumulateAndGet(rss("VmRSS:"), ::maxOf)
                    try { Thread.sleep(10) } catch (_: InterruptedException) { break }
                }
            }, "generator-probe-memory").apply { isDaemon = true; start() }
        }
        override fun close() {
            running.set(false)
            thread?.interrupt()
            thread?.join()
            thread = null
        }
    }
}
