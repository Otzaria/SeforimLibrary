package io.github.kdroidfilter.seforimlibrary.search

import ai.onnxruntime.OnnxTensor
import ai.onnxruntime.OrtEnvironment
import ai.onnxruntime.OrtException
import ai.onnxruntime.OrtSession
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path

/**
 * Run each mode in a new JVM with :search:benchmarkOnnxLifecycle. The baseline modes
 * deliberately reproduce the old options leak; never invoke them in a long-lived app.
 */
object OnnxLifecycleBenchmark {
    private class Counts(var allocated: Int = 0, var closed: Int = 0)

    private class CountedOptions(private val counts: Counts) : OrtSession.SessionOptions() {
        init { counts.allocated++ }
        override fun close() {
            super.close()
            counts.closed++
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val mode = args.getOrNull(0) ?: "managed"
        require(mode in setOf("baseline", "managed", "baseline-failure", "managed-failure"))
        val iterations = (args.getOrNull(1)?.toInt() ?: 2_000).also { require(it in 1..100_000) }
        val managed = mode.startsWith("managed")
        val failModel = mode.endsWith("failure")
        val env = OrtEnvironment.getEnvironment()
        OnnxTestModel.withModel { model ->
            val invalidModel = model.resolveSibling("invalid.onnx")
            Files.write(invalidModel, byteArrayOf(1, 2, 3))
            try {
                OnnxTensor.createTensor(env, arrayOf(floatArrayOf(1f, 2f, 3f, 4f))).use { input ->
                    repeat(30) {
                        openOnnxSession(env, model, OnnxSessionConfig(false, 1)).use { owned ->
                            owned.useSession { session -> verifyOutput(session, input) }
                        }
                    }
                    val path = if (failModel) invalidModel else model
                    val counts = Counts()
                    val timings = LongArray(iterations)
                    val beforeRss = procBytes("VmRSS")
                    var sampledHeapPeak = usedHeap()
                    val beforeGc = gcMillis()
                    val start = System.nanoTime()
                    repeat(iterations) { index ->
                        val iterationStart = System.nanoTime()
                        var failed = false
                        try {
                            if (managed) {
                                openOnnxSession(
                                    env, path, OnnxSessionConfig(false, 1),
                                    createOptions = { CountedOptions(counts) },
                                ).use { owned -> owned.useSession { verifyOutput(it, input) } }
                            } else {
                                // Same CPU settings as the original implementation. It closed
                                // the session but never closed these options, even after failure.
                                val options = CountedOptions(counts)
                                options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT)
                                options.setIntraOpNumThreads(1)
                                env.createSession(path.toString(), options).use { verifyOutput(it, input) }
                            }
                        } catch (failure: OrtException) {
                            if (!failModel) throw failure
                            failed = true
                        }
                        check(failed == failModel) { "Unexpected model-load outcome" }
                        timings[index] = System.nanoTime() - iterationStart
                        if (index % 32 == 0) sampledHeapPeak = maxOf(sampledHeapPeak, usedHeap())
                    }
                    val elapsed = System.nanoTime() - start
                    timings.sort()
                    val afterRss = procBytes("VmRSS")
                    val report = linkedMapOf<String, Any?>(
                        "mode" to mode,
                        "iterations" to iterations,
                        "java" to System.getProperty("java.version"),
                        "onnxRuntime" to env.version,
                        "os" to System.getProperty("os.name"),
                        "arch" to System.getProperty("os.arch"),
                        "intraOpThreads" to 1,
                        "optionsAllocated" to counts.allocated,
                        "optionsClosed" to counts.closed,
                        "optionsUnclosed" to counts.allocated - counts.closed,
                        "elapsedMs" to elapsed / 1_000_000.0,
                        "medianMs" to percentile(timings, 0.50) / 1_000_000.0,
                        "p95Ms" to percentile(timings, 0.95) / 1_000_000.0,
                        "p99Ms" to percentile(timings, 0.99) / 1_000_000.0,
                        "sampledPeakHeapBytes" to sampledHeapPeak,
                        "rssBeforeBytes" to beforeRss,
                        "rssAfterBytes" to afterRss,
                        "processPeakRssBytes" to procBytes("VmHWM"),
                        "gcTimeMs" to gcMillis() - beforeGc,
                    )
                    println(report.entries.joinToString(",", prefix = "{", postfix = "}") { (key, value) ->
                        "\"$key\":" + when (value) {
                            is String -> "\"${value.replace("\\", "\\\\").replace("\"", "\\\"")}\""
                            else -> value.toString()
                        }
                    })
                    check(!managed || counts.allocated == counts.closed) { "Native options leaked" }
                }
            } finally {
                Files.deleteIfExists(invalidModel)
            }
        }
    }

    private fun verifyOutput(session: OrtSession, input: OnnxTensor) {
        session.run(mapOf("input" to input)).use { result ->
            @Suppress("UNCHECKED_CAST")
            val output = (result[0].value as Array<FloatArray>)[0]
            check(output.contentEquals(floatArrayOf(1f, 2f, 3f, 4f)))
        }
    }

    private fun percentile(sorted: LongArray, fraction: Double): Long =
        sorted[(kotlin.math.ceil(sorted.size * fraction).toInt() - 1).coerceAtLeast(0)]

    private fun usedHeap(): Long = Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() }

    private fun gcMillis(): Long = ManagementFactory.getGarbageCollectorMXBeans()
        .sumOf { it.collectionTime.coerceAtLeast(0) }

    // Linux reports the process high-water RSS, including warm-up. Other platforms emit null.
    private fun procBytes(field: String): Long? = runCatching {
        Files.readAllLines(Path.of("/proc/self/status"))
            .firstOrNull { it.startsWith("$field:") }
            ?.substringAfter(':')?.trim()?.substringBefore(' ')?.toLong()?.times(1024)
    }.getOrNull()
}
