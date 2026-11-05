package io.github.kdroidfilter.seforimlibrary.search

import ai.onnxruntime.OnnxTensor
import ai.onnxruntime.OrtEnvironment
import ai.onnxruntime.OrtException
import ai.onnxruntime.OrtProvider
import ai.onnxruntime.OrtSession
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OnnxSessionFactoryTest {
    private val env = OrtEnvironment.getEnvironment()

    private class TrackingOptions(private val rejectGpu: Boolean = false) : OrtSession.SessionOptions() {
        var closes = 0
        var intraThreads: Int? = null
        var interThreads: Int? = null
        var mode: ExecutionMode? = null
        var memoryPatterns: Boolean? = null
        var provider = OrtProvider.CPU
        var beforeClose: () -> Unit = {}

        override fun setIntraOpNumThreads(numThreads: Int) {
            intraThreads = numThreads
            super.setIntraOpNumThreads(numThreads)
        }

        override fun setInterOpNumThreads(numThreads: Int) {
            interThreads = numThreads
            super.setInterOpNumThreads(numThreads)
        }

        override fun setExecutionMode(value: ExecutionMode) {
            mode = value
            super.setExecutionMode(value)
        }

        override fun setMemoryPatternOptimization(enabled: Boolean) {
            memoryPatterns = enabled
            super.setMemoryPatternOptimization(enabled)
        }

        // Deterministic provider injection: even on a GPU host we can exercise failed
        // registration and the exact fallback path without depending on its drivers.
        override fun addCUDA(deviceNum: Int) = addGpu(OrtProvider.CUDA)
        override fun addDirectML(deviceId: Int) = addGpu(OrtProvider.DIRECT_ML)
        override fun addCoreML() = addGpu(OrtProvider.CORE_ML)

        private fun addGpu(value: OrtProvider) {
            provider = value
            if (rejectGpu) throw IllegalStateException("injected $value failure")
        }

        override fun close() {
            beforeClose()
            super.close()
            closes++
        }
    }

    @Test
    fun threadSettingsArePositiveBoundedAndPreserveTheDefault() {
        fun config(threads: String? = null, envThreads: String? = null, cores: Int = 8) =
            OnnxSessionConfig.fromSettings(cores, null, null, threads, envThreads)
        assertEquals(8, config().intraOpThreads)
        assertEquals(2, config("2").intraOpThreads)
        assertEquals(3, config(envThreads = "3").intraOpThreads)
        assertEquals(2, config("2", "3").intraOpThreads)
        assertEquals(8, config("1000").intraOpThreads)
        for (invalid in listOf("0", "-1", "not-a-number", "999999999999999")) {
            assertEquals(8, config(invalid).intraOpThreads)
        }
        assertEquals(1, config(cores = 0).intraOpThreads)
        assertFailsWith<IllegalArgumentException> { OnnxSessionConfig(false, 0) }
    }

    @Test
    fun gpuOptInAcceptsTheDocumentedNumericEnvironmentValue() {
        fun config(property: String?, environment: String?) =
            OnnxSessionConfig.fromSettings(1, property, environment, null, null)
        assertTrue(config(null, "1").preferGpu)
        assertTrue(config("true", null).preferGpu)
        assertTrue(config(null, "TRUE").preferGpu)
        assertFalse(config(null, null).preferGpu)
        assertFalse(config("false", "0").preferGpu)
    }

    @Test
    fun cpuOutputMatchesThePreviousSessionConfigurationAndOptionsOutliveSession() = withModel { model ->
        val input = floatArrayOf(0f, 1f, -2f, 3.5f)
        val reference = OrtSession.SessionOptions().use { options ->
            options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT)
            options.setIntraOpNumThreads(1)
            env.createSession(model.toString(), options).use { infer(it, input) }
        }
        val options = TrackingOptions()
        val owned = openOnnxSession(env, model, OnnxSessionConfig(false, 1), createOptions = { options })
        owned.useSession { session ->
            assertContentEquals(reference, infer(session, input))
            assertEquals(0, options.closes)
            options.beforeClose = {
                assertFailsWith<IllegalStateException>("session must close before its options") {
                    session.inputNames
                }
            }
        }
        owned.close()
        owned.close()
        assertEquals(1, options.closes)
        assertFailsWith<IllegalStateException> { options.setIntraOpNumThreads(1) }
        assertEquals(1, options.intraThreads)
        assertEquals(1, options.interThreads)
        assertEquals(OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL, options.mode)
    }

    @Test
    fun failedGpuAttemptsReleaseTheirOptionsBeforeCpuFallback() = withModel { model ->
        val options = mutableListOf<TrackingOptions>()
        val owned = openOnnxSession(
            env, model, OnnxSessionConfig(true, 1), GPU_PROVIDERS,
            createOptions = { TrackingOptions(rejectGpu = true).also { options += it } },
        )
        try {
            assertEquals(listOf(OrtProvider.CUDA, OrtProvider.DIRECT_ML, OrtProvider.CORE_ML, OrtProvider.CPU),
                options.map { it.provider })
            assertEquals(listOf(1, 1, 1, 0), options.map { it.closes })
            assertTrue(options.all { it.intraThreads == 1 && it.interThreads == 1 })
            assertEquals(false, options[1].memoryPatterns)
            owned.useSession { assertContentEquals(floatArrayOf(1f, 2f, 3f, 4f), infer(it, floatArrayOf(1f, 2f, 3f, 4f))) }
        } finally {
            owned.close()
        }
        assertTrue(options.all { it.closes == 1 })
    }

    @Test
    fun unavailableProvidersDoNotAllocateNativeOptions() = withModel { model ->
        val options = mutableListOf<TrackingOptions>()
        openOnnxSession(
            env, model, OnnxSessionConfig(true, 1), setOf(OrtProvider.CPU),
            createOptions = { TrackingOptions().also { options += it } },
        ).use { owned ->
            owned.useSession { assertEquals(setOf("input"), it.inputNames) }
        }
        assertEquals(1, options.size)
        assertEquals(1, options.single().closes)
    }

    @Test
    fun invalidModelReleasesEveryAttemptAndARetryCanLoad() = withModel { model ->
        val invalid = model.resolveSibling("invalid.onnx")
        Files.write(invalid, byteArrayOf(1, 2, 3))
        try {
            val options = mutableListOf<TrackingOptions>()
            repeat(5) {
                assertFailsWith<OrtException> {
                    openOnnxSession(
                        env, invalid, OnnxSessionConfig(true, 1), GPU_PROVIDERS,
                        createOptions = { TrackingOptions().also { options += it } },
                    )
                }
            }
            assertEquals(20, options.size)
            assertTrue(options.all { it.closes == 1 }, "all failed native allocations must be closed")
            openOnnxSession(env, model, OnnxSessionConfig(false, 1)).use { owned ->
                owned.useSession { assertContentEquals(floatArrayOf(4f, 3f, 2f, 1f), infer(it, floatArrayOf(4f, 3f, 2f, 1f))) }
            }
        } finally {
            Files.deleteIfExists(invalid)
        }
    }

    @Test
    fun directMlConfigurationDisablesMemoryPatternsAndUsesSequentialExecution() = withModel { model ->
        val options = TrackingOptions()
        openOnnxSession(
            env, model, OnnxSessionConfig(true, 1), setOf(OrtProvider.DIRECT_ML), { options },
        ).use { owned ->
            owned.useSession { assertEquals(setOf("input"), it.inputNames) }
            assertEquals(false, options.memoryPatterns)
            assertEquals(OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL, options.mode)
        }
        assertEquals(1, options.closes)
    }

    private fun infer(session: OrtSession, input: FloatArray): FloatArray =
        OnnxTensor.createTensor(env, arrayOf(input)).use { tensor ->
            session.run(mapOf("input" to tensor)).use { result ->
                @Suppress("UNCHECKED_CAST")
                (result[0].value as Array<FloatArray>)[0]
            }
        }

    private fun withModel(block: (Path) -> Unit) {
        OnnxTestModel.withModel(block)
    }

    companion object {
        private val GPU_PROVIDERS = setOf(OrtProvider.CUDA, OrtProvider.DIRECT_ML, OrtProvider.CORE_ML)
    }
}
