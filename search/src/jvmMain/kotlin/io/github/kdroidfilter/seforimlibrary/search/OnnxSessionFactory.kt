package io.github.kdroidfilter.seforimlibrary.search

import ai.onnxruntime.OrtEnvironment
import ai.onnxruntime.OrtProvider
import ai.onnxruntime.OrtSession
import co.touchlab.kermit.Logger
import java.nio.file.Path
import java.util.concurrent.CancellationException

internal data class OnnxSessionConfig(val preferGpu: Boolean, val intraOpThreads: Int) {
    init {
        require(intraOpThreads > 0) { "ONNX intra-op thread count must be positive" }
    }

    companion object {
        fun fromSettings(
            processors: Int = Runtime.getRuntime().availableProcessors(),
            gpuProperty: String? = System.getProperty("seforimEmbedGpu"),
            gpuEnvironment: String? = System.getenv("SEFORIM_EMBED_GPU"),
            threadsProperty: String? = System.getProperty("seforimEmbedThreads"),
            threadsEnvironment: String? = System.getenv("SEFORIM_EMBED_THREADS"),
        ): OnnxSessionConfig {
            val cores = processors.coerceAtLeast(1)
            val requested = (threadsProperty ?: threadsEnvironment)?.trim()?.toIntOrNull()
            val threads = requested?.takeIf { it > 0 }?.coerceAtMost(cores) ?: cores
            fun enabled(value: String?) = value?.trim()?.let { it == "1" || it.equals("true", true) } == true
            return OnnxSessionConfig(enabled(gpuProperty) || enabled(gpuEnvironment), threads)
        }
    }
}

/** Creates fresh, independently owned options for every provider attempt. */
internal fun openOnnxSession(
    env: OrtEnvironment,
    model: Path,
    config: OnnxSessionConfig = OnnxSessionConfig.fromSettings(),
    availableProviders: Set<OrtProvider> = OrtEnvironment.getAvailableProviders(),
    createOptions: () -> OrtSession.SessionOptions = { OrtSession.SessionOptions() },
): OwnedOnnxSession<OrtSession, OrtSession.SessionOptions> {
    val logger = Logger.withTag("SeforimEmbedder")

    fun open(provider: OrtProvider): OwnedOnnxSession<OrtSession, OrtSession.SessionOptions> =
        OwnedOnnxSession.open(
            createOptions = createOptions,
            configure = { options ->
                options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT)
                options.setExecutionMode(OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL)
                options.setIntraOpNumThreads(config.intraOpThreads)
                options.setInterOpNumThreads(1)
                when (provider) {
                    OrtProvider.CUDA -> options.addCUDA(0)
                    OrtProvider.DIRECT_ML -> {
                        // DirectML requires both sequential execution and disabled memory patterns.
                        options.setMemoryPatternOptimization(false)
                        options.addDirectML(0)
                    }
                    OrtProvider.CORE_ML -> options.addCoreML()
                    OrtProvider.CPU -> Unit // ORT always adds CPU as the final fallback.
                    else -> error("Unsupported embedding provider: $provider")
                }
            },
            createSession = { env.createSession(model.toString(), it) },
            serializeUse = provider == OrtProvider.DIRECT_ML,
        )

    if (config.preferGpu) {
        for (provider in listOf(OrtProvider.CUDA, OrtProvider.DIRECT_ML, OrtProvider.CORE_ML)) {
            // The standard Maven artifact is CPU-only: do not allocate options just to
            // discover that a provider was not compiled into the loaded native library.
            if (provider !in availableProviders) continue
            try {
                logger.i { "Trying ONNX EP: $provider (GPU)" }
                return open(provider)
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                logger.i { "ONNX EP $provider unavailable: ${failure.message}" }
            } catch (failure: LinkageError) {
                logger.i { "ONNX EP $provider could not load: ${failure.message}" }
            }
        }
        logger.i { "No GPU EP available; using CPU" }
    }
    return open(OrtProvider.CPU)
}
