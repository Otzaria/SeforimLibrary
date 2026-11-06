# JVM ONNX resource lifetime

The JVM search module remains on ONNX Runtime **1.20.0** and the existing CPU Maven
artifact. No model, tokenizer, normalization, tensor shape, graph-optimization level,
or embedding/index identity is changed.

## Ownership and failure handling

The original loader allocated `OrtSession.SessionOptions` for each provider attempt
but never closed them. A failed provider registration or failed model load leaked its
options, and a successful session did not retain an owner responsible for closing them.
The tokenizer also needed rollback if subsequent environment/session initialization
failed.

`OwnedOnnxSession` retains each successful session's options. It closes the session
before the options, as required by the [ORT 1.20 Java
contract](https://github.com/microsoft/onnxruntime/blob/v1.20.0/java/src/main/java/ai/onnxruntime/OrtSession.java).
Failed attempts close their own options before the next attempt starts. Rollback
preserves the original exception and attaches cleanup failures as suppressed exceptions.

Each embedding call takes a lifetime lease covering tokenization, input tensors,
inference, and output extraction. `close()` waits for these calls, rejects subsequent
calls, and is idempotent after successful cleanup. If session close fails, its options
remain live and a later close can retry. Independent tokenizer cleanup is still attempted.
The shared `OrtEnvironment` is borrowed from the JVM; it is not owned by one embedder
([its `close()` has been a no-op since ORT 1.11](https://github.com/microsoft/onnxruntime/blob/v1.20.0/java/src/main/java/ai/onnxruntime/OrtEnvironment.java)).

## Providers and CPU threads

The default is still CPU execution with full graph optimization and the number of
processors visible to the JVM as the intra-op thread count. An optional
`-DseforimEmbedThreads=N` or `SEFORIM_EMBED_THREADS=N` limits that count for deployments
sharing CPU with other work. A positive override is clamped to the visible processor
count; invalid/nonpositive values fall back to the existing default. The JVM property
takes precedence over the environment variable. Sequential graph execution is explicit,
and the inter-op count is set to one; this does not add a parallel inter-op pool.

GPU selection remains opt-in through `-DseforimEmbedGpu=true` or
`SEFORIM_EMBED_GPU=true`. The previously documented `SEFORIM_EMBED_GPU=1` now works as
well. The loader only attempts providers reported by the loaded native runtime, in the
existing CUDA → DirectML → CoreML order, followed by CPU. A provider's reported presence
does not guarantee working drivers or support for the graph, so failed attempts still
release their options and fall back. CPU thread limits also apply to CPU nodes within
a GPU-backed session.

[DirectML requires sequential execution, disabled memory-pattern optimization, and
no concurrent `Run` calls on one session](https://onnxruntime.ai/docs/execution-providers/DirectML-ExecutionProvider.html#configuration-options).
Those requirements are enforced only for a selected DirectML session; CPU/CUDA/CoreML
sessions retain concurrent inference. The stock dependency does not include DirectML,
CUDA, or CoreML, and this change does not add GPU native libraries.

## Validation

Run the lifecycle, failure, concurrency, provider-fallback, and real-native parity tests:

```sh
./gradlew :search:jvmTest --tests '*OwnedOnnxSessionTest' --tests '*OnnxSessionFactoryTest'
```

The native tests generate a self-contained **130-byte ONNX IR 8 / opset 13 identity
graph**. They require no model download, GPU, tokenizer native download, or external
service. The tests compare output bit-for-bit against the previous CPU session settings,
exercise invalid-model cleanup and successful retry, and count native options closed
on each failed provider attempt. Provider failures and DirectML configuration are injected
deterministically so these tests have the same meaning on CPU and GPU hosts.

The existing `SeforimEmbedderTest` remains the optional integration test for the actual
embedding model. The tiny graph validates native session/API behavior; it does not stand
in for production-model quality, ranking, throughput, or hardware-provider validation.

## Focused lifecycle benchmark

Each invocation starts a disposable JVM with a 256 MiB heap, one intra-op thread, and
30 warm-up iterations. The `baseline` modes intentionally reproduce the original
unclosed-options behavior. Run each mode in a separate process:

```sh
./gradlew :search:benchmarkOnnxLifecycle -PonnxBenchmarkMode=baseline -PonnxBenchmarkIterations=2000
./gradlew :search:benchmarkOnnxLifecycle -PonnxBenchmarkMode=managed -PonnxBenchmarkIterations=2000
./gradlew :search:benchmarkOnnxLifecycle -PonnxBenchmarkMode=baseline-failure -PonnxBenchmarkIterations=20000
./gradlew :search:benchmarkOnnxLifecycle -PonnxBenchmarkMode=managed-failure -PonnxBenchmarkIterations=20000
```

The JSON report contains session-load/close latency, native options allocated/closed,
sampled JVM heap, GC time, and Linux RSS/high-water RSS when available. Managed modes
fail if any allocated options remain unclosed. These exact ownership counts are the
regression criterion; no flaky absolute memory or timing threshold is added to CI.
RSS includes native allocator caching and JVM warm-up, and the process high-water RSS
includes the warm-up phase. The benchmark measures resource lifetime and setup overhead,
not transformer inference speed. Benchmarks are opt-in and never run in `jvmTest`.

## Hybrid initialization and shutdown

`HybridSearchEngine` uses `LazyDenseResources` to own one immutable embedder/searcher
pair. A coroutine mutex serializes initialization, while a short JVM monitor makes
publication and shutdown mutually exclusive. Shutdown detaches the published pair
before native disposal; it does not wait for a model constructor to finish. If shutdown
wins, the initializer closes its unpublished candidates instead of making them live.
Blocking model creation and both native close calls run outside the publication monitor.
Resources whose close fails remain privately owned for a later `close()` retry, including
unpublished candidates from failed, cancelled, or late initialization. Successfully closed
resources are never retried, and failed resources cannot become available to queries again.
Concurrent closes do not wait for an in-flight disposal or dispose its resources twice;
retry after that disposal finishes. The hybrid engine logs cleanup failures and still
closes the lexical engine.

Cancellation is checked before, between, and after the factories. Unpublished resources
are cleaned in `finally`, including when loading the vector searcher fails. Publication
happens inside the IO block, so prompt cancellation on its return cannot lose ownership.
Calls made after shutdown cannot restart initialization. A loaded pair is read atomically.
The embedder's inference/close barrier is included here. Lucene reader leases and their
query/close guarantees belong to the separate Lucene lifecycle change; this standalone
change does not alter VectorSearcher ownership.

`LazyDenseResourcesTest` uses barrier-controlled fake factories, without downloading a
model, to check both initialization/close orders, cancellation at either factory,
concurrent once-only loading, unavailable/failed searcher cleanup, and disposal outside
the publication monitor. Run it with `./gradlew :search:jvmTest --tests '*LazyDenseResourcesTest'`.
