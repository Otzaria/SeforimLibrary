plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

// No timing threshold or extra workload in jvmTest. The focused CI workflow
// runs this task only for perf/lucene-* pull requests or a manual dispatch.
// Compare each mode in a fresh JVM, e.g. --args="legacy 10000 1000 1" and
// --args="managed 10000 1000 1". The first argument selects reader lifecycle only.
tasks.register<JavaExec>("benchmarkLuceneSearchers") {
    group = "verification"
    description = "Compare per-request readers with managed searchers on a deterministic synthetic index."
    dependsOn("jvmTestClasses")
    val tests = tasks.named<Test>("jvmTest")
    classpath = tests.get().classpath
    javaLauncher.set(tests.flatMap { it.javaLauncher })
    mainClass.set("io.github.kdroidfilter.seforimlibrary.search.LuceneSearcherBenchmark")
    jvmArgs("-Xms128m", "-Xmx512m", "--enable-native-access=ALL-UNNAMED")
}

group = "io.github.kdroidfilter.seforimlibrary"

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        jvmMain.dependencies {
            api(project(":core"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.lucene.core)
            implementation(libs.lucene.analysis.common)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.kermit)
            implementation(libs.jsoup)
            // Dense semantic search: ONNX Runtime (query embedding) + HuggingFace tokenizer.
            // Stock Maven `onnxruntime` is CPU-only on desktop JVM (no DirectML/CoreML/XNNPACK
            // native — those need a custom build; CUDA needs the separate onnxruntime_gpu).
            implementation(libs.onnxruntime)
            implementation(libs.djl.huggingface.tokenizers)
        }

        jvmTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}
