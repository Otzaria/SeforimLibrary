package io.github.kdroidfilter.seforimlibrary.common.reports

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * Side-channel for generator diagnostics that are too long for the build log.
 *
 * The logging rule for this pipeline is "every noise cut becomes one summary
 * line with counts" — but a summary line is only actionable if the full list it
 * summarises is recoverable somewhere. Findings such as "430 priority entries
 * are missing" or "1,116 metadata records matched no book" therefore log one
 * bounded line and write the complete list here, as a small deterministic JSON
 * file next to the build output.
 *
 * Location: `-DgeneratorReportDir=` / `GENERATOR_REPORT_DIR`, else
 * `build/generator-reports`. Files are written atomically (temp + ATOMIC_MOVE)
 * like the other generator reports, so a crash never leaves a half-written one.
 *
 * A failure to write a report is never fatal: these files are diagnostics, and
 * losing one must not fail a build that otherwise succeeded.
 *
 * JSON is emitted by hand rather than through kotlinx.serialization because
 * `:generator-common` does not depend on it, and because a fixed insertion
 * order keeps the files byte-comparable between builds.
 */
object GeneratorReport {

    const val DIR_PROPERTY: String = "generatorReportDir"
    const val DIR_ENV: String = "GENERATOR_REPORT_DIR"
    const val DEFAULT_DIR: String = "build/generator-reports"

    /** Directory the reports are written to. Not created until something writes. */
    fun directory(): Path {
        val explicit = System.getProperty(DIR_PROPERTY) ?: System.getenv(DIR_ENV)
        return Paths.get(explicit ?: DEFAULT_DIR)
    }

    /**
     * Writes `<directory>/<name>.json`, returning the path, or `null` when the
     * write failed (already logged as a warning).
     *
     * [name] is the bare report name — no extension, no directory.
     */
    fun write(name: String, logger: Logger, build: ReportBuilder.() -> Unit): Path? {
        val body = ReportBuilder().apply(build).build()
        return runCatching {
            val dir = directory().toAbsolutePath()
            Files.createDirectories(dir)
            val target = dir.resolve("$name.json")
            val tmp = Files.createTempFile(dir, name, ".json.tmp")
            Files.writeString(tmp, body)
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            logger.i { "report written to $target" }
            target
        }.onFailure { logger.w(it) { "Failed to write $name report to ${directory()}" } }.getOrNull()
    }
}

/**
 * Builds one report object. Keys keep insertion order; only the shapes the
 * generator reports actually need are supported (scalars, string lists and
 * lists of flat records).
 */
class ReportBuilder internal constructor() {

    private val entries = ArrayList<Pair<String, String>>()

    fun put(key: String, value: String) {
        entries += key to quote(value)
    }

    fun put(key: String, value: Long) {
        entries += key to value.toString()
    }

    fun put(key: String, value: Int) = put(key, value.toLong())

    fun putStrings(key: String, values: Collection<String>) {
        if (values.isEmpty()) {
            entries += key to "[]"
            return
        }
        val body = values.joinToString(separator = ",\n") { "    " + quote(it) }
        entries += key to "[\n$body\n  ]"
    }

    /**
     * A list of flat records. Every value is rendered as a JSON string or, for
     * a [Number], as a JSON number; `null` becomes `null`. Field order follows
     * each map's iteration order, so pass a `LinkedHashMap` (`mapOf(...)` is).
     */
    fun putRows(key: String, rows: Collection<Map<String, Any?>>) {
        if (rows.isEmpty()) {
            entries += key to "[]"
            return
        }
        val body = rows.joinToString(separator = ",\n") { row ->
            val fields = row.entries.joinToString(separator = ", ") { (k, v) ->
                "${quote(k)}: ${scalar(v)}"
            }
            "    { $fields }"
        }
        entries += key to "[\n$body\n  ]"
    }

    internal fun build(): String = buildString {
        append("{\n")
        entries.forEachIndexed { index, (key, rendered) ->
            append("  ").append(quote(key)).append(": ").append(rendered)
            if (index != entries.lastIndex) append(',')
            append('\n')
        }
        append("}\n")
    }

    private fun scalar(value: Any?): String = when (value) {
        null -> "null"
        is Number -> value.toString()
        is Boolean -> value.toString()
        else -> quote(value.toString())
    }

    private fun quote(value: String): String = buildString(value.length + 2) {
        append('"')
        for (ch in value) {
            when (ch) {
                '"' -> append("\\\"")
                '\\' -> append("\\\\")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                // Locale-free: String.format would depend on the runner's locale.
                else -> if (ch < ' ') append("\\u").append(ch.code.toString(16).padStart(4, '0'))
                else append(ch)
            }
        }
        append('"')
    }
}
