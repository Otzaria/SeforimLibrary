package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import org.apache.lucene.store.AlreadyClosedException
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Streaming dictionary index backed by SQLite (tables: surface, variant, base).
 * Previously we loaded the entire dictionary into memory; now we stream lookups
 * on-demand with a small LRU cache while preserving the exact expansion shape
 * used by the search ranking logic. Close the index when its owner is finished;
 * closing waits for synchronous lookups and releases every worker's JDBC context.
 */
class MagicDictionaryIndex internal constructor(
    private val norm: (String) -> String,
    private val dbFile: Path,
    private val openConnection: (String) -> Connection = DriverManager::getConnection
) : Closeable {
    data class Expansion(
        val surface: List<String>,
        val variants: List<String>,
        val base: List<String>
    )

    private val url = "jdbc:sqlite:${dbFile.toAbsolutePath()}"
    private val lifecycle = ReentrantReadWriteLock()
    private var closed = false
    private val contexts = ConcurrentHashMap.newKeySet<LookupContext>()

    /**
     * Prepared statement per thread to avoid re-opening connections on every token.
     */
    private val stmtProvider: ThreadLocal<LookupContext> = ThreadLocal.withInitial {
        val conn = openConnection(url)
        var stmt: PreparedStatement? = null
        try {
            conn.autoCommit = false
            // Enforce read-only queries without altering connection flags post-open.
            conn.createStatement().use { pragma -> pragma.execute("PRAGMA query_only=ON") }
            stmt = conn.prepareStatement(LOOKUP_SQL)
            LookupContext(conn, stmt).also { contexts.add(it) }
        } catch (failure: Throwable) {
            try {
                closeLookupResources(conn, stmt)
            } catch (cleanupFailure: Throwable) {
                if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    /**
     * Cache expansions per normalized token to avoid repeated DB hits.
     */
    private val tokenCache = object : LinkedHashMap<String, List<Expansion>>(TOKEN_CACHE_SIZE, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, List<Expansion>>?): Boolean =
            size > TOKEN_CACHE_SIZE
    }

    /**
     * Cache fully-normalized expansions per base id so repeated hits to the same base
     * avoid re-normalizing rows.
     */
    private val baseCache = object : LinkedHashMap<Long, Expansion>(BASE_CACHE_SIZE, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, Expansion>?): Boolean =
            size > BASE_CACHE_SIZE
    }

    fun expansionsFor(tokens: List<String>): List<Expansion> = whileOpen {
        tokens.flatMap { expansionsForToken(it) }.distinct()
    }

    fun expansionFor(token: String): Expansion? = whileOpen {
        val expansions = expansionsForToken(token)
        if (expansions.isEmpty()) return@whileOpen null

        val normalized = norm(token)
        // Strategy: prefer the expansion whose base matches the token
        val matchingBase = expansions.firstOrNull { exp ->
            exp.base.any { it == normalized }
        }
        if (matchingBase != null) return@whileOpen matchingBase

        // Otherwise, prefer the largest expansion (more terms = more complete paradigm)
        expansions.maxByOrNull { it.surface.size }
    }

    private inline fun <T> whileOpen(block: () -> T): T = lifecycle.read {
        if (closed) throw AlreadyClosedException("MagicDictionaryIndex is closed")
        block()
    }

    /**
     * Reject new lookups and release statements and connections from all threads.
     * Existing synchronous lookups finish first; repeated closes are harmless.
     */
    override fun close() {
        lifecycle.write {
            if (closed) return
            closed = true
            var failure: Throwable? = null
            for (context in contexts) {
                try {
                    context.close()
                } catch (cleanupFailure: Throwable) {
                    if (failure == null) failure = cleanupFailure
                    else if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
                }
            }
            contexts.clear()
            stmtProvider.remove()
            tokenCache.clear()
            baseCache.clear()
            failure?.let { throw it }
        }
    }

    private fun expansionsForToken(token: String): List<Expansion> {
        val normalized = norm(token)
        if (normalized.isEmpty()) return emptyList()

        synchronized(tokenCache) {
            tokenCache[normalized]?.let { return it }
        }

        // Try raw, normalized, and final-form variants to match DB values.
        val candidates = buildLookupCandidates(token, normalized)
        val mergedByBase = LinkedHashMap<Long, Expansion>()

        for (candidate in candidates) {
            val fetched = fetchExpansions(candidate, normalized)
            for ((baseId, exp) in fetched) {
                val existing = mergedByBase[baseId]
                if (existing == null) {
                    mergedByBase[baseId] = exp
                } else {
                    val surfaces = (existing.surface + exp.surface).distinct()
                    val variants = (existing.variants + exp.variants).distinct()
                    val base = (existing.base + exp.base).distinct()
                    mergedByBase[baseId] = Expansion(surfaces, variants, base)
                }
            }
        }

        val expansions = mergedByBase.values.toList()
        synchronized(tokenCache) {
            tokenCache[normalized] = expansions
        }
        return expansions
    }

    /**
     * Fetch expansions for a token. Returns a list of (baseId, Expansion) pairs so callers can merge by base id.
     */
    private fun fetchExpansions(rawToken: String, normalizedToken: String): List<Pair<Long, Expansion>> {
        val expansions = mutableListOf<Pair<Long, Expansion>>()
        val ctx = stmtProvider.get()

        runCatching {
            synchronized(ctx) {
                repeat(3) { idx -> ctx.stmt.setString(idx + 1, rawToken) }
                val accum = ctx.stmt.executeQuery().use { rs ->
                    val buckets = mutableMapOf<Long, BaseBucket>()
                    while (rs.next()) {
                        val baseId = rs.getLong("base_id")
                        val bucket = buckets.getOrPut(baseId) {
                            BaseBucket(
                                baseRaw = rs.getString("base") ?: "",
                                surfaces = linkedSetOf(),
                                variants = linkedSetOf()
                            )
                        }
                        rs.getString("surface")?.let { bucket.surfaces += it }
                        rs.getString("variant")?.let { bucket.variants += it }
                    }
                    buckets
                }

                for ((baseId, bucket) in accum) {
                    val cached = synchronized(baseCache) { baseCache[baseId] }
                    if (cached != null) {
                        expansions += baseId to cached
                        continue
                    }

                    val surfaceN = bucket.surfaces.mapNotNull { v -> norm(v).takeIf { it.isNotEmpty() } }
                    val variantsN = bucket.variants.mapNotNull { v -> norm(v).takeIf { it.isNotEmpty() } }
                    val baseN = norm(bucket.baseRaw).takeIf { it.isNotEmpty() }
                        ?: surfaceN.firstOrNull()
                        ?: normalizedToken

                    val baseTerms = listOfNotNull(baseN.takeIf { it.isNotEmpty() })
                    val allTerms = (surfaceN + variantsN + baseTerms).distinct()
                    if (allTerms.isEmpty()) continue

                    val exp = Expansion(
                        surface = allTerms,
                        variants = emptyList(),
                        base = baseTerms
                    )

                    synchronized(baseCache) {
                        baseCache[baseId] = exp
                    }
                    expansions += baseId to exp
                }
            }
        }.onFailure { failure ->
            // A failed statement must not leave a result/transaction pinned or
            // poison this thread's next uncached lookup.
            stmtProvider.remove()
            contexts.remove(ctx)
            try {
                ctx.close()
            } catch (cleanupFailure: Throwable) {
                if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
            }
            logger.d { "[MagicDictionary] Failed to fetch expansions for '$rawToken' : ${failure.message}" }
        }

        return expansions
    }

    companion object {
        private val logger = Logger.withTag("MagicDictionary")
        private const val TOKEN_CACHE_SIZE = 1024
        private const val BASE_CACHE_SIZE = 512

        /**
         * Load from SQLite DB (expected tables: surface(value, base_id), variant(value, surface_id), base(value)).
         * Uses streaming lookup to avoid holding the entire dictionary in memory.
         */
        fun load(norm: (String) -> String, candidate: Path?): MagicDictionaryIndex? {
            val file = candidate?.takeIf { Files.isRegularFile(it) && hasRequiredTables(it) } ?: run {
                if (candidate != null) {
                    logger.d { "[MagicDictionary] Ignoring candidate $candidate because required tables are missing" }
                }
                return null
            }
            return runCatching {
                // Validate DB is reachable
                DriverManager.getConnection("jdbc:sqlite:${file.toAbsolutePath()}").use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.executeQuery("SELECT 1").use { it.next() }
                    }
                }
                logger.d { "[MagicDictionary] Streaming lexical db from $file (lazy on-demand)" }
                MagicDictionaryIndex(norm, file)
            }.onFailure {
                logger.d { "[MagicDictionary] Failed to load from $file : ${it.message}" }
            }.getOrNull()
        }

        /**
         * Find the first candidate path that exists and contains the required tables.
         */
        fun findValidDictionary(candidates: List<Path>): Path? {
            for (candidate in candidates) {
                if (!Files.isRegularFile(candidate)) continue
                if (hasRequiredTables(candidate)) {
                    logger.d { "[MagicDictionary] Using validated lexical db at $candidate" }
                    return candidate
                } else {
                    logger.d {
                        "[MagicDictionary] Candidate $candidate is present but missing required tables; skipping"
                    }
                }
            }
            return null
        }

        private fun hasRequiredTables(file: Path): Boolean = runCatching {
            DriverManager.getConnection("jdbc:sqlite:${file.toAbsolutePath()}").use { conn ->
                val sql = """
                    SELECT name FROM sqlite_master
                    WHERE type = 'table' AND name IN ('surface', 'variant', 'base', 'surface_variant')
                """.trimIndent()
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(sql).use { rs ->
                        val names = mutableSetOf<String>()
                        while (rs.next()) names += rs.getString("name") ?: ""
                        names.containsAll(listOf("surface", "variant", "base", "surface_variant"))
                    }
                }
            }
        }.getOrElse { false }

        private fun closeLookupResources(conn: Connection, stmt: PreparedStatement?) {
            var failure: Throwable? = null
            fun attempt(block: () -> Unit) {
                try {
                    block()
                } catch (cleanupFailure: Throwable) {
                    val previous = failure
                    if (previous == null) failure = cleanupFailure
                    else if (cleanupFailure !== previous) previous.addSuppressed(cleanupFailure)
                }
            }
            attempt { stmt?.close() }
            attempt { if (!conn.autoCommit) conn.rollback() }
            attempt { conn.close() }
            failure?.let { throw it }
        }

        private const val LOOKUP_SQL = """
            WITH matches AS (
                SELECT s.base_id AS base_id FROM surface s WHERE s.value = ?
                UNION
                SELECT b.id FROM base b WHERE b.value = ?
                UNION
                SELECT s.base_id FROM variant v
                JOIN surface_variant sv ON sv.variant_id = v.id
                JOIN surface s ON sv.surface_id = s.id
                WHERE v.value = ?
            )
            SELECT b.id as base_id,
                   b.value as base,
                   s.value as surface,
                   v.value as variant
            FROM base b
            JOIN matches m ON m.base_id = b.id
            LEFT JOIN surface s ON s.base_id = b.id
            LEFT JOIN surface_variant sv ON sv.surface_id = s.id
            LEFT JOIN variant v ON sv.variant_id = v.id
        """
    }

    private class LookupContext(
        val conn: Connection,
        val stmt: PreparedStatement
    ) : Closeable {
        override fun close() = closeLookupResources(conn, stmt)
    }

    private data class BaseBucket(
        val baseRaw: String,
        val surfaces: MutableSet<String>,
        val variants: MutableSet<String>
    )

    private fun buildLookupCandidates(rawToken: String, normalized: String): List<String> {
        val finalsMap = mapOf(
            'כ' to 'ך',
            'מ' to 'ם',
            'נ' to 'ן',
            'פ' to 'ף',
            'צ' to 'ץ'
        )

        fun applyFinalForm(t: String): String {
            if (t.isEmpty()) return t
            val last = t.last()
            val final = finalsMap[last] ?: last
            return if (final == last) t else t.dropLast(1) + final
        }

        return listOf(
            rawToken,
            normalized,
            applyFinalForm(rawToken),
            applyFinalForm(normalized)
        ).filter { it.isNotBlank() }.distinct()
    }

    /**
     * Load all surface forms whose base lemma directly from the underlying SQLite DB.
     * This is used for snippet highlighting of Hashem names, independent of token-level expansions.
     */
    fun loadHashemSurfaces(): List<String> = whileOpen {
        val terms = linkedSetOf<String>()
        runCatching {
            openConnection(url).use { conn ->
                val sql = """
                    SELECT s.value AS surface
                    FROM surface s
                    JOIN base b ON s.base_id = b.id
                    WHERE b.value = 'יהוה'
                """.trimIndent()
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(sql).use { rs ->
                        while (rs.next()) {
                            val v = rs.getString("surface") ?: continue
                            val trimmed = v.trim()
                            if (trimmed.isNotEmpty()) {
                                terms += trimmed
                            }
                        }
                    }
                }
            }
        }.onFailure {
            logger.d { "[MagicDictionary] Failed to load Hashem surfaces: ${it.message}" }
        }
        terms.toList()
    }
}
