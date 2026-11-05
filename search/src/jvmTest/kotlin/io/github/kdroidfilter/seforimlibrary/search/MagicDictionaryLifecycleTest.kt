package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.runBlocking
import org.apache.lucene.store.AlreadyClosedException
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MagicDictionaryLifecycleTest {
    private val norm: (String) -> String = { it.trim().lowercase() }

    @Test
    fun `close releases every worker connection before file replacement`() = withDatabase { db ->
        val connections = ConcurrentLinkedQueue<Connection>()
        MagicDictionaryIndex(norm, db) { url ->
            DriverManager.getConnection(url).also { connections.add(it) }
        }.use { index ->
            val pool = Executors.newFixedThreadPool(4)
            val ready = CountDownLatch(4)
            val finish = CountDownLatch(1)
            try {
                val workers = (0 until 4).map { worker ->
                    pool.submit {
                        // Distinct uncached tokens force each worker to use its
                        // own context; a second token must reuse that connection.
                        repeat(2) { assertNull(index.expansionFor("unknown-$worker-$it")) }
                        ready.countDown()
                        check(finish.await(10, TimeUnit.SECONDS))
                        assertFailsWith<AlreadyClosedException> { index.expansionFor("unknown-$worker-0") }
                    }
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS))
                assertEquals(4, connections.size)
                assertTrue(connections.none { it.isClosed })
                index.close()
                index.close()
                assertTrue(connections.all { it.isClosed })
                // Workers remain alive with ThreadLocal slots while Windows
                // must permit the database to be renamed and deleted.
                val moved = Files.move(db, db.resolveSibling("replaced.db"))
                Files.delete(moved)
                finish.countDown()
                workers.forEach { it.get(10, TimeUnit.SECONDS) }
                assertEquals(4, connections.size)
            } finally {
                finish.countDown()
                pool.shutdownNow()
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            }
        }
    }

    @Test
    fun `closed dictionary rejects cached empty and surface lookups without reopening`() = withDatabase { db ->
        val opens = AtomicInteger()
        MagicDictionaryIndex(norm, db) { url ->
            opens.incrementAndGet()
            DriverManager.getConnection(url)
        }.use { index ->
            assertNotNull(index.expansionFor("word1"))
            assertTrue(index.loadHashemSurfaces().isNotEmpty())
            val before = opens.get()
            index.close()
            assertFailsWith<AlreadyClosedException> { index.expansionFor("word1") }
            assertFailsWith<AlreadyClosedException> { index.expansionFor("") }
            assertFailsWith<AlreadyClosedException> { index.expansionsFor(emptyList()) }
            assertFailsWith<AlreadyClosedException> { index.loadHashemSurfaces() }
            assertEquals(before, opens.get())
        }
    }

    @Test
    fun `close waits for a result set being read on another thread`() = withDatabase { db ->
        val entered = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val closeStarted = CountDownLatch(1)
        val closeFinished = CountDownLatch(1)
        val blockOnce = AtomicBoolean(true)
        val connections = ConcurrentLinkedQueue<Connection>()
        val results = ConcurrentLinkedQueue<ResultSet>()
        MagicDictionaryIndex(norm, db) { url ->
            val connection = DriverManager.getConnection(url).also { connections.add(it) }
            intercept(Connection::class.java, connection) { method, _, call ->
                if (method.name != "prepareStatement") call() else {
                    intercept(PreparedStatement::class.java, call() as PreparedStatement) { statementMethod, _, execute ->
                        if (statementMethod.name != "executeQuery") execute() else {
                            val result = (execute() as ResultSet).also { results.add(it) }
                            intercept(ResultSet::class.java, result) { resultMethod, _, read ->
                                if (resultMethod.name == "next" && blockOnce.compareAndSet(true, false)) {
                                    entered.countDown()
                                    check(finish.await(10, TimeUnit.SECONDS))
                                }
                                read()
                            }
                        }
                    }
                }
            }
        }.use { index ->
            val pool = Executors.newFixedThreadPool(2)
            try {
                val lookup = pool.submit<MagicDictionaryIndex.Expansion?> { index.expansionFor("word1") }
                assertTrue(entered.await(10, TimeUnit.SECONDS))
                val closing = pool.submit {
                    closeStarted.countDown()
                    index.close()
                    closeFinished.countDown()
                }
                assertTrue(closeStarted.await(10, TimeUnit.SECONDS))
                assertFalse(closeFinished.await(100, TimeUnit.MILLISECONDS))
                assertFalse(connections.single().isClosed)
                finish.countDown()
                assertNotNull(lookup.get(10, TimeUnit.SECONDS))
                closing.get(10, TimeUnit.SECONDS)
                assertTrue(results.single().isClosed)
                assertTrue(connections.single().isClosed)
            } finally {
                finish.countDown()
                pool.shutdownNow()
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            }
        }
    }

    @Test
    fun `failed context initialization closes its connection and permits retry`() = withDatabase { db ->
        for (stage in listOf("setAutoCommit", "pragma", "prepareStatement")) {
            val failOnce = AtomicBoolean(true)
            val connections = mutableListOf<Connection>()
            MagicDictionaryIndex(norm, db) { url ->
                val connection = DriverManager.getConnection(url).also { connections.add(it) }
                intercept(Connection::class.java, connection) { method, _, call ->
                    when {
                        method.name == stage && failOnce.compareAndSet(true, false) -> throw SQLException(stage)
                        method.name == "createStatement" -> {
                            intercept(Statement::class.java, call() as Statement) { statementMethod, _, execute ->
                                if (stage == "pragma" && statementMethod.name == "execute" && failOnce.compareAndSet(true, false)) {
                                    throw SQLException(stage)
                                }
                                execute()
                            }
                        }
                        else -> call()
                    }
                }
            }.use { index ->
                assertEquals(stage, assertFailsWith<SQLException> { index.expansionFor("word1") }.message)
                assertTrue(connections.single().isClosed, stage)
                assertNotNull(index.expansionFor("word1"))
                assertNotNull(index.expansionFor("word2"))
                assertEquals(2, connections.size, "retry should create one reusable context")
            }
            assertTrue(connections.all { it.isClosed }, stage)
        }
    }

    @Test
    fun `failed result consumption closes the result and discards the broken context`() = withDatabase { db ->
        val failOnce = AtomicBoolean(true)
        val connections = mutableListOf<Connection>()
        val results = mutableListOf<ResultSet>()
        MagicDictionaryIndex(norm, db) { url ->
            val connection = DriverManager.getConnection(url).also { connections.add(it) }
            intercept(Connection::class.java, connection) { method, _, call ->
                if (method.name != "prepareStatement") call() else {
                    intercept(PreparedStatement::class.java, call() as PreparedStatement) { statementMethod, _, execute ->
                        if (statementMethod.name != "executeQuery") execute() else {
                            val result = (execute() as ResultSet).also { results.add(it) }
                            intercept(ResultSet::class.java, result) { resultMethod, _, read ->
                                if (resultMethod.name == "next" && failOnce.compareAndSet(true, false)) {
                                    throw SQLException("result read failed")
                                }
                                read()
                            }
                        }
                    }
                }
            }
        }.use { index ->
            // Preserve the existing best-effort lookup behavior on a SQL error.
            assertNull(index.expansionFor("word1"))
            assertTrue(results.single().isClosed)
            assertTrue(connections.single().isClosed)
            assertNotNull(index.expansionFor("word2"))
            assertEquals(2, connections.size)
            assertTrue(results.all { it.isClosed })
        }
        assertTrue(connections.all { it.isClosed })
    }

    @Test
    fun `cleanup failures do not prevent closing the remaining worker connections`() = withDatabase { db ->
        val connections = ConcurrentLinkedQueue<Connection>()
        val statementCloses = AtomicInteger()
        val rollbacks = AtomicInteger()
        val connectionCloses = AtomicInteger()
        MagicDictionaryIndex(norm, db) { url ->
            val connection = DriverManager.getConnection(url).also { connections.add(it) }
            intercept(Connection::class.java, connection) { method, _, call ->
                when (method.name) {
                    "prepareStatement" -> intercept(PreparedStatement::class.java, call() as PreparedStatement) { statementMethod, _, execute ->
                        val result = execute()
                        if (statementMethod.name == "close") {
                            statementCloses.incrementAndGet()
                            throw SQLException("statement close failed")
                        }
                        result
                    }
                    "rollback" -> {
                        call()
                        rollbacks.incrementAndGet()
                        throw SQLException("rollback failed")
                    }
                    "close" -> {
                        connectionCloses.incrementAndGet()
                        call()
                    }
                    else -> call()
                }
            }
        }.use { index ->
            val pool = Executors.newFixedThreadPool(2)
            val ready = CountDownLatch(2)
            val finish = CountDownLatch(1)
            try {
                val workers = (0 until 2).map { worker ->
                    pool.submit {
                        index.expansionFor("unknown-$worker")
                        ready.countDown()
                        check(finish.await(10, TimeUnit.SECONDS))
                    }
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS))
                val failure = assertFailsWith<SQLException> { index.close() }
                assertTrue(failure.suppressed.isNotEmpty())
                assertEquals(2, statementCloses.get())
                assertEquals(2, rollbacks.get())
                assertEquals(2, connectionCloses.get())
                assertTrue(connections.all { it.isClosed })
                index.close()
                assertEquals(2, connectionCloses.get(), "close remains idempotent after reporting cleanup errors")
                finish.countDown()
                workers.forEach { it.get(10, TimeUnit.SECONDS) }
            } finally {
                finish.countDown()
                pool.shutdownNow()
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            }
        }
    }

    @Test
    fun `engine closes an initialized dictionary while a cursor retains its index snapshot`() = withDatabase { db ->
        val indexPath = db.parent.resolve("index")
        LuceneLifecycleFixture.create(indexPath)
        LuceneSearchEngine(indexPath, dictionaryPath = db).use { engine ->
            assertNotNull(engine.openSession("alpha")).use { cursor ->
                engine.buildSnippet("השם", "ה׳")
                engine.close()
                val moved = Files.move(db, db.resolveSibling("replaced.db"))
                Files.delete(moved)
                assertEquals(24, runBlocking { assertNotNull(cursor.nextPage(100)).hits.size })
                assertFailsWith<AlreadyClosedException> { engine.buildSnippet("השם", "ה׳") }
            }
        }
    }

    @Test
    fun `closing an unused engine never probes or initializes its dictionary`() = withDatabase { db ->
        val probes = AtomicInteger()
        val indexPath = observeDictionaryProbe(db.parent.resolve("unused-index")) { probes.incrementAndGet() }
        LuceneSearchEngine(indexPath, dictionaryPath = db).use { engine ->
            engine.close()
            assertEquals(0, probes.get())
            assertFailsWith<AlreadyClosedException> { engine.openSession("word1") }
            assertFailsWith<AlreadyClosedException> { engine.buildSnippet("השם", "ה׳") }
            assertEquals(0, probes.get(), "a closed engine must not perform a late lazy load")
        }
    }

    @Test
    fun `engine close waits for dictionary initialization instead of missing its publication`() = withDatabase { db ->
        val entered = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val closeStarted = CountDownLatch(1)
        val closeFinished = CountDownLatch(1)
        val blockOnce = AtomicBoolean(true)
        val indexPath = observeDictionaryProbe(db.parent.resolve("unused-index")) {
            if (blockOnce.compareAndSet(true, false)) {
                entered.countDown()
                check(finish.await(10, TimeUnit.SECONDS))
            }
        }
        LuceneSearchEngine(indexPath, dictionaryPath = db).use { engine ->
            val pool = Executors.newFixedThreadPool(2)
            try {
                val lookup = pool.submit {
                    try {
                        engine.buildSnippet("השם", "ה׳")
                    } catch (_: AlreadyClosedException) {
                        // close may win before the first dictionary lookup.
                    }
                }
                assertTrue(entered.await(10, TimeUnit.SECONDS))
                val closing = pool.submit {
                    closeStarted.countDown()
                    engine.close()
                    closeFinished.countDown()
                }
                assertTrue(closeStarted.await(10, TimeUnit.SECONDS))
                assertFalse(closeFinished.await(100, TimeUnit.MILLISECONDS))
                finish.countDown()
                lookup.get(10, TimeUnit.SECONDS)
                closing.get(10, TimeUnit.SECONDS)
                assertFailsWith<AlreadyClosedException> { engine.buildSnippet("השם", "ה׳") }
                val moved = Files.move(db, db.resolveSibling("replaced.db"))
                Files.delete(moved)
            } finally {
                finish.countDown()
                pool.shutdownNow()
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
            }
        }
    }

    private fun observeDictionaryProbe(path: Path, probe: () -> Unit): Path = object : Path by path {
        override fun resolveSibling(other: String): Path {
            probe()
            return path.resolveSibling(other)
        }
    }

    private fun <T> intercept(
        type: Class<T>,
        target: T,
        block: (Method, Array<out Any?>?, () -> Any?) -> Any?
    ): T = type.cast(Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, args ->
        block(method, args) {
            try {
                method.invoke(target, *(args ?: emptyArray()))
            } catch (failure: InvocationTargetException) {
                throw failure.targetException
            }
        }
    })

    private inline fun withDatabase(block: (Path) -> Unit) {
        val parent = Files.createTempDirectory("dictionary-lifecycle")
        val db = parent.resolve("lexical.db")
        try {
            DriverManager.getConnection("jdbc:sqlite:$db").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
                    statement.execute("CREATE TABLE surface (id INTEGER PRIMARY KEY, value TEXT NOT NULL, base_id INTEGER NOT NULL)")
                    statement.execute("CREATE TABLE variant (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
                    statement.execute("CREATE TABLE surface_variant (surface_id INTEGER NOT NULL, variant_id INTEGER NOT NULL)")
                    statement.execute("INSERT INTO base VALUES (1, 'root1'), (2, 'root2'), (3, 'יהוה')")
                    statement.execute("INSERT INTO surface VALUES (1, 'word1', 1), (2, 'word2', 2), (3, 'השם', 3)")
                    statement.execute("INSERT INTO variant VALUES (1, 'variant1')")
                    statement.execute("INSERT INTO surface_variant VALUES (1, 1)")
                }
            }
            block(db)
        } finally {
            LuceneLifecycleFixture.delete(parent)
        }
    }
}
