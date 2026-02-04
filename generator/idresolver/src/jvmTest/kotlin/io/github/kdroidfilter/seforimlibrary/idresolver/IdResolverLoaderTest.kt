package io.github.kdroidfilter.seforimlibrary.idresolver

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.io.File

/**
 * Test to verify IdResolverLoader works with an existing DB.
 */
class IdResolverLoaderTest {

    @Test
    fun testLoadFromExistingDb() {
        // Try multiple paths to find the DB
        val possiblePaths = listOf(
            "build/seforim.db",
            "../build/seforim.db",
            "../../build/seforim.db",
            System.getProperty("user.dir") + "/build/seforim.db"
        )
        
        val dbFile = possiblePaths.map { File(it) }.firstOrNull { it.exists() }
        
        if (dbFile == null) {
            println("Skipping test: DB not found in any of: $possiblePaths")
            println("Current dir: ${System.getProperty("user.dir")}")
            return
        }
        
        val dbPath = dbFile.absolutePath
        println("Loading from: $dbPath")
        
        val startTime = System.currentTimeMillis()
        val resolver = IdResolverLoader.load(dbPath)
        val elapsed = System.currentTimeMillis() - startTime
        
        println("Load completed in ${elapsed}ms")
        
        val stats = resolver.getStatistics()
        println("\nIdResolver Statistics:")
        println("  Loaded books: ${stats.loadedBooks}")
        println("  Loaded lines: ${stats.loadedLines}")
        println("  Loaded links: ${stats.loadedLinks}")
        println("  Loaded tocEntries: ${stats.loadedTocEntries}")
        println("  Loaded tocTexts: ${stats.loadedTocTexts}")
        println("  Loaded categories: ${stats.loadedCategories}")
        println("  Loaded authors: ${stats.loadedAuthors}")
        println("  Loaded topics: ${stats.loadedTopics}")
        println("  Loaded sources: ${stats.loadedSources}")
        println("  Loaded connectionTypes: ${stats.loadedConnectionTypes}")
        println("\nEstimated memory usage: ${String.format("%.1f", stats.estimatedMemoryUsageMB())} MB")
        
        // Should have loaded some books
        assertTrue(stats.loadedBooks > 0, "Should have loaded books")
        
        println("\n--- Testing resolution ---")
        
        // Test resolving a book - same key twice should return same ID
        val bookId1 = resolver.resolveBookId("test/book.txt", 1, "txt")
        val bookId2 = resolver.resolveBookId("test/book.txt", 1, "txt")
        assertEquals(bookId1, bookId2, "Same key should return same ID")
        
        println("Test completed!")
    }
}
