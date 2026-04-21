package io.github.kdroidfilter.seforimlibrary.idresolver

import co.touchlab.kermit.Logger
import java.io.File
import java.sql.Connection
import java.sql.DriverManager

/**
 * Loads ID mappings from an existing SQLite database.
 * 
 * This class reads the previous database and builds HashMap lookups
 * for all entity types that need stable IDs across regenerations.
 */
object IdResolverLoader {

    private val logger = Logger.withTag("IdResolverLoader")

    /**
     * Load IdResolver from an existing database file.
     * 
     * @param dbPath Path to the existing SQLite database
     * @return IdResolver instance with loaded mappings, or empty if file doesn't exist
     */
    fun load(dbPath: String): IdResolver {
        val file = File(dbPath)
        if (!file.exists()) {
            logger.i { "No previous DB found at $dbPath; starting with empty IdResolver" }
            return IdResolver.empty()
        }

        println("IdResolverLoader: Loading ID mappings...")
        logger.i { "Loading ID mappings from $dbPath..." }
        val startTime = System.currentTimeMillis()

        val data = loadFromDatabase(dbPath)
        
        val elapsed = System.currentTimeMillis() - startTime
        println("IdResolverLoader: Loaded in ${elapsed}ms. loadedBooks=${data.bookIdMap.size}, loadedTocTexts=${data.tocTextIdMap.size}")
        logger.i { "Loaded ID mappings in ${elapsed}ms" }
        logger.i { "  Books: ${data.bookIdMap.size}" }
        logger.i { "  Lines: ${data.lineIdMap.size}" }
        logger.i { "  Links: ${data.linkIdMap.size}" }
        logger.i { "  TocEntries: ${data.tocEntryIdMap.size}" }
        logger.i { "  TocTexts: ${data.tocTextIdMap.size}" }
        logger.i { "  Categories: ${data.categoryIdMap.size}" }
        logger.i { "  Authors: ${data.authorIdMap.size}" }
        logger.i { "  Generations: ${data.generationIdMap.size}" }
        logger.i { "  Topics: ${data.topicIdMap.size}" }
        logger.i { "  Sources: ${data.sourceIdMap.size}" }
        logger.i { "  ConnectionTypes: ${data.connectionTypeIdMap.size}" }
        logger.i { "  PubPlaces: ${data.pubPlaceIdMap.size}" }
        logger.i { "  PubDates: ${data.pubDateIdMap.size}" }
        logger.i { "  AltTocStructures: ${data.altTocStructureIdMap.size}" }
        
        val resolver = IdResolver.fromLoadedData(data)
        val stats = resolver.getStatistics()
        logger.i { "Estimated memory usage: ${String.format("%.1f", stats.estimatedMemoryUsageMB())} MB" }
        
        return resolver
    }

    private fun loadFromDatabase(dbPath: String): IdResolverData {
        val jdbcUrl = "jdbc:sqlite:$dbPath"
        
        DriverManager.getConnection(jdbcUrl).use { conn ->
            // Enable read-only optimizations
            conn.createStatement().use { stmt ->
                stmt.execute("PRAGMA query_only = ON")
                stmt.execute("PRAGMA cache_size = -64000") // 64MB cache
            }

            return IdResolverData(
                bookIdMap = loadBookIds(conn),
                lineIdMap = loadLineIds(conn),
                linkIdMap = loadLinkIds(conn),
                tocEntryIdMap = loadTocEntryIds(conn),
                tocTextIdMap = loadTocTextIds(conn),
                categoryIdMap = loadCategoryIds(conn),
                authorIdMap = loadAuthorIds(conn),
                generationIdMap = loadGenerationIds(conn),
                topicIdMap = loadTopicIds(conn),
                sourceIdMap = loadSourceIds(conn),
                connectionTypeIdMap = loadConnectionTypeIds(conn),
                pubPlaceIdMap = loadPubPlaceIds(conn),
                pubDateIdMap = loadPubDateIds(conn),
                altTocStructureIdMap = loadAltTocStructureIds(conn),
                altTocEntryIdMap = loadAltTocEntryIds(conn),
                tocEntryIdToTextIdMap = loadTocEntryTextIds(conn),
                tocTextUsageCountMap = loadTocTextUsageCounts(conn),
                maxBookId = getMaxId(conn, "book"),
                maxLineId = getMaxId(conn, "line"),
                maxLinkId = getMaxId(conn, "link"),
                maxTocEntryId = getMaxId(conn, "tocEntry"),
                maxTocTextId = getMaxId(conn, "tocText"),
                maxCategoryId = getMaxId(conn, "category"),
                maxAuthorId = getMaxId(conn, "author"),
                maxGenerationId = getMaxId(conn, "generation"),
                maxTopicId = getMaxId(conn, "topic"),
                maxSourceId = getMaxId(conn, "source"),
                maxConnectionTypeId = getMaxId(conn, "connection_type"),
                maxPubPlaceId = getMaxId(conn, "pub_place"),
                maxPubDateId = getMaxId(conn, "pub_date"),
                maxAltTocStructureId = getMaxId(conn, "alt_toc_structure"),
                maxAltTocEntryId = getMaxId(conn, "alt_toc_entry")
            )
        }
    }

    private fun getMaxId(conn: Connection, tableName: String): Long {
        return try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT COALESCE(MAX(id), 0) FROM $tableName").use { rs ->
                    if (rs.next()) rs.getLong(1) else 0L
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to get max ID for $tableName: ${e.message}" }
            0L
        }
    }

    /**
     * Load book IDs: filePath|sourceId|fileType → id
     * Note: If there are duplicate keys, the first ID is kept (to match the order they were inserted).
     */
    private fun loadBookIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                // Order by id to ensure we take the first (oldest) record for duplicates
                stmt.executeQuery("SELECT id, filePath, sourceId, fileType FROM book ORDER BY id").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val filePath = rs.getString("filePath")
                        val sourceId = rs.getLong("sourceId")
                        val fileType = rs.getString("fileType") ?: "txt"
                        
                        val key = IdResolver.buildBookKey(filePath, sourceId, fileType)
                        // First wins - don't overwrite if key already exists
                        if (key !in map) {
                            map[key] = id
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load book IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load line IDs: bookId|lineIndex → id
     */
    private fun loadLineIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>(6_000_000) // Pre-size for ~6M lines
        try {
            conn.createStatement().use { stmt ->
                // Use batch fetching for large result sets
                stmt.fetchSize = 100_000
                stmt.executeQuery("SELECT id, bookId, lineIndex FROM line").use { rs ->
                    var count = 0
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val bookId = rs.getLong("bookId")
                        val lineIndex = rs.getInt("lineIndex")
                        
                        val key = IdResolver.buildLineKey(bookId, lineIndex)
                        map[key] = id
                        
                        count++
                        if (count % 1_000_000 == 0) {
                            logger.i { "  Loaded $count line IDs..." }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load line IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load link IDs: sourceBookId|targetBookId|sourceLineId|targetLineId|connectionTypeId → id
     */
    private fun loadLinkIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>(7_000_000) // Pre-size for ~6.5M links
        try {
            conn.createStatement().use { stmt ->
                stmt.fetchSize = 100_000
                stmt.executeQuery(
                    "SELECT id, sourceBookId, targetBookId, sourceLineId, targetLineId, connectionTypeId FROM link"
                ).use { rs ->
                    var count = 0
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val sourceBookId = rs.getLong("sourceBookId")
                        val targetBookId = rs.getLong("targetBookId")
                        val sourceLineId = rs.getLong("sourceLineId")
                        val targetLineId = rs.getLong("targetLineId")
                        val connectionTypeId = rs.getLong("connectionTypeId")
                        
                        val key = IdResolver.buildLinkKey(
                            sourceBookId, targetBookId, sourceLineId, targetLineId, connectionTypeId
                        )
                        map[key] = id
                        
                        count++
                        if (count % 1_000_000 == 0) {
                            logger.i { "  Loaded $count link IDs..." }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load link IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load TOC entry IDs: bookId|lineId|level|parentId → id
     */
    private fun loadTocEntryIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>(1_500_000)
        try {
            conn.createStatement().use { stmt ->
                stmt.fetchSize = 100_000
                stmt.executeQuery("SELECT id, bookId, lineId, level, parentId FROM tocEntry").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val bookId = rs.getLong("bookId")
                        val lineId = rs.getLong("lineId")
                        val level = rs.getInt("level")
                        val parentId = rs.getLong("parentId")
                        val parentIdNullable = if (rs.wasNull()) null else parentId
                        
                        val key = IdResolver.buildTocEntryKey(bookId, lineId, level, parentIdNullable)
                        map[key] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load tocEntry IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load TOC text IDs: text → id
     */
    private fun loadTocTextIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, text FROM tocText").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val text = rs.getString("text") ?: continue
                        map[text] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load tocText IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load mapping from TocEntry ID to Text ID.
     */
    private fun loadTocEntryTextIds(conn: Connection): Map<Long, Long> {
        val map = HashMap<Long, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.fetchSize = 50_000
                stmt.executeQuery("SELECT id, textId FROM tocEntry WHERE textId IS NOT NULL").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val textId = rs.getLong("textId")
                        map[id] = textId
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load tocEntry text IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load usage counts for TocText IDs.
     */
    private fun loadTocTextUsageCounts(conn: Connection): Map<Long, Int> {
        val map = HashMap<Long, Int>()
        try {
            conn.createStatement().use { stmt ->
                 // Count how many times each textId is used in tocEntry
                stmt.executeQuery("SELECT textId, COUNT(*) as count FROM tocEntry WHERE textId IS NOT NULL GROUP BY textId").use { rs ->
                    while (rs.next()) {
                        val textId = rs.getLong("textId")
                        val count = rs.getInt("count")
                        map[textId] = count
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load tocText usage counts: ${e.message}" }
        }
        return map
    }

    /**
     * Load category IDs: title|parentId|level → id
     */
    private fun loadCategoryIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, title, parentId, level FROM category").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val title = rs.getString("title") ?: continue
                        val parentId = rs.getLong("parentId")
                        val parentIdNullable = if (rs.wasNull()) null else parentId
                        val level = rs.getInt("level")
                        
                        val key = IdResolver.buildCategoryKey(title, parentIdNullable, level)
                        map[key] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load category IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load author IDs: name → id
     */
    private fun loadAuthorIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM author").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load author IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load generation IDs: name → id
     * Gracefully returns empty map if the table doesn't exist (old DB schema).
     */
    private fun loadGenerationIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM generation").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            // Expected when loading from a DB that predates the generation table
            logger.i { "generation table not found in previous DB (expected for older schemas): ${e.message}" }
        }
        return map
    }

    /**
     * Load topic IDs: name → id
     */
    private fun loadTopicIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM topic").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load topic IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load source IDs: name → id
     */
    private fun loadSourceIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM source").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load source IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load connection type IDs: name → id
     */
    private fun loadConnectionTypeIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM connection_type").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load connection_type IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load publication place IDs: name → id
     */
    private fun loadPubPlaceIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, name FROM pub_place").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val name = rs.getString("name") ?: continue
                        map[name.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load pub_place IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load publication date IDs: date → id
     */
    private fun loadPubDateIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, date FROM pub_date").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val date = rs.getString("date") ?: continue
                        map[date.trim()] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load pub_date IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load alt TOC structure IDs: bookId|key → id
     */
    private fun loadAltTocStructureIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, bookId, key FROM alt_toc_structure").use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val bookId = rs.getLong("bookId")
                        val key = rs.getString("key") ?: continue
                        
                        val mapKey = IdResolver.buildAltTocStructureKey(bookId, key)
                        map[mapKey] = id
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load alt_toc_structure IDs: ${e.message}" }
        }
        return map
    }

    /**
     * Load alt TOC entry IDs: structureId|parentId|level|lineId|text → id
     */
    private fun loadAltTocEntryIds(conn: Connection): Map<String, Long> {
        val map = HashMap<String, Long>()
        try {
            conn.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    SELECT e.id, e.structureId, e.parentId, e.level, e.lineId, tt.text
                    FROM alt_toc_entry e
                    JOIN tocText tt ON tt.id = e.textId
                    ORDER BY e.id
                    """.trimIndent()
                ).use { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val structureId = rs.getLong("structureId")
                        val parentId = rs.getLong("parentId")
                        val parentIdNullable = if (rs.wasNull()) null else parentId
                        val level = rs.getInt("level")
                        val lineId = rs.getLong("lineId")
                        val lineIdNullable = if (rs.wasNull()) null else lineId
                        val text = rs.getString("text") ?: continue

                        val mapKey = IdResolver.buildAltTocEntryKey(
                            structureId = structureId,
                            parentId = parentIdNullable,
                            level = level,
                            lineId = lineIdNullable,
                            text = text
                        )
                        if (mapKey !in map) {
                            map[mapKey] = id
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.w { "Failed to load alt_toc_entry IDs: ${e.message}" }
        }
        return map
    }
}
