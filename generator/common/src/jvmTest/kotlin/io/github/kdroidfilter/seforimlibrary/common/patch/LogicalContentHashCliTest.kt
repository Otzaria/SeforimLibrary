package io.github.kdroidfilter.seforimlibrary.common.patch

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals

class LogicalContentHashCliTest {

    @Test
    fun `writes the shared oracle's whole hash for a DB stamped with its schema`() {
        val fixture = Json.parseToJsonElement(
            requireNotNull(javaClass.getResourceAsStream("/logical_hash_contract.json")) {
                "logical_hash_contract.json missing from test resources"
            }.bufferedReader(Charsets.UTF_8).readText(),
        ).jsonObject
        val dir = createTempDirectory("content-hash")
        try {
            val db = dir.resolve("seforim.db")
            DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
                conn.createStatement().use { st ->
                    fixture.getValue("setupSql").jsonArray.forEach { st.execute(it.jsonPrimitive.content) }
                }
            }
            val out = dir.resolve("content_hash.txt")

            val hash = writeLogicalContentHash(db, out)

            assertEquals(fixture.getValue("wholeHash").jsonPrimitive.content, hash)
            assertEquals(hash + "\n", Files.readString(out))
        } finally {
            dir.toFile().deleteRecursively()
        }
    }
}
