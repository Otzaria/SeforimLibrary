package io.github.kdroidfilter.seforimlibrary.packaging

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

class DumpLinesTest {

    @Test
    fun `the snapshot is identical for the schema 5 and the schema 6 line shape`() {
        val legacy = dump(split = false)
        val split = dump(split = true)
        assertEquals(listOf("src|Book|0|first|Book 1", "src|Book|1|second|Book"), legacy)
        assertEquals(legacy, split)
    }

    private fun dump(split: Boolean): List<String> {
        val dir = Files.createTempDirectory("dumpLines")
        try {
            val db = dir.resolve("seforim.db")
            val out = dir.resolve("lines_snapshot.db")
            DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st ->
                    st.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                    st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, sourceId INTEGER, title TEXT, heRef TEXT)")
                    st.execute(
                        "CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, lineIndex INTEGER, " +
                            "content TEXT NOT NULL, heRef TEXT)",
                    )
                    st.execute("INSERT INTO source VALUES (1, 'src')")
                    st.execute("INSERT INTO book VALUES (1, 1, 'Book', NULL)")
                    st.execute("INSERT INTO line VALUES (7, 1, 0, 'first', 'Book 1'), (3, 1, 1, 'second', NULL)")
                    if (split) {
                        st.execute("CREATE TABLE line_content (id INTEGER PRIMARY KEY NOT NULL, content TEXT NOT NULL)")
                        st.execute("INSERT INTO line_content SELECT id, content FROM line")
                        st.execute("ALTER TABLE line DROP COLUMN content")
                    }
                }
            }
            withProperties("seforimDb" to db.toString(), "linesSnapshot" to out.toString()) {
                // Several files of this package define a top-level main.
                Class.forName("io.github.kdroidfilter.seforimlibrary.packaging.DumpLinesKt")
                    .getMethod("main", Array<String>::class.java)
                    .invoke(null, arrayOf<String>() as Any)
            }
            return rows(out)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    private fun rows(snapshot: Path): List<String> =
        DriverManager.getConnection("jdbc:sqlite:$snapshot").use { c ->
            c.createStatement().use { st ->
                st.executeQuery(
                    "SELECT source_name, canonical_he_title, line_index, content, context_ref " +
                        "FROM lines_snapshot ORDER BY line_index",
                ).use { rs ->
                    buildList {
                        while (rs.next()) add((1..5).joinToString("|") { rs.getString(it) })
                    }
                }
            }
        }

    private fun withProperties(vararg values: Pair<String, String>, block: () -> Unit) {
        val saved = values.associate { (key, _) -> key to System.getProperty(key) }
        try {
            values.forEach { (key, value) -> System.setProperty(key, value) }
            block()
        } finally {
            saved.forEach { (key, value) -> if (value == null) System.clearProperty(key) else System.setProperty(key, value) }
        }
    }
}
