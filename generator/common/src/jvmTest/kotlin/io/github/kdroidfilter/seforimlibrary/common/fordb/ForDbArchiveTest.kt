package io.github.kdroidfilter.seforimlibrary.common.fordb

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals

class ForDbArchiveTest {
    @Test
    fun theArchiveYieldsTheTextFilesDirectlyUnderForDb() {
        val bytes = ByteArrayOutputStream().also { out ->
            ZipOutputStream(out).use { zip ->
                for ((name, text) in listOf(
                    "ForDB/title_acronyms.txt" to "שו״ת\nהש״ס",
                    "ForDB/book_info.csv" to "bookName",
                    "ForDB/README.md" to "x",
                    "ForDB/sub/inner.csv" to "x",
                )) {
                    zip.putNextEntry(ZipEntry(name))
                    zip.write(text.toByteArray(Charsets.UTF_8))
                    zip.closeEntry()
                }
            }
        }.toByteArray()
        val files = readForDbZip(ByteArrayInputStream(bytes))
        assertEquals(setOf("title_acronyms.txt", "book_info.csv"), files.keys)
        assertEquals(listOf("שו״ת", "הש״ס"), files.getValue("title_acronyms.txt"))
    }
}
