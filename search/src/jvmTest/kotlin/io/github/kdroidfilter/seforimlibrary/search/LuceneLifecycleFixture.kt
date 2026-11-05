package io.github.kdroidfilter.seforimlibrary.search

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.KnnFloatVectorField
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.NoMergePolicy
import org.apache.lucene.index.VectorSimilarityFunction
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.math.sqrt

/** Synthetic content: no external database, model, dictionary or corpus required. */
internal object LuceneLifecycleFixture {
    const val BOOK_COUNT = 8

    fun create(path: Path, lines: Int = 24) = write(path, 1..lines, recreate = true)

    fun write(path: Path, ids: IntRange, recreate: Boolean = false) {
        FSDirectory.open(path).use { directory ->
            StandardAnalyzer().use { analyzer ->
                val config = IndexWriterConfig(analyzer).apply {
                    openMode = if (recreate) IndexWriterConfig.OpenMode.CREATE else IndexWriterConfig.OpenMode.APPEND
                    mergePolicy = NoMergePolicy.INSTANCE
                }
                IndexWriter(directory, config).use { writer ->
                    for (id in ids) {
                        writer.addDocument(line(id))
                        // A stable multi-segment fixture exercises reader reuse, not just one segment.
                        if (id % 512 == 0) writer.commit()
                    }
                    if (recreate) {
                        for (bookId in 1..BOOK_COUNT) writer.addDocument(title(bookId))
                    }
                }
            }
        }
    }

    fun line(id: Int): Document {
        val bookId = (id - 1) % BOOK_COUNT + 1
        val category = (bookId - 1) % 2 + 1
        val base = bookId % 2
        val text = "alpha beta token${id % 11} sample line $id"
        return Document().apply {
            add(StringField("type", "line", Field.Store.NO))
            add(IntPoint("book_id", bookId))
            add(StoredField("book_id", bookId))
            add(StoredField("book_title", "Book $bookId"))
            add(IntPoint("line_id", id))
            add(StoredField("line_id", id))
            add(StoredField("line_index", id - 1))
            add(IntPoint("category_id", category))
            add(StoredField("ancestor_category_ids", "$category,100"))
            add(IntPoint("is_base_book", base))
            add(StoredField("is_base_book", base))
            add(StoredField("order_index", bookId))
            add(TextField("text", text, Field.Store.NO))
            add(StoredField("text_raw", text))
            add(KnnFloatVectorField("vec", vector(id), VectorSimilarityFunction.DOT_PRODUCT))
        }
    }

    fun title(bookId: Int) = Document().apply {
        add(StringField("type", "book_title", Field.Store.NO))
        add(StoredField("book_id", bookId))
        add(TextField("title", "book title $bookId", Field.Store.NO))
    }

    fun vector(id: Int): FloatArray {
        val values = FloatArray(16) { dimension -> (((id * 17 + dimension * 31) % 101) + 1).toFloat() }
        val norm = sqrt(values.sumOf { (it * it).toDouble() }).toFloat()
        return FloatArray(values.size) { values[it] / norm }
    }

    fun delete(path: Path) {
        if (Files.exists(path)) {
            Files.walk(path).use { files ->
                files.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
            }
        }
    }
}
