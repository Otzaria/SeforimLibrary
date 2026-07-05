package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * One Hebrew edition ("version") of a book. The book's own lines hold Sefaria's
 * merged text (per-segment mosaic by priority).
 *
 * @property id Stable id (IdAllocator, natural key = bookId + versionTitle)
 * @property bookId The book this edition belongs to
 * @property versionTitle Sefaria versionTitle (unique within the book)
 * @property heVersionTitle Hebrew display title, when Sefaria provides one
 * @property versionSource Provenance URL
 * @property priority Sefaria merge priority (higher wins per segment)
 * @property license License string as stored by Sefaria
 * @property versionNotes Edition notes (English)
 * @property heVersionNotes Edition notes (Hebrew)
 * @property hasContent true when the edition's full text is stored in version_line;
 *   false for metadata-only rows (single-version books, where the merged text IS the edition)
 */
@Serializable
data class BookVersion(
    val id: Long,
    val bookId: Long,
    val versionTitle: String,
    val heVersionTitle: String? = null,
    val versionSource: String? = null,
    val priority: Double? = null,
    val license: String? = null,
    val versionNotes: String? = null,
    val heVersionNotes: String? = null,
    val hasContent: Boolean = false,
)

/**
 * Content of one book line in one edition. A missing (versionId, lineId) row
 * means the edition lacks that segment — renderers must show it empty, never
 * fall back to the merged text.
 */
@Serializable
data class VersionLine(
    val versionId: Long,
    val lineId: Long,
    val content: String,
    val charCount: Int,
)
