package io.github.kdroidfilter.seforimlibrary.common.ids

// Appends one per-parent emitted ordinal to its parent's path, forming the
// (structure_id, ancestor_path) key. Ordinals keep sibling ids ascending —
// the shipped reader orders alt-TOC siblings by alt_toc_entry.id.
fun altTocChildPath(parentPath: String, ordinal: Int): String =
    if (parentPath.isEmpty()) "$ordinal" else "$parentPath/$ordinal"
