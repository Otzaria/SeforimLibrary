package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.serialization.Serializable

internal object SefariaImportTuning {
    const val LINE_BATCH_SIZE = 5_000
    const val LINK_BATCH_SIZE = 2_000
    const val FILE_PARALLELISM = 8
}

/**
 * Sefaria's `dependence` flag — the canonical signal for "this book is a
 * dependant text of another book". `null` means the book stands on its own
 * (Tanakh, Talmud, etc.).
 *
 * `OTHER_DEPENDANT` covers values present in the schema that don't have an
 * exact mapping (e.g. "Guides", "Sub-Commentary"). Treated identically to
 * COMMENTARY for orientation purposes — what matters is "dependant or not".
 */
internal enum class Dependence { COMMENTARY, TARGUM, MIDRASH, OTHER_DEPENDANT }

internal data class BookMeta(
    val isBaseBook: Boolean,
    val categoryLevel: Int,
    val priorityRank: Int?,
    // Schema-derived: dependant kind ("Commentary"/"Targum"/...), null = base book.
    val dependence: Dependence? = null,
    // All known base bookIds: starts from Sefaria's declared `base_text_titles`,
    // then gets extended by `inferPrimaryBasesForEmptyDeclaredBookmeta` and by
    // density-based sibling chaining. Used by the resolver to orient links.
    val baseTextBookIds: Set<Long> = emptySet(),
    // **Strict** subset of `baseTextBookIds` — only the bookIds that came from
    // Sefaria's own `base_text_titles` declaration in the schema. Inference and
    // density chaining never mutate this set. Used by the SOURCE virtual view
    // to boost Sefaria-confirmed bases above lateral citations (e.g. Mishnah
    // Avot above Tehillim for Nachalat Avot on Pirkei Avot). → baseProvenance=2.
    val sefariaDeclaredBaseTextBookIds: Set<Long> = emptySet(),
    // Bases recovered from the title's "X on Y" pattern (no `base_text_titles`
    // in schema). Disjoint from the declared set; density chaining never
    // mutates it. → baseProvenance=1 (declared wins if a book is in both).
    val inferredBaseTextBookIds: Set<Long> = emptySet(),
    // Schema-derived: Sefaria's `collective_title.en` — the commentator name shared
    // across all volumes of a multi-volume work (e.g. "Rashi" for "Rashi on Genesis",
    // "Rashi on Exodus"…). Used by the density chain to aggregate per-collective
    // signal so volume-level noise doesn't tip the per-pair ratio.
    val collectiveTitleEn: String? = null,
)

/// Marker value in [BookPayload.cleanShiftByLineIndex]: the line's stored
/// content differs from the raw Sefaria segment (cleanSefariaLine modified
/// it), so raw char offsets cannot be mapped exactly onto it.
internal const val CLEAN_MODIFIED = -1

internal data class BookPayload(
    val heTitle: String,
    val enTitle: String,
    val categoriesHe: List<String>,
    val lines: List<String>,
    val refEntries: List<RefEntry>,
    val headings: List<Heading>,
    val authors: List<String>,
    // Long description (Sefaria heDesc) → book.heDesc
    val description: String?,
    // Short one-line summary (Sefaria heShortDesc) → book.heShortDesc
    val heShortDesc: String?,
    val pubDates: List<PubDate>,
    val altStructures: List<AltStructurePayload>,
    // Schema metadata used for link orientation. The *Keys lists hold the
    // *normalized* titles (en+he) of base texts; resolution to bookIds happens
    // in a second pass once all books have been inserted.
    val dependence: Dependence? = null,
    // Raw `dependence` value (trim + lowercase), persisted to book.dependenceType.
    val rawDependence: String? = null,
    // From schema `base_text_titles` — provenance SEFARIA_DECLARED.
    val declaredBaseTextTitleKeys: List<String> = emptyList(),
    // Recovered from the "X on Y" title pattern — provenance INFERRED_TITLE.
    val inferredBaseTextTitleKeys: List<String> = emptyList(),
    val collectiveTitleHe: String? = null,
    val collectiveTitleEn: String? = null,
    // All Sefaria-known aliases for the book (titleVariants + heTitleVariants),
    // normalized. Indexed alongside the primary titles in normalizedTitleToBookId
    // so that title-pattern base parsing ("X on Y") can resolve "Y" to a bookId
    // when "Y" is a Sefaria-recognised alias (e.g. "Avot" → Pirkei Avot).
    val titleAliasKeys: List<String> = emptyList(),
    // merged.json versions: when the book was merged from exactly ONE version,
    // the merged text is that version verbatim — the precondition for exact
    // charLevelData offset import. null when multi-version (or unknown).
    val singleVersionTitle: String? = null,
    // Sparse per-line offset bookkeeping for charLevelData mapping:
    //   absent          -> stored content == raw segment, no prefix
    //   n >= 0          -> stored content == "<prefix of raw-length n>" + raw
    //   CLEAN_MODIFIED  -> cleanSefariaLine changed the content; offsets unusable
    val cleanShiftByLineIndex: Map<Int, Int> = emptyMap(),
    // All [versionTitle, versionSource] pairs from merged.json's `versions` array
    // (the versions that CONTRIBUTED to the merge). book_version metadata-only
    // fallback when no per-version sibling files exist.
    val versionsMeta: List<VersionMeta> = emptyList(),
    // Book directory (parent of merged.json, where per-version sibling files
    // live) and the resolved schema file — inputs for the versions pass.
    val sourceDirPath: String? = null,
    val schemaFilePath: String? = null,
    // Raw English `categories` from the index record. Only the whole-unit ref
    // sets need them (Sefaria's own gate is English-category based).
    val categoriesEn: List<String> = emptyList(),
)

internal data class VersionMeta(
    val title: String,
    val source: String?,
)

internal data class RefEntry(
    val ref: String,
    val heRef: String,
    val path: String,
    val lineIndex: Int
)

/**
 * A links-CSV `Char Level Data` cell waiting for offset resolution. Collected
 * while streaming the CSVs (where the ref→line resolution and the stored link
 * direction are known) and resolved after all lines/links exist, where the
 * anchored line's stored content is available (see [SefariaCharLevelAnchors]).
 */
internal data class PendingCharLevelAnchor(
    /** bookPath + 0-based lineIndex of the line the offsets refer to. */
    val path: String,
    val lineIndex0: Int,
    /** The stored link row this anchor belongs to (post direction-swap). */
    val srcLineId: Long,
    val tgtLineId: Long,
    /** 0 = the anchored line is the stored source line, 1 = the stored target. */
    val side: Int,
    val startChar: Int,
    val endChar: Int,
    val versionTitle: String,
    val language: String,
    /** Sefaria uses startWord/endWord (word indices) for Tanakh verse sides. */
    val isWordBased: Boolean,
)

internal data class Heading(
    val title: String,
    val level: Int,
    val lineIndex: Int
)

internal data class AltStructurePayload(
    val key: String,
    val title: String?,
    val heTitle: String?,
    val nodes: List<AltNodePayload>
)

internal data class AltNodePayload(
    val title: String?,
    val heTitle: String?,
    val wholeRef: String?,
    val refs: List<String>,
    val addressTypes: List<String>,
    val childLabel: String?,
    val addresses: List<Int>,
    val skippedAddresses: List<Int>,
    val startingAddress: String?,
    val offset: Int?,
    val children: List<AltNodePayload>,
    // True when a `match_templates` entry has scope "any"/"alone" — Sefaria's
    // own predicate for "this node can be cited on its own" (a whole perek).
    val referenceableAlone: Boolean = false
)

@Serializable
internal data class DefaultCommentatorsEntry(
    val book: String,
    val commentators: List<String>
)

@Serializable
internal data class DefaultTargumEntry(
    val book: String,
    val targumim: List<String>
)
