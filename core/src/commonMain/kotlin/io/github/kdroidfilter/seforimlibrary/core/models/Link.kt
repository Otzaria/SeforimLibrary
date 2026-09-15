package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Link between two texts (commentary, reference, etc.)
 *
 * Storage convention: links are persisted in a single canonical direction
 * `source → target` (base book → dependant book when applicable). The reverse
 * `SOURCE` view is synthesized at read time from links where the line appears
 * as `targetLineId`. `ConnectionType.SOURCE` therefore never appears as a
 * stored row — it is a virtual type produced by the repository.
 *
 * @property id The unique identifier of the link
 * @property sourceBookId The identifier of the source book
 * @property targetBookId The identifier of the target book
 * @property sourceLineId The identifier of the source line
 * @property targetLineId The identifier of the target line
 * @property targetLineIndex The 0-based index of the target line within its book.
 *           Denormalized from `line.lineIndex` so that commentaries can be ordered
 *           by their natural position in the target book without an extra JOIN.
 * @property connectionType The type of connection between the texts
 */
@Serializable
data class Link(
    val id: Long = 0,
    val sourceBookId: Long,
    val targetBookId: Long,
    val sourceLineId: Long,
    val targetLineId: Long,
    val targetLineIndex: Int,
    val connectionType: ConnectionType,
    /**
     * Provenance of this link's base→dependant orientation: 0=NONE,
     * 1=INFERRED_TITLE ("X on Y" title parse), 2=SEFARIA_DECLARED
     * (`base_text_titles`). Used by the SOURCE virtual view's ORDER BY to
     * surface declared bases above inferred ones above lateral citations.
     */
    val baseProvenance: Int = 0,
)

/**
 * Types of connections between texts.
 *
 * Persisted types are everything except [SOURCE]. [SOURCE] is a virtual type
 * exposed by the repository when answering "what does this line comment on?"
 * — it is derived by querying `targetLineId` of stored COMMENTARY/TARGUM/etc.
 * links and swapping source/target columns at read time.
 */
@Serializable
enum class ConnectionType {
    COMMENTARY,
    SUPER_COMMENTARY,
    TARGUM,
    REFERENCE,

    /** Virtual: never stored. Derived by the repository from reverse-direction links. */
    SOURCE,

    MIDRASH,
    QUOTATION,
    MESORAT_HASHAS,
    EIN_MISHPAT,
    DIBUR_HAMATCHIL,
    PARSHANUT,
    MISHNAH_IN_TALMUD,
    RELATED,
    OTHER,

    /**
     * Citations detected by the Sefaria linker (LinkerToOtzaria). A one-directional
     * forward citation layer: source = the citing line (anchor side=0 spans the whole
     * citation phrase), target = the cited Sefaria ref. Appended last to keep existing
     * ordinals — and therefore stable ids — unchanged. Excluded from the SOURCE virtual
     * view (it is not a base/dependant relation). See LINKER_DELTA_PLAN.md.
     */
    LINKER,

    // Named Sefaria connection types, appended after LINKER to keep ids 1–15 stable.
    SIFREI_MITZVOT,
    ESSAY,
    ALLUSION,
    LITURGY,
    ELUCIDATION,
    EXPLICATION,
    LAW,
    SUMMARY,

    /**
     * Footnotes on the base text: a separate "הערות על X" companion whose every
     * entry is anchored to a base line. Behaves as a dependent text (it is shown
     * in the commentary panel like a commentary), but carries its own type so the
     * pairing base↔notes is data rather than a guess at the companion's title.
     * Appended last to keep existing ordinals — and therefore stable ids —
     * unchanged.
     */
    FOOTNOTES,
    ;

    companion object {
        /**
         * Creates a ConnectionType from a string value.
         *
         * Accepts Sefaria's `Conection Type` (sic) CSV values verbatim — case,
         * whitespace and underscore/space variations are normalized. Unknown
         * values fall back to [OTHER].
         */
        fun fromString(value: String): ConnectionType = fromKnownStringOrNull(value) ?: OTHER

        /**
         * Strict variant of [fromString]: returns `null` for unrecognized values
         * instead of falling back to [OTHER]. Empty/`"none"`/`"other"` map to [OTHER].
         */
        fun fromKnownStringOrNull(value: String): ConnectionType? {
            val v = value.trim().lowercase().replace(' ', '_')
            return when (v) {
                "commentary" -> COMMENTARY
                "super_commentary", "supercommentary" -> SUPER_COMMENTARY
                "targum" -> TARGUM
                "reference" -> REFERENCE
                "source" -> SOURCE
                "midrash" -> MIDRASH
                "quotation", "quotation_auto", "quotation_auto_tanakh" -> QUOTATION
                "mesorat_hashas" -> MESORAT_HASHAS
                "ein_mishpat", "ein_mishpat_/_ner_mitsvah", "ein_mishpat_/_ner_mitzvah" -> EIN_MISHPAT
                "dibur_hamatchil" -> DIBUR_HAMATCHIL
                "parshanut" -> PARSHANUT
                "mishnah_in_talmud" -> MISHNAH_IN_TALMUD
                "related", "related_passage" -> RELATED
                "linker" -> LINKER
                "sifrei_mitzvot" -> SIFREI_MITZVOT
                "essay" -> ESSAY
                "allusion" -> ALLUSION
                "liturgy" -> LITURGY
                "ellucidation", "elucidation" -> ELUCIDATION
                "explication" -> EXPLICATION
                "law" -> LAW
                "summary" -> SUMMARY
                "footnotes", "footnote" -> FOOTNOTES
                "", "none", "other" -> OTHER
                else -> null
            }
        }
    }
}
