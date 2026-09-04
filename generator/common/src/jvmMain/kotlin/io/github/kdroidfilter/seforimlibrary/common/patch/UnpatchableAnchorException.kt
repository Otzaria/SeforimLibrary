package io.github.kdroidfilter.seforimlibrary.common.patch

/**
 * Thrown by [PatchDbProducer] when a (prev → new) pair cannot be expressed as
 * a delta **at all**, so the release fan must drop this anchor and let those
 * clients fall back to the full bundle.
 *
 * This is deliberately narrow. A column added to the new schema without a
 * `db_schema_version` bump is *not* unpatchable: the producer emits an
 * `ALTER TABLE … ADD COLUMN` migration for it (see
 * `PatchDbProducer.planColumnMigrations`), and a non-unique index that arrives
 * with it is shipped as a plain `CREATE INDEX`. Only these shapes are hopeless:
 *
 *  - a **PRIMARY KEY** column is missing from prev — the upsert/delete join
 *    key itself does not exist there, and `ADD COLUMN` cannot extend a PK;
 *  - a column present in prev was **dropped** from the new schema without a
 *    bump — the patch cannot remove it, so the applied DB could never hash
 *    equal to the target;
 *  - a drifted table gained a **UNIQUE** index or constraint — migrations run
 *    before the upserts, so the index would be built over pre-patch rows (a
 *    freshly added column holds only its back-fill), and a table-level
 *    constraint cannot be added to an existing table at all.
 *
 * [table] and [columns] name the exact offenders so the workflow log points
 * straight at the schema change that needs a `db_schema_version` bump.
 *
 * Exit-code contract: `PatchPipelineCli` catches this, writes a
 * `<out>.unpatchable` marker file next to the patch, and terminates with
 * [EXIT_CODE]. The Gradle `producePatchAndVerify` task treats that single
 * code as "skip this anchor" (task succeeds, marker left behind for the
 * caller) and any other non-zero code as a genuine failure.
 */
class UnpatchableAnchorException(
    val table: String,
    val columns: List<String>,
    message: String,
) : IllegalStateException(message) {
    companion object {
        /** Process exit code reserved for this condition. */
        const val EXIT_CODE: Int = 3

        /** Suffix of the marker file written next to `-Pout` before exiting. */
        const val MARKER_SUFFIX: String = ".unpatchable"
    }
}
