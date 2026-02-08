package io.github.kdroidfilter.seforimlibrary.dbversion

import kotlinx.serialization.Serializable

/**
 * Patch entry in the manifest file.
 * Describes a migration from one content version to another.
 */
@Serializable
data class PatchEntry(
    val from: Int,
    val to: Int,
    val file: String,
    val sha256: String
)

/**
 * Manifest file structure for version tracking and patch management.
 * This file is updated each time a new database version is created.
 */
@Serializable
data class Manifest(
    val latest_schema: Int,
    val latest_content: Int,
    val patches: List<PatchEntry> = emptyList()
)
