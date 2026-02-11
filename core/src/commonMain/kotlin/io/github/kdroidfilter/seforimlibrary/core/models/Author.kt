package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a generation / era code (e.g. חז"ל, ראשונים, אחרונים …)
 *
 * @property id The unique identifier of the generation
 * @property name The name of the generation
 */
@Serializable
data class Generation(
    val id: Long = 0,
    val name: String
)

/**
 * Represents a book author in the library
 *
 * @property id The unique identifier of the author
 * @property name The name of the author
 * @property generationId The identifier of the generation/era of this author (nullable)
 * @property generationName The display name of the generation (nullable, populated on read)
 */
@Serializable
data class Author(
    val id: Long = 0,
    val name: String,
    val generationId: Long? = null,
    val generationName: String? = null
)
