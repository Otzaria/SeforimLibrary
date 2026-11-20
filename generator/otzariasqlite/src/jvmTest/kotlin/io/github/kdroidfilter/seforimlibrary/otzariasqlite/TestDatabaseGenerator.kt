package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.nio.file.Path

/** Stand-in for ForDB's title_acronyms.txt: the forms the tests rely on. */
internal val TEST_TITLE_ACRONYMS = listOf(
    "# fixture",
    "שו״ת", "הש״ס", "עה״ת", "הריטב״א", "להריטב״א", "להרמב״ם",
)

/** [DatabaseGenerator] with [TEST_TITLE_ACRONYMS] unless a test passes its own. */
internal fun testDatabaseGenerator(
    sourceDirectory: Path,
    repository: SeforimRepository,
    acronymDbPath: String? = null,
    filterSourcesForLinks: Boolean = true,
    allocator: IdAllocator = InMemoryIdAllocator.load(path = null),
    buildVersion: Int = 0,
    allowBookIdLoss: Boolean = false,
    inputBufferBytes: Long = configuredGeneratorInputBytes(),
    inputSpillDirectory: Path? = null,
    titleAcronyms: List<String> = TEST_TITLE_ACRONYMS,
) = DatabaseGenerator(
    sourceDirectory = sourceDirectory,
    repository = repository,
    acronymDbPath = acronymDbPath,
    filterSourcesForLinks = filterSourcesForLinks,
    allocator = allocator,
    buildVersion = buildVersion,
    allowBookIdLoss = allowBookIdLoss,
    inputBufferBytes = inputBufferBytes,
    inputSpillDirectory = inputSpillDirectory,
    titleAcronyms = titleAcronyms,
)
