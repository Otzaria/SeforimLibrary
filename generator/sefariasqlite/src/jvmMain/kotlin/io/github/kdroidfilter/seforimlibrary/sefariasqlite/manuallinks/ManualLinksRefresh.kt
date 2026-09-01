package io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks

import co.touchlab.kermit.Logger
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.io.path.relativeTo

internal enum class ManualLinksMode { BOOTSTRAP, REFRESH, MIGRATE }

internal data class ManualLinksArguments(
    val mode: ManualLinksMode,
    val repository: Path,
    val configPath: Path,
    val lineagePath: Path?,
    val expectedOldConfigSha256: String?,
    val expectedOldToolCommit: String?,
    val sefariaExport: Path,
    val releaseMetadataPath: Path,
    val releaseMetadataSha256: String,
    val changelogDir: Path?,
    val seforimToolCommit: String,
    val output: Path,
)

internal data class ManualLinksCounters(
    var filesScanned: Int = 0,
    var filesChanged: Int = 0,
    var filesRenamed: Int = 0,
    var recordsScanned: Int = 0,
    var relevant: Int = 0,
    var sourceRelevant: Int = 0,
    var targetRelevant: Int = 0,
    var irrelevant: Int = 0,
    var unchanged: Int = 0,
    var shifted: Int = 0,
    var enriched: Int = 0,
    var refsRenamed: Int = 0,
    var anchorsChecked: Int = 0,
)

internal data class ManualLinksResult(
    val status: String,
    val lineage: ManualLinksLineage,
    val reportPath: Path,
    val markerPath: Path,
)

internal class ManualLinksRefresh(
    private val arguments: ManualLinksArguments,
    private val logger: Logger,
) {
    private val counters = ManualLinksCounters()
    private lateinit var config: ManualLinksConfig
    private lateinit var index: SefariaCorpusIndex
    private lateinit var titleAliases: SefariaTitleAliases
    private val documents = LinkedHashMap<String, ManualLinksDocument>()
    private val usedOverrides = LinkedHashSet<Pair<String, String>>()
    private var currentFailureFile: String? = null
    private var currentFailureRecordIndex: Int? = null
    private var currentFailureRecordHash: String? = null

    fun run(): ManualLinksResult = try {
        runInternal()
    } catch (error: Throwable) {
        runCatching { writeFailureReport(error) }
        throw error
    }

    private fun runInternal(): ManualLinksResult {
        validateInputs()
        config = ManualLinksConfig.read(arguments.configPath)
        titleAliases = SefariaTitleAliases(config.heTitleAliases)
        val configSha = ManualLinksJson.rawSha256(arguments.configPath)
        val releaseDigest = ManualLinksJson.rawSha256(arguments.releaseMetadataPath)
        require(releaseDigest == arguments.releaseMetadataSha256) { "Sefaria release metadata digest mismatch" }
        val metadata = ReleaseMetadata.read(arguments.releaseMetadataPath)
        val inputLineage = readInputLineage()
        validateMigrationGates(inputLineage, configSha, metadata)
        val chain = ManualLinksChangelog.verifiedChain(
            targetMetadataPath = arguments.releaseMetadataPath,
            targetMetadataSha256 = releaseDigest,
            target = metadata,
            base = inputLineage,
            changelogDir = arguments.changelogDir,
        )

        ManualLinksTreeHash.copyConfiguredRoots(arguments.repository, arguments.output, config)
        val mayBootstrap = inputLineage == null || arguments.mode == ManualLinksMode.MIGRATE
        if (mayBootstrap) applyBootstrapFileRenames()
        loadDocuments()
        if (chain.isNotEmpty()) applyRenameChain(chain)

        val candidates = collectCandidateTitles(chain)
        val requiresTashmaProof = config.linksRoots.any {
            it.expectedState == ExpectedState.PRESENT && it.path == "tashmaToOtzaria/links"
        } && mayBootstrap
        index = SefariaCorpusIndex.load(
            exportRoot = arguments.sefariaExport,
            candidates = candidates,
            retainedLines = collectRetainedLineRequirements(),
            fullLineTitles = if (requiresTashmaProof) setOf("משנה ברורה") else emptySet(),
            logger = logger,
        )
        titleAliases.requireEachAliasResolvesToOneBook(index)
        titleAliases.requireEveryAliasIsUsed()
        val tashmaProof = if (requiresTashmaProof) {
            proveTashmaVector()
        } else null
        processDocuments(initialBootstrap = inputLineage == null, mayBootstrap = mayBootstrap, tashmaProof = tashmaProof)
        persistChangedDocuments()

        val scan = ManualLinksTreeHash.scan(arguments.output, config)
        val outputLineage = ManualLinksLineage(
            sefariaTag = metadata.tag,
            releaseMetadataSha256 = releaseDigest,
            runId = metadata.runId,
            runAttempt = metadata.runAttempt,
            archiveSha256 = metadata.archiveSha256,
            archiveSize = metadata.archiveSize,
            archiveParts = metadata.archiveParts,
            appliedChangelogChain = if (chain.isEmpty() && inputLineage?.sefariaTag == metadata.tag) {
                inputLineage.appliedChangelogChain
            } else {
                chain.map { verified ->
                    AppliedChangelog(
                        tag = verified.metadata.tag,
                        metadataSha256 = verified.metadataSha256,
                        previous = verified.metadata.previous,
                        changelogName = verified.metadata.changelog.name,
                        changelogSha256 = verified.metadata.changelog.sha256,
                    )
                }
            },
            seforimToolCommit = arguments.seforimToolCommit,
            sourceLinksTreeSha256 = scan.sourceTreeSha256,
            packagedLinksTreeSha256 = scan.packagedTreeSha256,
            configSha256 = configSha,
        )
        val status = if (inputLineage != null && inputLineage == outputLineage &&
            counters.filesChanged == 0 && counters.filesRenamed == 0
        ) "no_op" else "ok"
        if (arguments.mode == ManualLinksMode.BOOTSTRAP && inputLineage != null) {
            require(status == "no_op") { "bootstrap with existing lineage is restricted to an exact idempotent no-op" }
        }
        val lineagePath = arguments.output.resolve("manual_links_lineage.json")
        Files.write(lineagePath, outputLineage.canonicalBytes())
        logger.i {
            "Manual-links reader: scanned=${index.stats.mergedFilesScanned}, loaded=${index.stats.payloadsLoaded}, " +
                "retainedAnchors=${index.stats.retainedAnchorLines}, fullProofLines=${index.stats.fullLinesLoaded}, " +
                "peakRssBytes=${peakRssBytes()}"
        }
        val reportPath = writeReport(status, inputLineage, outputLineage)
        val markerPath = writeMarker(status, lineagePath, reportPath)
        return ManualLinksResult(status, outputLineage, reportPath, markerPath)
    }

    private fun validateInputs() {
        require(arguments.repository.isAbsolute && Files.isDirectory(arguments.repository)) { "manualLinksRepo must be a directory" }
        require(arguments.configPath.isRegularFile()) { "manualLinksConfig missing" }
        require(Files.isDirectory(arguments.sefariaExport)) { "sefariaExport must be a directory" }
        require(arguments.releaseMetadataPath.isRegularFile()) { "sefariaReleaseMetadata missing" }
        require(arguments.releaseMetadataSha256.matches(Regex("[0-9a-f]{64}"))) { "Invalid metadata SHA-256" }
        require(arguments.seforimToolCommit.matches(Regex("[0-9a-f]{40}"))) { "seforimToolCommit must be a full commit" }
        arguments.lineagePath?.let { require(it.isRegularFile()) { "manualLinksLineage missing" } }
        arguments.changelogDir?.let { require(Files.isDirectory(it)) { "sefariaChangelogDir must be a directory" } }
    }

    private fun readInputLineage(): ManualLinksLineage? {
        val explicit = arguments.lineagePath
        if (explicit != null) return ManualLinksLineage.read(explicit)
        if (arguments.mode == ManualLinksMode.BOOTSTRAP) {
            val existing = arguments.repository.resolve("manual_links_lineage.json")
            if (existing.isRegularFile()) return ManualLinksLineage.read(existing)
        }
        return null
    }

    private fun validateMigrationGates(input: ManualLinksLineage?, configSha: String, metadata: ReleaseMetadata) {
        when (arguments.mode) {
            ManualLinksMode.BOOTSTRAP -> if (input != null) {
                require(input.configSha256 == configSha) { "bootstrap with lineage requires identical config" }
                require(input.seforimToolCommit == arguments.seforimToolCommit) {
                    "bootstrap with lineage requires identical tool commit"
                }
                require(input.sefariaTag == metadata.tag && input.releaseMetadataSha256 == arguments.releaseMetadataSha256) {
                    "bootstrap with lineage requires the identical Sefaria target"
                }
            }
            ManualLinksMode.REFRESH -> {
                requireNotNull(input) { "refresh requires lineage" }
                require(input.configSha256 == configSha) { "config_drift: use migrate" }
                if (input.sefariaTag == metadata.tag) {
                    require(input.seforimToolCommit == arguments.seforimToolCommit) {
                        "Changing the tool commit for the same Sefaria target requires migrate"
                    }
                }
            }
            ManualLinksMode.MIGRATE -> {
                requireNotNull(input) { "migrate requires lineage" }
                require(arguments.expectedOldConfigSha256 == input.configSha256) { "expectedOldConfigSha256 mismatch" }
                require(arguments.expectedOldToolCommit == input.seforimToolCommit) { "expectedOldToolCommit mismatch" }
                require(input.sefariaTag == metadata.tag && input.releaseMetadataSha256 == arguments.releaseMetadataSha256) {
                    "migrate may not silently change the Sefaria target"
                }
            }
        }
    }

    private fun applyBootstrapFileRenames() {
        config.bootstrapFileRenames.forEach { rename ->
            val source = arguments.output.resolve(rename.from)
            val target = arguments.output.resolve(rename.to)
            require(arguments.repository.resolve(rename.localBookPath).isRegularFile()) { "Missing local book proof: ${rename.localBookPath}" }
            require(target.name == "${rename.expectedDbTitle}_links.json") { "Rename target does not match expected_db_title" }
            when {
                source.exists() && !target.exists() -> {
                    Files.move(source, target)
                    counters.filesRenamed++
                }
                !source.exists() && target.exists() -> Unit
                else -> error("Invalid bootstrap rename state: ${rename.from} -> ${rename.to}")
            }
        }
    }

    private fun loadDocuments() {
        val scan = ManualLinksTreeHash.scan(arguments.output, config)
        scan.files.filter { it.source.name.endsWith("_links.json") }.forEach { file ->
            documents[file.repositoryPath] = ManualLinksDocument.read(file.source)
            counters.filesScanned++
        }
    }

    private fun applyRenameChain(chain: List<VerifiedChangelog>) {
        chain.flatMap { it.renames }.forEach { event ->
            if (event.oldEn != null && event.newEn != null && event.oldEn != event.newEn) {
                documents.forEach { (_, document) ->
                    repeat(document.records.size()) { index ->
                        listOf("ref_1", "ref_2").forEach { field ->
                            val value = document.record(index).get(field)?.takeIf { it.isTextual }?.textValue() ?: return@forEach
                            rewriteAtTitleBoundary(value, event.oldEn, event.newEn)?.let { rewritten ->
                                document.setString(index, field, rewritten)
                                counters.refsRenamed++
                            }
                        }
                    }
                }
            }
            if (event.oldHe != null && event.newHe != null && event.oldHe != event.newHe) {
                applyHebrewRename(event.oldHe, event.newHe)
            } else if (event.oldEn != null && event.newEn != null &&
                (event.oldHe == null || event.newHe == null) && documents.values.any { doc ->
                    (0 until doc.records.size()).any { i ->
                        listOf("ref_1", "ref_2").any { field -> doc.record(i).get(field)?.asText()?.startsWith(event.newEn) == true }
                    }
                }
            ) {
                error("English rename touching manual links is missing old_he/new_he")
            }
        }
    }

    private fun applyHebrewRename(oldHe: String, newHe: String) {
        val fileRenames = documents.keys.mapNotNull { repositoryPath ->
            val document = documents.getValue(repositoryPath)
            if (sourceTitle(Path.of(repositoryPath).name) == oldHe) {
                requireHebrewRenameRef(
                    hasRequiredRef = (0 until document.records.size()).all { document.record(it).has("ref_1") },
                    failure = "$repositoryPath: Hebrew source rename requires ref_1 on every record; new_source_ref_required",
                )
                repositoryPath to Path.of(repositoryPath).parent.resolve("${newHe}_links.json").toString().replace('\\', '/')
            } else null
        }
        fileRenames.forEach { (oldPath, newPath) ->
            require(newPath !in documents && !arguments.output.resolve(newPath).exists()) { "Hebrew source rename target exists: $newPath" }
            Files.move(arguments.output.resolve(oldPath), arguments.output.resolve(newPath))
            documents[newPath] = documents.remove(oldPath)!!
            counters.filesRenamed++
        }
        documents.forEach { (repositoryPath, document) ->
            repeat(document.records.size()) { index ->
                val record = document.record(index)
                val path = record.get("path_2").textValue()
                val renamedPath = replaceFinalPathComponent(path, "$oldHe.txt", "$newHe.txt")
                val heRef = record.get("heRef_2").textValue()
                val renamedHeRef = rewriteAtTitleBoundary(heRef, oldHe, newHe)
                if ((renamedPath != null || renamedHeRef != null) && !record.has("ref_2")) {
                    requireHebrewRenameRef(false, "Hebrew target rename requires ref_2; new_target_ref_required")
                }
                if (renamedPath != null) {
                    requireNotNull(renamedHeRef) { "path_2 was renamed but heRef_2 did not have the exact old title boundary" }
                    document.setString(index, "path_2", renamedPath)
                    document.setString(index, "heRef_2", renamedHeRef)
                } else if (renamedHeRef != null) {
                    error(hebrewTargetRenameFailure(repositoryPath, index, oldHe, newHe))
                }
            }
        }
    }

    /** path_2 keeps the Otzaria title, so an aliased target rename is a config edit, never an automatic rewrite. */
    private fun hebrewTargetRenameFailure(repositoryPath: String, recordIndex: Int, oldHe: String, newHe: String): String {
        val otzariaTitle = titleAliases.otzariaTitleFor(repositoryPath, oldHe)
            ?: return "$repositoryPath[$recordIndex] heRef_2 has the old Hebrew title but path_2 does not"
        return "$repositoryPath[$recordIndex]: Sefaria renamed '$oldHe' to '$newHe' but path_2 keeps the Otzaria " +
            "title '$otzariaTitle'; repoint he_title_aliases['$otzariaTitle'] to '$newHe' and re-run in migrate mode"
    }

    private fun collectCandidateTitles(chain: List<VerifiedChangelog>): Set<String> = buildSet {
        documents.forEach { (path, document) ->
            add(titleAliases.sefariaHeTitle(path, sourceTitle(Path.of(path).name)))
            repeat(document.records.size()) { index ->
                targetTitleOrNull(document.record(index).get("path_2").textValue())
                    ?.let { add(titleAliases.sefariaHeTitle(path, it)) }
            }
        }
        chain.flatMap { it.renames }.forEach { event ->
            event.oldHe?.let(::add)
            event.newHe?.let(::add)
        }
    }

    private fun collectRetainedLineRequirements(): Map<String, RetainedLineRequirements> {
        data class MutableRequirements(
            val refs: MutableSet<String> = linkedSetOf(),
            val lineIndexes: MutableSet<Int> = linkedSetOf(),
        )

        val requirements = linkedMapOf<String, MutableRequirements>()
        documents.forEach { (path, document) ->
            val title = titleAliases.sefariaHeTitle(path, sourceTitle(Path.of(path).name))
            repeat(document.records.size()) { recordIndex ->
                val record = document.record(recordIndex)
                if (!record.has("start")) return@repeat
                val target = requirements.getOrPut(title, ::MutableRequirements)
                val ref = record.get("ref_1")
                if (ref != null) {
                    require(ref.isTextual && ref.textValue().isNotEmpty()) { "$path[$recordIndex] ref_1 must be text" }
                    target.refs += ref.textValue()
                } else {
                    target.lineIndexes += ManualLinksDocument.exactInt(record.get("line_index_1"), "line_index_1")
                }
            }
        }
        return requirements.mapValues { (_, value) ->
            RetainedLineRequirements(refs = value.refs, lineIndexes = value.lineIndexes)
        }
    }

    private fun processDocuments(initialBootstrap: Boolean, mayBootstrap: Boolean, tashmaProof: TashmaProof?) {
        documents.forEach { (path, document) ->
            repeat(document.records.size()) { recordIndex ->
                counters.recordsScanned++
                val record = document.record(recordIndex)
                currentFailureFile = path
                currentFailureRecordIndex = recordIndex
                currentFailureRecordHash = document.stableRecordHash(recordIndex)
                val before = ManualLinksJson.canonicalString(record.deepCopy())
                require(!(record.has("ref_1") && record.has("ref_2"))) { "$path[$recordIndex] has both ref_1 and ref_2" }
                val sourceTitle = titleAliases.sefariaHeTitle(path, sourceTitle(Path.of(path).name))
                val targetTitle = targetTitleOrNull(record.get("path_2").textValue())
                    ?.let { titleAliases.sefariaHeTitle(path, it) }
                val sourceCount = index.primaryHeTitleCount(sourceTitle)
                val targetCount = targetTitle?.let(index::primaryHeTitleCount) ?: 0
                require(sourceCount <= 1) { "$path[$recordIndex] source book is ambiguous" }
                require(targetCount <= 1) { "$path[$recordIndex] target book is ambiguous" }
                val sourceIsSefaria = sourceCount == 1
                val targetIsSefaria = targetCount == 1
                val managedSource = record.has("ref_1") || sourceIsSefaria
                val managedTarget = record.has("ref_2") || targetIsSefaria
                require(!record.has("end") || (!managedSource && !managedTarget)) {
                    "$path[$recordIndex]: unsupported_sefaria_range"
                }
                require(!record.has("start") || managedSource) {
                    "$path[$recordIndex]: start is allowed only on a Sefaria source with an anchor hash"
                }

                when {
                    record.has("ref_1") -> {
                        require(sourceIsSefaria && !targetIsSefaria) { "$path[$recordIndex] ref_1 side classification changed" }
                        counters.relevant++
                        counters.sourceRelevant++
                        processSource(document, recordIndex, sourceTitle, tashmaProof, allowEnrich = mayBootstrap)
                    }
                    record.has("ref_2") -> {
                        require(targetIsSefaria && !sourceIsSefaria) { "$path[$recordIndex] ref_2 side classification changed" }
                        counters.relevant++
                        counters.targetRelevant++
                        processExistingTarget(
                            path,
                            document,
                            recordIndex,
                            requireNotNull(targetTitle),
                            verifyBootstrapAdapter = mayBootstrap,
                        )
                    }
                    sourceIsSefaria && targetIsSefaria -> error("$path[$recordIndex] is Sefaria↔Sefaria")
                    sourceIsSefaria -> {
                        counters.relevant++
                        counters.sourceRelevant++
                        require(mayBootstrap) { "$path[$recordIndex]: new_source_ref_required" }
                        require(path.startsWith("tashmaToOtzaria/links/") && sourceTitle == "משנה ברורה") {
                            "$path[$recordIndex]: no bootstrap adapter for Sefaria source"
                        }
                        processSource(document, recordIndex, sourceTitle, requireNotNull(tashmaProof), allowEnrich = true)
                    }
                    targetIsSefaria -> {
                        counters.relevant++
                        counters.targetRelevant++
                        require(mayBootstrap) { "$path[$recordIndex]: new_target_ref_required" }
                        processBootstrapTarget(path, document, recordIndex, requireNotNull(targetTitle))
                    }
                    else -> counters.irrelevant++
                }
                val after = ManualLinksJson.canonicalString(record)
                if (before == after) counters.unchanged++
                clearFailureContext()
            }
        }
        if (initialBootstrap) {
            val expected = config.bootstrapRecordOverrides.map { it.path to it.recordSha256 }.toSet()
            require(usedOverrides == expected) { "Bootstrap overrides not used exactly once: missing=${expected - usedOverrides}" }
        }
        require(counters.recordsScanned == counters.relevant + counters.irrelevant) {
            "Record accounting is not closed"
        }
    }

    private fun processExistingTarget(
        path: String,
        document: ManualLinksDocument,
        recordIndex: Int,
        expectedHeTitle: String,
        verifyBootstrapAdapter: Boolean,
    ) {
        val record = document.record(recordIndex)
        val book = index.bookByHeTitle(expectedHeTitle)
        val entry = index.resolveRef(book, record.get("ref_2").textValue())
        val adapterEntry = when (if (verifyBootstrapAdapter) adapterFor(path) else null) {
            "national_library_mishneh_torah_v1" -> {
                val heRef = ManualLinksBootstrap.nationalLibraryHeRef(record.get("heRef_2").textValue(), expectedHeTitle)
                index.resolveHeRef(book, heRef)
            }
            "morebooks_heref_v1" -> resolveExistingMoreBooks(path, document, recordIndex, book)
            "dicta_heref_v1" -> index.resolveHeRef(
                book,
                ManualLinksBootstrap.dictaHeRef(record.get("heRef_2").textValue(), expectedHeTitle),
            )
            null -> null
            else -> error("Unknown bootstrap adapter for $path")
        }
        require(adapterEntry == null || adapterEntry.ref == entry.ref) {
            "$path[$recordIndex]: ref_2 does not match the deterministic heRef_2 adapter"
        }
        val oldIndex = ManualLinksDocument.exactInt(record.get("line_index_2"), "line_index_2")
        document.setInt(recordIndex, "line_index_2", entry.lineIndex)
        if (oldIndex != entry.lineIndex) counters.shifted++
    }

    private fun processBootstrapTarget(
        path: String,
        document: ManualLinksDocument,
        recordIndex: Int,
        expectedHeTitle: String,
    ) {
        val adapter = adapterFor(path)
            ?: error("No bootstrap adapter for $path")
        val record = document.record(recordIndex)
        val oldIndex = ManualLinksDocument.exactInt(record.get("line_index_2"), "line_index_2")
        val book = index.bookByHeTitle(expectedHeTitle)
        val entry = when (adapter) {
            "national_library_mishneh_torah_v1" -> {
                val built = ManualLinksBootstrap.nationalLibraryHeRef(record.get("heRef_2").textValue(), expectedHeTitle)
                index.resolveHeRef(book, built)
            }
            "morebooks_heref_v1" -> {
                val built = ManualLinksBootstrap.moreBooksHeRef(record.get("heRef_2").textValue())
                index.resolveHeRefOrNullIfMissing(book, built)
                    ?: resolveOverride(path, document, recordIndex, book)
            }
            "dicta_heref_v1" -> index.resolveHeRef(
                book,
                ManualLinksBootstrap.dictaHeRef(record.get("heRef_2").textValue(), expectedHeTitle),
            )
            else -> error("Unknown bootstrap adapter: $adapter")
        }
        document.setString(recordIndex, "ref_2", entry.ref)
        document.setInt(recordIndex, "line_index_2", entry.lineIndex)
        if (oldIndex != entry.lineIndex) counters.shifted++
        counters.enriched++
    }

    private fun resolveExistingMoreBooks(
        path: String,
        document: ManualLinksDocument,
        recordIndex: Int,
        book: ManualBookIndex,
    ): io.github.kdroidfilter.seforimlibrary.sefariasqlite.RefEntry {
        val record = document.record(recordIndex)
        val built = ManualLinksBootstrap.moreBooksHeRef(record.get("heRef_2").textValue())
        return index.resolveHeRefOrNullIfMissing(book, built)
            ?: run {
                val storedRef = record.get("ref_2").textValue()
                val lineIndex = ManualLinksDocument.exactInt(record.get("line_index_2"), "line_index_2")
                val override = exactPostStateOverride(
                    overrides = config.bootstrapRecordOverrides,
                    path = path,
                    stableRecordHash = document.stableRecordHash(recordIndex),
                    heRef2 = record.get("heRef_2").textValue(),
                    ref2 = storedRef,
                    lineIndex2 = lineIndex,
                )
                require(usedOverrides.add(path to override.recordSha256)) {
                    "Bootstrap post-state override was used twice: $path/${override.postRecordSha256}"
                }
                index.resolveRef(book, override.ref2).also {
                    require(it.lineIndex == override.lineIndex2) { "Bootstrap override index no longer matches its stable ref" }
                }
            }
    }

    private fun adapterFor(path: String): String? {
        val matches = config.bootstrapAdapters.entries.filter { path.startsWith(it.key + "/") }
        require(matches.size <= 1) { "Multiple bootstrap adapters match $path" }
        return matches.singleOrNull()?.value
    }

    private fun resolveOverride(
        path: String,
        document: ManualLinksDocument,
        recordIndex: Int,
        book: ManualBookIndex,
    ): io.github.kdroidfilter.seforimlibrary.sefariasqlite.RefEntry {
        val record = document.record(recordIndex)
        val preHash = document.stableRecordHash(recordIndex)
        val override = config.bootstrapRecordOverrides.singleOrNull {
            it.path == path && it.recordSha256 == preHash && it.requireHeRef2 == record.get("heRef_2").textValue()
        } ?: error("$path[$recordIndex]: MoreBooks Grammar A failed without an exact override")
        require(usedOverrides.add(path to preHash)) { "Override used twice: $path/$preHash" }
        val entry = index.resolveRef(book, override.ref2)
        require(entry.lineIndex == override.lineIndex2) { "Override line index drift for $path[$recordIndex]" }
        document.setString(recordIndex, "ref_2", entry.ref)
        document.setInt(recordIndex, "line_index_2", entry.lineIndex)
        require(document.stableRecordHash(recordIndex) == override.postRecordSha256) { "Override post hash mismatch" }
        return entry
    }

    private fun processSource(
        document: ManualLinksDocument,
        recordIndex: Int,
        expectedHeTitle: String,
        tashmaProof: TashmaProof?,
        allowEnrich: Boolean,
    ) {
        val record = document.record(recordIndex)
        val book = index.bookByHeTitle(expectedHeTitle)
        val entry = if (record.has("ref_1")) {
            index.resolveRef(book, record.get("ref_1").textValue())
        } else {
            require(allowEnrich && tashmaProof?.book === book) { "Missing ref_1 outside proven Tashma bootstrap" }
            val oldIndex = ManualLinksDocument.exactInt(record.get("line_index_1"), "line_index_1")
            index.resolveLine(book, oldIndex)
        }
        val oldIndex = ManualLinksDocument.exactInt(record.get("line_index_1"), "line_index_1")
        if (!record.has("ref_1")) {
            document.setString(recordIndex, "ref_1", entry.ref)
            counters.enriched++
        }
        document.setInt(recordIndex, "line_index_1", entry.lineIndex)
        if (oldIndex != entry.lineIndex) counters.shifted++
        val startNode = record.get("start")
        val anchorNode = record.get("anchor_src_hash")
        if (startNode == null) {
            require(anchorNode == null) { "anchor_src_hash without start" }
            return
        }
        val start = ManualLinksDocument.exactInt(startNode, "start", allowZero = true)
        val content = book.retainedContent(entry.lineIndex) ?: error("Required anchor content was not retained")
        require(start <= content.length) { "start exceeds raw source content" }
        require(content.take(start).none { Character.isSurrogate(it) }) { "Surrogate pair occurs before start" }
        val expectedHash = "sha256:${ManualLinksJson.sha256(content.toByteArray(Charsets.UTF_8))}"
        if (anchorNode == null) {
            require(allowEnrich) { "pending_anchor_hash is forbidden in refresh" }
            document.setString(recordIndex, "anchor_src_hash", expectedHash)
        } else {
            require(anchorNode.isTextual && anchorNode.textValue() == expectedHash) { "anchor_content_drift" }
        }
        counters.anchorsChecked++
    }

    private fun persistChangedDocuments() {
        documents.forEach { (path, document) ->
            if (!document.changed) return@forEach
            Files.writeString(arguments.output.resolve(path), document.render(), Charsets.UTF_8)
            counters.filesChanged++
        }
    }

    private fun writeReport(
        status: String,
        input: ManualLinksLineage?,
        output: ManualLinksLineage,
    ): Path {
        val report = ManualLinksJson.mapper.createObjectNode().apply {
            put("schema_version", 1)
            put("status", status)
            put("mode", arguments.mode.name.lowercase())
            put("tool_commit", arguments.seforimToolCommit)
            if (input == null) putNull("input_lineage") else set<ObjectNode>("input_lineage", input.toJson())
            set<ObjectNode>("output_lineage", output.toJson())
            set<ObjectNode>("files", ManualLinksJson.mapper.createObjectNode().apply {
                put("scanned", counters.filesScanned)
                put("changed", counters.filesChanged)
                put("renamed", counters.filesRenamed)
            })
            set<ObjectNode>("records", ManualLinksJson.mapper.createObjectNode().apply {
                put("scanned", counters.recordsScanned)
                put("relevant", counters.relevant)
                put("source_sefaria_relevant", counters.sourceRelevant)
                put("target_sefaria_relevant", counters.targetRelevant)
                put("irrelevant", counters.irrelevant)
                put("unchanged", counters.unchanged)
                put("shifted", counters.shifted)
                put("enriched", counters.enriched)
            })
            set<ObjectNode>("refs", ManualLinksJson.mapper.createObjectNode().apply {
                put("renamed", counters.refsRenamed)
                put("missing", 0)
                put("duplicate", 0)
            })
            set<ObjectNode>("anchors", ManualLinksJson.mapper.createObjectNode().apply {
                put("checked", counters.anchorsChecked)
                put("drifted", 0)
            })
            put("packaging_collisions", 0)
            set<ObjectNode>("reader", ManualLinksJson.mapper.createObjectNode().apply {
                put("merged_files_scanned", index.stats.mergedFilesScanned)
                put("payloads_loaded", index.stats.payloadsLoaded)
                put("payloads_accepted", index.stats.payloadsAccepted)
                put("payloads_blacklisted", index.stats.payloadsBlacklisted)
                put("retained_anchor_lines", index.stats.retainedAnchorLines)
                put("full_line_books_loaded", index.stats.fullLineBooksLoaded)
                put("full_lines_loaded", index.stats.fullLinesLoaded)
            })
            set<ArrayNode>("failures", ManualLinksJson.mapper.createArrayNode())
        }
        val path = arguments.output.resolve("manual_links_refresh_report.json")
        writeCanonical(path, report)
        return path
    }

    private fun writeMarker(status: String, lineage: Path, report: Path): Path {
        val marker = ManualLinksJson.mapper.createObjectNode().apply {
            put("schema_version", 1)
            put("status", status)
            put("lineage_sha256", ManualLinksJson.rawSha256(lineage))
            put("report_sha256", ManualLinksJson.rawSha256(report))
        }
        val path = arguments.output.resolve(".manual-links-refresh-complete")
        writeCanonical(path, marker)
        return path
    }

    private fun writeFailureReport(error: Throwable) {
        if (!Files.isDirectory(arguments.output)) return
        Files.deleteIfExists(arguments.output.resolve(".manual-links-refresh-complete"))
        val failure = ManualLinksJson.mapper.createObjectNode().apply {
            put("type", error::class.qualifiedName ?: error::class.simpleName ?: "Throwable")
            put("message", error.message ?: "unspecified failure")
            currentFailureFile?.let { put("file", it) }
            currentFailureRecordIndex?.let { put("record_index", it) }
            currentFailureRecordHash?.let { put("record_sha256", it) }
        }
        val report = ManualLinksJson.mapper.createObjectNode().apply {
            put("schema_version", 1)
            put("status", "failed")
            put("mode", arguments.mode.name.lowercase())
            put("tool_commit", arguments.seforimToolCommit)
            set<ArrayNode>("failures", ManualLinksJson.mapper.createArrayNode().add(failure))
        }
        writeCanonical(arguments.output.resolve("manual_links_refresh_report.json"), report)
    }

    private fun clearFailureContext() {
        currentFailureFile = null
        currentFailureRecordIndex = null
        currentFailureRecordHash = null
    }

    private fun peakRssBytes(): Long {
        val proc = Path.of("/proc/self/status")
        if (proc.isRegularFile()) {
            val kb = proc.readText().lineSequence()
                .firstOrNull { it.startsWith("VmHWM:") }
                ?.split(Regex("\\s+"))
                ?.getOrNull(1)
                ?.toLongOrNull()
            if (kb != null) return kb * 1024
        }
        val runtime = Runtime.getRuntime()
        return runtime.totalMemory() - runtime.freeMemory()
    }

    private fun proveTashmaVector(): TashmaProof {
        val snapshot = arguments.repository.resolve("tashmaToOtzaria/סקריפטים/ביאור הלכה/otzaria_mb.txt")
        require(snapshot.isRegularFile()) { "Missing Tashma snapshot" }
        require(ManualLinksJson.rawSha256(snapshot) == TASHMA_SNAPSHOT_SHA) { "Tashma snapshot hash mismatch" }
        val raw = snapshot.readText(Charsets.UTF_8)
        require('\r' !in raw && raw.endsWith('\n') && !raw.endsWith("\n\n")) { "Tashma snapshot newline contract failed" }
        val oldLines = raw.dropLast(1).split('\n')
        require(oldLines.size == 18_119) { "Tashma snapshot line count changed" }
        val book = index.bookByHeTitle("משנה ברורה")
        val currentLines = book.fullProofLines()
        require(currentLines.size == oldLines.size) { "Mishnah Berurah vector length changed" }
        var identity = 0
        var stripped = 0
        oldLines.indices.forEach { zeroBased ->
            val position = zeroBased + 1
            val old = oldLines[zeroBased]
            val current = currentLines[zeroBased]
            when {
                old == current -> identity++
                stripOneLeadingMarker(old) == current -> stripped++
                position in TASHMA_EXCEPTIONS -> {
                    val exception = TASHMA_EXCEPTIONS.getValue(position)
                    require(ManualLinksJson.sha256(old.toByteArray(Charsets.UTF_8)) == exception.first) { "Tashma old exception hash drift at $position" }
                    require(ManualLinksJson.sha256(current.toByteArray(Charsets.UTF_8)) == exception.second) { "Tashma new exception hash drift at $position" }
                }
                else -> error("Tashma vector mismatch at physical line $position")
            }
        }
        require(identity == 732 && stripped == 17_378) { "Tashma vector classification changed: identity=$identity stripped=$stripped" }
        book.releaseFullProofLines()
        return TashmaProof(book)
    }

    internal data class TashmaProof(val book: ManualBookIndex)

    companion object {
        private const val TASHMA_SNAPSHOT_SHA = "0b67db43e6f2dedc2aa63fd670368b1ab23e9995d4c6458044e87b43d2c772e6"
        private val LEADING_MARKER = Regex("""^\s*[({][א-ת\"׳״]{1,5}[)}]\s""")
        private val TASHMA_EXCEPTIONS = mapOf(
            // Sefaria upstream edits observed 2026-07-18 (gloss/doubled-marker removals).
            1454 to ("d35ca6f38affcd0729ee0d9637b6a15ffc0d228e8cb3ecd4088ab777c88641ea" to "f6a76d7e7af916a8ace79c26b71d4c1b21c831585014f77c629465a1651275dd"),
            1459 to ("0d67f595e80541820df897bb07adfb3e15eb69ecf95732b983679b94a965bd02" to "9580679666a89229180937347ec87ee21d4637cb20664083f879d99068ea3c3a"),
            9725 to ("2f8b124758343d785824cb9094fe1158126b9eeb138c61922dd643376321c2ec" to "fc07ecb6a4944b58f38e67ef5327d6931b806c453ec7d5bb3507b3ece33beeca"),
            4717 to ("cb563f858d2275238b7d8da58bfb61ae18a44ca939dee2265e59cb58eb3b3183" to "6f1cc2f903f4cb48c4da408028bba9bf0fff6d7f8654662a854ffc38e3419e86"),
            6364 to ("bf846cd97d29d82d617da2bce572e57c04ca87660234de6b07955b28298f1c5a" to "49e66d24cffc5346896c3614698e0abfeca689b31b47f958cdf1b89cd38256c2"),
            6400 to ("dd3494ce9136748f777fba0006acadfe461eb320ec6bd03b48f2b7b0427dbe41" to "b5aa7bbaa64553260bb5e34e5a407d3564671964ad1069b5395ed977d0420600"),
            11277 to ("bfd9aaac809853bd6811c61abcecdb4144a25f00d095681f786db4829c5cee63" to "8f7d9f4b3986fa8e281bff1c0f7c371b53c3aeb4c5a43b1f228f3354fbc58a43"),
            15130 to ("5c4808b7b5aa061b60dae188874a13d646c3ef40236aaad09c4ea0a12392b75a" to "205f9cb7deff294507d1107cfb5d5ce33278649652e9177d29470b1b76d289b9"),
            15162 to ("bffce9c82d51455f8f75a64705749f8fd55e814e040e7a3ba2d82cd091160809" to "0ae824929f27fb60bc1054b58fb220a2eef064c71a0737596294a6afacc72f69"),
        )

        internal fun stripOneLeadingMarker(value: String): String = LEADING_MARKER.replaceFirst(value, "")

        internal fun rewriteAtTitleBoundary(value: String, old: String, new: String): String? {
            if (value == old) return new
            if (value.startsWith(old + " ") || value.startsWith(old + ", ")) return new + value.substring(old.length)
            if (value.startsWith(old)) error("Invalid rename boundary after '$old' in '$value'")
            return null
        }

        internal fun replaceFinalPathComponent(path: String, old: String, new: String): String? {
            val slash = maxOf(path.lastIndexOf('/'), path.lastIndexOf('\\'))
            val component = path.substring(slash + 1)
            if (component != old) return null
            return path.substring(0, slash + 1) + new
        }

        internal fun requireHebrewRenameRef(hasRequiredRef: Boolean, failure: String) {
            require(hasRequiredRef) { failure }
        }

        internal fun exactPostStateOverride(
            overrides: List<BootstrapRecordOverride>,
            path: String,
            stableRecordHash: String,
            heRef2: String,
            ref2: String,
            lineIndex2: Int,
        ): BootstrapRecordOverride = overrides.singleOrNull {
            it.path == path &&
                it.postRecordSha256 == stableRecordHash &&
                it.requireHeRef2 == heRef2 &&
                it.ref2 == ref2 &&
                it.lineIndex2 == lineIndex2
        } ?: error("$path: zero-ref adapter result lacks exactly one post-state override")
    }
}
