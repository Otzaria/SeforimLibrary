package io.github.kdroidfilter.seforimlibrary.externalbooks

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val GOOGLE_DRIVE_FILE_ID = "1MQaelAU6_F1qDlW1ZbZ4joFZGbJD7Hdr"
private const val DOWNLOAD_URL =
    "https://drive.google.com/uc?export=download&id=$GOOGLE_DRIVE_FILE_ID&confirm=t"

/**
 * Downloads `books.db` from Google Drive if it doesn't already exist locally.
 *
 * @return the [Path] to the local `books.db` file.
 */
fun ensureBooksDb(targetPath: Path, logger: Logger): Path {
    if (Files.exists(targetPath) && Files.isRegularFile(targetPath) && Files.size(targetPath) > 0) {
        logger.i { "books.db already exists at ${targetPath.toAbsolutePath()}, skipping download." }
        return targetPath
    }

    Files.createDirectories(targetPath.parent)
    downloadBooksDb(targetPath, logger)
    return targetPath
}

private fun downloadBooksDb(outFile: Path, logger: Logger) {
    logger.i { "Downloading books.db from Google Drive (file ID: $GOOGLE_DRIVE_FILE_ID)..." }

    val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .connectTimeout(java.time.Duration.ofSeconds(30))
        .build()

    val request = HttpRequest.newBuilder(URI(DOWNLOAD_URL))
        .header("User-Agent", "SeforimLibrary-BooksDbFetcher/1.0")
        .timeout(java.time.Duration.ofMinutes(10))
        .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofInputStream())
    if (response.statusCode() !in 200..299) {
        throw IllegalStateException("Failed to download books.db: HTTP ${response.statusCode()}")
    }

    val tmp = outFile.resolveSibling(outFile.fileName.toString() + ".part")
    Files.deleteIfExists(tmp)

    response.body().use { input ->
        Files.newOutputStream(tmp).use { output ->
            val buffer = ByteArray(1 shl 20) // 1 MB buffer
            var totalBytes = 0L
            var lastLoggedMb = 0L
            while (true) {
                val read = input.read(buffer)
                if (read == -1) break
                output.write(buffer, 0, read)
                totalBytes += read
                val currentMb = totalBytes / (1024 * 1024)
                if (currentMb > lastLoggedMb) {
                    logger.i { "Downloaded ${currentMb} MB..." }
                    lastLoggedMb = currentMb
                }
            }
            logger.i { "Download complete: ${totalBytes / (1024 * 1024)} MB total." }
        }
    }

    Files.move(tmp, outFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    logger.i { "Saved books.db to ${outFile.toAbsolutePath()}" }
}

/** Standalone entry point for the downloadBooksDb Gradle task. */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BooksDbFetcher")
    val targetPath = Paths.get(args.getOrNull(0) ?: "generator/externalbooks/build/books.db")
    ensureBooksDb(targetPath, logger)
}
