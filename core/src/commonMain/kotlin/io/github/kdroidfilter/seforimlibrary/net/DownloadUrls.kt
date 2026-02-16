package io.github.kdroidfilter.seforimlibrary.net

object DownloadUrls {
    const val GOOGLE_DRIVE_BOOKS_DB_FILE_ID = "1MQaelAU6_F1qDlW1ZbZ4joFZGbJD7Hdr"
    private const val GOOGLE_DRIVE_DOWNLOAD_TEMPLATE =
        "https://drive.google.com/uc?export=download&id=%s&confirm=t"

    fun booksDbDownloadUrl(): String =
        GOOGLE_DRIVE_DOWNLOAD_TEMPLATE.format(GOOGLE_DRIVE_BOOKS_DB_FILE_ID)

    const val OTZARIA_LIBRARY_LATEST_API =
        "https://api.github.com/repos/otzaria/otzaria-library/releases/latest"
    const val SEFARIA_EXPORT_LATEST_API =
        "https://api.github.com/repos/kdroidFilter/SefariaExport/releases/latest"
    const val ACRONYMIZER_LATEST_API =
        "https://api.github.com/repos/kdroidFilter/SeforimAcronymizer/releases/latest"
    const val MAGIC_INDEXER_LATEST_API =
        "https://api.github.com/repos/kdroidFilter/SeforimMagicIndexer/releases/latest"

    const val OTZARIA_GENERATION_CSV =
        "https://raw.githubusercontent.com/Otzaria/otzaria-library/refs/heads/main/" +
        "MoreBooks/%D7%A1%D7%A4%D7%A8%D7%99%D7%9D/%D7%90%D7%95%D7%A6%D7%A8%D7%99%D7%90/" +
        "%D7%90%D7%95%D7%93%D7%95%D7%AA%20%D7%94%D7%AA%D7%95%D7%9B%D7%A0%D7%94/" +
        "%D7%A1%D7%93%D7%A8%20%D7%94%D7%93%D7%95%D7%A8%D7%95%D7%AA.csv"
}
