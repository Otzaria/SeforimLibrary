package io.github.kdroidfilter.seforimlibrary.sefariadiff

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.system.exitProcess

/**
 * Main function to generate a diff report comparing Sefaria books between old and new databases.
 * 
 * @param args Array containing:
 *   - [0]: Path to the new database
 *   - [1]: Path to the old database (backup)
 *   - [2]: Path to the output diff file
 */
fun main(args: Array<String>) {
    if (args.size < 3) {
        println("Usage: GenerateSefariaDiff <newDbPath> <oldDbPath> <diffFilePath>")
        println("  newDbPath: Path to the newly generated seforim.db")
        println("  oldDbPath: Path to the backup database (seforim.db.bak)")
        println("  diffFilePath: Path where the diff report will be saved")
        exitProcess(1)
    }

    val newDbPath = args[0]
    val oldDbPath = args[1] 
    val diffFilePath = args[2]

    println("Starting Sefaria database comparison...")
    println("New DB: $newDbPath")
    println("Old DB: $oldDbPath")
    println("Output: $diffFilePath")

    runBlocking {
        try {
            generateSefariaDiff(newDbPath, oldDbPath, diffFilePath)
            println("\nDiff report generated successfully at: $diffFilePath")
        } catch (e: Exception) {
            println("Error generating diff report: ${e.message}")
            e.printStackTrace()
            exitProcess(1)
        }
    }
}

suspend fun generateSefariaDiff(newDbPath: String, oldDbPath: String, diffFilePath: String) {
    val newDbFile = File(newDbPath)
    val oldDbFile = File(oldDbPath)
    val diffFile = File(diffFilePath)
    
    if (!newDbFile.exists()) {
        throw IllegalArgumentException("קובץ מסד הנתונים החדש לא נמצא: $newDbPath")
    }
    
    if (!oldDbFile.exists()) {
        println("אזהרה: קובץ מסד הנתונים הישן לא נמצא: $oldDbPath")
        println("נראה שזו התקנה חדשה. לא תתבצע השוואה.")
        
        // יצירת דוח בסיסי להתקנה חדשה
        diffFile.writeText(buildString {
            appendLine("# דוח השוואת מסדי נתונים של ספריא")
            appendLine("נוצר בתאריך: ${java.time.LocalDateTime.now()}")
            appendLine()
            appendLine("## סטטוס: התקנה חדשה")
            appendLine("לא נמצא מסד נתונים קודם להשוואה.")
            appendLine("כל הספרים במסד הנתונים הנוכחי הם חדשים.")
            appendLine()
        })
        return
    }
    
    println("Connecting to databases...")
    
    // התחברות לשני מסדי הנתונים
    val newDriver = JdbcSqliteDriver("jdbc:sqlite:$newDbPath")
    val oldDriver = JdbcSqliteDriver("jdbc:sqlite:$oldDbPath")
    
    val newDb = SeforimDb(newDriver)
    val oldDb = SeforimDb(oldDriver)
    
    try {
        println("טוען ספרים ממקור ספריא משני מסדי הנתונים...")
        
        // קבלת ספרים ממקור ספריא משני מסדי הנתונים
        val sefaria = getSefariaSourceId(newDb) ?: getSefariaSourceId(oldDb)
        if (sefaria == null) {
            println("אזהרה: מקור ספריא לא נמצא באף מסד נתונים")
            return
        }
        
        val newSefariaBooks = getSefariaBooks(newDb, sefaria)
        val oldSefariaBooks = getSefariaBooks(oldDb, sefaria)
        
        println("מנתח הבדלים...")
        
        val addedBooks = newSefariaBooks.filter { newBook ->
            oldSefariaBooks.none { oldBook -> oldBook.title == newBook.title }
        }
        
        val removedBooks = oldSefariaBooks.filter { oldBook ->
            newSefariaBooks.none { newBook -> newBook.title == oldBook.title }
        }
        
        val commonBooks = newSefariaBooks.filter { newBook ->
            oldSefariaBooks.any { oldBook -> oldBook.title == newBook.title }
        }
        
        // יצירת דוח השוואה בעברית
        val report = buildString {
            appendLine("# דוח השוואת מסדי נתונים של ספריא")
            appendLine("נוצר בתאריך: ${java.time.LocalDateTime.now()}")
            appendLine()
            
            appendLine("## סיכום")
            appendLine("- סה\"כ ספרים במסד החדש: ${newSefariaBooks.size}")
            appendLine("- סה\"כ ספרים במסד הישן: ${oldSefariaBooks.size}")
            appendLine("- ספרים שנוספו: ${addedBooks.size}")
            appendLine("- ספרים שהוסרו: ${removedBooks.size}")
            appendLine("- ספרים ללא שינוי: ${commonBooks.size}")
            appendLine()
            
            if (addedBooks.isNotEmpty()) {
                appendLine("## ספרים שנוספו (${addedBooks.size})")
                addedBooks.sortedBy { it.title }.forEach { book ->
                    appendLine("- ${book.title}")
                    if (book.heShortDesc != null) {
                        appendLine("  תיאור עברי: ${book.heShortDesc}")
                    }
                    appendLine("  סה\"כ שורות: ${book.totalLines}")
                    appendLine()
                }
                appendLine()
            }
            
            if (removedBooks.isNotEmpty()) {
                appendLine("## ספרים שהוסרו (${removedBooks.size})")
                removedBooks.sortedBy { it.title }.forEach { book ->
                    appendLine("- ${book.title}")
                    if (book.heShortDesc != null) {
                        appendLine("  תיאור עברי: ${book.heShortDesc}")
                    }
                    appendLine("  סה\"כ שורות: ${book.totalLines}")
                    appendLine()
                }
                appendLine()
            }
            
            if (commonBooks.isNotEmpty()) {
                appendLine("## ספרים ללא שינוי (${commonBooks.size})")
                appendLine("ספרים אלו מופיעים בשני מסדי הנתונים:")
                commonBooks.sortedBy { it.title }.forEach { book ->
                    val oldBook = oldSefariaBooks.find { it.title == book.title }
                    appendLine("- ${book.title}")
                    if (oldBook != null && book.totalLines != oldBook.totalLines) {
                        appendLine("  שינוי במספר שורות: ${oldBook.totalLines} → ${book.totalLines}")
                    }
                }
                appendLine()
            }
        }
        
        // כתיבת הדוח
        diffFile.parentFile?.mkdirs()
        diffFile.writeText(report)
        
        println("סיכום:")
        println("- ספרים שנוספו: ${addedBooks.size}")
        println("- ספרים שהוסרו: ${removedBooks.size}")
        println("- ספרים ללא שינוי: ${commonBooks.size}")
        
    } finally {
        newDriver.close()
        oldDriver.close()
    }
}

private fun getSefariaSourceId(db: SeforimDb): Long? {
    return try {
        db.sourceQueriesQueries.selectByName("Sefaria").executeAsOneOrNull()?.id
    } catch (e: Exception) {
        null
    }
}

private fun getSefariaBooks(db: SeforimDb, sefariaSourceId: Long): List<SefariaBook> {
    return try {
        db.bookQueriesQueries.selectBySource(sefariaSourceId).executeAsList().map { book ->
            SefariaBook(
                title = book.title,
                heShortDesc = book.heShortDesc,
                totalLines = book.totalLines.toInt()
            )
        }
    } catch (e: Exception) {
        println("Warning: Could not fetch books from database: ${e.message}")
        emptyList()
    }
}

data class SefariaBook(
    val title: String,
    val heShortDesc: String?,
    val totalLines: Int
)
