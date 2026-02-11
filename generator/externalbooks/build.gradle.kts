plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kermit)
        }

        jvmMain.dependencies {
            implementation(project(":idresolver"))
            implementation(libs.kotlinx.coroutines.swing)
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

// Import external book metadata (HebrewBooks + OtzarHaChochma) into seforim.db
tasks.register<JavaExec>("importExternalBooks") {
    group = "application"
    description = "Import HebrewBooks and OtzarHaChochma metadata from books.db into seforim.db."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.externalbooks.ImportExternalBooksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultSeforimDb = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val seforimDb = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        defaultSeforimDb
    }

    val defaultBooksDb = layout.buildDirectory.file("books.db").get().asFile.absolutePath
    val booksDb = if (project.hasProperty("booksDb")) {
        project.property("booksDb") as String
    } else {
        defaultBooksDb
    }

    args(seforimDb, booksDb)

    // Provide project root path for finding backup DB
    systemProperty("projectRoot", rootProject.projectDir.absolutePath)

    jvmArgs = listOf(
        "-Xmx2g",
        "-XX:+UseG1GC"
    )
}
