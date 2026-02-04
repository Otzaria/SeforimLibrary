plugins {
    alias(libs.plugins.multiplatform)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":dao"))
            
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kermit)
        }

        jvmMain.dependencies {
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

tasks.register<JavaExec>("generateSefariaDiff") {
    group = "application"
    description = "Compare old and new Sefaria books and generate a diff report."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariadiff.GenerateSefariaDiffKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultNewDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val defaultOldDbPath = rootProject.layout.buildDirectory.file("seforim.db.bak").get().asFile.absolutePath
    val defaultDiffFilePath = rootProject.layout.buildDirectory.file("SeferiaDiff.txt").get().asFile.absolutePath
    
    val newDbPath = if (project.hasProperty("newSeforimDb")) {
        val prop = project.property("newSeforimDb") as String
        if (prop.startsWith("/")) prop else rootProject.projectDir.resolve(prop).absolutePath
    } else {
        defaultNewDbPath
    }
    
    val oldDbPath = if (project.hasProperty("oldSeforimDb")) {
        val prop = project.property("oldSeforimDb") as String
        if (prop.startsWith("/")) prop else rootProject.projectDir.resolve(prop).absolutePath
    } else {
        defaultOldDbPath
    }
    
    val diffFilePath = if (project.hasProperty("diffFile")) {
        val prop = project.property("diffFile") as String
        if (prop.startsWith("/")) prop else rootProject.projectDir.resolve(prop).absolutePath
    } else {
        defaultDiffFilePath
    }

    args(newDbPath, oldDbPath, diffFilePath)
    
    // Ensure this runs after the Sefaria DB generation
    mustRunAfter(":sefariasqlite:generateSefariaSqlite")
}