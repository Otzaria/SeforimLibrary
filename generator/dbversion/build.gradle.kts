plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

// Write version metadata to the database and run integrity checks
// Usage:
//   ./gradlew :dbversion:writeDbVersion -PschemaVersion=1
//   ./gradlew :dbversion:writeDbVersion -PschemaVersion=1 -PcontentVersion=5 -PbuildId=abc123
//   ./gradlew generateSeforimDb -PschemaVersion=1  # Uses auto-increment from backup
tasks.register<JavaExec>("writeDbVersion") {
    group = "application"
    description = "Write version metadata to SQLite DB and run integrity checks."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.dbversion.WriteDbVersionKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // DB path is always build/seforim.db
    val dbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    args(dbPath)

    // Use provided properties or defaults
    val schemaVer = project.findProperty("schemaVersion")?.toString()
        ?: rootProject.findProperty("schemaVersion")?.toString()
        ?: "1"
    
    // contentVersion is optional - will auto-increment from backup if not provided
    val contentVer = project.findProperty("contentVersion")?.toString()
        ?: rootProject.findProperty("contentVersion")?.toString()

    systemProperty("schemaVersion", schemaVer)
    contentVer?.let { systemProperty("contentVersion", it) }

    // Optional properties
    val buildId = project.findProperty("buildId")?.toString()
        ?: rootProject.findProperty("buildId")?.toString()
    buildId?.let { systemProperty("buildId", it) }

    val appId = project.findProperty("appId")?.toString()
        ?: rootProject.findProperty("appId")?.toString()
    appId?.let { systemProperty("appId", it) }

    val manifestPath = project.findProperty("manifestPath")?.toString()
        ?: rootProject.findProperty("manifestPath")?.toString()
    manifestPath?.let { systemProperty("manifestPath", it) }

    jvmArgs = listOf("-Xmx512m")
}
