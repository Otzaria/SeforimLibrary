plugins {
    alias(libs.plugins.multiplatform)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        jvmMain.dependencies {
            implementation(project(":core"))
            implementation(libs.kermit)
            implementation(libs.sqlDelight.driver.sqlite)
        }
        jvmTest.dependencies {
            implementation(kotlin("test"))
        }
    }
}

tasks.withType<Test> {
    testLogging {
        showStandardStreams = true
    }
    // Allocate 2GB for tests that load large DB mappings
    maxHeapSize = "2g"
}
