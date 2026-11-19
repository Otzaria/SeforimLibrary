// Central first: a plugin-portal outage (19/11, kotlin-stdlib 404) must not fail buildSrc.
pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}
