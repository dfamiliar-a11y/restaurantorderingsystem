plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
}

allprojects {
    group = "com.restaurant"
    version = "1.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
