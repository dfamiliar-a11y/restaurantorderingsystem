plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.rabbitmq.client)
    implementation(libs.typesafe.config)
    implementation("org.slf4j:slf4j-api:2.0.16")
}
