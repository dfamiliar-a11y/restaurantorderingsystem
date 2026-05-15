plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.restaurant.runner.RunnerServiceKt")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.rabbitmq.client)
    implementation(libs.logback.classic)
}
