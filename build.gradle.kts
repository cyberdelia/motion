import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    `maven-publish`
    id("org.jetbrains.dokka") version "1.7.10"
    id("org.jmailen.kotlinter") version "3.12.0"
}

group = "com.lapanthere"

repositories {
    mavenCentral()
}

dependencies {
    // Logging
    implementation("org.slf4j:slf4j-api:2.0.3")

    // AWS
    implementation(platform("software.amazon.awssdk:bom:2.17.290"))
    implementation("software.amazon.awssdk:kinesis")
    testImplementation("software.amazon.awssdk:sts")

    // Rate-limiting
    implementation("com.github.vladimir-bukhtoyarov:bucket4j-core:7.6.0")

    // Tests
    testImplementation(kotlin("test-junit5"))
    testImplementation("io.mockk:mockk:1.13.2")
    testImplementation("ch.qos.logback:logback-classic:1.4.3")

    // Jackson
    testImplementation(platform("com.fasterxml.jackson:jackson-bom:2.13.4"))
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}

java {
    withJavadocJar()
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

kotlin {
    explicitApiWarning()
}

kotlinter {
    disabledRules = arrayOf("filename")
}
