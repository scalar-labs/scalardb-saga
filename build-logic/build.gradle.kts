plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${libs.versions.spotless.get()}")
    implementation("com.github.spotbugs:com.github.spotbugs.gradle.plugin:${libs.versions.spotbugs.plugin.get()}")
    implementation("net.ltgt.gradle:gradle-errorprone-plugin:${libs.versions.errorprone.plugin.get()}")
    implementation("net.ltgt.gradle:gradle-nullaway-plugin:${libs.versions.nullaway.plugin.get()}")

    // Enable version catalog accessors in convention plugins
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
