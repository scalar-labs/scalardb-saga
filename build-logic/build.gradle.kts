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
    implementation("com.google.protobuf:protobuf-gradle-plugin:${libs.versions.protobuf.plugin.get()}")
    implementation("com.vanniktech:gradle-maven-publish-plugin:${libs.versions.maven.publish.plugin.get()}")
    implementation("com.github.jk1:gradle-license-report:${libs.versions.license.report.get()}")

    // Workaround: expose version catalog type-safe accessors (LibrariesForLibs) to convention
    // plugins. Gradle does not officially support this yet (https://github.com/gradle/gradle/issues/15383).
    // Revisit when Gradle adds native support. The alternative is the string-based
    // VersionCatalogsExtension API, which is stable but loses compile-time safety.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
