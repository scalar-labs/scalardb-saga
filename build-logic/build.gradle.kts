plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    // Catalog accessors rather than interpolated coordinates: a version Dependabot can only reach
    // through libs.versions.<name>.get() is one it drops, because it cannot evaluate the accessor to
    // learn which module the version belongs to.
    implementation(libs.spotless.gradle.plugin)
    implementation(libs.spotbugs.gradle.plugin)
    implementation(libs.errorprone.gradle.plugin)
    implementation(libs.nullaway.gradle.plugin)
    implementation(libs.protobuf.gradle.plugin)
    implementation(libs.maven.publish.gradle.plugin)
    implementation(libs.license.report.gradle.plugin)

    // Workaround: expose version catalog type-safe accessors (LibrariesForLibs) to convention
    // plugins. Gradle does not officially support this yet (https://github.com/gradle/gradle/issues/15383).
    // Revisit when Gradle adds native support. The alternative is the string-based
    // VersionCatalogsExtension API, which is stable but loses compile-time safety.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
