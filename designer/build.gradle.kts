plugins {
    id("scalardb-saga.java-conventions")
    application
}

// A local tool, not a library: it is not published to Maven Central and is not in the BOM, for the
// same reason as the server. It runs from `./gradlew :designer:run` or an `installDist` directory.
application {
    mainClass = "com.scalar.db.saga.designer.DesignerCommand"
    applicationName = "scalardb-saga-designer"
}

tasks.jar {
    manifest {
        attributes("Implementation-Version" to project.version)
    }
}

// `run` starts in the designer's own directory by default, which is never what anyone wants as the
// workspace; start it where Gradle was invoked instead, so a relative --workspace means what it
// says.
tasks.named<JavaExec>("run") {
    workingDir = rootDir
}

dependencies {
    implementation(project(":core"))
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.javalin)
    implementation(libs.picocli)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

// The round-trip tests open every definition the repository ships. Declared as inputs so that a
// changed fixture reruns the tests instead of leaving them UP-TO-DATE.
tasks.named<Test>("test") {
    inputs.dir(rootProject.file("getting-started/conf/definitions"))
        .withPropertyName("gettingStartedDefinitions")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(rootProject.file("core/src/test/resources/sagas"))
        .withPropertyName("coreSagaFixtures")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    systemProperty("designer.fixtures.root", rootDir.absolutePath)
    // Lets the HTTP tests send a foreign Host header, as a DNS-rebinding page would.
    systemProperty("jdk.httpclient.allowRestrictedHeaders", "host")
}
