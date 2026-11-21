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

// The UI's own files get only the basics: Spotless's JavaScript and CSS formatters need Node, which
// the build does not use. Vendored libraries stay exactly as published.
spotless {
    format("designerUi") {
        target(
            "src/main/resources/designer-ui/**/*.js",
            "src/main/resources/designer-ui/**/*.css",
            "src/main/resources/designer-ui/**/*.html",
        )
        targetExclude("src/main/resources/designer-ui/vendor/**")
        trimTrailingWhitespace()
        endWithNewline()
        leadingTabsToSpaces(2)
    }
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

// Browser tests of the UI, with Playwright for Java, so no Node is needed. Not part of `check`,
// which stays browser-free: CI runs them in a job of their own (`./gradlew :designer:uiTest`).
testing {
    suites {
        register<JvmTestSuite>("uiTest") {
            useJUnitJupiter(libs.versions.junit.jupiter.get())
            dependencies {
                implementation(project())
                implementation(libs.playwright)
                implementation(libs.assertj.core)
                implementation(platform(libs.jackson.bom))
                implementation(libs.jackson.databind)
            }
        }
    }
}

// Playwright's browser builds go under the repository's own Gradle folder, which git ignores,
// rather than the user's cache directory. Only Chromium's headless shell is installed: the tests
// drive nothing else, and never a headed browser.
val playwrightBrowsers = rootProject.layout.projectDirectory.dir(".gradle/ms-playwright").asFile.absolutePath
val playwrightWithDeps = providers.gradleProperty("playwrightWithDeps").isPresent

val installPlaywrightBrowser by tasks.registering(JavaExec::class) {
    description = "Downloads the Chromium build the UI tests drive. -PplaywrightWithDeps also " +
        "installs its system libraries, which CI's Linux runners need."
    classpath = sourceSets["uiTest"].runtimeClasspath
    mainClass = "com.microsoft.playwright.CLI"
    args(listOfNotNull("install", "--only-shell", if (playwrightWithDeps) "--with-deps" else null, "chromium"))
    environment("PLAYWRIGHT_BROWSERS_PATH", playwrightBrowsers)
}

tasks.named<Test>("uiTest") {
    dependsOn(installPlaywrightBrowser)
    environment("PLAYWRIGHT_BROWSERS_PATH", playwrightBrowsers)
    // Without this, Playwright would download every browser it supports on first use.
    environment("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD", "1")
    systemProperty("designer.fixtures.root", rootDir.absolutePath)
}
