import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    `java-library`
    id("scalardb-saga.base-conventions")
    id("com.diffplug.spotless")
    id("com.github.spotbugs")
    id("net.ltgt.errorprone")
    id("net.ltgt.nullaway")
}

val libs = the<LibrariesForLibs>()

base.archivesName = "scalardb-saga-${project.name}"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

// ---------------------------------------------------------------------------
// Reproducible archives
// ---------------------------------------------------------------------------

// Build timestamps and filesystem ordering would otherwise make every jar, distribution tar, and
// zip byte-different across builds of the same source. Pinning both makes a published artifact
// verifiable against a rebuild, and keeps the Docker layer holding the daemon distribution stable
// when nothing changed.
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

// ---------------------------------------------------------------------------
// Spotless
// ---------------------------------------------------------------------------

spotless {
    java {
        googleJavaFormat()
        formatAnnotations()
    }
}

// ---------------------------------------------------------------------------
// SpotBugs + FindSecBugs
// ---------------------------------------------------------------------------

spotbugs {
    effort = Effort.MAX
    reportLevel = Confidence.MEDIUM
    showStackTraces = true
    maxHeapSize = "1g"
    excludeFilter = rootProject.file("config/spotbugs/exclude.xml")
}

dependencies {
    spotbugsPlugins(libs.findsecbugs.plugin)
}

tasks.withType<com.github.spotbugs.snom.SpotBugsTask>().configureEach {
    reports.create("html") {
        required = true
        outputLocation = layout.buildDirectory.file("reports/spotbugs/${name}.html")
    }
}

// ---------------------------------------------------------------------------
// Error Prone + NullAway
// ---------------------------------------------------------------------------

nullaway {
    annotatedPackages.add("com.scalar.db.saga")
}

dependencies {
    errorprone(libs.errorprone.core)
    errorprone(libs.nullaway)
}

tasks.withType<JavaCompile>().configureEach {
    options.errorprone {
        disableWarningsInGeneratedCode = true
        enable(
            "MissingOverride",
            "UnusedVariable",
            "FieldCanBeFinal",
        )
    }
}

tasks.compileJava {
    options.errorprone {
        error("NullAway")
    }
}

// ---------------------------------------------------------------------------
// Testing
// ---------------------------------------------------------------------------

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(libs.versions.junit.jupiter.get())
        }

        val integrationTest by registering(JvmTestSuite::class) {
            useJUnitJupiter(libs.versions.junit.jupiter.get())

            dependencies {
                implementation(project())
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
    // Javadoc is not part of `check` by default, so an unresolvable {@link} or a malformed tag fails
    // nothing until something builds the javadoc jar — which only happens when publishing, i.e. at
    // release time. The javadoc is a published artifact, so hold it to the same bar as the code and
    // find out on the pull request instead.
    dependsOn(tasks.named("javadoc"))
}

dependencies {
    compileOnly(libs.jspecify)
    compileOnly(libs.jcip.annotations)
    compileOnly(libs.spotbugs.annotations)

    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.assertj.core)

    "integrationTestImplementation"(libs.assertj.core)
}

tasks.withType<Test>().configureEach {
    testLogging {
        events("passed", "skipped", "failed")
    }
}
