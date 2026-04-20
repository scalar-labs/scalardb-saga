import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    `java-library`
    id("com.diffplug.spotless")
    id("com.github.spotbugs")
    id("net.ltgt.errorprone")
    id("net.ltgt.nullaway")
}

val libs = the<LibrariesForLibs>()

group = "com.scalar.db.saga"

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
// Spotless
// ---------------------------------------------------------------------------

spotless {
    java {
        googleJavaFormat()
        removeUnusedImports()
        formatAnnotations()
    }
}

// ---------------------------------------------------------------------------
// SpotBugs + FindSecBugs
// ---------------------------------------------------------------------------

spotbugs {
    effort = Effort.valueOf("MAX")
    reportLevel = Confidence.valueOf("MEDIUM")
    ignoreFailures = false
    showStackTraces = true
    maxHeapSize = "1g"
    excludeFilter = file("${rootDir}/config/spotbugs/exclude.xml")
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
        if (name.contains("Test", ignoreCase = true)) {
            disable("NullAway")
        }
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
            useJUnitJupiter()
        }
    }
}

dependencies {
    implementation(platform(libs.jackson.bom))
    compileOnly(libs.jspecify)

    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.assertj.core)
}

tasks.withType<Test>().configureEach {
    testLogging {
        events("passed", "skipped", "failed")
    }
}
