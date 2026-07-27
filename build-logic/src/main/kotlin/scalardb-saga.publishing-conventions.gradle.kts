plugins {
    id("scalardb-saga.base-conventions")
    id("com.vanniktech.maven.publish")
}

// Publishing to Maven Central for the consumable modules (api, core, rpc, grpc-client, bom). The
// daemon is deliberately NOT published: it ships as a container image, so a jar on Central would be
// an artifact nobody consumes but everyone has to keep secure.
//
// The Vanniktech plugin auto-detects the platform from the applied plugins — `java-library` gets a
// sources + javadoc jar, `java-platform` (the BOM) gets neither — and uploads through the Central
// Portal API. Everything specific to this project lives in this one file, so swapping the upload
// mechanism (e.g. to JReleaser, as ScalarDB Cluster uses) is a single-file change.

// Ship the license inside every published jar (META-INF/LICENSE) so the terms travel with the
// artifact itself, not just the repository it came from — the jar is what ends up vendored into
// someone else's build.
tasks.withType<Jar>().configureEach {
    metaInf {
        from(rootProject.file("LICENSE"))
    }
}

mavenPublishing {
    // The artifactId must be set explicitly: `from(components["java"])` defaults it to the Gradle
    // project name ("api", "core", ...), which would publish four generically-named artifacts under
    // our group. This mirrors `base.archivesName` in java-conventions, so the POM coordinate and the
    // jar file name stay in step.
    coordinates(
        groupId = project.group.toString(),
        artifactId = "scalardb-saga-${project.name}",
        version = project.version.toString(),
    )

    // Uploads a deployment to the Central Portal and leaves it VALIDATED for a human to release.
    // Passing `true` here would publish to Central the moment a tag builds green, with no way to
    // take it back — a released version is immutable. SNAPSHOT versions bypass this and go straight
    // to the Central snapshot repository.
    publishToMavenCentral()

    // Central rejects unsigned releases, but requiring a signature unconditionally would also break
    // `publishToMavenLocal` for anyone without a key. Sign only when a key is actually configured
    // (CI sets ORG_GRADLE_PROJECT_signingInMemoryKey); local builds simply publish unsigned.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    pom {
        // Central shows the name verbatim, so keeping it equal to the artifactId makes the listing
        // match what people type in their build file. The human-readable text is the description,
        // which each module sets via `description = "..."`.
        name = "scalardb-saga-${project.name}"
        description = provider {
            checkNotNull(project.description) {
                "Project '${project.path}' is published, so it must set `description` — Maven " +
                    "Central rejects a POM without one."
            }
        }
        url = "https://github.com/scalar-labs/scalardb-saga"
        inceptionYear = "2026"

        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }

        developers {
            developer {
                id = "scalar-labs"
                name = "Scalar, Inc."
                url = "https://scalar-labs.com/"
            }
        }

        scm {
            url = "https://github.com/scalar-labs/scalardb-saga"
            connection = "scm:git:https://github.com/scalar-labs/scalardb-saga.git"
            developerConnection = "scm:git:ssh://git@github.com/scalar-labs/scalardb-saga.git"
        }

        issueManagement {
            system = "GitHub Issues"
            url = "https://github.com/scalar-labs/scalardb-saga/issues"
        }
    }
}
