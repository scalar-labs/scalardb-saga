plugins {
    id("scalardb-saga.base-conventions")
    id("com.vanniktech.maven.publish")
}

// Publishing to Maven Central for the consumable modules (api, core, rpc, client, bom). The
// server is deliberately NOT published: it ships as a container image, so a jar on Central would be
// an artifact nobody consumes but everyone has to keep secure.
//
// The Vanniktech plugin auto-detects the platform from the applied plugins — `java-library` gets a
// sources + javadoc jar, `java-platform` (the BOM) gets neither — and uploads through the Central
// Portal API. Everything specific to this project lives in this one file, so swapping the upload
// mechanism (e.g. to JReleaser, as ScalarDB Cluster uses) is a single-file change.

// Ship the license inside every published jar (META-INF/LICENSE) so the terms travel with the
// artifact itself, not just the repository it came from. The jar is what ends up vendored into
// someone else's build.
//
// The type has to be the `org.gradle.jvm.tasks` base class rather than the `bundling` subclass that
// an unqualified `Jar` resolves to. The javadoc jar is contributed by the publishing plugin and
// extends only the base class, so an unqualified `Jar` would silently leave it out, as would any
// future jar that comes from a plugin instead of from `java-library`.
tasks.withType<org.gradle.jvm.tasks.Jar>().configureEach {
    metaInf {
        from(rootProject.file("LICENSE"))
    }
}

mavenPublishing {
    // The artifactId must be set explicitly: `from(components["java"])` defaults it to the Gradle
    // project name ("api", "core", ...), which would publish four generically-named artifacts under
    // our group. This mirrors `base.archivesName` in java-conventions, so the POM coordinate and the
    // jar file name stay in step.
    //
    // A module whose published name is not derivable from its directory reassigns this from its own
    // build file, where the later call wins. Three names come from the module name here and in
    // java-conventions, so such a module has to override all three together; the client SDK is the
    // only one that does.
    coordinates(
        groupId = project.group.toString(),
        artifactId = "scalardb-saga-${project.name}",
        version = project.version.toString(),
    )

    // Uploads a USER_MANAGED deployment to the Central Portal for a human to release; the build
    // ends at the upload and does not wait for the Portal's validation verdict. Passing `true` here
    // would publish to Central the moment a tag builds green, with no way to take it back — a
    // released version is immutable, and waiting for the verdict is only available bundled with
    // that. SNAPSHOT versions bypass this and go straight to the Central snapshot repository.
    publishToMavenCentral()

    // Central rejects unsigned releases, but requiring a signature unconditionally would also break
    // `publishToMavenLocal` for anyone without a key. Sign only when a key is actually configured
    // (CI sets ORG_GRADLE_PROJECT_signingInMemoryKey); local builds simply publish unsigned.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    pom {
        // Central shows the name verbatim, so keeping it equal to the artifactId makes the listing
        // match what people type in their build file — which is also why a module that overrides
        // the artifactId must override this with it. The human-readable text is the description,
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
