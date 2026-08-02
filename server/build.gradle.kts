plugins {
    id("scalardb-saga.java-conventions")
    id("scalardb-saga.license-report-conventions")
    application
}

application {
    mainClass = "com.scalar.db.saga.server.SagaServerCommand"

    // Without this, the start script, its install directory, and the distribution archives are all
    // named after the Gradle project ("server"), which is too generic to identify the product on an
    // operator's disk. `base.archivesName` in java-conventions renames only the jar.
    applicationName = "scalardb-saga-server"

    // Baked into the start script as DEFAULT_JVM_OPTS. The script appends JAVA_OPTS afterwards, so
    // an operator overrides any of these without rebuilding the image.
    applicationDefaultJvmArgs = listOf(
        // Size the heap from the cgroup memory limit rather than the host's RAM. Without it the JVM
        // sees the container limit but still defaults to 25% of it, leaving most of the pod's memory
        // unusable. 75% leaves headroom for the JVM's own off-heap (Netty direct buffers, metaspace,
        // thread stacks), which is substantial for a server holding many connections.
        "-XX:MaxRAMPercentage=75.0",
        // Die on heap exhaustion instead of thrashing GC while half-serving requests. A saga
        // coordinator that is up but failing is worse than one that is restarted: the recovery scan
        // picks up in-flight sagas on the next start, whereas a livelocked process holds saga leases
        // without making progress.
        "-XX:+ExitOnOutOfMemoryError",
    )
}

// The version picocli reports for --version. Read from the manifest at run time rather than baked
// into the annotation, so it cannot drift from the build. Deterministic, so it does not disturb the
// reproducible-archive settings in java-conventions.
tasks.jar {
    manifest {
        attributes("Implementation-Version" to project.version)
    }
}

dependencies {
    implementation(project(":core"))
    implementation(project(":rpc"))
    implementation(platform(libs.grpc.bom))
    // Aligns every transitively-resolved Netty module onto one version — see the netty entry in
    // libs.versions.toml. gRPC and the Azure/AWS SDKs otherwise leave the epoll transport on an
    // older 4.1.x than netty-common, which is the mixed-version arrangement Netty's native loader
    // cannot be relied on to survive.
    implementation(platform(libs.netty.bom))
    // The epoll native transports gRPC's Netty server uses on Linux, declared for both container
    // architectures. Only linux-x86_64 arrives transitively, so an arm64 image would silently fall
    // back to NIO; naming both makes the multi-arch image behave the same on either. Declared here
    // rather than left to the transitive graph so dropping an unrelated dependency cannot quietly
    // remove epoll support.
    runtimeOnly(variantOf(libs.netty.transport.native.epoll) { classifier("linux-x86_64") })
    runtimeOnly(variantOf(libs.netty.transport.native.epoll) { classifier("linux-aarch_64") })
    implementation(libs.grpc.stub)
    implementation(libs.grpc.netty)
    implementation(libs.grpc.services)
    implementation(libs.grpc.protobuf)
    implementation(libs.protobuf.java)
    implementation(libs.javalin)
    implementation(libs.nimbus.jose.jwt)
    implementation(libs.picocli)
    implementation(libs.commons.text)
    implementation(libs.slf4j.api)
    // Not runtimeOnly: SagaServerCommand installs the bridge handler itself, so SLF4JBridgeHandler has
    // to be on the compile classpath.
    implementation(libs.jul.to.slf4j)
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.grpc.bom))
    testImplementation(libs.grpc.inprocess)
    // The bridge test captures what actually reaches Logback, which needs its appender types at
    // compile time — the main source set only needs Logback at runtime.
    testImplementation(libs.logback.classic)

    "integrationTestImplementation"(project(":core"))
    "integrationTestImplementation"(project(":grpc-client"))
    "integrationTestImplementation"(platform(libs.grpc.bom))
    "integrationTestImplementation"(libs.grpc.netty)
    "integrationTestImplementation"(libs.mockito.core)
    "integrationTestImplementation"(libs.sqlite.jdbc)
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
    "integrationTestRuntimeOnly"(libs.logback.classic)
}

// ---------------------------------------------------------------------------
// Container image
// ---------------------------------------------------------------------------

val imageName = "ghcr.io/scalar-labs/scalardb-saga-server"

// Assembles the Docker build context. Kept separate from the build itself so that the release
// workflow can hand this directory to docker/build-push-action — which gets multi-arch, layer
// caching, SBOM, and provenance attestation that an `Exec` task invoking `docker build` cannot — while
// a local build uses the identical context.
val dockerContext by tasks.registering(Sync::class) {
    description = "Assembles the Docker build context for the server image."
    group = "distribution"

    into(layout.buildDirectory.dir("docker"))

    // Dockerfile and the configuration template. The operator documentation next to them is for
    // readers of this repository, not an input to the build, so it stays out of the context.
    from("docker") { exclude("README.md") }
    // The unpacked distribution, not distTar: see the COPY comment in the Dockerfile.
    from(tasks.installDist) { into("dist") }
    from(rootProject.file("LICENSE"))
    from(tasks.named("generateLicenseReport")) { include("THIRD-PARTY-NOTICES.txt") }
}

// Local convenience only: single-architecture, loaded into the local Docker daemon. The release workflow
// builds the same context for both architectures with attestations, so this task deliberately does
// not push.
tasks.register<Exec>("dockerBuild") {
    description = "Builds the server image for the local architecture from the assembled context."
    group = "distribution"

    dependsOn(dockerContext)
    workingDir = layout.buildDirectory.dir("docker").get().asFile
    executable = "docker"

    // The argument provider below is a script lambda, which captures the build script object and so
    // cannot be serialized into the configuration cache. Computing the arguments eagerly instead
    // would avoid that, but it would run `git rev-parse` on every build that configures this project
    // and tie the cache entry to HEAD, invalidating it on every commit. Declaring the
    // incompatibility is the cheaper trade: this task already cannot use the cache, because
    // dockerContext depends on generateLicenseReport, which is itself incompatible.
    notCompatibleWithConfigurationCache(
        "the image tag and revision are resolved lazily by a script argument provider",
    )

    // Read through a provider rather than by shelling out at configuration time, so the git call is
    // cached and does not run on unrelated builds.
    val revision = providers.exec {
        commandLine("git", "rev-parse", "HEAD")
    }.standardOutput.asText.map { it.trim() }
    val imageVersion = project.version.toString()

    argumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "buildx", "build",
                "--load",
                "--build-arg", "IMAGE_VERSION=$imageVersion",
                "--build-arg", "IMAGE_REVISION=${revision.get()}",
                "--tag", "$imageName:$imageVersion",
                ".",
            )
        },
    )
}
