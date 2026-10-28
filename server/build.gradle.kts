import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("scalardb-saga.java-conventions")
    id("scalardb-saga.license-report-conventions")
    // Core Gradle plugin (not external, so it does not need to route through build-logic): hosts
    // the TLS test-certificate generator both the unit and integration suites consume.
    `java-test-fixtures`
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
        // Two dependencies load a native library through JNI: sqlite-jdbc, and Netty's epoll
        // transport. JDK 24 made System.load a restricted method, so without this the JVM prints a
        // warning naming whichever of them reaches it first, and a future release will block the
        // call outright. Netty would then fall back to NIO and SQLite would fail to load at all.
        // ALL-UNNAMED is the only granularity available, because the distribution ships a flat
        // classpath rather than named modules.
        "--enable-native-access=ALL-UNNAMED",
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
    // AdvancedTlsX509KeyManager, the one key manager both transports serve TLS from. grpc-util is
    // already on the runtime classpath through grpc-core; naming it here puts it on the compile
    // classpath.
    implementation(libs.grpc.util)
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

    // Not runtimeOnly: BoundedQueueWarningFilter extends Logback's TurboFilter.
    implementation(libs.logback.classic)

    testImplementation(platform(libs.grpc.bom))
    testImplementation(libs.grpc.inprocess)

    "integrationTestImplementation"(project(":core"))
    "integrationTestImplementation"(testFixtures(project(":core")))
    "integrationTestImplementation"(project(":client"))
    // The TLS test-certificate generator. It shells out to the JDK's own keytool, deliberately
    // adding no crypto dependency: BouncyCastle on this classpath would silently widen Netty's
    // PEM parsing (it accepts PKCS#1 only when BC is present), and these suites exist to exercise
    // the production parse path.
    "integrationTestImplementation"(testFixtures(project(":server")))
    "integrationTestImplementation"(platform(libs.grpc.bom))
    "integrationTestImplementation"(libs.grpc.netty)
    "integrationTestImplementation"(libs.mockito.core)
    "integrationTestImplementation"(libs.sqlite.jdbc)
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
    // Not runtimeOnly: the TLS integration test asserts the handshake-noise log policy by
    // capturing what reaches Logback, which needs its appender types at compile time.
    "integrationTestImplementation"(libs.logback.classic)
    // For @SuppressFBWarnings on the TLS suite's deliberately-plaintext probe socket.
    "integrationTestCompileOnly"(libs.spotbugs.annotations)
    // Same, for the test-certificate generator's deliberate keytool/throwaway-password usage.
    "testFixturesCompileOnly"(libs.spotbugs.annotations)
    // Api, not implementation: LogCapture's surface exposes Logback's appender/event types to the
    // consuming suites.
    "testFixturesApi"(libs.logback.classic)
}

// ---------------------------------------------------------------------------
// Status-code page
// ---------------------------------------------------------------------------

// The page is rendered from SagaErrorCode and the wire statuses the error-mapper tests check, so
// its renderer lives with those tests. StatusCodesPageTest fails when the committed page is stale.
val statusCodesPage = layout.projectDirectory.file("../guides/scalardb-saga-status-codes.md")

tasks.register<JavaExec>("generateStatusCodes") {
    description = "Rewrites the status-code page from SagaErrorCode."
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.scalar.db.saga.server.docs.StatusCodesPage"
    args(statusCodesPage.asFile.absolutePath)
}

// The freshness test reads the page from outside this module; declaring it keeps an edit to the
// page from being answered by an up-to-date or cached test run.
tasks.test {
    inputs.file(statusCodesPage).withPathSensitivity(PathSensitivity.RELATIVE)
}

// ---------------------------------------------------------------------------
// Container image
// ---------------------------------------------------------------------------

val imageName = "ghcr.io/scalar-labs/scalardb-saga-server"

// Temporary fix for https://emudhra.we-have-a-problem.com/
// Compiles Oracle's CheckCerts.java so that the image can include CheckCerts.class. Users can run
// `java CheckCerts` in the container's working directory to confirm that the compromised CA
// certificates are not trusted by the actual runtime. We can remove this task, its `from` in
// dockerContext, docker/CheckCerts.java and the COPY in the Dockerfile after we update to the fixed
// version of Java.
val compileCheckCerts = tasks.register<JavaCompile>("compileCheckCerts") {
    description = "Compiles CheckCerts.java to include it in the server image."
    source(layout.projectDirectory.file("docker/CheckCerts.java"))
    classpath = files()
    destinationDirectory = layout.buildDirectory.dir("check-certs")
    // The class must run on the JRE in the image (Java 25).
    options.release = 25
    // An unmodified copy of Oracle's checker, kept outside every source set so Spotless leaves it
    // alone; Error Prone is off for the same reason.
    options.errorprone.enabled = false
}

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
    // Temporary fix for https://emudhra.we-have-a-problem.com/ (CheckCerts.java arrives with docker/).
    // Only the class: from(task) copies every declared output, and JavaCompile also declares its
    // incremental-compilation state file (build/tmp/<task>/previous-compilation-data.bin) as one.
    from(compileCheckCerts) { include("*.class") }
}

// Local convenience only: single-architecture, loaded into the local Docker daemon. The release workflow
// builds the same context for both architectures with attestations, so this task deliberately does
// not push.
tasks.register<Exec>("dockerBuild") {
    description = "Builds the server image for the local architecture from the assembled context."
    group = "distribution"

    dependsOn(dockerContext)
    workingDir = layout.buildDirectory.dir("docker").get().asFile

    // A bare executable name is resolved against the Gradle daemon's PATH, which is whatever the
    // shell that first started the daemon had, not the PATH of the shell running this build. Docker
    // Desktop installs its CLI outside the system directories, so a daemon started from a shell
    // without that directory fails to start the process while `docker` works in the caller's
    // terminal. Gradle injects the caller's environment into the build, so resolving the name here
    // against the caller's PATH gives the process launcher an absolute path to run. Done in doFirst
    // so PATH is read only when this task runs, and stays out of every other build's configuration
    // inputs. A candidate has to be a regular file as well as executable, since a directory with
    // the search bit set also reports as executable and a directory named `docker` on an earlier
    // PATH entry would otherwise shadow the real binary; that is the lookup rule a shell applies.
    // The fallback keeps Gradle's own error when Docker is not installed at all.
    doFirst {
        executable = providers.environmentVariable("PATH").map { path ->
            path.split(File.pathSeparator)
                .flatMap { dir -> listOf("docker", "docker.exe").map { File(dir, it) } }
                .firstOrNull { it.isFile && it.canExecute() }
                ?.absolutePath
                ?: "docker"
        }.get()
    }

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
