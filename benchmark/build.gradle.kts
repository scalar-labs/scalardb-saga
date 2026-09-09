plugins {
    id("scalardb-saga.java-conventions")
    application
}

application {
    mainClass = "com.scalar.db.saga.benchmark.BenchmarkCli"
    applicationName = "scalardb-saga-benchmark"
}

dependencies {
    implementation(project(":api"))
    implementation(project(":core"))
    // The Java 8 client SDK: the GRPC modes drive the daemon through GrpcSagaOrchestratorClient,
    // the same SagaOrchestrator the embedded mode implements — switching modes switches nothing
    // but the wiring.
    implementation(project(":client"))
    // The SERVER mode boots the real daemon in-process (ephemeral ports) so a single command can
    // benchmark the full gRPC round-trip without a separately managed server.
    implementation(project(":server"))
    implementation(libs.picocli)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)
    // The default store when no --properties file is given: a throwaway SQLite database, the same
    // zero-infrastructure backend the integration tests and the image smoke test use.
    runtimeOnly(libs.sqlite.jdbc)

    "integrationTestImplementation"(project(":api"))
    "integrationTestImplementation"(project(":core"))
}
