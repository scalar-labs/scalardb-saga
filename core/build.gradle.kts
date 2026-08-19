plugins {
    id("scalardb-saga.java-conventions")
    id("scalardb-saga.publishing-conventions")
}

// The saga engine itself. Published because embedded mode — running the orchestrator in the
// application's own JVM — is a first-class way to use ScalarDB Saga, not just an internal detail of
// the daemon.
description =
    "The ScalarDB Saga engine: the saga orchestrator, its execution and compensation logic, and " +
    "the ScalarDB-backed durable store. Embed it in an application to run sagas in-process, or " +
    "run the daemon to use it as a service."

dependencies {
    api(project(":api"))

    implementation(platform(libs.jackson.bom))
    implementation(libs.scalardb)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.slf4j.api)

    // testImplementation, not testRuntimeOnly: tests capture log output through logback's
    // ListAppender, which needs the logback API on the test compile classpath.
    testImplementation(libs.logback.classic)
    testImplementation(libs.sqlite.jdbc)

    "integrationTestRuntimeOnly"(libs.logback.classic)
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
    "integrationTestImplementation"(libs.scalardb)
    "integrationTestImplementation"(libs.sqlite.jdbc)
    "integrationTestCompileOnly"(libs.jspecify)
}
