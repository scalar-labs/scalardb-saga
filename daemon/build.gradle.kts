plugins {
    id("scalardb-saga.java-conventions")
    application
}

application {
    mainClass = "com.scalar.db.saga.daemon.SagaServer"
}

dependencies {
    implementation(project(":core"))
    implementation(libs.javalin)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)

    "integrationTestImplementation"(project(":core"))
    "integrationTestImplementation"(libs.sqlite.jdbc)
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
    "integrationTestRuntimeOnly"(libs.logback.classic)
}
