plugins {
    id("scalardb-saga.java-conventions")
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
