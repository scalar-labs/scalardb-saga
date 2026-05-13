plugins {
    id("scalardb-saga.java-conventions")
}

dependencies {
    implementation(platform(libs.jackson.bom))
    implementation(libs.scalardb)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.slf4j.api)

    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.sqlite.jdbc)
}
