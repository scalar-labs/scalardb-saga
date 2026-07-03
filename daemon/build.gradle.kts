plugins {
    id("scalardb-saga.java-conventions")
    application
}

application {
    mainClass = "com.scalar.db.saga.daemon.SagaServer"
}

dependencies {
    implementation(project(":core"))
    implementation(project(":rpc"))
    implementation(platform(libs.grpc.bom))
    implementation(libs.grpc.stub)
    implementation(libs.grpc.netty)
    implementation(libs.grpc.services)
    implementation(libs.protobuf.java)
    implementation(libs.javalin)
    implementation(libs.slf4j.api)
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.grpc.bom))
    testImplementation(libs.grpc.inprocess)

    "integrationTestImplementation"(project(":core"))
    "integrationTestImplementation"(project(":grpc-client"))
    "integrationTestImplementation"(libs.sqlite.jdbc)
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
    "integrationTestRuntimeOnly"(libs.logback.classic)
}
