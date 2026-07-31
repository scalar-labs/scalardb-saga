plugins {
    id("scalardb-saga.java8-conventions")
    id("scalardb-saga.publishing-conventions")
}

description =
    "The Java 8 client SDK for the ScalarDB Saga daemon: starts, inspects, and administers sagas " +
    "over gRPC. Depends only on the API and the wire contract, never on the engine."

// The Java 8 gRPC client SDK: GrpcSagaOrchestratorClient implements api.SagaOrchestrator over a gRPC
// stub. Depends only on :api (the interface + value types) and :rpc (the wire
// contract) — never on :core. Published as scalardb-saga-grpc-client. Client transport is the shaded
// Netty (isolates the SDK's Netty from downstream apps + bundles tcnative for Java 8 TLS).
dependencies {
    api(project(":api"))
    api(project(":rpc"))
    api(platform(libs.grpc.bom))
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    runtimeOnly(libs.grpc.netty.shaded)

    // Serializes the Map<String, Object> saga input to JSON for the `bytes input_json` wire field.
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    // In-process transport for unit tests (no network, no Netty).
    testImplementation(platform(libs.grpc.bom))
    testImplementation(libs.grpc.inprocess)
}
