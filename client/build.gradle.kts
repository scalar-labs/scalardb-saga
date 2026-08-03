plugins {
    id("scalardb-saga.java8-conventions")
    id("scalardb-saga.publishing-conventions")
}

description =
    "The Java 8 client SDK for the ScalarDB Saga daemon: starts, inspects, and administers sagas " +
    "over gRPC. Depends only on the API and the wire contract, never on the engine."

// The one module whose published name is not scalardb-saga-<module>. A coordinate is what
// consumers type, so it belongs to the product line rather than to this repository's directory
// layout. The transport stays out of the name because it is an implementation detail this SDK is
// free to change, while a coordinate is fixed at the first release. "java" leaves room for an SDK
// in another language; the daemon speaks REST and gRPC precisely so other languages can talk to it.
//
// The conventions derive three names from the module name, and all three are overridden together:
// the jar file name, the Maven coordinate, and the POM name that Maven Central lists the artifact
// under. Leaving any one of them behind ships a jar, a coordinate, and a listing that disagree.
val artifactName = "scalardb-saga-java-client-sdk"

base.archivesName = artifactName

mavenPublishing {
    coordinates(artifactId = artifactName)
    pom {
        name = artifactName
    }
}

// The Java 8 gRPC client SDK: GrpcSagaOrchestratorClient implements api.SagaOrchestrator over a gRPC
// stub. Depends only on :api (the interface + value types) and :rpc (the wire contract) — never on
// :core. Client transport is the shaded Netty (isolates the SDK's Netty from downstream apps +
// bundles tcnative for Java 8 TLS).
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
