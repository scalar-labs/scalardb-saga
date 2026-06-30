import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("scalardb-saga.java8-conventions")
    id("scalardb-saga.proto-conventions")
}

// The Java-8 gRPC wire contract: the `.proto` plus the generated message + service stubs, shared by
// the daemon (server side) and the grpc-client SDK (client side). Generated under --release 8 so the
// Java 8 client can consume it; the Java 21 daemon consumes it too (newer-uses-older).
dependencies {
    api(platform(libs.grpc.bom))
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.protobuf.java)
}

// The generated stubs (build/generated/source/proto) are machine-generated, not hand-written, and live
// inside the NullAway-annotated `com.scalar.db.saga` package. Exclude them from the static-analysis
// stack so generated code can't fail the build. Module-local — this does not loosen analysis for the
// hand-written api/core/daemon modules.
spotless {
    java {
        targetExclude("build/generated/**")
    }
}

// Error Prone's excludedPaths also skips NullAway (NullAway runs as an Error Prone check), so this one
// setting keeps both off the generated sources.
tasks.withType<JavaCompile>().configureEach {
    options.errorprone.excludedPaths = ".*/build/generated/.*"
}
