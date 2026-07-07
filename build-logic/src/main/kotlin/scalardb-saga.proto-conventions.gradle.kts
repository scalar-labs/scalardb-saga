import com.google.protobuf.gradle.id
import org.gradle.accessors.dm.LibrariesForLibs

// Applies + configures the protobuf-gradle-plugin (protoc + the grpc-java codegen), pinned via the
// version catalog. Layered on top of java8-conventions by the `rpc` module. Generated sources land in
// build/generated/source/proto (gitignored). This follows the repo convention of applying third-party
// plugins from build-logic (the plugin dep lives in build-logic/build.gradle.kts), not from modules.
plugins {
    id("com.google.protobuf")
}

val libs = the<LibrariesForLibs>()

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.java.get()}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().configureEach {
            plugins {
                id("grpc")
            }
        }
    }
}
