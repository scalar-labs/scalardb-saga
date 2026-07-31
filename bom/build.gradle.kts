plugins {
    `java-platform`
    id("scalardb-saga.publishing-conventions")
}

// A bill of materials so consumers pin one ScalarDB Saga version instead of repeating it per
// artifact:
//
//     implementation(platform("com.scalar-labs:scalardb-saga-bom:<version>"))
//     implementation("com.scalar-labs:scalardb-saga-core")
//     implementation("com.scalar-labs:scalardb-saga-grpc-client")
//
// This module holds no code and applies no java-conventions — a `java-platform` cannot also be a
// `java-library`, which is why the shared coordinates live in base-conventions.
description =
    "A bill of materials pinning every ScalarDB Saga artifact to one version. Import it as a " +
    "platform and declare the ScalarDB Saga dependencies without versions."

dependencies {
    constraints {
        // Every published module, and only those: the daemon ships as a container image, so it has
        // no coordinate a consumer could declare. Listed explicitly rather than derived from the
        // subproject list so adding a module is a deliberate decision to publish it.
        api(project(":api"))
        api(project(":core"))
        api(project(":rpc"))
        api(project(":grpc-client"))
    }
}
