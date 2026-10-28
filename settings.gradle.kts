pluginManagement {
    includeBuild("build-logic")
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "scalardb-saga"

include("api")
include("benchmark")
include("bom")
include("client")
include("core")
include("rpc")
include("server")
