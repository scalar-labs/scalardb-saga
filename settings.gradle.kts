pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "scalardb-saga"

include("api")
include("core")
include("daemon")
include("rpc")
include("grpc-client")
