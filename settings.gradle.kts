pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "scalardb-saga"

include("api")
include("bom")
include("core")
include("server")
include("rpc")
include("grpc-client")
