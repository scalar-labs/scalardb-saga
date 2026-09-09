pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "scalardb-saga"

include("api")
include("benchmark")
include("bom")
include("client")
include("core")
include("rpc")
include("server")
