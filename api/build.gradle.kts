plugins {
    id("scalardb-saga.java8-conventions")
    id("scalardb-saga.publishing-conventions")
}

// The Java-8-clean public API surface consumed by core/daemon and (later) the Java 8 client and
// participant SDKs. Holds the public api/ and exception/ types, compiled under --release 8 by the
// java8-conventions plugin.
description =
    "The public API of ScalarDB Saga: the saga orchestrator interface, step and definition types, " +
    "and exceptions. Java 8 compatible. Pulled in transitively by scalardb-saga-core and " +
    "scalardb-saga-grpc-client."
