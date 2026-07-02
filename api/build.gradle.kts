plugins {
    id("scalardb-saga.java8-conventions")
}

// The Java-8-clean public API surface consumed by core/daemon and (later) the Java 8 client and
// participant SDKs. Holds the public api/ and exception/ types, compiled under --release 8 by the
// java8-conventions plugin.
