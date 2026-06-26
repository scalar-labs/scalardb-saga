plugins {
    id("scalardb-saga.java8-conventions")
}

// The Java-8-clean public API surface consumed by core/daemon and (later) the Java 8 client and
// participant SDKs. Types are relocated in here during the api-module reorg (PR 0, phases 2-3);
// the module is intentionally empty at scaffold time.
