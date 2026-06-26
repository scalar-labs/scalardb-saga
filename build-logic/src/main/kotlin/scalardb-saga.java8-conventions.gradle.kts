plugins {
    id("scalardb-saga.java-conventions")
}

// Thin override of java-conventions for the Java 8 SDK surface (api + future client/participant).
// Keeps the JDK 21 toolchain (so Error Prone / NullAway still run), but compiles against the
// Java 8 platform API + language level via --release 8 (backed by ct.sym). This enforces a
// Java-8-clean surface: no records, no java.net.http, no Map.of/copyOf, no instanceof patterns, etc.
tasks.withType<JavaCompile>().configureEach {
    options.release = 8
    // Suppress javac's "source/target value 8 is obsolete" notice for the deliberate Java 8 surface.
    options.compilerArgs.add("-Xlint:-options")
}
