plugins {
    id("scalardb-saga.java-conventions")
}

// Thin override of java-conventions for the Java 8 SDK surface (api + future client/participant).
// Keeps the JDK 25 toolchain (so Error Prone / NullAway still run), but compiles the MAIN source
// against the Java 8 platform API + language level via --release 8 (backed by ct.sym). This enforces
// a Java-8-clean published surface: no records, no java.net.http, no Map.of/copyOf, no instanceof
// patterns, etc. Tests are NOT constrained — they run at the JDK 25 toolchain.
tasks.named<JavaCompile>("compileJava") {
    options.release = 8
    // Keep all javac lint (notably -Xlint:unchecked and -Xlint:deprecation, which Error Prone /
    // NullAway do NOT replicate) on the published Java 8 surface, minus two categories we silence:
    //   - `options`: the benign "source/target value 8 is obsolete" notice inherent to --release 8.
    //   - `serial`: these exception classes are Serializable (via Throwable) but are never actually
    //     Java-serialized — daemon mode maps them to gRPC/HTTP error responses, embedded mode throws
    //     them in-process — so missing-serialVersionUID / non-serializable-field warnings are
    //     theoretical noise here. (If these types ever gain real serialization, enforce `serial` and
    //     fix them properly.)
    // One inherent notice still prints and is NOT a -Xlint category: "unknown enum constant
    // ElementType.MODULE", from javac reading jspecify's @NullMarked @Target (which lists the Java 9
    // MODULE constant, absent under --release 8; harmless — @NullMarked still applies to the
    // package/type targets that do exist). It is log noise only: the project does not compile with
    // -Werror, so it never fails the build, and suppressing it via -Xlint:none would also discard the
    // unchecked/deprecation coverage we want to keep here.
    options.compilerArgs.add("-Xlint:all,-options,-serial")
}
