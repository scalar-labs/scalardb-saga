plugins {
    id("scalardb-saga.java-conventions")
}

// Thin override of java-conventions for the Java 8 SDK surface (api + future client/participant).
// Keeps the JDK 21 toolchain (so Error Prone / NullAway still run), but compiles the MAIN source
// against the Java 8 platform API + language level via --release 8 (backed by ct.sym). This enforces
// a Java-8-clean published surface: no records, no java.net.http, no Map.of/copyOf, no instanceof
// patterns, etc. Tests are NOT constrained — they run at the JDK 21 toolchain.
tasks.named<JavaCompile>("compileJava") {
    options.release = 8
    // Silence two benign javac notices inherent to the deliberate Java 8 target (Error Prone +
    // NullAway run independently of -Xlint, so real code-quality checks are unaffected):
    //   - "source/target value 8 is obsolete"
    //   - "unknown enum constant ElementType.MODULE": jspecify's @NullMarked @Target lists the
    //     Java 9 MODULE constant, absent under --release 8 (harmless — @NullMarked still applies to
    //     the package/type targets that do exist). This one is not a -Xlint category, so -Xlint:none
    //     (not -Xlint:-classfile) is required to suppress it.
    options.compilerArgs.add("-Xlint:none")
}
