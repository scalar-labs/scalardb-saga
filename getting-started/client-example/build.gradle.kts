plugins {
    java
    application
}

repositories {
    mavenCentral()
}

dependencies {
    // The Java 8 client SDK; it brings the gRPC transport it needs.
    implementation("com.scalar-labs:scalardb-saga-java-client-sdk:3.19.0-alpha.1")
}

application {
    mainClass = "example.SagaClientExample"
}
