# Releasing

A release publishes two things from one tag: the Java artifacts to Maven Central, and the daemon
container image to `ghcr.io/scalar-labs/scalardb-saga-daemon`.

## What gets published

| Artifact | Coordinate | Consumed by |
| --- | --- | --- |
| API | `com.scalar-labs:scalardb-saga-api` | Everyone, transitively |
| Engine | `com.scalar-labs:scalardb-saga-core` | Applications embedding the engine in-process |
| Wire contract | `com.scalar-labs:scalardb-saga-rpc` | The gRPC client, transitively |
| Client SDK | `com.scalar-labs:scalardb-saga-grpc-client` | Java 8+ applications calling the daemon |
| BOM | `com.scalar-labs:scalardb-saga-bom` | Anyone pinning several of the above |
| Daemon image | `ghcr.io/scalar-labs/scalardb-saga-daemon` | Operators running daemon mode |

`:daemon` is deliberately not published to Maven Central — it ships as the image, so a jar on Central
would be an artifact nobody consumes and everyone has to keep patched.

Consumers pin one version through the BOM:

```kotlin
implementation(platform("com.scalar-labs:scalardb-saga-bom:1.0.0"))
implementation("com.scalar-labs:scalardb-saga-core")
implementation("com.scalar-labs:scalardb-saga-grpc-client")
```

## Cutting a release

1. Set the release version in `gradle.properties` (drop `-SNAPSHOT`) and merge it to `main`.
2. Tag that commit and push the tag:

   ```bash
   git tag v1.0.0 && git push origin v1.0.0
   ```

3. The `Release` workflow verifies the tag matches `gradle.properties`, uploads the Maven Central
   deployment, builds and pushes the multi-architecture image, signs it, and creates the GitHub
   release with the distribution archives.
4. Release the Maven Central deployment from the [Central Portal](https://central.sonatype.com/publishing/deployments).
   This step is a deliberate human action: a released version on Central is immutable and cannot be
   replaced or withdrawn, so the workflow leaves the deployment `VALIDATED` rather than releasing it.
   To drop a bad deployment instead, use `./gradlew dropMavenCentralDeployment`.
5. Bump `gradle.properties` to the next `-SNAPSHOT` on `main`.

The tag is the source of truth for *which commit*, and `gradle.properties` for *which version*; the
workflow fails if they disagree rather than deriving one from the other, so a jar whose internal
version differs from its tag can never be published.

Every commit on `main` with a `-SNAPSHOT` version publishes to the Central snapshot repository, so
downstream work does not have to wait for a tag.

## Required repository secrets

| Secret | Used for |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Central Portal token username |
| `MAVEN_CENTRAL_PASSWORD` | Central Portal token password |
| `MAVEN_CENTRAL_GPG_SECRET_KEY` | ASCII-armored private key used to sign artifacts |
| `MAVEN_CENTRAL_GPG_PASSPHRASE` | Passphrase for that key |

The image needs no secret: it pushes to `ghcr.io` with the workflow's own `GITHUB_TOKEN`, and cosign
signs keylessly using the job's OIDC identity, so there is no signing key to rotate or leak.

Artifacts are signed only when a key is present, so `./gradlew publishToMavenLocal` works unsigned on
a developer machine.

## Verifying a published image

```bash
cosign verify ghcr.io/scalar-labs/scalardb-saga-daemon:1.0.0 \
  --certificate-identity-regexp='^https://github.com/scalar-labs/scalardb-saga/' \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

Each image also carries an SBOM and a build provenance attestation:

```bash
docker buildx imagetools inspect ghcr.io/scalar-labs/scalardb-saga-daemon:1.0.0
```

## Building locally

```bash
./gradlew :daemon:dockerBuild     # single architecture, loaded into the local Docker
./gradlew publishToMavenLocal     # all published artifacts into ~/.m2, unsigned
```

`dockerBuild` deliberately does not push. Multi-architecture images, attestations, and signatures come
only from the release workflow, so a locally built image can never be mistaken for a released one.
