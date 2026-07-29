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

## Branching model

Every branch except `main` is named after the version it carries, following the same model as
[ScalarDB](https://github.com/scalar-labs/scalardb). Releases are cut from minor version branches
(called *the release branch* below), never from `main`.

The versions below are an example of the state once 1.1 has shipped and work on 2.0 has begun:

| Branch | Carries | Role |
| --- | --- | --- |
| `main` | `2.0.0-SNAPSHOT` | Trunk. Every change lands here first. Never tagged. |
| `1` | `1.2.0-SNAPSHOT` | Major version branch. Appears once `main` moves on to the next major. |
| `1.1` | `1.1.1-SNAPSHOT` | Minor version branch, current 1.x line; `v1.1.x` tags are cut from here. |
| `1.0` | `1.0.4-SNAPSHOT` | Minor version branch, in maintenance; still takes backported fixes. |

There is no patch version branch: a patch is a tag on its minor version branch, not a branch of its
own. Today only `main` exists, at `1.0.0-SNAPSHOT`; `1.0` gets cut when 1.0 is ready to ship.

Branch names carry no prefix — `1`, `1.0`, `1.1`. Every workflow matches them with the `[0-9]+` and
`[0-9]+.[0-9]+` patterns, so a name like `release/1.0` gets no CI and publishes no snapshot.

Fixes land on the newest branch that needs them and are **backported down** the lines they apply to.
Minor version branches never merge back: the commit that sets the release version, and every backport
after it, live only on that branch and are unreachable from `main` by design.

## Cutting a release

1. **Make sure the release branch exists and carries the change.**

   For the first release of a minor version, cut its branch from `main` and bump `main` to the next
   minor snapshot so trunk and the branch stop claiming the same version:

   ```bash
   git switch main && git pull
   git switch -c 1.0 && git push -u origin 1.0
   ```

   A newly cut branch also needs its Dependabot entries enabled — uncomment a copy of the template at
   the end of [.github/dependabot.yml](.github/dependabot.yml) naming it. Without them the branch gets
   no dependency updates at all, because every existing entry targets `main` only.

   For a patch release the branch already exists — backport the fix to it through a PR.

2. On the release branch, set the release version in `gradle.properties` (drop `-SNAPSHOT`) and merge
   it through a PR so CI runs on it:

   ```properties
   version=1.0.0
   ```

3. Tag that commit on the release branch and push the tag:

   ```bash
   git switch 1.0 && git pull
   git tag v1.0.0 && git push origin v1.0.0
   ```

4. The `Release` workflow verifies the tag against `gradle.properties`, verifies the tagged commit is
   on the release branch the version names, uploads the Maven Central deployment, builds and pushes
   the multi-architecture image, signs it, and creates the GitHub release with the distribution
   archives.
5. Release the Maven Central deployment from the [Central Portal](https://central.sonatype.com/publishing/deployments).
   This step is a deliberate human action: a released version on Central is immutable and cannot be
   replaced or withdrawn, so the workflow leaves the deployment `VALIDATED` rather than releasing it.
   To drop a bad deployment instead, use `./gradlew dropMavenCentralDeployment`.
6. Bump the release branch to the next patch `-SNAPSHOT` (`1.0.1-SNAPSHOT`).

The tag is the source of truth for *which commit*, and `gradle.properties` for *which version*; the
workflow fails if they disagree rather than deriving one from the other, so a jar whose internal
version differs from its tag can never be published. It fails the same way if the tagged commit is
not reachable from the release branch its version names — `v1.0.1` must be on `1.0` — so a tag on an
unreviewed branch cannot reach Maven Central or `ghcr.io`.

The image is pushed only after Central has accepted the deployment, and for the same reason: a
validated deployment can still be dropped, while an image tag is public from the moment it is pushed
and may already have been pulled. A release that fails at the upload therefore leaves nothing behind
on `ghcr.io`. The converse does not hold — the image goes out before step 5, so abandoning a release
at the Portal leaves its tags published.

Every commit on `main` and on each release branch publishes a `-SNAPSHOT` to the Central snapshot
repository, so downstream work can track either trunk or a maintenance line without waiting for a tag.

### Re-running a failed release

A release can fail partway — the image push breaking after Maven Central has already accepted the
upload, say. The tag is published by then and a released version is immutable, so cutting a new tag is
usually the wrong answer. Re-run the workflow instead: **Actions → Release → Run workflow**, selecting
the **tag** in the ref picker rather than a branch. A branch is rejected by the first step, since
every job derives its version from the tag.

Not every step is idempotent, so check what already succeeded before re-running:

| Step | On a second run |
| --- | --- |
| GitHub release | Safe. Assets are replaced with `--clobber`, and existing notes are left alone in case they were edited by hand. |
| Image push and signature | Safe in itself: the same tags are overwritten, and an extra signature is harmless. It runs only once the Central upload has succeeded, though, so a re-run aimed at the image still needs the previous deployment dropped. |
| Maven Central | **Not automatic.** Drop the previous deployment first with `./gradlew dropMavenCentralDeployment`, or the upload is rejected as a duplicate, which stops the image and the GitHub release with it. If it was already released, that version can never be replaced — ship the fix as the next patch instead. |

## Required repository secrets

| Secret | Used for |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Central Portal token username |
| `MAVEN_CENTRAL_PASSWORD` | Central Portal token password |
| `MAVEN_CENTRAL_GPG_SECRET_KEY` | ASCII-armored private key used to sign artifacts |
| `MAVEN_CENTRAL_GPG_PASSPHRASE` | Passphrase for that key |

There is no public-key secret: an OpenPGP private key carries its own public material, and Gradle's
signing plugin needs nothing else. (ScalarDB publishes with JReleaser, which does additionally require
`JRELEASER_GPG_PUBLIC_KEY` — that difference is the publishing tool, not the key.)

### Publishing the public key

The signing key needs one manual step outside this repository, once per key. Maven Central verifies
the signature by fetching the public key from a public keyserver, so the key has to be there before
the first release. If it is not, the deployment fails **validation at the Portal** rather than in the
workflow, so the build goes green and the release still cannot be completed.

```bash
gpg --list-secret-keys --keyid-format=long   # the 40-hex-character fingerprint is on its own line
gpg --keyserver keyserver.ubuntu.com --send-keys <fingerprint>
gpg --keyserver keys.openpgp.org --send-keys <fingerprint>
```

Confirm the key comes back *with its User ID* before tagging anything. Checking that something comes
back is not enough: `keys.openpgp.org` serves a key stripped of its User IDs until the address is
verified, and GnuPG will not import a key that has none. A `--send-keys` upload does not start that
verification — it has to be requested from <https://keys.openpgp.org/upload>, so no mail arrives
until someone does. Both servers return an armored block either way, so only the packet contents
distinguish them.

```bash
curl -sSf "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x<fingerprint>" | gpg --show-keys
```

A `uid` line in the output means the key is complete. Empty output means the key is present but
UID-stripped, which will not satisfy a verifier.

`MAVEN_CENTRAL_GPG_SECRET_KEY` is the armored **private** key, and `--export-secret-keys` is what
produces it; plain `--export` yields the public key, which signs nothing:

```bash
gpg --armor --export-secret-keys <fingerprint>
```

Paste that output straight into the GitHub secret. It is the private key: it must never be written to
a file in the repository, pasted into an issue or chat, or echoed into a shell that keeps history.

The image needs no secret: it pushes to `ghcr.io` with the workflow's own `GITHUB_TOKEN`, and cosign
signs keylessly using the job's OIDC identity, so there is no signing key to rotate or leak.

Artifacts are signed only when a key is present, so `./gradlew publishToMavenLocal` works unsigned on
a developer machine.

## Verifying a published image

```bash
cosign verify ghcr.io/scalar-labs/scalardb-saga-daemon:1.0.0 \
  --certificate-identity=https://github.com/scalar-labs/scalardb-saga/.github/workflows/release.yml@refs/tags/v1.0.0 \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

The identity is the whole check, and it has to be exact. A keyless signature on its own proves only
that *some* GitHub Actions job signed this image, so a pattern that stops at the repository — say
`--certificate-identity-regexp='^https://github.com/scalar-labs/scalardb-saga/'` — is satisfied by a
signature from any workflow in this repository running on any branch, since the certificate names the
workflow file and ref after the repository. Any job here granted `id-token: write` would pass it.
Naming `release.yml@refs/tags/v1.0.0` is what makes the signature evidence that the release workflow,
running on that tag, produced this image.

Both halves carry the version, and they have to agree: a `v1.0.0` identity verifying a `:1.1.0` image
would mean the image was not built by the release it claims to be. Re-runs do not change this — a
dispatched re-run is rejected unless it targets the tag, so it signs under the same identity.

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
