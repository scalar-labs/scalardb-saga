# Releasing

A release publishes two things from one tag: the Java artifacts to Maven Central, and the daemon
container image to `ghcr.io/scalar-labs/scalardb-saga-server`.

## What gets published

| Artifact | Coordinate | Consumed by |
| --- | --- | --- |
| API | `com.scalar-labs:scalardb-saga-api` | Everyone, transitively |
| Engine | `com.scalar-labs:scalardb-saga-core` | Applications embedding the engine in-process |
| Wire contract | `com.scalar-labs:scalardb-saga-rpc` | The gRPC client, transitively |
| Client SDK | `com.scalar-labs:scalardb-saga-java-client-sdk` | Java 8+ applications calling the daemon |
| BOM | `com.scalar-labs:scalardb-saga-bom` | Anyone pinning several of the above |
| Server image | `ghcr.io/scalar-labs/scalardb-saga-server` | Operators running daemon mode |

`:server` is deliberately not published to Maven Central — it ships as the image, so a jar on Central
would be an artifact nobody consumes and everyone has to keep patched.

The image carries one immutable tag per release — `1.0.0`, `1.0.1` — and nothing that floats. There is
deliberately no `1.0` tag: the minor version line is a branch in this repository, and a second
representation of it in the registry would be one nothing keeps truthful. There is no `latest` either,
for that reason and a stronger one: a moving tag cannot be verified by the recipe in
[Verifying a published image](#verifying-a-published-image), because that binds the signature to the
release tag which built the image, and a tag that moves has no version for the certificate identity to
agree with. `latest` would be the tag most people pull and the only one nobody can check.

Every other Scalar image publishes exact versions only, so pin one. Let Dependabot or Renovate bump it,
which puts the change in your repository rather than silently in ours. (The GitHub release page still
marks the newest release *Latest*; that is a badge on the release, not a tag on the image.)

Consumers pin one version through the BOM, and declare the artifact for the mode they run in. The two
are alternatives, not a pair: `core` embeds the engine in the application's own process, while the
client SDK calls a daemon that runs it elsewhere. The SDK deliberately never depends on `core`, so
declaring both puts the whole engine into an application that only wanted a client.

```kotlin
// Daemon mode — calling the daemon from a Java 8+ application
implementation(platform("com.scalar-labs:scalardb-saga-bom:VERSION"))
implementation("com.scalar-labs:scalardb-saga-java-client-sdk")
```

```kotlin
// Embedded mode — running the engine in-process
implementation(platform("com.scalar-labs:scalardb-saga-bom:VERSION"))
implementation("com.scalar-labs:scalardb-saga-core")
```

Neither snippet needs `scalardb-saga-api` or `scalardb-saga-rpc` declared: each module exposes what
it needs with `api(project(...))` — `core` the API, the client SDK the API and the wire contract —
so they arrive transitively with their versions constrained by the BOM. The client SDK also brings
`grpc-netty-shaded` as `runtimeOnly`, so a consumer never picks a transport.

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
own. Today only `main` exists; the first minor version branch gets cut when its release is ready to
ship.

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
   git switch -c <release-branch> && git push -u origin <release-branch>
   ```

   A newly cut branch also needs its own Dependabot entries. Dependabot reads
   [.github/dependabot.yml](.github/dependabot.yml) from the default branch only, and every entry
   there targets that branch, so a newly cut line gets no dependency updates until an entry names it.
   **On `main`, through a pull request**, copy the three `updates:` entries already in that file to the
   end of it, adding `target-branch: "1.0"` to each — substituting the branch you just cut — and delete
   the copies again when that line reaches end of life.

   Copy whole entries rather than retyping them, so the `groups:` blocks come along. Without them each
   build-tool bump arrives as its own pull request, and the five-PR limit fills with those before a
   real dependency update can open.

   A repository ruleset covering version branches is worth having before one is cut, for the same
   reason `main` has one: a release is built from the commit the tag names, and the workflow's check
   that the commit sits on its release branch is only worth as much as the branch's own rules. An
   unprotected `1.0` also takes a force-push or a deletion, which loses a maintenance line's
   history. One ruleset covers every version branch at once, so this is a one-time setup rather than
   a per-branch one — see [Repository settings](#repository-settings).

   Nothing enforces it: `verify-version` checks that the branch exists and that the tagged commit is
   reachable from it, not how the branch is protected. ScalarDB cuts its releases without one, so
   skipping it is a defensible choice for a line nothing depends on yet. What it costs is that the
   branch can be force-pushed or deleted, and that a commit can reach a tag without review.

   For a patch release the branch already exists — backport the fix to it through a PR.

2. On the release branch, set the release version in `gradle.properties` (drop `-SNAPSHOT`) and
   update the image default the getting-started walkthrough pulls, then merge both through a PR so
   CI runs on them:

   ```properties
   version=<version>
   ```

   ```yaml
   # getting-started/docker-compose.yaml
   image: ghcr.io/scalar-labs/scalardb-saga-server:${SAGA_VERSION:-<version>}
   ```

   That default is the only image a reader who has built nothing pulls, so it has to name a version
   that exists. CI fails the pull request when the two disagree, and `verify-version` refuses the
   tag, so a release cannot ship with a stale one.

3. Tag that commit on the release branch and push the tag:

   ```bash
   git switch <release-branch> && git pull
   git tag v<version> && git push origin v<version>
   ```

4. The `Release` workflow verifies the tag against `gradle.properties`, verifies the tagged commit is
   on the release branch the version names, uploads the Maven Central deployment, builds and pushes
   the multi-architecture image, signs it, and creates the GitHub release with the distribution
   archives.
5. Release the Maven Central deployment from the [Central Portal](https://central.sonatype.com/publishing/deployments).
   Confirm the deployment reached `VALIDATED` before releasing it: the workflow uploads the bundle
   and stops there, so a deployment the Portal rejects leaves the release green (see
   [Publishing the public key](#publishing-the-public-key)). Releasing is a deliberate human action —
   a released version on Central is immutable and cannot be replaced or withdrawn — so the workflow
   never releases the deployment itself. To drop a bad deployment instead, use the Portal's own Drop
   button: it needs no id and no local credentials, and the operator is already signed in here. The
   Gradle task is the fallback: `./gradlew dropMavenCentralDeployment --deployment-id=<id>` needs
   the id from the `publish-maven` log (`Uploaded bundle to Central Portal as USER_MANAGED,
   deployment id: <id>`) and the Portal token in `~/.gradle/gradle.properties` as
   `mavenCentralUsername` and `mavenCentralPassword`. Without the token the task itself still
   reports success and the build fails afterwards, during its end-of-build actions.
6. Bump the release branch to the next patch `-SNAPSHOT` (`1.0.1-SNAPSHOT`).

The tag is the source of truth for *which commit*, and `gradle.properties` for *which version*; the
workflow fails if they disagree rather than deriving one from the other, so a jar whose internal
version differs from its tag can never be published. It fails the same way if the tagged commit is
not reachable from the release branch its version names — `v1.0.1` must be on `1.0` — so a tag on a
scratch branch reaches neither Maven Central nor `ghcr.io`.

That check inherits whatever the release branch requires: with the ruleset from step 1 in place, the
published commit went through a pull request, and without it the check proves only that someone put
the commit on a branch with the right name.

The image is pushed only after the Maven Central upload has succeeded, and for the same reason: a
deployment can still be dropped, while an image tag is public from the moment it is pushed and may
already have been pulled. A release that fails at the upload — a bad credential, a signing failure,
a network fault — therefore leaves nothing behind on `ghcr.io`.

The ordering guarantees nothing beyond the upload. The build does not wait for the Portal to
validate the deployment (see [Publishing the public key](#publishing-the-public-key)), so an image
tag does not mean validation passed, and it does not mean a human released the deployment either —
the image goes out before step 5. Abandoning a release at the Portal, for either reason, leaves its
tags published.

Clean that up rather than leaving it behind: delete the git tag, the GitHub release and the `ghcr.io`
package version, so nothing advertises a version Central never published. The tag matters most, because
both the *Latest* badge and the pre-release classification are derived from tags rather than from what
was published — so an abandoned tag left in place stops every later release on an older line from taking
the badge. Dropping the deployment costs nothing else: the version was never released, so the same tag
can be cut again once whatever caused the abandonment is fixed.

Every commit on `main` and on each release branch publishes a `-SNAPSHOT` to the Central snapshot
repository, so downstream work can track either trunk or a maintenance line without waiting for a tag.

### Rehearsing the pipeline

Nothing in `publish-maven`, `publish-image` or `github-release` has ever run: the Central credentials,
the emulated arm64 build, the keyless signature and the release creation are all untested. The first
release is the worst version to discover that with, because it is the one that cannot be taken back.

A pre-release version is not a substitute. It lowers the stakes — nothing depends on it, and the
suffix keeps it from taking the *Latest* badge — but a released Central version is immutable
whatever it is called. An alpha or rc is still a first release, not a rehearsal.

A rehearsal has to look like a real release, because the guards in `verify-version` are doing their
job. A throwaway tag pushed on its own is refused by the version check, since the tag has to equal
the `version` in `gradle.properties`; then by the branch check, since `v0.0.1` derives the line
`0.0` and looks for a branch by that name; and then by the Compose-default check, which wants that
default to name the version being released. So it takes three pushes:

```bash
git switch -c 0.0 && git push -u origin 0.0    # scratch release branch
# set version=0.0.1-rc.1 in gradle.properties, and the SAGA_VERSION default in
# getting-started/docker-compose.yaml to match, then commit and push
git tag v0.0.1-rc.1 && git push origin v0.0.1-rc.1
```

Use a pre-release version rather than a bare `0.0.1`. The GitHub release's *Latest* badge follows the
highest released version, and with no other tags in the repository a bare `0.0.1` *is* the highest, so
a throwaway release would take the badge. A version carrying a suffix is classified as a pre-release,
which never takes it.

Three rungs are worth climbing in order, because each leaves more behind than the last.

**Guards only.** Leave the four `MAVEN_CENTRAL_*` secrets unset. `verify-version` runs end to end
for real, and `publish-maven` then fails at the signing task: an unset secret arrives as an empty
string, which Gradle reports as *present*, so `signAllPublications()` applies with no key to sign
with. The upload never happens — it is an end-of-build action that runs only when the build
succeeded — so nothing reaches Central or `ghcr.io`, and there is nothing to clean up but the tag
and the branch. A forgotten GPG secret therefore cannot publish unsigned artifacts.

**Snapshot.** Set `version=0.0.1-SNAPSHOT` on the scratch branch and push. That runs
[release-snapshot.yml](.github/workflows/release-snapshot.yml), which exercises the credentials, the
GPG key and a genuine upload, against the Central snapshot repository, which is mutable and
expendable. It does not cover the Portal deployment, the arm64 leg, cosign or the GitHub release.

**The full path.** Secrets in place, and the pre-release tag above. This is the only rung that
exercises the Portal deployment, the `linux/arm64` build under QEMU, the signature and the release
creation. **Do not release the deployment at the Portal** — dropping it is what keeps the rehearsal
reversible, and a released version is immutable.

Clean up afterwards:

| Left behind | Removal |
| --- | --- |
| Central deployment | Drop it, as in step 5. It was never released, so Central never published the version and nothing was ever downloadable. |
| Image tag | Delete the package version in `ghcr.io`. While this repository is private the package is private too, so it was never world-visible. |
| GitHub release, tag, scratch branch | Delete all three. |
| Cosign signature | **Cannot be removed.** Keyless signing records the image digest and this workflow's identity in Sigstore's public transparency log, which is append-only. It reveals the repository path and that a release ran — nothing confidential, but it is permanent, and it is the only part of a rehearsal that is. |

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
| Maven Central | **Not automatic.** Drop the previous deployment first, as in step 5; the plugin drops one by itself only when the build that uploaded it failed, which a green release is not. Nothing in the workflow enforces this: the build ends at the upload without waiting for the Portal's verdict, so a duplicate can surface as a rejected deployment while the run stays green and the image and the GitHub release go out. If the version was already released, it can never be replaced — ship the fix as the next patch instead. |

## Repository settings

Two things live in GitHub's own settings rather than in this repository, and neither is set today.
The rulesets are a one-time setup, best done before the first release branch is cut though nothing
enforces them; the environment is optional and can be added whenever.

### Branch and tag rulesets

`main` is covered by a ruleset requiring a pull request. The version branches are covered by
nothing, so the release workflow's check that a tagged commit sits on its release branch proves only
that someone put it on a branch with the right name.

Under **Settings → Rules → Rulesets → New ruleset → New branch ruleset**:

| Field | Value |
| --- | --- |
| Name | `version-branches` |
| Enforcement status | Active |
| Bypass list | `Organization admin`, `Repository admin`, matching the ruleset on `main` |
| Target branches | Include by pattern: `[0-9]*` |
| Rules | Restrict deletions; Block force pushes; Require a pull request before merging, with 2 approvals and conversation resolution; Require status checks to pass, naming `check`, `dockerfile-lint`, `image-smoke-test` and `image-arm64-native-test` |

The pattern is typed without `refs/heads/`, which the UI adds itself. Ruleset patterns are fnmatch,
not regex, so the `[0-9]+` patterns the workflows key off would match nothing here; `[0-9]*` is what
matches `1`, `1.0` and `1.10`. Leave **Restrict creations** unticked, or step 1's
`git push -u origin <release-branch>` is refused. The four check names may not appear in the picker
until they have run on a pull request; they can be typed in by hand.

A tag ruleset is worth adding alongside it, so that cutting a release is limited to whoever is
allowed to release. Under **New ruleset → New tag ruleset**: name `release-tags`, Active, target
tags `v*`, and the Restrict creations, Restrict deletions and Block force pushes rules. **Restrict
creations needs a bypass list** — with an empty one, nobody can push a release tag at all.

### Scoping the Central secrets to an environment

The four `MAVEN_CENTRAL_*` secrets reach these workflows from the organization: this repository
defines none of its own, and neither publishing job declares an environment, which leaves the
organization as the only scope they can come from. Organization access is granted per repository,
not per ref, so GitHub still makes them available to a workflow run on any branch here. No ruleset
changes that: the workflow doing the reading need not be one of the two in this repository, since a
branch can carry its own. An environment is the only mechanism that scopes secrets by ref, and it
fails closed — a run on a ref the environment does not admit is refused the environment and never
sees them.

1. **Settings → Environments → New environment**, named `maven-central`.
2. **Deployment branches and tags → Selected branches and tags**: add ref type *Branch* `main`, ref
   type *Tag* `v*`, and one *Branch* entry per version branch as it is cut. Name the branches
   explicitly rather than by pattern — deployment policies take wildcards, not the character classes
   the ruleset above uses, and a policy that matches nothing locks the release out.
3. Add the four secrets to the environment, then **remove this repository from the organization
   secrets' access list**. Copying rather than moving gains nothing: the organization grant stays
   readable from any ref here.
4. Add `environment: maven-central` to the publishing job in
   [release.yml](.github/workflows/release.yml) and
   [release-snapshot.yml](.github/workflows/release-snapshot.yml). Without it both jobs lose access
   to the secrets.

Adding a required reviewer to the environment would also give the release an approval gate, which
the manual release step at the Portal covers today.

## Required repository secrets

| Secret | Used for |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Central Portal token username |
| `MAVEN_CENTRAL_PASSWORD` | Central Portal token password |
| `MAVEN_CENTRAL_GPG_SECRET_KEY` | ASCII-armored private key used to sign artifacts |
| `MAVEN_CENTRAL_GPG_PASSPHRASE` | Passphrase for that key |

Any branch's workflows can read these, so write access to this repository is effectively access to
the signing key. Scoping them by ref takes an environment, which is not set up today — see
[Repository settings](#repository-settings).

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
cosign verify ghcr.io/scalar-labs/scalardb-saga-server:<version> \
  --certificate-identity=https://github.com/scalar-labs/scalardb-saga/.github/workflows/release.yml@refs/tags/v<version> \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

The identity is the whole check, and it has to be exact. A keyless signature on its own proves only
that *some* GitHub Actions job signed this image, so a pattern that stops at the repository — say
`--certificate-identity-regexp='^https://github.com/scalar-labs/scalardb-saga/'` — is satisfied by a
signature from any workflow in this repository running on any branch, since the certificate names the
workflow file and ref after the repository. Any job here granted `id-token: write` would pass it.
Naming `release.yml@refs/tags/v<version>` is what makes the signature evidence that the release
workflow, running on that tag, produced this image.

Both halves carry the version, and they have to agree: a `v1.0.0` identity verifying a `:1.1.0` image
would mean the image was not built by the release it claims to be. Re-runs do not change this — a
dispatched re-run is rejected unless it targets the tag, so it signs under the same identity.

Each image also carries an SBOM and a build provenance attestation:

```bash
docker buildx imagetools inspect ghcr.io/scalar-labs/scalardb-saga-server:<version>
```

## Building locally

```bash
./gradlew :server:dockerBuild     # single architecture, loaded into the local Docker
./gradlew publishToMavenLocal     # all published artifacts into ~/.m2, unsigned
```

`dockerBuild` deliberately does not push. Multi-architecture images, attestations, and signatures come
only from the release workflow, so a locally built image can never be mistaken for a released one.
