# ScalarDB Saga

A saga-based distributed transaction coordination engine.

Refer to `~/git/scalardb-saga-design/docs/scalardb-saga-design.md` for architecture decisions and implementation details.

## Language

- **Java 21** for all modules (core engine, framework integrations, daemon, testing, dev server, etc.)
- **Java 8** only for daemon client SDKs (`scalardb-saga-client`, `scalardb-saga-grpc-client`) to maximize adoption
- Users on Java 8 use daemon mode via client SDK or call HTTP/gRPC endpoints directly

## Build

- **Gradle 9.x with Kotlin DSL** (`build.gradle.kts`)
- Format apply: `./gradlew spotlessApply`
- Check (test + format + static analysis): `./gradlew check`
- Check for compiler warnings (hidden when cached): `./gradlew clean compileTestJava --no-build-cache`
- **Always run all three in order (`spotlessApply` → `check` → `clean compileTestJava --no-build-cache`) before confirming code changes are OK**
- **Convention plugins** in `build-logic/` — shared build logic lives here, not in `subprojects {}` / `allprojects {}`
- **Version catalog** in `gradle/libs.versions.toml` — single source of truth for dependency versions
- **Configuration cache** enabled (`org.gradle.configuration-cache=true`)
- New subprojects apply `id("scalardb-saga.java-conventions")` and declare only their specific dependencies

## Code Style

- Follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- Enforced by [Spotless](https://github.com/diffplug/spotless) with google-java-format (run `./gradlew spotlessApply`)
- **Comment prose:** google-java-format reflows adjacent `//` comment lines as a paragraph and greedily rewraps at 100 columns. Dense prose makes it strand a short token (a lone `/`, `or`, or a single word) on its own line. To keep comments reflowing cleanly:
  - Don't join words with `/` in prose (`putAll/copyOf` → `putAll or copyOf`)
  - Prefer `;` or `.` over `—` to break clauses
  - After editing a `//` comment, run `spotlessApply` and eyeball the result — a one-token orphan line is the tell that the prose needs rewording (you can't fix it by hand-wrapping; the reflow just re-breaks it)

## Static Analysis

- **Error Prone** — compiler plugin, catches semantic bugs during `compileJava`
- **NullAway** — Error Prone check enforcing null-safety via JSpecify annotations
- **SpotBugs** + **FindSecBugs** — bytecode analysis for bugs and security vulnerabilities
- Use `@NullMarked` on `package-info.java` to enable null-safety per package; annotate nullable types with `@Nullable` from `org.jspecify.annotations`
- **Null-check policy:**
  - **Public API** — use `Objects.requireNonNull` for defense in depth (callers may not be compiled with NullAway)
  - **Internal classes** (package-private, or `public` solely for cross-package access within the module) — rely on `@NullMarked` + NullAway; do not add redundant `Objects.requireNonNull`

## Package Naming

- Base package: `com.scalar.db.saga`
- Public API classes use `Saga` prefix when the remainder is too generic to stand alone (e.g., `SagaManager`, `SagaContext`, `SagaStatus`). Domain-specific names that are already unambiguous within the package omit the prefix (e.g., `Step`, `StepResult`, `RetryPolicy`, `TccStep`).
- Internal classes use domain-specific names without prefix (e.g., `CompensationManager`)

## Design Principles

- Follow **SOLID** and **DRY**
- Prefer **immutable objects** — they simplify concurrency and reasoning
- Ensure **thread-safety** when immutability is not feasible — this is essential in distributed systems
- Design for **testability** — difficulty in writing unit tests is a sign of poor design. Use Dependency Injection (DI) to keep classes testable.

## Testing

- **JUnit 5**, **Mockito**, **AssertJ**
- Never use PowerMock — needing it indicates a design problem. Refactor instead.
- **Co-locate unit tests with implementation** — write tests in the same task as the classes they test. Do not defer tests to a separate bulk task. Integration tests that span multiple classes may be a separate task.
- **Test all public methods** — every public method must have at least one test. Important private methods should also be tested (via public API or package-private access).
- **Cover both success and failure cases** — test normal (success) paths and abnormal (failure) paths. Failure cases should be covered **extensively** — they are where bugs hide. Include edge cases, invalid inputs, exception paths, concurrency errors, and timeout scenarios.
- Test method naming: `methodName_condition_expectedResult()`
  - The condition must read as a scenario, not a method-overload label
  - Use `Given` suffix for **inputs/arguments** passed to the method: `of_mapGiven_...`, `constructor_messageOnlyGiven_...`
  - Use `with` prefix for **configuration/state** set via builder or setup: `build_withDefaults_...`, `step_withRetryPolicy_...`
  - Conditions that naturally read as states need neither: `insufficientBalance`, `noSteps`, `duplicateStepNames`
  ```java
  @Test
  public void transfer_insufficientBalance_throwsException() { ... }
  @Test
  public void of_singleKeyValueGiven_returnsResultWithEntry() { ... }
  @Test
  public void build_withAllOptions_setsAllFields() { ... }
  ```
- Group test code by `// Arrange`, `// Act`, and `// Assert`
- **Exception assertions** — assert `isInstanceOf` (required) but omit `hasMessageContaining` unless the same exception type is thrown by multiple validation paths that the test input could trigger. Craft test inputs to be specific enough that only one path fires; message assertions couple tests to wording and hurt maintainability.

## Module Structure

Subproject directories use short names; artifacts are prefixed with `scalardb-saga-` (via `base.archivesName` for the jar and the publication's `artifactId` for the coordinate — these are set separately).

- `api` — Java-8-clean public API surface (interfaces, value types, exceptions)
- `core` — Core engine (engine, store, recovery, testing harness)
- `rpc` — gRPC wire contract (`.proto` plus generated stubs), Java 8
- `grpc-client` — Java 8 daemon client SDK
- `server` — Standalone server (REST + gRPC); ships as a container image, not a Maven artifact
- `bom` — `java-platform` BOM pinning every published artifact to one version
- Future: `spring`, `quarkus`, `participant`, `client` (HTTP SDK), `dev-server`, `lra`

## Publishing

See [RELEASING.md](RELEASING.md) for the release process.

- **Maven group is `com.scalar-labs`**, not the Java package — set in `scalardb-saga.base-conventions`. Matches the other Scalar artifacts on Central; cannot change after the first release.
- Published modules apply `id("scalardb-saga.publishing-conventions")` and **must set `description`** (Maven Central rejects a POM without one). The convention sets the `artifactId` explicitly — `base.archivesName` does not affect it.
- The server is deliberately unpublished: it ships as a container image, so a jar on Central would be an artifact nobody consumes and everyone has to keep patched.
- Snapshots publish from `main` and every release branch (`.github/workflows/release-snapshot.yml`); releases leave the Central deployment `VALIDATED` for a human to release, because a released version is immutable.

## Container image

See [server/docker/README.md](server/docker/README.md) for running it.

- **Every external Gradle plugin goes through `build-logic`**, never a subproject's own `plugins {}` block: declaring one in a subproject gives that project a separate classloader scope and breaks Spotless' shared build service.
- Netty must stay on a single version across all modules (`libs.versions.toml` `netty` + the BOM import in `:server`) — the native transports load version-matched `.so` files.
- Built from `installDist`, not `distTar` (whose internal root embeds the version): `./gradlew :server:dockerBuild` locally, or `:server:dockerContext` + `docker/build-push-action` in CI.

## CI

- **GitHub Actions** (`.github/workflows/ci.yml`) — on push to `main` and the release branches, and on every PR:
  - `check` — `./gradlew check` (`test` + `integrationTest` + `javadoc` + `spotlessCheck` + `spotbugsMain` + Error Prone), then a no-build-cache compile to surface warnings
  - `dockerfile-lint` — hadolint over `server/docker/Dockerfile`
  - `image-smoke-test` — builds the image and runs it against SQLite, asserting health on both transports, non-root uid, `INFO` logging, a clean `SIGTERM` drain, and that the epoll native transport actually loaded (a silent NIO fallback keeps the server healthy, so `/proc/1/maps` is the evidence)
  - `image-arm64-native-test` — the same boot under QEMU on `linux/arm64`, asserting the `aarch_64` epoll native loads; only `linux-x86_64` arrives transitively, so nothing else catches a dropped classifier
  - Both smoke jobs boot from the shared fixture in `.github/smoke/`
- **Release** (`release.yml`, on `v<major>.<minor>.<patch>` tags, with or without a pre-release suffix) — asserts the tag matches `gradle.properties` and that the tagged commit is on the release branch its version names, then publishes to Maven Central, pushes the multi-arch image, signs it, and creates the GitHub release
- **Dependabot** (`.github/dependabot.yml`) — gradle (incl. the version catalog), github-actions, and the Dockerfile base-image digest
  - Dependabot resolves against Maven Central only: it reads repositories from the root build file and root settings, never from `build-logic`'s own block. The `repositories {}` block in `build.gradle.kts` is therefore load-bearing despite resolving nothing at build time — it is what makes the portal-only Error Prone, NullAway, SpotBugs, and license-report plugins visible. Deleting it freezes them silently.
  - Every `[versions]` key needs a `[libraries]` entry pointing at it with `version.ref`, even one nothing resolves (see `junit-jupiter`). Dependabot reads only `[libraries]` and `[plugins]`; a version consumed solely as `libs.versions.<name>.get()` interpolation is one it cannot map to a module, so it never bumps it. Consume plugin coordinates in `build-logic` through catalog accessors, not interpolated strings.

## Git

- **Trunk-based development with release branches** — every change lands on `main` first; releases are cut from minor version branches (`1.0`, `1.1`), never from `main`. Fixes are backported down the lines, never merged up. See [RELEASING.md](RELEASING.md).
- Branches are named after the version they carry, with no prefix — major version branch `1`, minor version branch `1.0` — so they match the `[0-9]+` and `[0-9]+.[0-9]+` patterns every workflow keys off. A patch is a tag, not a branch.
- **Conventional Commits** (e.g., `feat: add saga engine`, `fix: handle timeout`)

## TODO

- [ ] Verify the release workflow end to end on the first tag (Maven Central credentials and the ghcr push are untested)
- [ ] Add the UBI variant of the server image if Red Hat Marketplace / OpenShift certification is needed
