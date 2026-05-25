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

Subproject directories use short names; published artifacts are prefixed with `scalardb-saga-` (via `base.archivesName`).

- `core` — Core engine (API, engine, store, recovery, testing harness)
- Future: `spring`, `quarkus`, `participant`, `daemon`, `client`, `dev-server`, `lra`

## CI

- **GitHub Actions** (`.github/workflows/ci.yml`) — runs `./gradlew check` (includes `test` + `spotlessCheck` + `spotbugsMain` + Error Prone) on push/PR to `main`

## Git

- **Trunk-based development**
- **Conventional Commits** (e.g., `feat: add saga engine`, `fix: handle timeout`)

## TODO

- [ ] Add CI workflow details once GitHub Actions are verified in CI
