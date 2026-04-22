---
title: "feat: Implement ScalarDB Saga Orchestration Engine"
type: feat
status: active
date: 2026-04-20
deepened: 2026-04-20
---

## Enhancement Summary

**Deepened on:** 2026-04-20
**Sections enhanced:** 7 phases + cross-cutting concerns
**Research agents used:** performance-oracle, security-sentinel, architecture-strategist, code-simplicity-reviewer, spec-flow-analyzer, data-integrity-guardian, best-practices-researcher, framework-docs-researcher

### Key Improvements

1. **Structural simplification**: Remove Phase 7b (TCP/Netty, saves ~1,500 LoC), merge Phases 4+5, defer admin SagaStore methods from Phase 1 to Phase 5, collapse exception hierarchy (10 → 7 classes)
2. **Data integrity hardening**: Reverse `register()` order (persist before memory), add tombstone records for idempotent-create preservation, handle multi-row secondary index results, guard `markForRecovery()` against stale reads
3. **Security tightening**: SSRF protection for callback URLs (allowlist), HTTP body size limits, Jackson ObjectMapper safety (`disableDefaultTyping`), saga/step name validation, recovery timeout constraint (`recoveryTimeoutMs > max(stepTimeoutMs)`)
4. **Architecture refinement**: Split `SagaStore` into 4 focused interfaces, define `SagaEventListener` in Phase 1, replace `startAsync()` overloads with `StartSagaRequest` builder, make `NUM_BUCKETS` configurable
5. **Missing flows identified**: Saga cancellation flow absent from engine, `StepResult.pending()` behavior undefined in embedded mode, definition resolution failure handling missing, data retention mechanism unspecified

### New Considerations Discovered

- ScalarDB secondary index limitations vary across backends (Cassandra/DynamoDB: eventually consistent) — `markForRecovery()` should accept `SagaStateSnapshot` parameter instead of reading via secondary index
- Java 24 (JEP 491) eliminates `synchronized` pinning — for Java 21 target, use `ReentrantLock` only where blocking I/O occurs inside locks, and monitor with `jdk.VirtualThreadPinned` JFR events
- DTM's subtransaction barrier technique solves null-compensation, anti-suspension, and idempotency with two INSERT checks — worth considering for TCC empty-cancel handling
- Event sequence gaps on `appendEvent` failure may cause recovery to miss steps — add post-failure verification or make events idempotent by sequence number

---

# feat: Implement ScalarDB Saga Orchestration Engine

## Overview

Build a saga orchestration engine on top of ScalarDB that coordinates distributed transactions across microservices using both the Saga pattern (sequential steps with compensations) and TCC pattern (Try-Confirm-Cancel with resource reservations). The engine runs as an embedded JAR or standalone daemon, stores state via ScalarDB's database-agnostic transactions, and requires no message broker.

This plan replaces the previous plan (`plans/2026-04-01-001-feat-scalardb-saga-implementation-plan.md`) and reflects the significantly updated design document (`scalardb-saga-design.md`).

## Problem Statement / Motivation

Teams building microservices need eventual consistency across services (e.g., order placement spanning payment, inventory, and shipping). Current options either require heavy infrastructure (Temporal, Eventuate), are limited to specific databases (Seata), or lack both embedded and daemon deployment (Narayana LRA). ScalarDB Saga fills this gap: zero mandatory infrastructure in embedded mode, any-database via ScalarDB, and both Saga and TCC patterns in a single engine.

## Proposed Solution

A 7-phase incremental build:

1. **Phase 1 — Core Engine**: API interfaces, SagaEngine (unified pivot-based execution), CompensationManager, ScalarDbSagaStore (append-only events + bucket-partitioned saga_state), SagaRecoveryManager, TCC mode, testing harness
2. **Phase 2 — Communication & Frameworks**: ServiceInvoker (Layer 2), declarative step communication (Layer 2b), Spring Boot integration, Quarkus integration, participant SDK
3. **Phase 3 — Daemon Mode**: Standalone coordinator process with REST API, RemoteSagaManager client SDK
4. **Phase 4 — DX & Observability**: OpenTelemetry listener, SagaDevServer (local dev with SQLite + web UI)
5. **Phase 5 — Admin API**: SagaAdminService for production operations (list, inspect, retry, force-complete)
6. **Phase 6 — LRA Compliance**: MicroProfile LRA coordinator module (separate from SagaEngine), TCK compliance
7. **Phase 7 — Additional Transports**: gRPC transport (Protocol Buffers schema) + TCP/Netty transport

## Technical Approach

### Architecture

```
com.scalar.db.saga/
├── api/                         # Public interfaces (Step, TccStep, SagaManager, etc.)
├── engine/                      # Core execution (SagaEngine, CompensationManager, etc.)
├── parser/                      # JSON/YAML → SagaDefinition
├── store/                       # SagaStore interface + ScalarDbSagaStore
├── recovery/                    # SagaRecoveryManager + RecoveryConfig
├── exception/                   # 10 exception classes
├── timeout/                     # TimeoutPolicy
├── observability/               # SagaEventListener + OpenTelemetrySagaListener
├── admin/                       # SagaAdminService + query/metrics
├── testing/                     # SagaTestHarness + MockStep + CrashingStoreDecorator
└── devserver/                   # SagaDevServer + config
```

**Key design decisions reflected in this plan:**

- **Unified pivot-based execution model**: Both Saga (BACKWARD/FORWARD/MIXED) and TCC run through the same `executeSagaSteps()` loop. TCC definitions are expanded into a 2N-step plan with `TccReserveStep`/`TccConfirmStep` adapters.
- **PivotPolicy record**: Encapsulates pivot index + crossing event (e.g., CONFIRMING for TCC). BACKWARD = pivot at last step, FORWARD = pivot at -1, MIXED = user-specified pivot step.
- **Append-only event store**: 1 INSERT per step to `saga_events`. Mutable `saga_state` table for recovery scans. 3 tables total (saga_events, saga_state, saga_definitions).
- **Bucket-partitioned recovery**: `saga_state` uses `bucket = hash(sagaId) % NUM_BUCKETS` as partition key with `(status, updated_at, saga_id)` as clustering key.
- **Client-supplied saga IDs**: Idempotent-create semantics with `SagaAlreadyExistsException`.
- **SagaContext as interface**: `SagaContext` is the public interface; `ExecutionContext` is the engine-internal implementation with type validation, event sequencing, and failure tracking.
- **SagaDefinitionRegistry**: Two-tier versioned lookup (in-memory → store fallback) for recovery.
- **Testing harness in Phase 1**: `SagaTestHarness` with `ScalarDbSagaStore` backed by in-memory SQLite.
- **Two-phase compensation retry**: Immediate retry (CompensationManager) then periodic recovery retry (SagaRecoveryManager) with time-based escalation.

### Project Structure (Gradle Multi-Module)

Subproject directories use short names. Published artifacts are prefixed with `scalardb-saga-` via `base.archivesName`.

| Directory | Artifact | Phase | Purpose |
|-----------|----------|-------|---------|
| `core` | `scalardb-saga-core` | 1 | Core engine, API, store, recovery, testing harness |
| `spring` | `scalardb-saga-spring` | 2 | Spring Boot auto-config |
| `quarkus` | `scalardb-saga-quarkus` | 2 | Quarkus extension |
| `participant` | `scalardb-saga-participant` | 2 | Participant SDK |
| `daemon` | `scalardb-saga-daemon` | 3 | Standalone coordinator server |
| `client` | `scalardb-saga-client` | 3 | Client SDK (Java 8) |
| `dev-server` | `scalardb-saga-dev-server` | 4 | Local dev server (SQLite + web UI) |
| `lra` | `scalardb-saga-lra` | 6 | MicroProfile LRA coordinator |

Modules are added to `settings.gradle.kts` as each phase begins.

### Implementation Phases

---

#### Phase 1: Core Engine

**Scope:** Core engine + SagaStore + recovery + timeouts + virtual threads + graceful shutdown + saga versioning + compensation retry + SagaContext validation + TCC mode + MIXED recovery strategy (pivot transaction) + testing harness.

**Estimated: ~4,235 LoC, ~5.5-7.5 working days**

##### Task 1.1: Project Scaffolding

- [ ] Set up Gradle multi-module project with Kotlin DSL (`build.gradle.kts`)
- [ ] Configure google-java-format plugin
- [ ] Add dependencies: ScalarDB, Jackson (core + dataformat-yaml), JUnit 5, Mockito, AssertJ
- [ ] Configure SQLite for testing (ScalarDB JDBC backend)

**Files:**
- `build.gradle.kts` (root)
- `settings.gradle.kts`
- `core/build.gradle.kts`

##### Task 1.2: API Layer — Interfaces, Enums, POJOs

- [ ] `api/package-info.java` — `@NullMarked` for null-safety enforcement
- [ ] `api/Step.java` — Interface: `getName()`, `execute(SagaContext)`, `compensate(SagaContext)` (~15 LoC)
- [ ] `api/TccStep.java` — Interface: `getName()`, `reserve(SagaContext)`, `confirm(SagaContext)`, `cancel(SagaContext)` (~15 LoC)
- [ ] `api/SagaContext.java` — Public interface: `getSagaId()`, `get(key, type)`, `put(key, value)` (~10 LoC)
- [ ] `api/StepResult.java` — Data class with `of()`, `empty()`, `pending()` factory methods (~30 LoC)
- [ ] `api/SagaStatus.java` — Enum: RUNNING, CONFIRMING, COMPLETED, COMPENSATING, COMPENSATED, ESCALATED (~10 LoC)
- [ ] `api/RetryPolicy.java` — Config POJO with builder, `defaultPolicy()`, `compensationDefault()`, `confirmDefault()` factories (~40 LoC). Execution logic (`sleepWithBackoff()`) deferred to Task 1.4.
- [ ] `api/SagaDefinition.java` — POJO with `StepDefinition` inner class, `SagaMode` (SAGA/TCC), `RecoveryStrategy` (BACKWARD/FORWARD/MIXED), `getPivotIndex()`, validation, builder API (~80 LoC). `getPivotPolicy()` deferred to Task 1.4 (depends on `SagaEvent` from Task 1.5).
- [ ] `api/SagaStateSnapshot.java` — Immutable read-only view with `withTransition()` (~40 LoC)
- [ ] `api/SagaManager.java` — Interface: register ×2, start ×2, startAsync ×4, resume, compensate, getStateSnapshot, completeStep, startRecovery (~35 LoC)
- [ ] `api/SagaCallback.java` — Callback interface: onCompleted, onCompensated, onEscalated (~10 LoC)
- [ ] `exception/package-info.java` — `@NullMarked` for null-safety enforcement
- [ ] `exception/StepExecutionException.java` — Minimal stub with `retryable` flag (default: true). Full constructors added in Task 1.3.
- [ ] `exception/StepCompensationException.java` — Minimal stub. Full constructors added in Task 1.3.
- [ ] `exception/SagaDefinitionException.java` — Minimal stub. Needed for `SagaDefinition.validate()`.
- [ ] **Unit tests** (~200 LoC):
  - SagaDefinition: `getPivotIndex` for all strategies (BACKWARD/FORWARD/MIXED), validation rules (no steps, duplicate names, invalid pivot index), builder API
  - StepResult: factory methods (`of`, `empty`, `pending`), immutability
  - SagaStateSnapshot: `withTransition()` state transitions, version propagation

> **Implementation notes:**
> - `RetryPolicy` placed in `api` package (not `engine`) since it's part of the public `SagaDefinition` builder API. Task 1.4 adds `sleepWithBackoff()` execution logic to a separate engine-internal class or as a method on this class.
> - Exception stubs created here are minimal (enough for API compilation). Task 1.3 adds remaining exception classes and fleshes out constructors.
> - `getPivotPolicy()` deferred because `PivotPolicy` record references `SagaEvent` (Task 1.5). `getPivotIndex()` has no external dependencies and is included.

##### Task 1.3: Exception Hierarchy

> Note: `StepExecutionException`, `StepCompensationException`, and `SagaDefinitionException` stubs are created in Task 1.2. This task adds full constructors and the remaining exception classes.

- [ ] `exception/StepExecutionException.java` — Add full constructor variants (message+cause+retryable, etc.)
- [ ] `exception/StepCompensationException.java` — Add `stepName`/`stepIndex` fields and constructors
- [ ] `exception/StepTimeoutException.java`
- [ ] `exception/SagaTimeoutException.java` (extends StepTimeoutException)
- [ ] `exception/SagaPersistenceException.java`
- [ ] `exception/SagaAlreadyExistsException.java` — carries existing `SagaStateSnapshot`
- [ ] `exception/SagaDefinitionException.java`
- [ ] `exception/SagaDefinitionNotFoundException.java`
- [ ] `exception/SagaNotFoundException.java`
- [ ] `exception/SagaConcurrentModificationException.java`

Total: ~80 LoC

- [ ] **Unit tests** (~50 LoC):
  - Exception classes: `retryable` flag default and override, `SagaAlreadyExistsException` snapshot attachment, inheritance hierarchy

> **Research Insight (Simplicity + Spec Flow):** Consider collapsing to 7 exceptions: (1) merge `SagaDefinitionException` and `SagaDefinitionNotFoundException` — a missing definition IS a definition error, (2) reconsider `SagaTimeoutException extends StepTimeoutException` — a saga-level timeout is semantically different from a step-level timeout (a saga timeout fires *between* steps, a step timeout fires *within* a step). If kept, document clearly that catch blocks for `StepTimeoutException` will also catch `SagaTimeoutException`.

##### Task 1.4: Engine Internals — ExecutionContext, RetryPolicy, StepWithPolicy, TCC Adapters

- [ ] `engine/ExecutionContext.java` — Implements `SagaContext`, adds type validation (strict allowlist: primitives, String, BigDecimal, List, Map), event sequencing, state tracking, failure tracking (~70 LoC)
- [ ] `engine/RetryPolicy.java` — Add `sleepWithBackoff()` execution logic (equal jitter backoff, virtual thread execution) to `api/RetryPolicy` created in Task 1.2 (~20 LoC). Add `getPivotPolicy()` to `SagaDefinition`.
- [ ] `engine/StepWithPolicy.java` — Record: `Step` + `RetryPolicy` + `stepTimeoutMs` (~5 LoC)
- [ ] `engine/TccReserveStep.java` — Wraps `TccStep`: `execute()` → `reserve()`, `compensate()` → `cancel()` (~20 LoC)
- [ ] `engine/TccConfirmStep.java` — Wraps `TccStep`: `execute()` → `confirm()`, `compensate()` → no-op (~20 LoC)
- [ ] `timeout/TimeoutPolicy.java` — Per-step and per-saga deadline calculation (~30 LoC)
- [ ] **Unit tests** (~250 LoC):
  - ExecutionContext: type validation (allowed/rejected types), `merge()`, sequence tracking, `put()`/`get()` round-trip
  - RetryPolicy: backoff calculation, jitter bounds within `[half, currentInterval)`, `defaultPolicy()`/`compensationDefault()`/`confirmDefault()` factory values
  - TccReserveStep/TccConfirmStep: delegation to `TccStep.reserve()`/`confirm()`/`cancel()`, compensate behavior

> **Research Insight (Best Practices — TCC):** The three classic TCC pitfalls from [DTM](https://en.dtm.pub/practice/barrier.html) and [Seata](https://seata.apache.org/blog/seata-tcc-fence/):
> 1. **Empty Rollback**: Cancel called when Try never executed. `TccStep.cancel()` must handle this gracefully (check if reservation exists before cancelling).
> 2. **Hanging Transaction**: Try executes after Cancel completed. The participant must detect prior cancellation and refuse to proceed.
> 3. **Idempotent Confirm/Cancel**: Both may be called multiple times on recovery.
>
> Consider providing a `TccBarrier` utility class that participants can use to solve all three problems with two INSERT-IGNORE operations against a local barrier table. Document these patterns prominently in the TCC documentation.

##### Task 1.5: Store Layer — SagaStore Interface, SagaEvent, SagaSchema

- [ ] `store/SagaStore.java` — Core interface (~35 LoC): createSaga, appendEvent, recordTransition, getEvents, getEventCount, getStateSnapshot, registerDefinition, getDefinition, deleteSaga
- [ ] `store/SagaRecoveryStore.java` — Recovery-specific interface (~15 LoC): findRecoverableByBucket, claimForRecovery, markForRecovery
- [ ] `store/SagaAdminStore.java` — Admin query interface (~15 LoC): listStateSnapshots, countByStatus, countBySagaName (defer implementation to Phase 5)
- [ ] `store/SagaEvent.java` — 10 event types with factory methods. Each saga-level event carries `targetStatus`. Step-level events carry `stepIndex` + `stepName`. (~90 LoC)
- [ ] `store/SagaSchema.java` — 3 `TableMetadata` definitions: `saga_events` (PK=saga_id, CK=sequence ASC), `saga_state` (PK=bucket, CK=(status ASC, updated_at ASC, saga_id ASC), secondary index on saga_id), `saga_definitions` (PK=saga_name, CK=definition_version). `bucketOf()` hash function, `createAll()` method. NUM_BUCKETS configurable (default 16). (~100 LoC)
- [ ] **Unit tests** (~100 LoC):
  - SagaEvent: factory methods for all 10 event types, `targetStatus` correctness
  - SagaSchema: `bucketOf()` hash distribution, table metadata structure

> **Research Insight (Architecture + Simplicity + Data Integrity):**
> - **Split SagaStore into focused interfaces**: `SagaStore` (core CRUD), `SagaRecoveryStore` (recovery-specific), `SagaAdminStore` (admin queries). `ScalarDbSagaStore` implements all three, but consumers depend only on what they need. This defers 4 admin methods from Phase 1 scope.
> - **Make NUM_BUCKETS configurable** via `SagaSchema.Builder` or constructor parameter. Document that changing it requires data migration. 16 is the right default; power-of-2 for uniform hash distribution.
> - **Bucket hash quality**: `String.hashCode() % N` has known distribution issues for client-supplied IDs with common patterns (sequential numbers, shared prefixes). Apply MurmurHash3 finalization mix: `int h = sagaId.hashCode(); h ^= h >>> 16; h *= 0x85ebca6b; h ^= h >>> 13; return (h & 0x7FFFFFFF) % numBuckets;`
> - **markForRecovery() signature**: Accept `SagaStateSnapshot` as parameter instead of `String sagaId` to avoid secondary index read during shutdown. The engine has the snapshot in `ExecutionContext` for active sagas.

##### Task 1.6: ScalarDbSagaStore Implementation

- [ ] `store/ScalarDbSagaStore.java` — Full implementation (~450 LoC, **medium complexity**):
  - `createSaga()`: 1 tx — 2 ops (INSERT event + INSERT state). Server-generated UUID or client-supplied ID with strict validation (`[a-zA-Z0-9._-]{1,128}`, must start with alphanumeric). Throws `SagaAlreadyExistsException` on collision.
  - `appendEvent()`: 1 tx — 1 op (INSERT event)
  - `recordTransition()`: 1 tx — 3 ops (INSERT event + DELETE old status row + INSERT new status row). Accepts cached `SagaStateSnapshot` to avoid reads.
  - `findRecoverableByBucket()`: Scan per bucket with status=RUNNING, CONFIRMING, COMPENSATING and updated_at <= threshold. **Add per-bucket claim limit (100)** to prevent one bucket from monopolizing recovery.
  - `claimForRecovery()`: Read current saga_state row, verify version, DELETE + INSERT with new owner_id and incremented version in one transaction
  - `markForRecovery(SagaStateSnapshot)`: Accept snapshot parameter (not saga ID) to avoid secondary index read during shutdown
  - `registerDefinition()` / `getDefinition()`: PUT/GET to saga_definitions table
  - `getStateSnapshot()`: Lookup via secondary index on saga_id. Handle multi-row case: collect all results, pick highest version, log warning.
  - `getEvents()`: Scan saga_events partition by saga_id
  - Admin methods deferred to Phase 5: `listStateSnapshots`, `countByStatus`, `countBySagaName`, `deleteSaga`
- [ ] **Unit tests** (~400 LoC):
  - `createSaga()`: server-generated ID, client-supplied ID, duplicate ID collision (`SagaAlreadyExistsException`), invalid ID format rejection
  - `appendEvent()`: normal append, sequence ordering
  - `recordTransition()`: status transitions, event + state consistency
  - `findRecoverableByBucket()`: stale saga detection, per-bucket scanning, threshold filtering
  - `claimForRecovery()`: version-based optimistic concurrency, concurrent claim rejection
  - `markForRecovery()`: snapshot parameter usage, immediate recovery pickup
  - `registerDefinition()` / `getDefinition()`: PUT idempotency, versioned lookup

> **Research Insight (Data Integrity):**
> - **Event sequence gaps**: If `appendEvent()` throws after ScalarDB has committed (e.g., network timeout on response path), the engine enters failure handling and may try to write a STEP_FAILED event at the same sequence number. The Insert "fail-if-exists" semantics reject this, leaving the saga stuck. **Mitigation**: After `SagaPersistenceException` from `appendEvent()`, verify whether the event was actually persisted before proceeding to failure handling. Alternatively, treat Insert conflicts on matching event type/sequence as success.
> - **saga_state double transition**: If `recordTransition()` commits but throws after commit (OOM, etc.), the catch block may attempt a second `transition()` call. Guard all `transition()` calls with a status check against the last known status.
> - **Secondary index staleness**: On eventually-consistent backends (Cassandra, DynamoDB), `getStateSnapshot()` via secondary index may return stale results. Document this behavior. For consistent reads, provide `getStateSnapshotConsistent(sagaId)` that computes the bucket and scans the partition directly.
> - **Recovery claiming delay**: If `claimForRecovery()` commits but the process crashes before `recoverOne()`, the saga's `updated_at` has been refreshed, delaying re-claim by `recoveryTimeoutMs`. This is a known liveness property — document it.

##### Task 1.7: SagaEngine — Core Execution Logic

- [ ] `engine/SagaEngine.java` (~310 LoC, **medium complexity**):
  - `createSaga()`: Persist saga (rejects if shutting down). Server- or client-supplied ID.
  - `executeSaga()`: Execute from known SagaStateSnapshot (avoids read-back)
  - `execute()`: Convenience create + execute
  - `resumeFrom()`: Resume from specific step index (crash recovery)
  - `executeSteps()`: Build execution plan — Saga steps directly, TCC via `expandTccPlan()`
  - `executeSagaSteps()`: **Unified pivot-based loop**:
    - Steps `0..pivot.index`: compensatable (backward recovery on failure)
    - Steps `pivot.index+1..last`: retriable (forward recovery on failure, STEP_FAILED emitted at most once)
    - PivotPolicy crossing event (e.g., CONFIRMING for TCC) emitted when crossing the pivot boundary
    - Graceful shutdown check between steps
    - Saga-level timeout check before each step
  - `expandTccPlan()`: N TccSteps → 2N StepWithPolicy entries (reserves with user retry policy, confirms with aggressive `confirmDefault()`)
  - `executeWithRetry()`: Virtual thread execution with per-step timeout via `Future.get(timeout)`, participant-driven error classification
  - `compensateFrom()`: Trigger compensation from a given step index downward
  - `replayEvents()`: Reconstruct `ExecutionContext` from event stream (for recovery + completeStep). **Track STEP_COMPENSATED events** to skip already-compensated steps during recovery.
  - `registerActive()` / `unregisterActive()`: Track active sagas for graceful shutdown
- [ ] **Unit tests** (~500 LoC):
  - All execution paths: happy path (3-step saga completes), failure at each step position, pivot boundary behavior
  - TCC expansion: `expandTccPlan()` produces correct 2N-step plan
  - Graceful shutdown: rejects new sagas, drains active
  - Timeout enforcement: saga-level, step-level via `Future.get(timeout)`
  - Client-supplied IDs: accepted when valid, rejected when invalid
  - `replayEvents()`: state reconstruction from event stream, compensated step tracking

> **Research Insight (Best Practices + Security + Spec Flow):**
> - **Saga cancellation flow**: The engine currently has no explicit cancellation mechanism. Add a `cancelSaga(sagaId)` method that sets a cancellation flag checked between steps in `executeSagaSteps()`. When the flag is set, skip remaining steps and trigger compensation.
> - **`StepResult.pending()` in embedded mode**: Define behavior — in embedded mode, `pending()` should either throw `UnsupportedOperationException` (fail-fast) or be treated as `empty()` (permissive). Document this clearly.
> - **Definition resolution failure**: If `SagaDefinitionRegistry.resolve()` returns null during recovery (definition not in memory or store), the saga cannot be recovered. Add explicit handling — either escalate the saga with a clear reason or retry definition resolution.
> - **Event replay completeness**: Track compensated step indices during `replayEvents()` to provide defense-in-depth against re-compensating already-compensated steps, even though compensation idempotency is required.
> - **Validate step/saga names**: Restrict to `[a-zA-Z0-9._-]{1,64}` to prevent injection into log messages, metrics labels, and trace attributes.

##### Task 1.8: CompensationManager

- [ ] `engine/CompensationManager.java` (~70 LoC):
  - Reverse-loop compensation (LIFO order) with `compensationRetryPolicy` (default: 3 attempts, 1s initial, 2.0x backoff)
  - Stop on failure — throws `StepCompensationException`, saga stays in COMPENSATING for recovery
  - Works with both Saga and TCC steps (TccReserveStep.compensate → cancel)
  - Appends STEP_COMPENSATED / STEP_COMPENSATION_FAILED events
- [ ] **Unit tests** (~100 LoC):
  - Reverse ordering (LIFO): compensations execute in correct order
  - Retry behavior: transient failures retried up to `maxAttempts`
  - Stop on failure: loop stops on non-retryable compensation failure, saga stays COMPENSATING

##### Task 1.9: SagaDefinitionRegistry + DefaultSagaManager + Builder

- [ ] `engine/SagaDefinitionRegistry.java` (~40 LoC): ConcurrentHashMap + store fallback. Two-tier lookup: by name (latest), by name:version (recovery). **Persist to store BEFORE putting in memory** to ensure recoverability by other replicas.
- [ ] `engine/DefaultSagaManager.java` (~140 LoC): Implements SagaManager. Delegates to SagaEngine + SagaRecoveryManager. startAsync via virtual thread executor with SagaCallback dispatch. completeStep (daemon mode only).
- [ ] `engine/SagaManagerBuilder.java` (~60 LoC): DI-free builder: `.store()`, `.retryPolicy()`, `.compensationRetryPolicy()`, `.recoveryConfig()`, `.addEventListener()`, `.build()`.
- [ ] **Unit tests** (~100 LoC):
  - SagaDefinitionRegistry: two-tier lookup (name, name:version), store fallback, registration order (persist before memory)
  - SagaManagerBuilder: wiring correctness, missing required fields throw

> **Research Insight (Architecture + Data Integrity):**
> - **Definition registration order**: The current pseudocode does `definitions.put()` before `store.registerDefinition()`. If the store write fails, the definition is in memory but not persisted. Sagas created with this definition cannot be recovered by other replicas. **Reverse the order**: persist first, then put in memory. If the store write throws, the definition is not available — fail-fast.
> - **Replace `startAsync()` overloads with `StartSagaRequest`**: The 4 overloads (`start/startAsync` × `with/without sagaId` × `with/without callback`) create a combinatorial explosion. Use a builder: `sagaManager.start(StartSagaRequest.builder("transfer").sagaId("order-123").callback(cb).async(true).build())`.
> - **Define `SagaEventListener` in Phase 1**: The listener interface is used by `SagaEngine` (Phase 1), not just observability (Phase 4). Define the interface in Phase 1 with default no-op methods. Implement `OpenTelemetrySagaListener` in Phase 4.
> - **Guard `completeStep()` in embedded mode**: `completeStep()` resumes a parked saga — it only makes sense in daemon mode with async steps. In embedded mode, either throw `UnsupportedOperationException` or document that it is a no-op.

##### Task 1.10: SagaDefinitionParser

- [ ] `parser/SagaDefinitionParser.java` (~80 LoC): Jackson-based JSON/YAML → SagaDefinition. Detects format by file extension. Uses `jackson-dataformat-yaml`. Validates definition at parse time.
- [ ] **Unit tests** (~100 LoC):
  - JSON parsing: valid definition, minimal definition
  - YAML parsing: valid definition, comments preserved (ignored)
  - Validation errors: missing name, empty steps, invalid stepClass, unknown fields (`FAIL_ON_UNKNOWN_PROPERTIES`)

##### Task 1.11: SagaRecoveryManager

- [ ] `recovery/RecoveryConfig.java` (~5 LoC): Record: `recoveryTimeoutMs`, `recoveryIntervalSeconds`, `compensationGracePeriod`, `clock`
- [ ] `recovery/SagaRecoveryManager.java` (~220 LoC, **medium complexity**):
  - Periodic scheduled scan of all `saga_state` buckets (default: every 30s)
  - For each recoverable saga: claim via `claimForRecovery()`, replay events via `SagaEngine.replayEvents()`, resume via `SagaEngine.resumeFrom()` or `SagaEngine.compensateFrom()`
  - Handles all modes uniformly: RUNNING → resume forward, CONFIRMING → resume forward (TCC confirm phase), COMPENSATING → resume compensation
  - Time-based escalation: if stuck in COMPENSATING longer than `compensationGracePeriod` (default: 4 hours), escalate to ESCALATED
  - Definition lookup via `SagaDefinitionRegistry.resolve()` (versioned, with store fallback)
  - `start()` / `stop()` lifecycle methods
- [ ] **Unit tests** (~200 LoC):
  - Scan + claim + resume: recoverable saga detected and resumed
  - Escalation timing: saga in COMPENSATING beyond `compensationGracePeriod` escalates to ESCALATED
  - Definition lookup: versioned resolution with store fallback
  - Lifecycle: `start()`/`stop()` idempotency

> **Research Insight (Security + Performance):**
> - **Constraint**: Enforce `recoveryTimeoutMs > max(stepTimeoutMs)` across all step definitions. Otherwise, a saga may be claimed for recovery while a step is still executing within its timeout, leading to duplicate step execution.
> - **Shutdown signal**: Replace shutdown polling loop with `CountDownLatch` or `CompletableFuture` for cleaner thread wakeup. `ScheduledExecutorService.awaitTermination()` with the latch avoids busy-waiting.
> - **Clock skew**: The recovery scan uses `Instant.now().minusMillis(recoveryTimeoutMs)`. If the scanning replica's clock is ahead of the original replica's clock, sagas may be prematurely claimed. In cloud environments (NTP), skew is typically <1s vs. 60s default timeout — negligible. Document that `recoveryTimeoutMs` should include a safety margin for edge environments.

##### Task 1.12: Testing Harness (Phase 1 scope)

- [ ] `testing/MockStep.java` (~80 LoC): Configurable mock with execution/compensation history tracking, failure injection
- [ ] `testing/SagaTestHarness.java` (~150 LoC): Builder + execute + assertions (executionOrder, compensationOrder, finalContext). Uses `ScalarDbSagaStore` backed by in-memory SQLite.
- [ ] `testing/CrashingStoreDecorator.java` (~60 LoC): Decorator that throws `SimulatedCrashException` at configured step boundaries

##### Task 1.13: Integration Tests (Phase 1)

- [ ] Integration tests via SagaTestHarness (~400 LoC):
  - Full saga lifecycle (Saga mode: BACKWARD, FORWARD, MIXED)
  - Full TCC lifecycle (reserve → confirm all, reserve fail → cancel)
  - Crash recovery simulation (CrashingStoreDecorator: crash mid-execution, crash mid-compensation)
  - Timeout enforcement (saga-level, step-level)
  - Client-supplied ID idempotency
  - Concurrent saga execution

> **Research Insight (Best Practices — Testing):**
> - **Inject `Clock`** into all time-dependent components (already done via `RecoveryConfig`). Also inject into `SagaEngine` for saga/step deadline calculation. Enables deterministic timeout testing without real delays.
> - **Inject `Random`** into `RetryPolicy` for deterministic jitter in tests. Production uses `ThreadLocalRandom`; tests use `new Random(seed)`.
> - **Crash-between-steps pattern**: `CrashingStoreDecorator` should support crash AFTER event persist but BEFORE saga_state update — this tests the most critical recovery path (event is source of truth, saga_state is stale).
> - **Compensation order assertion**: `SagaTestHarness.assertCompensationOrder(sagaId, "step2", "step1")` — verify LIFO ordering.
> - **TCC empty-cancel test**: Start TCC saga, inject timeout during first reserve, verify cancel is NOT called (since reserve never completed).
> - **Virtual thread pinning detection**: In CI, enable `-Djdk.tracePinnedThreads=short` and fail tests if pinning is detected in engine code.

---

#### Phase 2: Communication & Framework Integration

**Scope:** ServiceInvoker layer, declarative step communication, Spring Boot integration, Quarkus integration, participant SDK.

**Estimated: ~3,370 LoC, ~7-10 working days**

##### Task 2.1: ServiceInvoker + Declarative Communication (Phase 2a)

- [ ] `invoker/ServiceInvoker.java` — Interface (~15 LoC)
- [ ] `invoker/ServiceInvokerRegistry.java` — Concurrent map lookup + dispatch (~40 LoC)
- [ ] `invoker/GrpcInvoker.java` — Typed lambda wrapper for gRPC stubs (~80 LoC)
- [ ] `invoker/HttpInvoker.java` — HTTP client wrapper with status code classification (~80 LoC)
- [ ] `invoker/DeclarativeStepAdapter.java` — JSON expression resolution (`${...}`) + output extraction (`$.path`) (~120 LoC)
- [ ] `invoker/TransportAdapter.java` — Interface + TransportException (~30 LoC)
- [ ] `invoker/GrpcTransportAdapter.java` — Protobuf message building from maps + gRPC metadata propagation (~100 LoC)
- [ ] `invoker/HttpTransportAdapter.java` — JSON body building + X-Saga-Id/X-Saga-Step propagation + status code mapping (~80 LoC)
- [ ] Update `SagaEngine` — Support `service`/`method` in addition to `stepClass` (~30 LoC)
- [ ] Update `SagaDefinitionParser` — Parse `call`, `compensate` blocks (~50 LoC)
- [ ] Tests (invoker unit + declarative integration + transport edge cases) (~800 LoC)

> **Research Insight (Security):**
> - **SSRF via callback URLs**: `HttpInvoker` must validate callback/participant URLs against an allowlist of permitted hosts/CIDRs. Without this, a malicious saga definition could use the coordinator to probe internal services (SSRF). Enforce allowlist at `ServiceInvokerRegistry` level: `config.allowedHosts("payment-svc.internal", "inventory-svc.internal")`.
> - **HTTP body size limit**: Both `HttpInvoker` (outbound) and callback receiver (inbound) should enforce body size limits. Default: 1MB. Configure via `maxRequestBodyBytes`.
> - **Jackson ObjectMapper safety**: All `ObjectMapper` instances used for request/response deserialization must have `mapper.deactivateDefaultTyping()` to prevent deserialization gadget attacks. Never enable `@class` or `DefaultTyping.EVERYTHING`.

**Phase 2a Total: ~1,425 LoC**

##### Task 2.2: Spring Boot Integration (Phase 2b)

- [ ] `SagaAutoConfiguration.java` — `@Bean SagaManager` from `SagaProperties` via builder (~80 LoC)
- [ ] `SagaProperties.java` — Spring Boot config properties (~40 LoC)
- [ ] `SagaAnnotationScanner.java` — `SmartInitializingSingleton` scanning `@SagaStep`/`@SagaCompensation` (~120 LoC)
- [ ] `SagaCallbackController.java` — REST endpoint for async step completion (~30 LoC)
- [ ] `SagaAdminController.java` — REST exposure of `SagaAdminService` (~60 LoC)
- [ ] `spring.factories` / `AutoConfiguration.imports` (~5 LoC)
- [ ] Tests (auto-config, annotation scanning, property binding, controller) (~400 LoC)

**Phase 2b Total: ~735 LoC**

##### Task 2.3: Quarkus Integration (Phase 2c)

- [ ] `SagaBuildStep.java` — Jandex annotation scan at build time (~120 LoC)
- [ ] `SagaRecorder.java` — Runtime recorder for saga registration (~60 LoC)
- [ ] `SagaCdiProducer.java` — CDI `@Produces SagaManager` (~40 LoC)
- [ ] `SagaCallbackResource.java` — JAX-RS resource for async step completion (~30 LoC)
- [ ] `quarkus-extension.yaml` (~10 LoC)
- [ ] Tests (build-time scan, CDI integration, recorder) (~350 LoC)

**Phase 2c Total: ~610 LoC**

##### Task 2.4: Participant Protocol & SDK (Phase 2d)

- [ ] Participant HTTP protocol spec (documented contract: request/response format, `X-Saga-Retryable`, `X-Saga-Id`/`X-Saga-Step`) (~50 LoC)
- [ ] `SagaParticipantServer.java` — Lightweight HTTP server (Javalin) hosting Step implementations (~120 LoC)
- [ ] `StepEndpoint.java` — HTTP endpoint: deserialize SagaContext, call Step, serialize StepResult (~100 LoC)
- [ ] `ParticipantConfig.java` — Config POJO (~30 LoC)
- [ ] Tests (participant SDK, protocol compliance, error mapping) (~300 LoC)

**Phase 2d Total: ~600 LoC**

---

#### Phase 3: Daemon Mode

**Scope:** Standalone coordinator process with REST API + Java client SDK.

**Estimated: ~1,400 LoC, ~3.5-5 working days**

##### Task 3.1: Coordinator Server

- [ ] `coordinator/CoordinatorServer.java` — Standalone process: CLI args, config loading, Javalin HTTP server, graceful shutdown hook (~150 LoC)
- [ ] `coordinator/CoordinatorConfig.java` — Config POJO (port, ScalarDB config path, saga definitions path) (~40 LoC)

##### Task 3.2: Coordinator REST API

- [ ] `coordinator/api/SagaResource.java` — REST endpoints (~200 LoC):
  - `POST /sagas` — Start saga with server-generated ID (sync by default, `?async=true` for async)
  - `PUT /sagas/{id}` — Start saga with client-supplied ID (idempotent, `409 Conflict` with existing snapshot)
  - `GET /sagas` — List sagas (status filter, pagination)
  - `GET /sagas/{id}` — Get saga status and step details
  - `PUT /sagas/{id}/cancel` — Request saga cancellation
  - `POST /sagas/{id}/steps/{stepName}/complete` — Complete async step (resumes parked saga)
- [ ] `coordinator/api/HealthResource.java` — Health/readiness check (~30 LoC)
- [ ] `coordinator/api/ErrorMapper.java` — Exception-to-HTTP-response mapping (~40 LoC)

> **Research Insight (Security + Framework Docs):**
> - **Rate limiting**: Add per-IP rate limiting on `POST /sagas` and `PUT /sagas/{id}` to prevent abuse. Javalin 6 supports before-handlers: `before("/sagas/*", rateLimiter::check)`.
> - **HMAC binding**: The HMAC callback token should bind to the saga name (not just `sagaId + stepName + iat`), preventing token reuse across different saga definitions.
> - **Javalin 6 patterns**: Use `config.router.apiBuilder()` (not `app.routes()`). Register exception handlers via `app.exception(SagaNotFoundException.class, ...)`. Use lifecycle events for shutdown coordination: `config.events(e -> e.serverStopping(() -> sagaManager.close()))`. Port 0 for tests.

##### Task 3.3: Java Client SDK — RemoteSagaManager

- [ ] `client/RemoteSagaManager.java` — Implements `SagaManager`, delegates to coordinator REST API via HTTP client (~150 LoC):
  - `start()` → `POST /sagas` (sync) or `PUT /sagas/{id}` (client-supplied)
  - `startAsync()` → `POST /sagas?async=true` (returns immediately)
  - `SagaCallback` variant polls `GET /sagas/{id}` on background thread
  - `409 Conflict` → `SagaAlreadyExistsException` translation

##### Task 3.4: Daemon Mode Tests

- [ ] Unit tests (REST API, config, error mapper, RemoteSagaManager) (~400 LoC)
- [ ] Integration tests (full lifecycle via REST, async polling, recovery after restart) (~350 LoC)

---

#### Phase 4: Developer Experience & Observability

**Scope:** OpenTelemetry observability + local development server.

**Estimated: ~1,465 LoC, ~1.5-2.5 working days**

##### Task 4.1: Observability — OpenTelemetry (Phase 4a)

- [ ] `observability/SagaEventListener.java` — **Define in Phase 1** (moved forward). Interface with default no-op methods: onSagaStarted, onStepStarted, onStepCompleted, onStepFailed, onCompensationStarted, onStepCompensated, onStepCompensationFailed, onSagaCompleted, onSagaCompensated, onSagaEscalated (~25 LoC)
- [ ] `observability/OpenTelemetrySagaListener.java` — Tracer spans per saga/step + 12 metrics instruments (~150 LoC):
  - Counters: `saga.started`, `saga.completed`, `saga.compensated`, `saga.escalated`, `saga.step.completed`, `saga.step.failed`, `saga.step.compensated`, `saga.step.compensation_failed`, `saga.recovery.claimed`
  - Histograms: `saga.duration`, `saga.step.duration`
  - UpDownCounter: `saga.active`
- [ ] Update `SagaEngine` — Call listener methods at each lifecycle point (~40 LoC)
- [ ] Update `SagaManagerBuilder` — `addEventListener()` support (~10 LoC)
- [ ] Tests (listener invocation, metrics assertions, span lifecycle) (~300 LoC)

**Phase 4a Total: ~525 LoC**

*Phase 4b (Testing Harness) moved to Phase 1.*

> **Research Insight (Framework Docs + Architecture):**
> - **Pass `Tracer`/`Meter` via constructor** (not `GlobalOpenTelemetry`) for testability. Test with `InMemoryMetricReader` and `InMemorySpanExporter`.
> - **Span parent-child**: Root span per saga (`saga:transferMoney`), child span per step (`step:debit`). Use `Context.current().with(parent)` to set parent. Always call `span.end()` even on error paths.
> - **ConcurrentHashMap for span tracking**: Spans start on one virtual thread and may end on another. Use `sagaSpans.put(sagaId, span)` / `sagaSpans.remove(sagaId)` for lifecycle management.
> - **Set status before ending**: `span.setStatus(StatusCode.ERROR, reason)` before `span.end()`. Record exceptions: `span.recordException(e)`.
> - **Metrics naming**: Follow OpenTelemetry semantic conventions. Use `{sagas}` unit for counters, `ms` for histograms.

##### Task 4.2: Local Development Server (Phase 4c)

- [ ] `devserver/SagaDevServer.java` — Javalin embedded HTTP + SQLite-backed ScalarDB + auto-schema creation (~150 LoC)
- [ ] `devserver/SagaDevServerConfig.java` — Config POJO (port, definition path, auto-open browser) (~30 LoC)
- [ ] REST routes — Admin API routes delegating to `SagaAdminService` + static Web UI (~80 LoC)
- [ ] `main()` entry point — CLI arg parsing (`--port`, `--definitions`) (~30 LoC)
- [ ] Embedded Web UI — Minimal HTML/JS dashboard (saga list, detail view, action buttons) (~300 LoC)
- [ ] Tests (server startup, API routes, config loading, CLI args) (~350 LoC)

**Phase 4c Total: ~940 LoC**

---

#### Phase 5: Admin API

**Scope:** Production operations API for saga management.

**Estimated: ~1,200 LoC, ~2.5-4 working days**

##### Task 5.1: Admin API Implementation

- [ ] `admin/SagaAdminService.java` — Interface: list, getDetail, compensate, retry, forceComplete, getMetrics (~30 LoC)
- [ ] `admin/DefaultSagaAdminService.java` — Implementation delegating to SagaStore + SagaEngine (~200 LoC)
- [ ] `admin/SagaQuery.java` — Query builder: status filter, saga name filter, date range, pagination (~60 LoC)
- [ ] `admin/SagaPage.java` — Paginated results (~20 LoC)
- [ ] `admin/SagaDetail.java` — Detailed saga view with event timeline (~40 LoC)
- [ ] `admin/SagaMetrics.java` — Aggregate metrics: counts by status, counts by saga name (~30 LoC)
- [ ] Tests: unit tests for admin service + integration tests via SagaTestHarness (~500 LoC)
- [ ] Web UI enhancements for Phase 4c dev server (~320 LoC)

---

#### Phase 6: LRA Compliance

**Scope:** MicroProfile LRA coordinator module (separate from SagaEngine). Requires Phase 3.

**Estimated: ~3,650 LoC, ~10.5-14 working days**

##### Task 6.1: LRA Coordinator

- [ ] LRA REST API (start/close/cancel/join/recovery per MicroProfile spec) (~500 LoC)
- [ ] LRA Coordinator execution logic (lifecycle management, close/cancel orchestration, participant ordering) (~250 LoC)
- [ ] Participant registry (track callback URLs per LRA) (~150 LoC)
- [ ] HTTP callback client (call @Compensate/@Complete endpoints) (~200 LoC)
- [ ] Async status polling (@Status + retry loop) (~150 LoC)
- [ ] @Forget / @AfterLRA lifecycle (~100 LoC)
- [ ] LRA state persistence (adapt SagaStore for LRA events + recovery) (~200 LoC)

##### Task 6.2: Quarkus LRA Extension

- [ ] Quarkus extension (build processor + dev services) (~400 LoC)

##### Task 6.3: LRA Tests

- [ ] Unit tests (all classes: REST API, coordinator, registry, callback client, poller, persistence) (~1,200 LoC)
- [ ] TCK compliance testing (~500 LoC)

---

#### Phase 7: Additional Transports (gRPC)

**Scope:** gRPC as an alternative transport.

**Estimated: ~1,150 LoC, ~4.5-6 working days**

##### Task 7.1: gRPC Transport

- [ ] `.proto` definitions (saga definition schema + coordinator API + participant protocol) (~150 LoC)
- [ ] gRPC coordinator server (serves saga lifecycle API over gRPC) (~200 LoC)
- [ ] gRPC participant SDK (hosts Steps over gRPC) (~200 LoC)
- [ ] `RemoteSagaManager` gRPC variant (~100 LoC)
- [ ] Tests (server, client, participant SDK, error handling) (~500 LoC)

> **Research Insight (Simplicity):** TCP/Netty transport (previously Phase 7b, ~1,500 LoC) removed. gRPC already provides efficient binary transport with framing, flow control, and connection management. A custom TCP protocol adds maintenance burden without clear user demand. If needed later, it can be added as Phase 8.

---

## System-Wide Impact

### Interaction Graph

- `SagaManager.start()` → `SagaEngine.createSaga()` → `SagaStore.createSaga()` (persist) → `SagaEngine.executeSaga()` → `executeSagaSteps()` (loop) → `Step.execute()` per step → `SagaStore.appendEvent()` per step → `SagaStore.recordTransition()` on terminal
- On failure: `CompensationManager.compensate()` → `Step.compensate()` in reverse → `SagaStore.appendEvent()` per compensation
- `SagaRecoveryManager` (periodic) → `SagaStore.findRecoverableByBucket()` → `SagaStore.claimForRecovery()` → `SagaEngine.replayEvents()` → `SagaEngine.resumeFrom()`
- `SagaEventListener` callbacks fire at each lifecycle point (saga start, step start/end, compensation, completion)

### Error & Failure Propagation

- `StepExecutionException(retryable=true)` → engine retries with backoff → exhausted → compensate (before pivot) or stay RUNNING (after pivot)
- `StepExecutionException(retryable=false)` → immediate compensation (before pivot) or STEP_FAILED event (after pivot)
- `StepCompensationException` → compensation loop stops → saga stays COMPENSATING → recovery retries → time-based escalation to ESCALATED
- `SagaPersistenceException` → saga state may be inconsistent → recovery scans detect and resume
- `SagaAlreadyExistsException` → propagated to caller with existing snapshot

### State Lifecycle Risks

- **Crash between step execution and event persistence**: Step may have completed externally but event not persisted. On recovery, step is re-executed → steps MUST be idempotent.
- **Crash between event append and saga_state transition**: Events are source of truth; saga_state is rebuilt from events during recovery.
- **Concurrent recovery claims**: Handled by version-based optimistic concurrency + ScalarDB transaction conflict detection.
- **Terminal state cleanup**: COMPLETED/COMPENSATED/ESCALATED rows in saga_state can be cleaned up after configurable retention period.

> **Research Insight (Data Integrity):**
> - **Terminal state cleanup breaks idempotency**: Deleting a completed saga via `deleteSaga()` destroys the data needed for idempotent-create. A client retry after cleanup creates a new saga, causing duplicate business effects (double charges). **Mitigation**: Use tombstone records — retain a minimal saga_state row (`saga_id`, `status=COMPLETED`, `deleted_at`) after event deletion. Tombstones preserve the idempotency check and can be cleaned on a longer schedule (90 days).
> - **Effective recovery SLA**: `findRecoverableByBucket()` scans 3 status ranges sequentially in one transaction. A concurrent status transition may cause a saga to be missed in one scan cycle (caught in the next). The effective SLA is `recoveryIntervalSeconds + recoveryTimeoutMs`, not just `recoveryTimeoutMs`. Document this.
> - **Caffeine cache inconsistency**: In multi-replica deployments, `getStateSnapshot()` may return a locally cached result that is stale because another replica has since transitioned the saga. This is acceptable for informational queries — document as known behavior.

## Acceptance Criteria

### Functional Requirements

- [ ] Saga mode: BACKWARD, FORWARD, and MIXED recovery strategies execute correctly
- [ ] TCC mode: reserve → confirm all (success), reserve fail → cancel all completed (failure)
- [ ] Unified pivot-based execution loop handles all modes (Saga + TCC)
- [ ] Append-only event store: 1 INSERT per step, no read-modify-write
- [ ] Bucket-partitioned recovery scan finds and claims stale sagas
- [ ] Crash recovery resumes sagas correctly from any point (mid-execution, mid-compensation, mid-confirm)
- [ ] Per-step and per-saga timeouts enforced
- [ ] Client-supplied saga IDs with idempotent-create semantics
- [ ] Compensation retry (immediate + recovery) with time-based escalation
- [ ] Graceful shutdown: drain in-flight sagas, mark for recovery
- [ ] Virtual thread execution for all step invocations and retries
- [ ] SagaDefinitionParser handles JSON and YAML
- [ ] SagaTestHarness provides integration testing with in-memory SQLite

### Non-Functional Requirements

- [ ] Thread-safe: all public APIs safe for concurrent use
- [ ] No message broker required — direct step invocation
- [ ] Any-database via ScalarDB
- [ ] SNAPSHOT isolation sufficient for all saga transactions
- [ ] Java 21 for all modules (except client SDKs: Java 8)

### Quality Gates

- [ ] Google Java Style Guide compliance (google-java-format)
- [ ] Unit test coverage for all classes
- [ ] Integration tests via SagaTestHarness for all execution modes
- [ ] Test method naming: `methodName_condition_expectedResult()`
- [ ] Tests grouped by `// Arrange`, `// Act`, `// Assert`

## Success Metrics

- All saga modes (BACKWARD, FORWARD, MIXED, TCC) pass integration tests
- Crash recovery resumes correctly from any failure point
- ScalarDbSagaStore operates with 1 INSERT per step (append-only)
- Full Phase 1 completes in ~1 week for a skilled engineer with AI
- Cumulative project completion in ~41-57.5 working days

## Dependencies & Prerequisites

- ScalarDB 3.x with JDBC backend (SQLite for testing, any supported DB for production)
- Java 21 (virtual threads, pattern matching, records)
- Jackson (core + dataformat-yaml) for definition parsing
- Javalin (Phase 3 coordinator, Phase 4c dev server)
- OpenTelemetry API/SDK (Phase 4a)
- Spring Boot 3.x (Phase 2b)
- Quarkus 3.x (Phase 2c)
- gRPC + Protobuf (Phase 7a)
- Netty (Phase 7b)

## Risk Analysis & Mitigation

| Risk | Impact | Mitigation |
|---|---|---|
| ScalarDB transaction semantics edge cases (CommitConflictException, AbortException) | Medium | Comprehensive testing in ScalarDbSagaStore; SNAPSHOT isolation analysis documented |
| Virtual thread pinning with JDBC drivers using `synchronized` | Low | Use ReentrantLock-based drivers (MySQL 9.x+, PostgreSQL 42.7+); monitor with `jdk.VirtualThreadPinned` JFR events |
| Complex recovery state machine (RUNNING/CONFIRMING/COMPENSATING × Saga/TCC) | Medium | Unified pivot-based model simplifies to single loop; thorough integration tests |
| LRA TCK compliance (Phase 6) may surface unexpected spec requirements | Medium | Start with spec analysis before coding; incremental compliance testing |
| ScalarDB secondary index eventual consistency (Cassandra/DynamoDB) | Medium | Pass `SagaStateSnapshot` to `markForRecovery()`; handle multi-row results in `getStateSnapshot()` |
| SSRF via participant callback URLs in declarative steps | High | Allowlist permitted hosts/CIDRs in `ServiceInvokerRegistry` |
| Terminal state cleanup breaking idempotent-create contract | High | Tombstone records; separate saga_events cleanup from saga_state retention |

## Cumulative Timeline

| Milestone | Cumulative Time |
|---|---|
| Phase 1 complete (core engine) | ~5.5-7.5 days |
| Phase 2 complete (+ communication, frameworks) | ~12.5-17.5 days |
| Phase 3 complete (+ daemon mode) | ~16-22.5 days |
| Phase 4 complete (+ DX & observability) | ~17.5-25 days |
| Phase 5 complete (+ admin API) | ~20-29 days |
| Phase 6 complete (+ LRA compliance) | ~30.5-43 days |
| Phase 7 complete (+ gRPC transport) | ~35-49 days |

*Note: Phase 7b (TCP/Netty, ~5-7 days) removed per simplicity review. Total savings: ~1,500 LoC, ~5-7 days.*

## Sources & References

### Internal References

- Design document: `docs/scalardb-saga-design.md`
- Previous plan: `docs/plans/2026-04-01-001-feat-scalardb-saga-implementation-plan.md`
- Project conventions: `CLAUDE.md`

### Key Changes from Previous Plan

1. **`SagaContext` → interface** (was concrete class). `ExecutionContext` is engine-internal implementation.
2. **`SagaInstance` → `SagaStateSnapshot`**: Renamed for clarity, now immutable with `withTransition()`.
3. **`saga_index` → `saga_state`**: Renamed. Now uses bucket-partitioned clustering key `(status, updated_at, saga_id)`.
4. **Unified pivot-based execution**: Both Saga and TCC share `executeSagaSteps()` loop. `PivotPolicy` record encapsulates behavior.
5. **`expandTccPlan()`**: TCC definitions expanded to 2N-step plan with `TccReserveStep`/`TccConfirmStep` adapters.
6. **`SagaDefinitionRegistry`**: Two-tier lookup (in-memory → store fallback) for crash recovery.
7. **Client-supplied saga IDs**: New `start(sagaId, ...)` overloads with strict ID validation.
8. **MIXED recovery strategy**: New recovery strategy with explicit pivot step.
9. **Testing harness moved to Phase 1**: `SagaTestHarness` with `ScalarDbSagaStore` + SQLite (no InMemorySagaStore).
10. **SagaEvent carries `targetStatus`**: Each saga-level event explicitly carries its target status.
11. **3 tables**: `saga_definitions` table added for definition persistence and versioned recovery lookup.
12. **Participant Protocol & SDK (Phase 2d)**: New sub-phase for `SagaParticipantServer`.
