# ScalarDB Saga Design

## Executive Summary

This document describes a saga orchestration engine built on top of ScalarDB, informed by analysis of Apache Seata's Saga component and production-grade saga frameworks (Temporal, Axon, Eventuate, MassTransit).

**What it is:** A saga orchestration engine that coordinates distributed transactions across microservices using the Saga pattern (sequential steps with compensations) and TCC pattern (Try-Confirm-Cancel with resource reservations).

### Target Users

ScalarDB Saga targets **teams building microservices that need eventual consistency across services** — e.g., order placement spanning payment, inventory, and shipping services where temporary inconsistency is acceptable as long as the system eventually converges.

For operations that require **strong consistency** (ACID guarantees across databases), use ScalarDB 2PC transactions. ScalarDB Saga complements ScalarDB 2PC: use 2PC where correctness requires immediate consistency, and Saga where eventual consistency with compensation-based rollback is sufficient.

Typical user profiles:

- **Enterprise teams decomposing monoliths** — need to coordinate writes across 3-10 services that were previously in a single database transaction
- **Greenfield microservice teams** — building distributed systems from scratch and need a compensation-based consistency mechanism from day one
- **Platform teams standardizing distributed transactions** — want a single saga framework for the organization rather than each team building ad-hoc retry/rollback logic

**Not the target**: Teams that need pure choreography (event-driven, no central coordinator), teams that already use Temporal/Cadence for general workflow orchestration, or teams that only need strong consistency across databases (where ScalarDB 2PC suffices).

**Key design decisions:**

1. **Orchestration over choreography** — an explicit coordinator drives the workflow, rather than services reacting to events. This gives a single point of visibility for the entire saga flow, deterministic compensation ordering, and straightforward debugging. Every major saga framework (Temporal, Seata, Eventuate, MassTransit, Axon) has converged on orchestration; choreography becomes unmanageable for sagas beyond 2-3 services.

2. **Embedded and daemon coordinator** — the engine runs as a JAR dependency (embedded mode) or as a standalone coordinator with REST API (daemon mode). Start embedded for simplicity; add daemon mode when polyglot clients need it. No other open-source saga framework supports both deployment models.

3. **Any-database via ScalarDB** — saga state is stored as append-only events in ScalarDB, working across any database backend (Cassandra, DynamoDB, PostgreSQL, MySQL, Cosmos DB, etc.). No storage vendor lock-in — unlike competitors that require specific stores (etcd, Cassandra, file system, or a message broker).

4. **No message broker required** — the engine invokes steps via direct method calls (`step.execute()`). What the step does internally (in-process call, HTTP, gRPC) is up to the step implementation. Unlike Eventuate (requires Kafka + Debezium) or MassTransit (requires RabbitMQ/SQS), there is no broker infrastructure to deploy or manage.

5. **Declarative saga definitions with framework integrations** — workflows are defined via Java builder API, JSON/YAML, or Protocol Buffers (daemon mode via gRPC). JSON/YAML separates workflow structure from step implementation, making sagas versionable, inspectable, and modifiable without recompiling. The Java builder provides compile-time type safety and IDE auto-completion. Optional Spring Boot and Quarkus modules add annotation-based step registration (`@SagaStep`/`@SagaCompensation`) for teams that prefer convention-over-configuration.

**How we compare:**

| | This Design | Seata | Temporal | Eventuate | MassTransit | Oracle MicroTx | Narayana LRA | Axon |
|---|---|---|---|---|---|---|---|---|
| **Deployment** | Embedded + daemon | Embedded only | Server cluster | Embedded + broker | Embedded + broker | Daemon | Daemon | Embedded + Axon Server |
| **Mandatory infra** | None (embedded) | None | Temporal Server + DB + ES | Kafka + CDC | RabbitMQ/SQS | Coordinator + etcd | Coordinator | Axon Server (prod) |
| **State store** | Any DB (ScalarDB) | JDBC only | Cassandra/PostgreSQL/MySQL | JDBC + Kafka | SQL/MongoDB/Redis | etcd/Oracle DB | File/JDBC | JDBC/Axon Server |
| **Language** | Java (polyglot via daemon) | Java | Go, Java, TS, Python, .NET | Java | .NET | Java, Node.js | Java | Java |
| **Saga definition** | Java builder, JSON/YAML, Protocol Buffers (daemon) + annotations (Spring, Quarkus) | JSON + visual designer | Code-as-workflow | Code (SagaDsl) | Code (state machine) | Annotations (@LRA) | Annotations (@LRA) | Annotations (@Saga) |
| **Framework coupling** | None (optional Spring Boot, Quarkus modules) | Spring required | None | Spring Boot required | .NET DI required | Jakarta EE | Quarkus/WildFly | Spring required |
| **Coordinator HA** | Any replica recovers | Any replica recovers | Server cluster (Raft) | Broker-based | Broker-based | Enterprise only (paid) | Single-node only | Axon Server Enterprise (paid) |
| **Outbox built-in** | No (not needed) | No | N/A (durable execution) | Yes (CDC/polling) | No | No | No | Yes (event store) |
| **License** | Open source | Open source (Apache 2.0) | Open source (MIT) | Open source (Apache 2.0) | Open source (Apache 2.0) | Proprietary | Open source (Apache 2.0) | Open source (Apache 2.0) |

> See [scalardb-saga-competitive-analysis.md](scalardb-saga-competitive-analysis.md) for detailed competitive analysis including architecture diagrams, head-to-head comparisons, and competitive advantages/disadvantages.

---

## Table of Contents

- [Part I: Architecture Overview](#part-i-architecture-overview)
  - [Background](#background)
  - [Package Structure & Module Structure](#package-structure)
  - [Component Summary](#component-summary)
  - [Deployment Architecture](#deployment-architecture-cross-service-orchestration)
- [Part II: Core Engine](#part-ii-core-engine)
  - [State Machine Parser + Execution Engine](#state-machine-parser--execution-engine)
  - [Compensation Manager](#compensation-manager)
  - [Retry with Backoff and Error Classification](#retry-with-backoff-and-error-classification)
  - [Persistence via ScalarDB](#persistence-via-scalardb)
  - [Crash Recovery](#crash-recovery)
  - [Timeout Management](#timeout-management)
  - [Graceful Shutdown](#graceful-shutdown)
  - [Data Retention & Periodic Cleanup](#data-retention--periodic-cleanup)
  - [Async Step Completion (Daemon Mode Only)](#async-step-completion-daemon-mode-only)
  - [TCC (Try-Confirm-Cancel) Mode](#tcc-try-confirm-cancel-mode)
- [Part III: Communication & Integration](#part-iii-communication--integration)
  - [Declarative Service Steps and Framework Integration](#declarative-service-steps-and-framework-integration)
  - [Step Implementation Patterns](#step-implementation-patterns)
  - [Message Broker Extensibility](#message-broker-extensibility)
- [Part IV: Developer Experience](#part-iv-developer-experience)
  - [Bootstrapping (DI-Free)](#bootstrapping-di-free)
  - [Bootstrap and Execute](#bootstrap-and-execute)
  - [Testing Harness (SagaTestHarness)](#testing-harness-sagatestharness)
  - [Local Development Server (SagaDevServer)](#local-development-server-sagadevserver)
- [Part V: Production Operations](#part-v-production-operations)
  - [Admin API](#admin-api)
  - [Observability (OpenTelemetry)](#observability-opentelemetry)
  - [Security](#security)
- [Part VI: LRA Compatibility](#part-vi-lra-compatibility)
  - [MicroProfile LRA Compatibility](#microprofile-lra-compatibility)
- [Part VII: Implementation Plan](#part-vii-implementation-plan)
  - [Phase 1: Core Engine](#phase-1-core-engine)
  - [Phase 2: Communication & Framework Integration](#phase-2-communication--framework-integration)
  - [Phase 3: Daemon Mode](#phase-3-daemon-mode)
  - [Phase 4: Developer Experience & Observability](#phase-4-developer-experience--observability)
  - [Phase 5: Admin API](#phase-5-admin-api)
  - [Phase 6: LRA Compliance](#phase-6-lra-compliance)
  - [Phase 7: Additional Transports (gRPC & TCP/Netty)](#phase-7-additional-transports-grpc--tcpnetty)
  - [Enhancement Roadmap](#enhancement-roadmap-v2)

---

# Part I: Architecture Overview

## Background

This document describes a concrete design for a saga orchestration engine built on top of ScalarDB. The design is informed by a thorough analysis of Apache Seata's Saga component (7 modules, ~10K+ lines of Java) and production-grade saga frameworks (Temporal, Axon, Eventuate, MassTransit).

## Package Structure

```
com.scalar.db.saga/
├── api/                         # Public interfaces
│   ├── SagaManager.java
│   ├── SagaDefinition.java
│   ├── SagaDefinitionParser.java   # Jackson JSON/YAML → SagaDefinition
│   ├── SagaStateSnapshot.java
│   ├── SagaContext.java
│   ├── SagaCallback.java           # Async completion/compensation/escalation callback
│   ├── Step.java
│   ├── TccStep.java              # Standalone TCC interface (reserve/confirm/cancel)
│   ├── StepResult.java
│   ├── SagaStatus.java
│   ├── RetryPolicy.java
│   ├── Named.java                # Qualifier annotation for named resource injection
├── engine/                      # Core execution
│   ├── SagaEngine.java
│   ├── ExecutionContext.java      # Internal: implements SagaContext, adds engine tracking
│   ├── StepWithPolicy.java         # Internal: record bundling Step + RetryPolicy
│   ├── TccReserveStep.java        # Internal: wraps TccStep.reserve/cancel → Step
│   ├── TccConfirmStep.java        # Internal: wraps TccStep.confirm → Step
│   ├── SagaDefinitionRegistry.java  # Definition registration + versioned lookup
│   ├── EmbeddedSagaManager.java   # Default SagaManager implementation (package-private)
│   ├── SagaManagerBuilder.java    # DI-free builder for wiring
│   ├── StepResolver.java         # FunctionalInterface: resolve step by name + FQCN
│   ├── ReflectiveStepResolver.java # Default StepResolver: reflection + constructor injection
│   ├── ResourceRegistry.java     # Type-keyed resource registry for constructor injection
│   ├── TimeoutPolicy.java        # Internal: step/saga timeout enforcement
│   └── EventPayloadSerializer.java # Internal: serializes step output for event storage
├── store/                       # Storage interface + implementations
│   ├── SagaStore.java           # Interface
│   ├── SagaSchema.java
│   └── ScalarDbSagaStore.java   # Default implementation (ScalarDB)
├── recovery/                    # Crash recovery
│   ├── RecoveryConfig.java         # Record: recovery timing + clock config
│   └── SagaRecoveryManager.java
├── retention/                   # Data retention & cleanup
│   ├── RetentionConfig.java        # Record: retention period + cleanup schedule
│   └── SagaRetentionManager.java
├── exception/                   # Exception types
│   ├── SagaPersistenceException.java
│   ├── SagaAlreadyExistsException.java
│   ├── SagaDefinitionException.java
│   ├── SagaDefinitionNotFoundException.java
│   ├── SagaNotFoundException.java
│   ├── SagaConcurrentModificationException.java
│   ├── StepExecutionException.java
│   ├── StepCompensationException.java
│   └── StepTimeoutException.java
├── timeout/                     # Timeout management
│   └── TimeoutPolicy.java
├── observability/               # Observability hooks
│   ├── SagaEventListener.java
│   └── OpenTelemetrySagaListener.java
├── admin/                       # Admin API
│   ├── SagaAdminService.java
│   ├── DefaultSagaAdminService.java
│   ├── SagaQuery.java
│   ├── SagaPage.java
│   ├── SagaDetail.java
│   └── SagaMetrics.java
├── testing/                     # Testing harness
│   ├── SagaTestHarness.java
│   ├── (uses ScalarDbSagaStore + SQLite internally)
│   └── MockStep.java
└── devserver/                   # Local development server
    ├── SagaDevServer.java
    └── SagaDevServerConfig.java
```

**Naming convention**: Public API classes use the `Saga` prefix when the remainder is too generic to stand alone (e.g., `SagaManager`, `SagaStore`, `SagaContext`, `SagaStatus`, `SagaTestHarness`). Domain-specific names that are already unambiguous within the package omit the prefix (e.g., `Step`, `StepResult`, `RetryPolicy`, `TccStep`). Internal engine components also use domain-specific names without the prefix (e.g., `TimeoutPolicy`, `StepResolver`, `ResourceRegistry`) since they live inside the engine package and are not part of the public API.

## Component Summary

| # | Component | Responsibility |
|---|-----------|---------------|
| 1 | **SagaEngine** | Owns saga lifecycle: `createSaga()` (persist), `executeSaga()` (run steps), `execute()` (convenience). Walks through steps sequentially, handles retry/compensation inline on failure. Features: per-step/per-saga timeouts, graceful shutdown, TCC mode (`TccStep` with try/confirm/cancel), plan caching. Compensation executes in reverse (LIFO) with retry; stops on failure for recovery to retry. Async step completion (daemon mode only): parks on `StepResult.pending()`, resumes via callback. |
| 3 | **RetryPolicy** | Exponential backoff + jitter. Participant-driven error classification (`StepExecutionException.isRetryable()`). Virtual thread execution. |
| 4 | **SagaStore** (interface) | Append-only event persistence: 1 INSERT per step. Bucket-partitioned `saga_state` table with `status` as clustering key for efficient recovery scans and distributed writes. Default implementation: `ScalarDbSagaStore`. |
| 5 | **SagaRecoveryManager** | Scans each `saga_state` bucket for `status=RUNNING` with stale `updated_at` (similar to Seata's model). Bucket partitioning distributes scans across database nodes. Replays events to reconstruct state. Conflict-based claiming via ScalarDB transaction conflict detection. |
| 5b | **SagaRetentionManager** | Periodically purges resolved sagas (COMPLETED, COMPENSATED) older than a configurable retention period. Scans `saga_state` using CK prefix per (bucket, status), then deletes both `saga_state` and `saga_events` rows via `deleteSaga()`. ESCALATED sagas are excluded (require manual admin resolution). |
| 6 | **Declarative Steps & Transport** | Declarative service steps (`ServiceStep` + `CallSpec`) realized via the `TransportAdapter` SPI + `DeclarativeBindingStep`/`DeclarativeBindingTccStep`; `SagaHttpClient` for code steps. HTTP transport today (gRPC future). Automatic `${...}`/`$.path` mapping, `X-Saga-Id`/`X-Saga-Step` propagation, SSRF allowlist + body limits. |
| 7 | **SagaTestHarness** | Integration testing: mock steps, crash simulation, assertion helpers. Uses `ScalarDbSagaStore` backed by in-memory SQLite. |
| 8 | **SagaAdminService** | Production operations API: list, inspect, compensate, retry, force-complete. |
| 9 | **SagaDevServer** | Zero-config dev environment: SQLite-backed database + embedded web UI. |
| 10 | **OpenTelemetrySagaListener** | Tracing spans per saga/step, metrics counters/histograms/gauges, lifecycle events. |

## Deployment Architecture: Cross-Service Orchestration

### User Deployment Model

The saga engine can run as a **library (JAR dependency)** embedded in the orchestrator service, or as a **standalone coordinator daemon** with a REST API (Phase 3). Both modes use the same engine internally. The orchestrator is deployed separately from the participant services.

#### Two Deployment Patterns

There are two distinct ways to use the `Step` interface. Understanding the difference is critical:

**Pattern A: Microservices (typical) — Step is a thin RPC client stub**

In the common microservices scenario, the orchestrator's Step implementations are **client-side proxies** that call remote services. The actual business logic lives in each participant service's own codebase and is deployed separately.

```
Orchestrator Service (location A)    Account Service (location B)   Shipping Service (location C)
┌──────────────────────────┐        ┌────────────────────────┐     ┌────────────────────────┐
│  Saga Engine (library)    │        │                        │     │                        │
│  SagaStore (ScalarDB)     │        │  DebitEndpoint (gRPC)   │     │  ShipEndpoint (HTTP)    │
│                           │        │  CreditEndpoint (gRPC)  │     │  CancelEndpoint (HTTP)  │
│  RemoteDebitStep ─────────┼──gRPC──►                        │     │                        │
│   (gRPC stub only, ~20    │        │  ┌──────────────────┐  │     │  ┌──────────────────┐  │
│    lines of code)         │        │  │ BUSINESS LOGIC    │  │     │  │ BUSINESS LOGIC    │  │
│  RemoteShipStep ──────────┼──HTTP───────────────────────────────►  │  HERE               │  │
│   (HTTP client only, ~20  │        │  │ + DB access       │  │     │  │ + DB access       │  │
│    lines of code)         │        │  └──────────────────┘  │     │  └──────────────────┘  │
│                           │        │                        │     │                        │
│  ScalarDB (saga state)    │        │  Account DB             │     │  Shipping DB           │
└──────────────────────────┘        └────────────────────────┘     └────────────────────────┘

Step implementations in the orchestrator are just RPC stubs.
Business logic and databases are in the participant services.
The participant services DON'T know about the saga engine.
```

Pattern characteristics:
- Each participant owns its own business logic, database, and API endpoints
- **Participants don't depend on the saga engine library** — they're normal microservices
- Each service is built, deployed, and scaled independently

How the saga engine fits:
- The orchestrator embeds the saga engine and SagaStore
- Step implementations are thin RPC stubs (~20 lines each) that call participant APIs
- Saga state lives in the orchestrator's ScalarDB instance, separate from business data

**Pattern B: Modular monolith — Step contains actual business logic**

If the orchestrator has direct database access to all business data (e.g., a monolith or a single service with a shared database), Steps can contain the actual business logic.

```
Single Service (location A)
┌──────────────────────────────────────┐
│  Saga Engine (library)                │
│  SagaStore (ScalarDB)                 │
│                                       │
│  DebitAccountStep                     │
│   └─► ScalarDB tx: UPDATE accounts   │  ← actual business logic HERE
│  CreditAccountStep                    │
│   └─► ScalarDB tx: UPDATE accounts   │  ← actual business logic HERE
│                                       │
│  ScalarDB (saga state + business data)│
└──────────────────────────────────────┘
```

Pattern characteristics:
- All business logic runs in a single service with a shared database
- No network calls between steps — all operations are local
- Simpler deployment and operations

How the saga engine fits:
- The service embeds the saga engine and SagaStore
- Step implementations contain actual business logic with direct database access
- Saga state and business data share the same ScalarDB instance

**The examples in this document primarily show Pattern A (microservices).** Pattern B (modular-monolith) with local database steps is covered in [Step Implementation Patterns](#step-implementation-patterns). In practice, a saga can **mix** both patterns — some steps are local, others are remote.

#### Deployment Steps (Pattern A — Microservices)

**Orchestrator Service** (the only service that needs the saga engine):

```groovy
// build.gradle for the orchestrator
dependencies {
    implementation 'com.scalar-labs:scalardb-saga-engine:1.0.0'
    implementation 'com.scalar-labs:scalardb:3.x.x'
    implementation 'io.grpc:grpc-stub:1.x.x'   // for calling remote services
}
```

```java
// Step implementation — just a gRPC client stub, NOT business logic
public class RemoteDebitStep implements Step {
    private final AccountServiceGrpc.AccountServiceBlockingStub stub;

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        try {
            DebitResponse resp = stub.debit(DebitRequest.newBuilder()
                .setAccountId(ctx.get("accountId", String.class))
                .setAmount(ctx.get("amount", Integer.class))
                .build());
            return StepResult.of("debitId", resp.getDebitId());
        } catch (StatusRuntimeException e) {
            throw new StepExecutionException(e);
        }
    }

    @Override
    public void compensate(SagaContext ctx) throws StepCompensationException {
        try {
            stub.reverseDebit(ReverseDebitRequest.newBuilder()
                .setDebitId(ctx.get("debitId", String.class))
                .build());
        } catch (StatusRuntimeException e) {
            throw new StepCompensationException(e);
        }
    }
}
```

**Participant Services** (no saga engine dependency at all):

```groovy
// build.gradle for the Account Service — no saga engine dependency
dependencies {
    implementation 'io.grpc:grpc-server:1.x.x'
    // ... normal service dependencies, any database ...
}
```

```java
// Normal gRPC endpoint — doesn't know about sagas
public class AccountServiceImpl extends AccountServiceGrpc.AccountServiceImplBase {
    @Override
    public void debit(DebitRequest req, StreamObserver<DebitResponse> observer) {
        // Business logic + DB access — completely independent of saga engine
        int newBalance = accountRepository.debit(req.getAccountId(), req.getAmount());
        observer.onNext(DebitResponse.newBuilder().setDebitId(generateId()).build());
        observer.onCompleted();
    }

    @Override
    public void reverseDebit(ReverseDebitRequest req, StreamObserver<Empty> observer) {
        // Compensation logic — called by orchestrator's RemoteDebitStep.compensate()
        accountRepository.credit(req.getDebitId());
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }
}
```

#### Saga Definition

```json
{
  "name": "PlaceOrder",
  "steps": [
    { "name": "debit",  "stepClass": "com.example.RemoteDebitStep" },
    { "name": "ship",   "stepClass": "com.example.RemoteShipStep" },
    { "name": "notify", "stepClass": "com.example.RemoteNotifyStep" }
  ]
}
```

#### Orchestrator Bootstrap

See [Bootstrap and Execute](#3-bootstrap-and-execute) in Part IV for the full bootstrap example (ScalarDB setup, SagaManagerBuilder, definition registration, and saga execution).

#### Configure ScalarDB (Orchestrator Only)

```properties
# database.properties — only needed in the orchestrator service
scalar.db.contact_points=cassandra-host
scalar.db.username=admin
scalar.db.password=<your-password>
scalar.db.storage=cassandra
```

#### Build and Deploy

Each service is built and deployed independently:

```
# Orchestrator (has saga engine + Step stubs)
cd orchestrator-service && ./gradlew shadowJar && docker build -t orchestrator .

# Account Service (normal microservice, no saga dependency)
cd account-service && ./gradlew shadowJar && docker build -t account-svc .

# Shipping Service (normal microservice, no saga dependency)
cd shipping-service && ./gradlew shadowJar && docker build -t shipping-svc .
```

For HA, scale the orchestrator replicas. The participant services scale independently.

```
                    ┌──────────────┐
                    │   ScalarDB    │  ← saga state only
                    └──────┬───────┘
              ┌────────────┼────────────┐
              ▼            ▼            ▼
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
     │ orchestrator  │ │ orchestrator  │ │ orchestrator  │
     │  replica 1   │ │  replica 2   │ │  replica 3   │
     │ (Step stubs  │ │ (Step stubs  │ │ (Step stubs  │
     │  + engine)   │ │  + engine)   │ │  + engine)   │
     └──────┬───────┘ └──────────────┘ └──────────────┘
            │ gRPC/HTTP
     ┌──────┴──────────────┐
     ▼                     ▼
┌──────────┐         ┌──────────┐
│ Account   │         │ Shipping  │
│ Service   │         │ Service   │      ← no saga engine, own databases
│ (own DB)  │         │ (own DB)  │
└──────────┘         └──────────┘
```

#### Summary

| Concern | Pattern A (Microservices) | Pattern B (Monolith) |
|---|---|---|
| **Where is business logic?** | In each participant service (B, C, ...) | In the orchestrator (A) |
| **What does the Step contain?** | RPC client stub (~20 lines) | Actual business logic + DB access |
| **Do participants need saga engine?** | No — they're normal microservices | N/A — everything is in one service |
| **How many services to deploy?** | Orchestrator + each participant | One service |
| **Where is saga state?** | Orchestrator's ScalarDB | Shared ScalarDB (saga state + business data) |
| **Where is business data?** | Each participant's own database | Shared ScalarDB (saga state + business data) |

### Embedded vs. Centralized Coordinator

This design supports both modes: an **embedded coordinator** (library mode, same as Seata Saga), where the saga engine runs inside the orchestrator service with low operational overhead, and a **standalone coordinator** (daemon mode, Phase 3), where the saga engine runs as a separate service with a REST API. Neither requires a heavy infrastructure cluster like Temporal/Cadence — the engine is stateless and relies on ScalarDB for persistence.

> See [scalardb-saga-competitive-analysis.md](scalardb-saga-competitive-analysis.md) for competitive analysis of this topic.

### High Availability: Recovering from Orchestrator Crashes

With embedded orchestration, the orchestrator service is a single point of coordination. If it crashes mid-saga, recovery depends on another replica detecting the failure and resuming the saga.

Since our design persists all saga state in ScalarDB (not in-memory), **any instance of the orchestrator service can recover any saga** — as long as it can detect the original owner has crashed. We use a **periodic scan protocol** (similar to Seata) for this:

```
                    ┌──────────────┐
                    │   ScalarDB    │  ← saga events + status live here,
                    │  (shared DB)  │     not in any single process
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Replica 1 │ │ Replica 2 │ │ Replica 3 │
        │ (saga     │ │ (saga     │ │ (saga     │
        │  engine)  │ │  engine)  │ │  engine)  │
        │           │ │           │ │           │
        │ executing │ │ periodic  │ │ periodic  │
        │  sagas    │ │ recovery  │ │ recovery  │
        │           │ │ scan      │ │ scan      │
        │           │ │           │ │           │
        └──────────┘ └──────────┘ └──────────┘

  Replica 1 crashes → saga's updated_at stops advancing.
  Replica 2's periodic scan finds the stale saga
  in saga_state, claims it via transactional write, and resumes.
```

**Recovery protocol (Seata-style periodic scan):**

1. **Saga registered on creation**: `SagaEngine.execute()` inserts a row into `saga_state` with `status=RUNNING` and `updated_at=now`.
2. **No per-step ownership writes**: The engine does not write any lease/ownership data per step — only appends events to `saga_events`. The `saga_state.updated_at` is updated on saga start and end only.
3. **Periodic recovery scan**: Every replica runs `SagaRecoveryManager` on a background scheduler (default: every 30s). It scans each `saga_state` bucket with clustering key prefix `status=RUNNING` (and `COMPENSATING`), reading only active sagas. Bucket-based partitioning distributes these scans across database nodes.
4. **Conflict-based claiming**: Before recovering a saga, the replica reads the current `saga_state` row to verify it hasn't been claimed since the scan, then writes a new row with updated `owner_id` and incremented `version` (read + DELETE + INSERT in one transaction). The read provides both version-based optimistic concurrency control (serial case) and transaction conflict detection (concurrent case).
5. **Event replay for state reconstruction**: The claiming replica reads all events from `saga_events` for the saga and replays them to reconstruct current state (e.g., finds the last `STEP_COMPLETED` event at step 3).
6. **Resume forward execution**: The engine resumes from `lastCompleted + 1` (step 4). The step may or may not have executed before the crash — **this is why `execute()` must be idempotent**.
7. **Resume compensation**: If the saga was `COMPENSATING`, the engine resumes compensation from the last compensated step downward. If a compensation fails again, the saga stays in `COMPENSATING` for the next recovery scan. After repeated failures (configurable threshold), the recovery manager escalates the saga to `ESCALATED` for manual intervention.
8. **Status updated on completion**: When a saga reaches a terminal state (`COMPLETED`, `COMPENSATED`, `ESCALATED`), the engine updates `saga_state.status` to the terminal state. Terminal entries can be cleaned up after a configurable retention period.

**Isolation level: SNAPSHOT is sufficient.**

ScalarDB supports configurable isolation levels (SNAPSHOT, SERIALIZABLE, READ_COMMITTED). The default SNAPSHOT level is a weak variant closer to read-committed snapshot isolation — each Get/Scan operation reads the latest committed data rather than from a fixed transaction-start snapshot. This means write-skew, read-skew, and read-only anomalies are theoretically possible.

However, the saga workload is safe under SNAPSHOT because no transaction performs multiple reads where cross-read consistency matters:

| Transaction | Reads | Writes | Why safe |
|---|---|---|---|
| `createSaga` | 0 | INSERT event + INSERT state | Writes only |
| `appendEvent` | 0 | INSERT event | Writes only |
| `recordTransition` | 1 Get (existence check) | INSERT event + DELETE/INSERT state | Single read — verifies row still exists at snapshot's CK. Row not found → abort. Concurrent transitions on same row produce write-write conflict. |
| `claimForRecovery` | 1 Get | DELETE/INSERT state | Single read — no second read to skew against. Concurrent claims produce write-write conflict. |
| `getEvents` | 1 Scan (single partition) | 0 | Single scan operation |
| `findRecoverable` | 3 Scans (1 bucket × 3 statuses) per page | 0 | Called with cursor from `recover()`. Each result is independently validated by a separate `claimForRecovery` transaction. |

Write-skew requires reading row X and writing row Y based on that read — no saga transaction does this. The read-only anomaly requires an invariant spanning multiple data items with concurrent write transactions — no invariant spans multiple sagas, and within a single saga, only one replica executes at a time (after claiming).

> **Guideline for future contributors**: If you add a new transaction that reads from multiple rows/tables and makes decisions based on the combined result, re-evaluate whether SNAPSHOT isolation is still sufficient. The current safety relies on every transaction having at most one read that influences writes.

**Append-only event persistence:**

The engine uses an **append-only event store** — each state change is a single INSERT into `saga_events`. There are no UPDATEs to the event table, and no separate "step started" writes. This is the same model used by Axon Framework, Eventuate Tram, and conceptually by Temporal (event history) and Restate (journal). In contrast, Seata Saga writes **both** `INSERT (STARTED)` and `UPDATE (COMPLETED)` per step — doubling the write volume.

**Per-step cost: 1 INSERT** (vs. Seata's 2 writes, vs. our previous design's 3 ops).

### Competitive Comparisons

> **Competitive Comparison: Oracle MicroTx Saga vs. Narayana LRA vs. This Design** -- See [scalardb-saga-competitive-analysis.md](scalardb-saga-competitive-analysis.md) for the full competitive comparison, including architecture diagrams, head-to-head tables, competitive advantages/disadvantages, positioning analysis, and daemon mode analysis.

> **Seata Saga Features NOT Included (and Why)** -- See [scalardb-saga-competitive-analysis.md](scalardb-saga-competitive-analysis.md) for competitive analysis of this topic.

---

# Part II: Core Engine

## State Machine Parser + Execution Engine

### What It Does

Parses a saga definition (JSON, YAML, or Java builder API), then executes each step sequentially. Each "step" is a user-provided function (action + compensation pair). The engine walks through steps in order, passing context between them.

```
 ┌─────────────────────────────────────────────────────┐
 │                   SagaEngine                         │
 │                                                      │
 │  SagaDefinition ──► Step1 ──► Step2 ──► Step3       │
 │  (from definition)  │         │         │            │
 │                     action    action    action        │
 │                     comp.     comp.     comp.         │
 │                                                      │
 │  On failure at Step3:                                │
 │    SagaEngine.compensate(Step2, Step1)               │
 └─────────────────────────────────────────────────────┘
```

### Key Interfaces

```java
// --- api/Step.java ---
// A single saga step = action + compensation pair.
//
// LIFECYCLE: Steps are non-static, application-level singletons. A single Step
// instance is shared across all concurrent saga executions. This means:
//
//   1. Steps MUST be thread-safe — multiple sagas execute concurrently and call
//      the same Step instance simultaneously.
//   2. Steps SHOULD hold expensive resources (gRPC channels, connection pools,
//      HTTP clients) as instance fields — they are created once at startup and
//      reused across all saga executions.
//   3. Steps MUST NOT hold per-execution state in instance fields. Use
//      SagaContext for passing data between steps.
//   4. Steps are resolved by StepResolver at definition registration time (via
//      reflective constructor injection, custom resolver, or DI framework) and cached.
//
// This is the same pattern used by Spring @Service beans and CDI @ApplicationScoped
// beans — a single instance per application, shared across request threads.
public interface Step {
    String getName();

    // Execute the forward action. Returns result to pass to next step.
    // MUST be idempotent — on crash recovery, a step whose action completed
    // externally (e.g., the gRPC call succeeded) but whose result was not
    // persisted will be re-executed. Use a dedup key (e.g., sagaId + stepName)
    // to detect and skip duplicate executions.
    //
    // ERROR SIGNALING: The step decides whether a failure is retryable:
    //   - throw StepExecutionException(cause, retryable: true)  → engine retries
    //   - throw StepExecutionException(cause, retryable: false) → engine compensates
    // The step knows its own error semantics (database type, business rules)
    // and signals accordingly. The engine never inspects exception class names.
    StepResult execute(SagaContext context) throws StepExecutionException;

    // Undo the action. Called during compensation.
    // MUST be idempotent — may be called multiple times on crash recovery.
    void compensate(SagaContext context) throws StepCompensationException;
}

// --- api/TccStep.java ---
// Standalone interface for TCC (Try-Confirm-Cancel) mode.
// Unlike Step (which is for Saga mode), TccStep has three methods whose names
// match the TCC phases directly. The engine internally adapts TccStep to Step
// via TccReserveStep and TccConfirmStep so that both Saga and TCC run through
// the same pivot-based execution loop. See "Unified Execution Model" below.
//
// LIFECYCLE: Same as Step — non-static, application-level singletons.
// Thread-safety requirements apply to all three methods.
public interface TccStep {
    String getName();

    /**
     * Try phase: reserve resources / tentative operation.
     * MUST be idempotent — may be re-executed on crash recovery.
     * Use a dedup key (e.g., sagaId + stepName) to detect duplicates.
     *
     * ERROR SIGNALING: Same as Step.execute() — throw
     * StepExecutionException(cause, retryable) to signal retry/cancel.
     */
    StepResult reserve(SagaContext context) throws StepExecutionException;

    /**
     * Confirm phase: make the reservation permanent.
     * Called only after ALL steps' reserve() succeed.
     * MUST be idempotent — may be called multiple times on crash recovery.
     * MUST eventually succeed — resources are reserved; confirmation should
     * not permanently fail (unless the reservation expired, indicating a
     * timeout misconfiguration).
     */
    void confirm(SagaContext context) throws StepExecutionException;

    /**
     * Cancel phase: release the reservation.
     * Called when any step's reserve() fails, for all completed Try steps.
     * MUST be idempotent — may be called multiple times on crash recovery.
     */
    void cancel(SagaContext context) throws StepCompensationException;
}

// --- api/SagaContext.java ---
// Public interface — what Step and TccStep implementations receive.
// Provides read-only access to the saga's shared data map and the saga ID.
// Steps return output via StepResult, which the engine merges into the context
// for subsequent steps. Engine-internal tracking (event sequencing, state
// transitions, failure tracking) is in ExecutionContext, which implements this
// interface.
public interface SagaContext {
    String getSagaId();
    <T> T get(String key, Class<T> type);
}

// --- engine/ExecutionContext.java ---
// Engine-internal. Implements SagaContext and adds tracking fields for event
// sequencing, state transitions, and failure tracking. Package-private — not
// visible to Step implementations.
//
// ALLOWED VALUE TYPES: Only primitives, strings, and collections/maps of
// primitives are allowed. This restriction ensures reliable JSON serialization
// and deserialization across crash recovery boundaries. Complex objects (custom
// POJOs, cyclic references, generic types) are explicitly rejected at put-time.
//
// Allowed: String, Integer, Long, Double, Float, Boolean, BigDecimal,
//          List<allowed>, Map<String, allowed>
// Rejected: Custom objects, null values with ambiguous types, Class references
class ExecutionContext implements SagaContext {
    private static final Set<Class<?>> ALLOWED_TYPES = Set.of(
        String.class, Integer.class, Long.class, Double.class,
        Float.class, Boolean.class, BigDecimal.class);

    private final String sagaId;
    private final Map<String, Object> data;
    private int nextEventSequence;       // tracks next saga_events sequence in memory
    private SagaStateSnapshot currentState;   // current saga state (replaced after each transition)
    private final Set<Integer> failedStepIndices = new HashSet<>();  // populated during replayEvents()

    // 3-arg constructor: used both for new sagas (empty input map) and crash
    // recovery (pre-populated input map from the SAGA_STARTED event payload).
    // currentState is the latest snapshot loaded before constructing this context.
    ExecutionContext(String sagaId, Map<String, Object> input, SagaStateSnapshot currentState) {
        this.sagaId = Objects.requireNonNull(sagaId);
        this.data = new HashMap<>(Objects.requireNonNull(input));
        this.currentState = Objects.requireNonNull(currentState);
    }

    // --- SagaContext interface (read-only, user-facing) ---
    @Override public String getSagaId() { return sagaId; }
    @Override public <T> T get(String key, Class<T> type) { ... }

    // --- Engine-internal write access (not exposed via SagaContext) ---
    void put(String key, Object value) {
        validateType(value);  // throws IllegalArgumentException if not allowed
        data.put(key, value);
    }
    // Returns a defensive copy of the data map (for serialization / event payload).
    Map<String, Object> getData() { return Map.copyOf(data); }

    // --- Engine-internal (not accessible from Step implementations) ---
    int nextSequence() { return nextEventSequence; }
    void advanceSequence() { nextEventSequence++; }
    void setNextEventSequence(int seq) { this.nextEventSequence = seq; }
    SagaStateSnapshot getCurrentState() { return currentState; }
    void setCurrentState(SagaStateSnapshot state) { this.currentState = state; }
    void markStepFailed(int stepIndex) { failedStepIndices.add(stepIndex); }
    boolean hasFailureEvent(int stepIndex) { return failedStepIndices.contains(stepIndex); }
    void merge(StepResult result) { ... }

    private void validateType(Object value) {
        if (value == null) {
            // Null values are rejected: the type is ambiguous after JSON round-trip
            // (e.g., null Integer vs null String are indistinguishable).
            throw new IllegalArgumentException(
                "SagaContext does not accept null values; use explicit removal instead.");
        }
        if (ALLOWED_TYPES.contains(value.getClass())) return;
        if (value instanceof List) {
            for (Object element : (List<?>) value) {
                if (element == null) {
                    throw new IllegalArgumentException(
                        "SagaContext does not accept null elements inside a List.");
                }
                validateType(element);
            }
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException(
                        "SagaContext Map keys must be Strings. Got: "
                        + (entry.getKey() == null ? "null" : entry.getKey().getClass().getName()));
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                        "SagaContext does not accept null values inside a Map.");
                }
                validateType(entry.getValue());
            }
            return;
        }
        throw new IllegalArgumentException(
            "SagaContext only accepts primitives, strings, and collections thereof. "
            + "Got: " + value.getClass().getName());
    }

    // --- Crash-recovery helper ---
    // After replayEvents() re-populates `data` from stored JSON, numeric values
    // may have drifted types (e.g., an Integer stored as Long by the JSON library).
    // coerceNumber(key, Integer.class) handles this transparently so that step
    // code using get("count", Integer.class) continues to work after recovery.
    <N extends Number> N coerceNumber(String key, Class<N> targetType) { ... }
}

// --- api/SagaStateSnapshot.java ---
// Read-only view of a saga, constructed from a saga_state row.
public class SagaStateSnapshot {
    private final String sagaId;
    private final String sagaName;
    private final SagaStatus status;
    private final String ownerId;
    private final int version;
    private final String definitionVersion;
    private final Instant createdAt;
    private final Instant updatedAt;

    public SagaStateSnapshot withTransition(SagaStatus newStatus, Instant newUpdatedAt) {
        return new SagaStateSnapshot(sagaId, sagaName, newStatus, ownerId,
            version, definitionVersion, createdAt, newUpdatedAt);
    }

    // Getters: getSagaId(), getSagaName(), getStatus(), getOwnerId(),
    //          getVersion(), getDefinitionVersion(), getCreatedAt(), getUpdatedAt()
}

// --- api/StepResult.java ---
// Output of a step execution, merged into SagaContext for subsequent steps.
public class StepResult {
    private final boolean pending;         // true = async, engine should park (daemon mode only)
    private final Map<String, Object> output;

    public static StepResult of(String key, Object value);
    public static StepResult of(Map<String, Object> output);
    public static StepResult empty();
    public static StepResult pending();    // daemon mode only: "I'm not done yet"
    public boolean isPending() { return pending; }
}

// --- api/SagaStatus.java ---
public enum SagaStatus {
    RUNNING,        // executing forward steps (Saga) or Try phase (TCC)
    CONFIRMING,     // TCC only: all Try steps succeeded, executing Confirm phase
    COMPLETED,      // all steps succeeded (and confirmed, in TCC mode)
    COMPENSATING,   // executing compensation steps (Saga) or Cancel phase (TCC)
    COMPENSATED,    // all compensations/cancellations completed
    ESCALATED;      // stuck beyond grace period, needs manual intervention

    /** Returns true if the engine will not autonomously process this saga further. */
    public boolean isTerminal() {
        return this == COMPLETED || this == COMPENSATED || this == ESCALATED;
    }

    /**
     * Returns true if sagas in this status may be automatically purged after
     * the retention period. ESCALATED sagas are excluded — they require manual
     * admin resolution before cleanup.
     */
    public boolean isPurgeable() {
        return this == COMPLETED || this == COMPENSATED;
    }
}

```

### Saga Definition (JSON / YAML)

Saga definitions can be written in JSON or YAML. YAML is often preferred for hand-edited files because it supports comments and has less syntactic noise.

**JSON:**

```json
{
  "name": "MoneyTransfer",
  "version": "1.0",
  "mode": "SAGA",
  "recoveryStrategy": "BACKWARD",
  "timeoutMillis": 300000,
  "defaultRetryPolicy": {
    "maxAttempts": 3,
    "initialIntervalMillis": 1000,
    "backoffMultiplier": 2.0,
    "maxIntervalMillis": 30000
  },
  "steps": [
    {
      "name": "debit",
      "stepClass": "com.example.DebitAccountStep",
      "timeoutMillis": 60000,
      "retryPolicy": { "maxAttempts": 5 }
    },
    {
      "name": "credit",
      "stepClass": "com.example.CreditAccountStep",
      "timeoutMillis": 30000
    }
  ]
}
```

**YAML equivalent:**

```yaml
# Money transfer saga — debits source, then credits destination.
name: MoneyTransfer
version: "1.0"
mode: SAGA
recoveryStrategy: BACKWARD
timeoutMillis: 300000

defaultRetryPolicy:
  maxAttempts: 3
  initialIntervalMillis: 1000
  backoffMultiplier: 2.0
  maxIntervalMillis: 30000

steps:
  - name: debit
    stepClass: com.example.DebitAccountStep
    timeoutMillis: 60000
    retryPolicy:
      maxAttempts: 5  # more retries for the debit step

  - name: credit
    stepClass: com.example.CreditAccountStep
    timeoutMillis: 30000
```

**Mixed recovery (pivot transaction) example — JSON:**

```json
{
  "name": "PlaceOrder",
  "version": "1.0",
  "mode": "SAGA",
  "recoveryStrategy": "MIXED",
  "timeoutMillis": 300000,
  "steps": [
    {
      "name": "reserveInventory",
      "stepClass": "com.example.ReserveInventoryStep",
      "timeoutMillis": 60000
    },
    {
      "name": "chargePayment",
      "stepClass": "com.example.ChargePaymentStep",
      "pivot": true,
      "timeoutMillis": 60000
    },
    {
      "name": "sendConfirmationEmail",
      "stepClass": "com.example.SendConfirmationEmailStep",
      "timeoutMillis": 30000
    },
    {
      "name": "updateAnalytics",
      "stepClass": "com.example.UpdateAnalyticsStep",
      "timeoutMillis": 10000
    }
  ]
}
```

**YAML equivalent:**

```yaml
# Place order saga — reserve + charge are compensatable,
# email + analytics are retriable (must eventually succeed).
name: PlaceOrder
version: "1.0"
mode: SAGA
recoveryStrategy: MIXED
timeoutMillis: 300000

steps:
  - name: reserveInventory
    stepClass: com.example.ReserveInventoryStep
    timeoutMillis: 60000

  - name: chargePayment         # pivot — if this fails, compensate above
    stepClass: com.example.ChargePaymentStep
    pivot: true
    timeoutMillis: 60000

  - name: sendConfirmationEmail  # retriable — must eventually succeed
    stepClass: com.example.SendConfirmationEmailStep
    timeoutMillis: 30000

  - name: updateAnalytics        # retriable — must eventually succeed
    stepClass: com.example.UpdateAnalyticsStep
    timeoutMillis: 10000
```

### Saga Definition (Java Builder API)

For embedded mode, a type-safe Java builder provides compile-time checks and IDE auto-completion:

```java
SagaDefinition def = SagaDefinition.newBuilder("MoneyTransfer", SagaMode.SAGA)
    .version("1.0")
    .recoveryStrategy(RecoveryStrategy.BACKWARD)
    .timeoutMillis(300000)
    .defaultRetryPolicy(RetryPolicy.newBuilder()
        .maxAttempts(3)
        .initialIntervalMillis(1000)
        .backoffMultiplier(2.0)
        .maxIntervalMillis(30000)
        .build())
    .step("debit", DebitAccountStep.class)
        .timeoutMillis(60000)
        .retryPolicy(RetryPolicy.newBuilder().maxAttempts(5).build())
        .add()
    .step("credit", CreditAccountStep.class)
        .timeoutMillis(30000)
        .add()
    .build();

sagaManager.register(def);
```

**Mixed recovery (pivot transaction) example:**

```java
SagaDefinition def = SagaDefinition.newBuilder("PlaceOrder", SagaMode.SAGA)
    .version("1.0")
    .recoveryStrategy(RecoveryStrategy.MIXED)
    .timeoutMillis(300000)
    .step("reserveInventory", ReserveInventoryStep.class)
        .timeoutMillis(60000)
        .add()
    .step("chargePayment", ChargePaymentStep.class)  // pivot — the go/no-go step
        .pivot(true)
        .timeoutMillis(60000)
        .add()
    .step("sendConfirmationEmail", SendConfirmationEmailStep.class)  // retriable — after pivot
        .timeoutMillis(30000)
        .add()
    .step("updateAnalytics", UpdateAnalyticsStep.class)  // retriable — after pivot
        .timeoutMillis(10000)
        .add()
    .build();
```

The Java builder supports both `step(...)` (a `stepClass` code step) and `serviceStep(...)` (a declarative service step). Use a code step when you need custom Java logic; use `serviceStep(...).operation()`/`.tccOperation()` for simple service calls where no Java code is needed (see [Layer 2: Declarative Service Steps](#layer-2-declarative-service-steps)).

#### Saga Definition Formats

| Format | `stepClass` (code step) | Declarative service step (no code) | Primary use |
|---|---|---|---|
| **Java builder** | Yes | Yes | Embedded mode |
| **JSON/YAML** | Yes (embedded only) | Yes | Both modes |
| **Protocol Buffers** | No | Yes | Daemon mode (Phase 7a) |

All formats produce the same `SagaDefinition` domain model.

### SagaDefinition Domain Model

```java
// --- api/SagaDefinition.java ---
public class SagaDefinition {
    private String name;
    private String version;
    private SagaMode mode;                    // SAGA (default) or TCC
    private List<StepDefinition> steps;
    private RecoveryStrategy recoveryStrategy;  // SAGA: BACKWARD/FORWARD/MIXED. TCC: PREDEFINED.
    private long timeoutMillis;                   // saga-level timeout (0 = no timeout)
    private RetryPolicy defaultRetryPolicy;

    public enum SagaMode { SAGA, TCC }
    public enum RecoveryStrategy { BACKWARD, FORWARD, MIXED, PREDEFINED }
    // BACKWARD:   on step failure, compensate all completed steps (default for SAGA)
    // FORWARD:    on step failure, saga stays RUNNING for automatic recovery retry
    // MIXED:      steps before the pivot are compensatable (backward recovery on failure),
    //             steps after the pivot are retriable (forward recovery on failure).
    //             The pivot step itself is the go/no-go point: if it fails, compensate
    //             all completed steps before it (backward). Once the pivot succeeds,
    //             all subsequent steps must succeed (retry on failure, never compensate).
    //             Exactly one step must have pivot=true when recoveryStrategy is MIXED.
    // PREDEFINED: recovery is fixed by the mode — reserved for TCC, whose recovery is the
    //             Cancel phase. TCC definitions default to (and must use) PREDEFINED;
    //             SAGA mode rejects it.

    // Sealed: a step is either a code step (ClassStep) or a declarative service
    // step (ServiceStep). The engine resolves both to a Step/TccStep before
    // execution — see "Single Dispatch Path".
    public abstract sealed static class StepDefinition permits ClassStep, ServiceStep {
        String name;                 // unique within the saga (idempotency key with sagaId)
        long timeoutMillis;          // per-step timeout (0 = use saga-level timeout only)
        RetryPolicy retryPolicy;     // per-step override (nullable)
        boolean pivot;               // true = this is the pivot step (at most one per saga)
    }

    // Layer 1: a hand-written Step/TccStep implementation.
    public static final class ClassStep extends StepDefinition {
        String stepClass;            // FQCN of the Step/TccStep implementation
    }

    // Layer 2: a declarative service step — no Java code.
    public static final class ServiceStep extends StepDefinition {
        String service;                  // logical endpoint name (resolved via config)
        Map<Phase, CallSpec> phases;     // one CallSpec per phase
        // SAGA: EXECUTION, COMPENSATION. TCC: RESERVATION, CONFIRMATION, CANCELLATION.
        public enum Phase {
            EXECUTION, COMPENSATION, RESERVATION, CONFIRMATION, CANCELLATION
        }
    }

    /**
     * Returns the pivot index for the execution plan.
     * For BACKWARD or PREDEFINED (TCC): lastIndex (last step; for TCC the last try step,
     *   whose confirm steps are added by the engine).
     * For FORWARD: -1 (all steps are retriable).
     * For MIXED: the index of the step with pivot=true.
     */
    public int getPivotIndex() {
        return switch (recoveryStrategy) {
            case BACKWARD, PREDEFINED -> steps.size() - 1;
            case FORWARD -> -1;
            case MIXED -> {
                for (int i = 0; i < steps.size(); i++) {
                    if (steps.get(i).isPivot()) yield i;
                }
                throw new IllegalStateException("No pivot step found in MIXED recovery strategy");
            }
        };
    }

    // Validation (called at registration time):
    // - TCC definitions must use PREDEFINED and must not specify pivot steps (pivot is implicit)
    // - PREDEFINED is reserved for TCC — SAGA mode rejects it
    // - MIXED definitions must have exactly one step with pivot=true
    // - BACKWARD/FORWARD definitions must not have pivot=true on any step
    public void validate() {
        if (mode == SagaMode.TCC) {
            if (recoveryStrategy != RecoveryStrategy.PREDEFINED) {
                throw new SagaDefinitionException(
                    "TCC mode must not specify a recovery strategy"
                        + " — recovery is predefined via the Cancel phase");
            }
            if (steps.stream().anyMatch(StepDefinition::isPivot)) {
                throw new SagaDefinitionException(
                    "TCC definitions must not specify pivot steps — the pivot is implicit");
            }
        } else if (recoveryStrategy == RecoveryStrategy.PREDEFINED) {
            throw new SagaDefinitionException(
                "PREDEFINED recovery strategy is reserved for TCC mode");
        }
    }
}
```

Step names must be unique within a saga definition — the engine throws a `SagaDefinitionException` on duplicate step names at registration time. The `sagaId + stepName` pair serves as the idempotency key propagated to participants via `X-Saga-Id` and `X-Saga-Step` headers.

**Pivot transaction (mixed recovery):** Some workflows have compensatable steps followed by non-compensatable steps (e.g., reserve inventory → charge payment → send email). Use `RecoveryStrategy.MIXED` with a pivot step:

- Steps **before** the pivot step: compensatable. On failure, all completed steps are compensated in reverse order.
- The **pivot step** itself: the go/no-go point. If it fails, compensate all completed steps before it. If it succeeds, the workflow is committed to moving forward.
- Steps **after** the pivot step: retriable. On failure, the saga stays `RUNNING` for automatic recovery retry. These steps are never compensated.

```
Compensatable steps         Pivot           Retriable steps
┌────────────────────┐  ┌────────────┐  ┌────────────────────┐
│ reserveInventory   │──│ chargePmt  │──│ sendEmail          │
│ reserveShipping    │  │ (go/no-go) │  │ updateAnalytics    │
└────────────────────┘  └────────────┘  └────────────────────┘
  On failure: compensate    On failure:     On failure: retry
  backward                  compensate      (never compensate)
                            backward
```

Note that `BACKWARD` and `FORWARD` are special cases of the mixed recovery model: `BACKWARD` is equivalent to a pivot at the last step (all steps are compensatable), and `FORWARD` is equivalent to a pivot before the first step (all steps are retriable). Internally, the engine uses a single pivot-based execution loop for all three strategies (see [Shared Internal Execution Logic](#shared-internal-execution-logic)).

**Validation at registration time:**

- `pivot: true` is required on exactly one step when `recoveryStrategy` is `MIXED`, rejected otherwise.
- The pivot step must not be the first or last step — use `FORWARD` or `BACKWARD` instead for those degenerate cases. This prevents confusion and makes the intent explicit.
- TCC definitions use the `PREDEFINED` strategy (recovery is the Cancel phase); specifying any other strategy on a TCC definition is rejected, and `PREDEFINED` is rejected in SAGA mode.

### SagaManager (Top-Level API)

```java
// --- api/SagaManager.java ---
public interface SagaManager extends AutoCloseable {
    // Register a saga definition (from programmatic builder).
    // Eagerly validates all steps (resolves classes, checks constructors).
    void register(SagaDefinition definition);

    // Parse a saga definition from a file and register it.
    // Detects JSON or YAML by extension (.json, .yaml, .yml).
    void register(Path definitionFile);

    // Start a new saga instance with a server-generated ID (synchronous — blocks until saga completes).
    // Returns the generated saga ID. Use the overload that accepts a client-supplied
    // ID if you need idempotent retries (see "Client-Supplied Saga IDs" section).
    String start(String sagaName, Map<String, Object> input);

    // Start a new saga instance with a server-generated ID (asynchronous — returns immediately).
    String startAsync(String sagaName, Map<String, Object> input);

    // Start a new saga instance with a server-generated ID (asynchronous with completion callback).
    String startAsync(String sagaName, Map<String, Object> input,
                      SagaCallback callback);

    // Start a new saga instance with a CLIENT-SUPPLIED ID (synchronous).
    // Enables idempotent retries: if the caller crashes or loses the response after
    // the saga has been persisted, it can retry with the same ID and either find the
    // existing saga (via SagaAlreadyExistsException) or create it fresh.
    // Throws SagaAlreadyExistsException if a saga with the given ID already exists.
    void start(String sagaId, String sagaName, Map<String, Object> input);

    // Start a new saga instance with a CLIENT-SUPPLIED ID (asynchronous).
    void startAsync(String sagaId, String sagaName, Map<String, Object> input);

    // Start a new saga instance with a CLIENT-SUPPLIED ID (asynchronous with callback).
    void startAsync(String sagaId, String sagaName, Map<String, Object> input,
                    SagaCallback callback);

    // Resume a failed/crashed saga (crash recovery)
    SagaStateSnapshot resume(String sagaId);

    // Manually trigger compensation for a saga
    SagaStateSnapshot compensate(String sagaId);

    // Query saga instance state. Throws SagaNotFoundException if not found.
    SagaStateSnapshot getStateSnapshot(String sagaId);

    // Daemon mode only: complete an async step via external callback (resumes parked saga)
    SagaStateSnapshot completeStep(String sagaId, String stepName, Map<String, Object> output);

    // Start periodic crash recovery scanning.
    // Call after registering all saga definitions. Delegates to SagaRecoveryManager.start().
    void startRecovery();
}

// --- api/SagaCallback.java ---
public interface SagaCallback {
    void onCompleted(SagaStateSnapshot saga);
    void onCompensated(SagaStateSnapshot saga);
    void onEscalated(SagaStateSnapshot saga);
}
```

### SagaEngine (Core Execution Logic)

```java
// --- engine/SagaEngine.java ---
public class SagaEngine {
    private final SagaStore store;
    private final StepResolver stepResolver;
    private final String ownerId;          // unique ID for this replica (e.g., UUID)
    private final ShutdownConfig shutdownConfig;
    private final Clock clock;
    private final ConcurrentHashMap<String, List<StepWithPolicy>> planCache =
        new ConcurrentHashMap<>();

    /**
     * Persist a new saga (saga_state + SAGA_STARTED event).
     * Returns the full SagaStateSnapshot so callers can use it without a read-back.
     * Rejects new sagas if the engine is shutting down.
     *
     * If sagaId is null, a UUID is generated by the store. If non-null, the
     * caller-supplied ID is used and SagaAlreadyExistsException is thrown on
     * collision (with the existing snapshot attached). See the
     * "Client-Supplied Saga IDs" section for the idempotent-retry pattern.
     */
    public SagaStateSnapshot createSaga(SagaDefinition def, @Nullable String sagaId,
                                         Map<String, Object> input) {
        if (shuttingDown) {
            throw new IllegalStateException("Engine is shutting down — not accepting new sagas");
        }
        return store.createSaga(sagaId, def.getName(), ownerId, input, def.getVersion());
    }

    /**
     * Execute a previously created saga from a known SagaStateSnapshot (avoids a read-back).
     * Used by startAsync() which already has the instance from createSaga().
     */
    public void executeSaga(SagaDefinition def, SagaStateSnapshot saga, Map<String, Object> input) {
        // createSaga wrote 1 event (SAGA_STARTED at seq 0) and set status RUNNING
        ExecutionContext context = new ExecutionContext(saga.getSagaId(), input, saga);
        context.setNextEventSequence(1);        // next sequence after SAGA_STARTED
        executeSteps(def, context, 0);
    }

    /**
     * Convenience: create + execute in one call (synchronous).
     * If sagaId is null, the store generates a UUID. If non-null, the
     * caller-supplied ID is used and SagaAlreadyExistsException is thrown on
     * collision.
     */
    public String execute(SagaDefinition def, @Nullable String sagaId,
                           Map<String, Object> input) {
        SagaStateSnapshot saga = createSaga(def, sagaId, input);
        executeSaga(saga, input);
        return saga.getSagaId();
    }

    /**
     * Resume execution from a specific step index (used by crash recovery).
     */
    public SagaStateSnapshot resumeFrom(SagaDefinition def, ExecutionContext context, int fromStep) {
        executeSteps(def, context, fromStep);
        return context.getCurrentState();
    }

    /**
     * Shared execution loop used by both execute() and resumeFrom().
     *
     * For Saga mode (BACKWARD / FORWARD / MIXED), uses a pivot-based model:
     * steps at or before the pivot index are compensatable (backward recovery
     * on failure), steps after the pivot are retriable (forward recovery on
     * failure). BACKWARD sets pivot index=lastStep, FORWARD sets pivot index=-1,
     * and MIXED sets pivot index to the step marked with pivot=true.
     *
     * Unified execution model: both Saga and TCC run through the same
     * pivot-based loop. For TCC, the engine expands the definition into a
     * 2N-step plan with adapters (see expandTccPlan) before running the loop.
     */
    private void executeSteps(SagaDefinition def, ExecutionContext context,
                               int startIndex) {
        String sagaId = context.getSagaId();

        if (!registerActive(sagaId)) {
            // Engine is shutting down. Saga is already persisted as RUNNING.
            // Mark for immediate recovery so another replica picks it up quickly.
            store.markForRecovery(sagaId);
            return;
        }

        try {
            // Plan is cached on first build (at registration time) and reused for all executions.
            List<StepWithPolicy> plan = getOrBuildPlan(def);

            executeSagaSteps(plan, def.getPivotIndex(), context, startIndex,
                             def.getTimeoutMillis());
        } finally {
            unregisterActive(sagaId);
        }
    }

    /**
     * Returns a cached execution plan for the given definition, building it on first access.
     * Plans are immutable (List.copyOf) and version-qualified to avoid stale plans after redeploy.
     * Package-private: called by EmbeddedSagaManager.register() for eager step validation.
     */
    List<StepWithPolicy> getOrBuildPlan(SagaDefinition def) {
        return planCache.computeIfAbsent(planCacheKey(def), k -> List.copyOf(buildPlan(def)));
    }

    private static String planCacheKey(SagaDefinition def) {
        return def.getName() + ":" + def.getVersion();
    }

    private List<StepWithPolicy> buildPlan(SagaDefinition def) {
        if (def.getMode() == SagaMode.TCC) {
            return expandTccPlan(def);
        }
        return def.getSteps().stream()
            .map(stepDef -> {
                Step step = resolveStep(stepDef, Step.class);
                return new StepWithPolicy(step, resolveRetryPolicy(stepDef, def),
                    RetryPolicy.compensationDefault(), stepDef.getTimeoutMillis());
            })
            .toList();
    }

    /**
     * Expand a TCC definition into a unified execution plan.
     * N user-defined TCC steps become 2N internal Step slots:
     *   [try_0, try_1, ..., try_N-1 (pivot), confirm_0, ..., confirm_N-1]
     *
     * Try adapters:    forward = reserve(), compensate = cancel()
     * Confirm adapters: forward = confirm(), compensate = no-op (retriable)
     *
     * The expansion is deterministic (same definition version → same plan →
     * same step indices), which is required for crash recovery correctness.
     */
    // --- engine/TccReserveStep.java ---
    // Top-level package-private class (not nested in SagaEngine).

    /**
     * Wraps TccStep for the Reserve (Try) phase: forward = reserve(), compensate = cancel().
     * Package-private — not part of the public API.
     */
    class TccReserveStep implements Step {
        private final TccStep tccStep;
        private final String name;

        TccReserveStep(TccStep tccStep) {
            this.tccStep = Objects.requireNonNull(tccStep);
            this.name = tccStep.getName() + ".reserve";
        }

        @Override public String getName() { return name; }
        @Override public StepResult execute(SagaContext context) throws StepExecutionException {
            return tccStep.reserve(context);
        }
        @Override public void compensate(SagaContext context) throws StepCompensationException {
            tccStep.cancel(context);
        }
    }

    // --- engine/TccConfirmStep.java ---
    // Top-level package-private class (not nested in SagaEngine).

    /**
     * Wraps TccStep for the Confirm phase: forward = confirm(), compensate = no-op.
     * Confirm steps are always after the pivot, so compensate() should never be called.
     * Package-private — not part of the public API.
     */
    class TccConfirmStep implements Step {
        private final TccStep tccStep;
        private final String name;

        TccConfirmStep(TccStep tccStep) {
            this.tccStep = Objects.requireNonNull(tccStep);
            this.name = tccStep.getName() + ".confirm";
        }

        @Override public String getName() { return name; }
        @Override public StepResult execute(SagaContext context) throws StepExecutionException {
            tccStep.confirm(context);
            return StepResult.empty();
        }
        @Override public void compensate(SagaContext context) throws StepCompensationException {
            throw new UnsupportedOperationException(
                "TCC confirm steps are after the pivot and must not be compensated");
        }
    }

    /**
     * Bundles a Step with its resolved RetryPolicy.
     * Internal record — not part of the public API.
     */
    record StepWithPolicy(Step step, RetryPolicy executionRetryPolicy,
                          RetryPolicy compensationRetryPolicy, long stepTimeoutMillis) {
        // Compact constructor validates invariants at construction time.
        StepWithPolicy {
            if (stepTimeoutMillis < 0) {
                throw new IllegalArgumentException("stepTimeoutMillis must be >= 0");
            }
        }
    }

    // Single-loop TCC expansion: resolve each TccStep once, create both reserve and confirm entries.
    private List<StepWithPolicy> expandTccPlan(SagaDefinition def) {
        List<StepWithPolicy> reserveSteps = new ArrayList<>();
        List<StepWithPolicy> confirmSteps = new ArrayList<>();
        RetryPolicy confirmPolicy = RetryPolicy.confirmDefault();
        RetryPolicy compensationPolicy = RetryPolicy.compensationDefault();
        for (StepDefinition stepDef : def.getSteps()) {
            TccStep tccStep = resolveStep(stepDef, TccStep.class);
            RetryPolicy reservePolicy = resolveRetryPolicy(stepDef, def);
            reserveSteps.add(new StepWithPolicy(
                new TccReserveStep(tccStep), reservePolicy,
                compensationPolicy, stepDef.getTimeoutMillis()));
            confirmSteps.add(new StepWithPolicy(
                new TccConfirmStep(tccStep), confirmPolicy,
                compensationPolicy, stepDef.getTimeoutMillis()));
        }
        reserveSteps.addAll(confirmSteps);
        return reserveSteps;
    }

    /**
     * Unified pivot-based execution loop. Handles both Saga and TCC
     * (TCC is pre-expanded into a Step list with adapters by the caller).
     *
     *   Steps 0..pivotIndex:       compensatable — on failure, compensate backward
     *   Steps pivotIndex+1..last:  retriable — on failure, emit STEP_FAILED, stay RUNNING for recovery
     *
     *   SAGA BACKWARD (pivotIndex = N-1):       all steps are compensatable
     *   SAGA FORWARD  (pivotIndex = -1):        all steps are retriable
     *   SAGA MIXED    (pivotIndex = P, 0≤P<N): steps 0..P compensatable, P+1.. retriable
     *   TCC           (pivotIndex = N-1):       try steps compensatable, confirm steps retriable
     */
    private void executeSagaSteps(List<StepWithPolicy> plan, int pivotIndex,
                                   ExecutionContext context, int startIndex,
                                   long sagaTimeoutMillis) {
        String sagaId = context.getSagaId();
        int completedIndex = startIndex - 1;
        long sagaDeadline = sagaTimeoutMillis > 0
            ? System.currentTimeMillis() + sagaTimeoutMillis : 0;

        for (int i = startIndex; i < plan.size(); i++) {
            // Graceful shutdown: stop between steps (saga stays RUNNING for recovery)
            if (shouldStopBetweenSteps()) {
                break;
            }

            // Saga-level timeout: check before each step
            if (sagaDeadline > 0 && clock.millis() > sagaDeadline) {
                // Timeout before/at pivot → compensate; after pivot → stay for recovery.
                if (i <= pivotIndex) {
                    compensate(plan, context, completedIndex);
                }
                // After pivot: saga stays RUNNING for recovery retry.
                return;
            }

            StepWithPolicy entry = plan.get(i);
            long stepDeadline = calculateStepDeadline(entry.stepTimeoutMillis(), sagaDeadline);

            // Execute the step. Only step execution exceptions trigger compensation.
            StepResult result;
            try {
                result = executeWithRetry(entry.step(), context,
                                           entry.executionRetryPolicy(), stepDeadline);
            } catch (Exception e) {
                // Step execution failed — compensate or stay for recovery based on pivot.
                if (i <= pivotIndex) {
                    // Failed at or before the pivot — compensate backward.
                    // For TCC: this means cancel all completed try steps.
                    compensate(plan, context, completedIndex);
                } else {
                    // Failed after the pivot — stay RUNNING for recovery retry.
                    // Emit STEP_FAILED (at most once per step) for observability
                    // and escalation timing.
                    if (!context.hasFailureEvent(i)) {
                        store.appendEvent(sagaId, context.nextSequence(),
                            SagaEvent.stepFailed(i, entry.step().getName(), e));
                        context.advanceSequence();
                    }
                }
                return;
            }

            // Record step completion. If this fails (SagaPersistenceException),
            // the exception propagates out — no compensation. Recovery re-reads
            // the actual database state and resumes correctly. The step may be
            // re-executed, so steps must tolerate at-least-once delivery
            // (consistent with the 1-write-per-step design choice).
            store.appendEvent(sagaId, context.nextSequence(),
                SagaEvent.stepCompleted(i, entry.step().getName(), result));
            context.advanceSequence();
            context.merge(result);
            completedIndex = i;
        }

        // All steps completed — record terminal transition.
        // If this fails, the saga stays RUNNING and recovery completes it.
        if (completedIndex == plan.size() - 1) {
            transition(context, SagaEvent.sagaCompleted());
        }
    }

    /**
     * Trigger compensation from the given step index down to step 0 (inclusive).
     * Uses the cached execution plan from getOrBuildPlan().
     */
    void compensateFrom(SagaDefinition def, ExecutionContext context, int fromStep) {
        List<StepWithPolicy> plan = getOrBuildPlan(def);
        compensate(plan, context, fromStep);
    }

    /**
     * Compensation logic: transitions to COMPENSATING, compensates steps backward,
     * and transitions to COMPENSATED on success.
     */
    private void compensate(List<StepWithPolicy> plan, ExecutionContext context, int fromStepIndex) {
        if (context.getCurrentState().getStatus() != SagaStatus.COMPENSATING) {
            transition(context, StatusEvent.compensating());
        }
        try {
            compensateSteps(plan, context, fromStepIndex);
            transition(context, StatusEvent.compensated());
        } catch (StepCompensationException e) {
            // Compensation failed — saga stays in COMPENSATING.
            // Recovery will retry from the failed compensation step.
        }
    }

    /**
     * Helper: record a status transition and update context tracking.
     * Passes the cached status metadata so recordTransition needs zero reads.
     */
    private void transition(ExecutionContext context, SagaEvent event) {
        context.setCurrentState(store.recordTransition(
            context.getCurrentState(), context.nextSequence(), event));
        context.advanceSequence();
    }

    /**
     * Resolve a step by name and class, verifying it implements the expected type.
     * Steps are application-level singletons — shared across all saga executions.
     * They MUST be thread-safe (stateless or internally synchronized).
     */
    private <T> T resolveStep(StepDefinition stepDef, Class<T> expectedType) {
        Object resolved = stepResolver.resolve(stepDef.getName(), stepDef.getStepClass());
        if (expectedType.isInstance(resolved)) {
            return expectedType.cast(resolved);
        }
        throw new SagaDefinitionException(
            "Step '" + stepDef.getName() + "' must implement " + expectedType.getName());
    }

    private RetryPolicy resolveRetryPolicy(StepDefinition stepDef, SagaDefinition def) {
        return stepDef.getRetryPolicy() != null
            ? stepDef.getRetryPolicy()
            : def.getDefaultRetryPolicy();
    }

    /**
     * Reconstruct an ExecutionContext by replaying a saga's event stream.
     * Package-private: used by SagaRecoveryManager and EmbeddedSagaManager.completeStep().
     *
     * Accumulates step outputs into the context data map, sets the next event sequence,
     * and derives the current saga state from the event stream.
     * A typical saga has 5-15 events — trivial cost.
     */
    ExecutionContext replayEvents(SagaStateSnapshot saga, List<SagaEvent> events) {
        ExecutionContext context = new ExecutionContext(saga.getSagaId(), Map.of());
        SagaStatus status = SagaStatus.RUNNING;

        for (SagaEvent event : events) {
            switch (event.getEventType()) {
                case SagaEvent.SAGA_STARTED:
                    Map<String, Object> input = fromJson(event.getPayload());
                    context = new ExecutionContext(saga.getSagaId(), input);
                    break;
                case SagaEvent.STEP_COMPLETED:
                    StepResult result = StepResult.fromJson(event.getPayload());
                    context.merge(result);
                    break;
                case SagaEvent.STEP_FAILED:
                case SagaEvent.STEP_COMPENSATION_FAILED:
                    context.markStepFailed(event.getStepIndex());
                    break;
                case SagaEvent.SAGA_COMPENSATING:
                case SagaEvent.SAGA_CONFIRMING:
                case SagaEvent.SAGA_COMPLETED:
                case SagaEvent.SAGA_COMPENSATED:
                case SagaEvent.SAGA_ESCALATED:
                    status = event.getTargetStatus();
                    break;
                // STEP_COMPENSATED doesn't change saga status or track failures.
            }
        }

        context.setNextEventSequence(events.size());
        context.setCurrentState(saga.withTransition(status, saga.getUpdatedAt()));

        return context;
    }
}
```

### SagaDefinitionRegistry

Centralized definition registry. `EmbeddedSagaManager` calls `register()`, while `SagaRecoveryManager` calls `resolve()`. Two-tier lookup: in-memory first, then store fallback (for definitions unregistered from memory after a redeploy).

```java
// --- engine/SagaDefinitionRegistry.java ---
public class SagaDefinitionRegistry {
    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();
    private final SagaStore store;

    public SagaDefinitionRegistry(SagaStore store) {
        this.store = store;
    }

    public void register(SagaDefinition definition) {
        definitions.put(definition.getName(), definition);
        definitions.put(definition.getName() + ":" + definition.getVersion(), definition);
        store.registerDefinition(definition);
    }

    // Lookup by name only (latest registered version) — used when starting new sagas
    @Nullable
    public SagaDefinition get(String sagaName) {
        return definitions.get(sagaName);
    }

    // Versioned lookup with store fallback — used during recovery and async completion
    @Nullable
    public SagaDefinition resolve(String sagaName, String version) {
        SagaDefinition def = definitions.get(sagaName + ":" + version);
        if (def == null) {
            def = store.getDefinition(sagaName, version).orElse(null);
        }
        return def;
    }
}
```

### EmbeddedSagaManager

`EmbeddedSagaManager` (package-private) implements the `SagaManager` interface. It delegates all saga lifecycle logic (creation, step execution, compensation) to `SagaEngine`, and handles definition registration (via `SagaDefinitionRegistry`), async threading, and callbacks.

- **`register()`**: Eagerly validates steps via `engine.getOrBuildPlan()`, then persists to registry. Invalid definitions are never stored.
- **`register(Path)`**: Parses via `SagaDefinitionParser`, then delegates to `register(SagaDefinition)`.
- **`start()`**: Delegates to `engine.execute()` — synchronous, blocks until the saga completes.
- **`startAsync()`**: Calls `engine.createSaga()` to persist the saga, then submits `engine.executeSaga()` to a virtual thread. The callback is dispatched in a `finally` block (both success and failure paths).
- **`resume()`, `compensate()`**: Replays events, then delegates to `SagaEngine`.
- **`getStateSnapshot()`**: Delegates to `SagaStore`. Throws `SagaNotFoundException` if not found.

```java
// --- engine/EmbeddedSagaManager.java ---
@ThreadSafe
class EmbeddedSagaManager implements SagaManager {
    private final SagaEngine engine;
    private final SagaStore store;
    private final SagaDefinitionRegistry registry;
    private final long shutdownTimeoutMillis;
    private final ExecutorService asyncExecutor;

    @Override
    public void register(SagaDefinition definition) {
        // Eagerly resolve all steps — fail fast on missing resources or unresolvable constructors.
        // This must happen before persisting to the store, so invalid definitions are never stored.
        engine.getOrBuildPlan(definition);
        registry.register(definition);
    }

    @Override
    public void register(Path definitionFile) {
        register(SagaDefinitionParser.parseFile(definitionFile));
    }

    @Override
    public String start(String sagaName, Map<String, Object> input) {
        // Synchronous — blocks until saga completes. Server-generated ID.
        return engine.execute(requireDefinition(sagaName), null, input);
    }

    @Override
    public void start(String sagaId, String sagaName, Map<String, Object> input) {
        // Synchronous — blocks until saga completes. Client-supplied ID.
        engine.execute(requireDefinition(sagaName), sagaId, input);
    }

    // ... startAsync overloads delegate to startAsyncInternal ...

    /**
     * Shared async-launch path. Persists the saga synchronously (so it is recoverable
     * even if the process crashes before the virtual thread starts), then submits
     * execution to a virtual thread.
     */
    private SagaStateSnapshot startAsyncInternal(@Nullable String sagaId, String sagaName,
                                                  Map<String, Object> input,
                                                  @Nullable SagaCallback callback) {
        SagaDefinition def = requireDefinition(sagaName);
        SagaStateSnapshot saga = engine.createSaga(def, sagaId, input);

        asyncExecutor.submit(() -> {
            try {
                engine.executeSaga(def, saga, input);
            } catch (Exception e) {
                // Saga state is persisted — recovery will pick it up.
                logger.error("Async saga {} failed unexpectedly", saga.getSagaId(), e);
            } finally {
                // Always dispatch callback — reads latest state from store.
                try {
                    dispatchCallback(saga.getSagaId(), callback);
                } catch (Exception e) {
                    logger.error("Failed to dispatch callback for saga {}", saga.getSagaId(), e);
                }
            }
        });

        return saga;
    }

    @Override
    public SagaStateSnapshot compensate(String sagaId) {
        SagaStateSnapshot saga = getStateSnapshot(sagaId);
        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            throw new IllegalStateException(
                "Cannot compensate saga " + sagaId + " in status " + saga.getStatus());
        }
        SagaDefinition def = resolveDefinition(saga);
        List<SagaEvent> events = store.getEvents(sagaId);
        ExecutionContext context = engine.replayEvents(saga, events);
        // ... compute fromStep from events ...
        engine.compensateFrom(def, context, fromStep);
        return context.getCurrentState();
    }

    @Override
    public SagaStateSnapshot getStateSnapshot(String sagaId) {
        return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
    }

    @Override
    public void close() {
        asyncExecutor.shutdown();
        engine.shutdown();
        try {
            if (!asyncExecutor.awaitTermination(shutdownTimeoutMillis, TimeUnit.MILLISECONDS)) {
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

For `startAsync()`, `engine.createSaga()` runs before the virtual thread starts, so the saga is recoverable even if the process crashes before execution begins. Each async saga runs on its own virtual thread (cheap, no thread pool sizing). Both sync and async sagas use the same persistence, recovery, and timeout mechanisms.

**Caller patterns:**

```java
// Fire-and-forget
String sagaId = sagaManager.startAsync("transferMoney", input);

// Fire-and-poll
String sagaId = sagaManager.startAsync("transferMoney", input);
SagaStateSnapshot result = sagaManager.getStateSnapshot(sagaId);  // poll later

// Fire-and-callback
sagaManager.startAsync("transferMoney", input, new SagaCallback() {
    @Override public void onCompleted(SagaStateSnapshot saga) { ... }
    @Override public void onCompensated(SagaStateSnapshot saga) { ... }
    @Override public void onEscalated(SagaStateSnapshot saga) { ... }
});
```

### Client-Supplied Saga IDs

The `start()` / `startAsync()` overloads shown above generate the saga ID server-side and return it to the caller. This works well for fire-and-forget scenarios, but has a crash-window problem for callers that need to reliably track the saga they triggered: if the process (or network, in daemon mode) fails after the saga has been persisted but before the caller receives the ID, the caller has no handle to query progress, resume, or coordinate retries. The recovery scanner will eventually pick up the orphan, but in the meantime the caller cannot tell whether the saga exists at all — and a naive retry would create a duplicate.

The standard solution is **client-supplied saga IDs with idempotent-create semantics** (the same pattern as Stripe's `Idempotency-Key`, AWS client request tokens, etc.). The caller generates the ID locally — typically deterministic from a business key — and passes it to an overload that takes the ID as its first parameter:

```java
void start(String sagaId, String sagaName, Map<String, Object> input);
void startAsync(String sagaId, String sagaName, Map<String, Object> input);
void startAsync(String sagaId, String sagaName, Map<String, Object> input,
                SagaCallback callback);
```

These overloads return `void`: the caller already has the ID, so there is nothing to return. They throw `SagaAlreadyExistsException` (carrying a fresh snapshot of the existing saga) if the ID is already in use. The caller catches the exception, inspects the status, and decides what to do — resume, query, or treat the retry as a no-op:

```java
// Deterministic ID from a business key makes retries safe
String sagaId = "order-" + orderId + "-refund";
try {
    sagaManager.start(sagaId, "RefundSaga", input);
} catch (SagaAlreadyExistsException e) {
    SagaStateSnapshot existing = e.getExisting();
    switch (existing.getStatus()) {
        case RUNNING, CONFIRMING, COMPENSATING -> { /* in flight — nothing to do */ }
        case COMPLETED                         -> { /* already done */ }
        case COMPENSATED, ESCALATED            -> { /* terminal failure — handle */ }
    }
}
```

**Why throw rather than silently return the existing snapshot?** Silent idempotency would require hashing and comparing `input` to detect "same ID, different input" bugs, which adds complexity and an extra read on the happy path. Throwing forces the caller to consciously handle the retry case and also surfaces client bugs where the same ID is accidentally reused across unrelated business operations. Callers that want "get-or-create" semantics can wrap the call in two lines (catch exception → use the attached snapshot).

**ID validation.** `SagaStore.createSaga()` validates caller-supplied IDs at the boundary:
- Non-null, non-empty
- Length: 1–128 characters
- **Strict character allowlist: `[a-zA-Z0-9._-]`** (enforced, not just recommended)
- Globally unique across all sagas in the cluster (not just per saga name)

The strict allowlist is a security requirement, not a style preference. Saga IDs are embedded in:
- HTTP headers (`X-Saga-Id`) — CR/LF characters would enable header injection
- URL path segments (`/api/sagas/{sagaId}/...`) — path traversal characters would manipulate callback URLs
- gRPC metadata (`x-saga-id`) — control characters could corrupt metadata frames

```java
private static final Pattern SAGA_ID_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,128}");

private void validateSagaId(String sagaId) {
    if (sagaId == null || !SAGA_ID_PATTERN.matcher(sagaId).matches()) {
        throw new IllegalArgumentException(
            "Saga ID must be 1-128 characters matching [a-zA-Z0-9._-], got: "
            + (sagaId == null ? "null" : "'" + sagaId.substring(0, Math.min(sagaId.length(), 20)) + "'"));
    }
}
```

Auto-generated IDs (`UUID.randomUUID().toString()`) already satisfy this pattern — UUIDs use `[0-9a-f-]`.

**Duplicate detection cost.** Collision is detected via ScalarDB's `Insert` "fail if exists" semantics — no extra read on the happy path. The collision path pays one follow-up read in a fresh transaction to populate the snapshot attached to `SagaAlreadyExistsException`. See the ScalarDbSagaStore section for details.

## Compensation Manager

### What It Does

When a step fails, walks backward through already-completed steps and calls each one's `compensate()` method in reverse order (LIFO). If a compensation fails after retries, the engine stops and leaves the saga in `COMPENSATING` status. Recovery will pick it up later and retry from the failed step.

```
Forward:   Step1 ──► Step2 ──► Step3 (FAILS)
                                 │
Compensate: Step2.compensate() ◄─┘
            Step1.compensate() ◄─┘
```

### Design Decisions

- **Two-phase compensation retry**: Compensation retries are split into two phases that handle different failure scenarios:
  - **Phase 1 — immediate retry** (SagaEngine inline compensation): Handles transient failures (network blips, momentary service unavailability). Quick retries with short backoff (default: 3 attempts, 1s/2s/4s intervals). If the failure is transient, this resolves it in seconds without involving recovery. After retries are exhausted, the saga is persisted as `COMPENSATING` and the thread is freed.
  - **Phase 2 — periodic recovery retry** (SagaRecoveryManager): Handles prolonged outages (participant down for minutes or hours, deployment in progress). Recovery scans run every ~30s, giving external systems time to recover without blocking a thread or burning through retries. If the saga has been stuck in `COMPENSATING` longer than the `compensationGracePeriod` (default: 4 hours), escalates to `ESCALATED` for manual intervention. Escalated sagas can be bulk-retried via the Admin API when the participant recovers.

  Combining both phases into one long immediate retry would block the saga thread for potentially minutes. The two-phase approach keeps Phase 1 fast (seconds) and delegates longer retries to the background recovery scheduler.

- **Stop on failure**: If a compensation fails after Phase 1 retries, the engine stops the compensation loop. The saga remains in `COMPENSATING` status, and the recovery manager retries from the failed step on the next scan. This preserves the reverse execution order — the application designed the compensation order to maintain business invariants, and the engine must respect it.
- **Time-based escalation**: The recovery manager escalates a saga to `ESCALATED` based on how long it has been stuck in `COMPENSATING`, not the number of retry attempts. Default: `compensationGracePeriod = 4 hours`. This tolerates prolonged outages — if a participant is down for 2 hours but comes back, sagas auto-recover without manual intervention. Count-based escalation (e.g., escalate after 10 attempts) would escalate within ~5 minutes, requiring manual recovery for every outage longer than that. Escalated sagas can be bulk-retried via the Admin API when the participant recovers.
- **Idempotency required**: Compensations may be called multiple times (on crash recovery), so they MUST check if already compensated.

```java
// --- engine/SagaEngine.java (compensation methods — inline, no separate CompensationManager) ---
// Compensation logic is part of SagaEngine, not a separate class.
class SagaEngine {
    // ... fields: store, stepResolver, planCache, etc.

    /**
     * Compensate steps [fromStepIndex..0] in reverse order.
     * Each compensation is retried with per-step compensationRetryPolicy.
     * If a compensation fails after all retries, throws StepCompensationException
     * to stop the loop — the saga stays in COMPENSATING for recovery to retry.
     *
     * Works with both Saga steps and TCC steps. For TCC, step.compensate() on a
     * TccReserveStep delegates to tccStep.cancel(). TccConfirmStep's compensate()
     * is a no-op — but those steps are never reached because only steps ≤ pivot
     * are compensated.
     */
    void compensateSteps(List<StepWithPolicy> plan, ExecutionContext context, int fromStepIndex) {
        for (int i = fromStepIndex; i >= 0; i--) {
            StepWithPolicy stepWithPolicy = plan.get(i);
            compensateWithRetry(stepWithPolicy.step(), context,
                stepWithPolicy.compensationRetryPolicy(), i);
        }
    }

    /**
     * Retry compensation with exponential backoff.
     * Default: 3 attempts with 1s/2s/4s backoff.
     * Transient failures during compensation are common (network blips),
     * so giving up immediately is too aggressive.
     * If all retries fail, throws StepCompensationException.
     */
    private void compensateWithRetry(Step step, ExecutionContext context,
                                      RetryPolicy retryPolicy, int stepIndex) {
        int attempt = 0;
        long interval = retryPolicy.getInitialIntervalMillis();

        while (attempt < retryPolicy.getMaxAttempts()) {
            try {
                attempt++;
                step.compensate(context);
                store.appendEvent(context.getSagaId(),
                    context.nextSequence(),
                    SagaEvent.stepCompensated(stepIndex));
                context.advanceSequence();
                return;
            } catch (Exception e) {
                if (attempt >= retryPolicy.getMaxAttempts()) {
                    store.appendEvent(context.getSagaId(),
                        context.nextSequence(),
                        SagaEvent.stepCompensationFailed(stepIndex, e));
                    context.advanceSequence();
                    throw new StepCompensationException(step.getName(), stepIndex, e);
                }
                // Exponential backoff (virtual thread — cheap to sleep)
                try {
                    interval = retryPolicy.sleepWithBackoff(interval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    store.appendEvent(context.getSagaId(),
                        context.nextSequence(),
                        SagaEvent.stepCompensationFailed(stepIndex, e));
                    context.advanceSequence();
                    throw new StepCompensationException(step.getName(), stepIndex, e);
                }
            }
        }
    }
}
```

The compensation retry policy is separate from the forward step retry policy. Default: 3 attempts with 1s initial interval and 2.0x backoff multiplier:

```java
// In RetryPolicy.java
public static RetryPolicy compensationDefault() {
    return new RetryPolicy(3, 1000, 2.0, 10000);
}
```

## Retry with Backoff and Error Classification

### What It Does

Wraps step execution with configurable retry logic. Distinguishes between:
- **Transient errors** (network timeout, database conflict) → retry with backoff
- **Business errors** (insufficient funds, invalid input) → don't retry, compensate immediately

### Who Decides: Participant-Driven Error Classification

**The participant (Step) decides whether a failure is retryable.** The coordinator (SagaEngine) respects that decision and applies the retry policy accordingly.

This is a deliberate design choice. Participants use various databases, RPC frameworks, and external services — each with different exception hierarchies. The coordinator cannot reliably classify errors by inspecting exception class names. Instead, the participant catches its own exceptions, knows whether they are transient or terminal, and signals via `StepExecutionException(cause, retryable)`:

```
Step catches SQLException              → knows it's "insufficient funds"
  → throws StepExecutionException(e, retryable: false)
  → Engine sees retryable=false → compensate immediately

Step catches SQLException              → knows it's "connection reset"
  → throws StepExecutionException(e, retryable: true)
  → Engine sees retryable=true → retry with backoff
  → exhausted maxAttempts? → then compensate

Step catches StatusRuntimeException     → knows gRPC UNAVAILABLE is transient
  → throws StepExecutionException(e, retryable: true)
  → Engine retries
```

This is the same model used by Temporal (`ApplicationFailure.nonRetryable`) and by our own `TransportException.retryable`.

### StepExecutionException

```java
// --- exception/StepExecutionException.java ---
public class StepExecutionException extends Exception {
    private final boolean retryable;

    // Retryable by default — transient failures are the common case
    public StepExecutionException(Throwable cause) {
        this(cause, true);
    }

    public StepExecutionException(String message) {
        this(message, true);
    }

    public StepExecutionException(Throwable cause, boolean retryable) {
        super(cause);
        this.retryable = retryable;
    }

    public StepExecutionException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public StepExecutionException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() { return retryable; }
}
```

**Default is retryable.** If a step throws `new StepExecutionException(e)` without specifying, the engine retries. This is the safe default — transient failures (network blips, connection pool exhaustion, temporary overload) are more common than business errors, and retrying a business error just wastes a few attempts before compensating anyway.

### RetryPolicy

```java
// --- engine/RetryPolicy.java ---
@Immutable
public final class RetryPolicy {
    private final int maxAttempts;            // default: 3
    private final long initialIntervalMillis; // default: 1000
    private final double backoffMultiplier;   // default: 2.0
    private final long maxIntervalMillis;     // default: 30000

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(3, 1000, 2.0, 30000);
    }

    /**
     * Sleep with exponential backoff + equal jitter. Returns the next interval.
     *
     * Jitter strategy: "Equal Jitter" from the AWS Architecture Blog
     * (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
     *
     * Three common approaches:
     *   Full jitter:    sleep = random(0, interval)        — widest spread, but can sleep near 0
     *   Equal jitter:   sleep = interval/2 + random(0, interval/2) — 50% spread, guaranteed minimum wait
     *   Decorrelated:   sleep = random(baseInterval, lastSleep * 3) — stateful, harder to reason about
     *
     * We use equal jitter because:
     *   - 50% spread mitigates thundering herds effectively
     *   - Guaranteed minimum of interval/2 prevents near-zero sleeps that
     *     would hammer a recovering participant
     *   - Stateless — each call depends only on the current interval
     */
    public long sleepWithBackoff(long currentInterval) throws InterruptedException {
        if (currentInterval <= 0) throw new IllegalArgumentException("currentInterval must be positive");
        long half = currentInterval / 2;
        long jitter = half > 0 ? ThreadLocalRandom.current().nextLong(half) : 0;
        Thread.sleep(half + jitter);
        return Math.min((long) (currentInterval * backoffMultiplier), maxIntervalMillis);
    }
}
```

`RetryPolicy` is purely about timing (how many attempts, how long to wait). Error classification is the participant's responsibility.

### Retry Execution Logic (inside SagaEngine)

Uses **virtual threads** (Java 21+) to eliminate thread pool exhaustion during exponential backoff. Virtual threads are cheap to create and block without consuming OS threads, making `Thread.sleep()` in retry loops a non-issue even with many concurrent sagas.

```java
// Virtual thread executor — used for all step executions and retries.
// Virtual threads are cheap (no thread pool sizing needed) and non-blocking on sleep.
// Note: If a step directly calls a JDBC driver that uses `synchronized` internally
// (e.g., MySQL Connector/J < 9.0), the virtual thread may pin to its carrier thread.
// This is uncommon — steps typically call participants via HTTP/gRPC, not JDBC directly.
// If needed, use drivers with ReentrantLock-based implementations (MySQL 9.x+, PostgreSQL 42.7+).
private static final ExecutorService STEP_EXECUTOR =
    Executors.newVirtualThreadPerTaskExecutor();

private StepResult executeWithRetry(Step step, ExecutionContext ctx, RetryPolicy policy,
                                     long stepDeadline)
        throws StepExecutionException, StepTimeoutException {

    int attempt = 0;
    long interval = policy.getInitialIntervalMillis();

    while (true) {
        attempt++;

        // Always submit to virtual thread executor — provides a cancellation
        // handle (future.cancel(true)) and unified exception handling.
        Future<StepResult> future = STEP_EXECUTOR.submit(() -> step.execute(ctx));
        try {
            if (stepDeadline > 0) {
                long remaining = stepDeadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    future.cancel(true);
                    throw new StepTimeoutException("Step timeout exceeded");
                }
                return future.get(remaining, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new StepTimeoutException("Step timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new StepExecutionException("Step interrupted", e, false);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StepExecutionException se) {
                // Participant-thrown: check retry policy
                if (!se.isRetryable() || attempt >= policy.getMaxAttempts()) {
                    throw se;
                }
                interval = policy.sleepWithBackoff(interval);
            } else {
                // Unexpected error (NPE, ClassCast, etc.) — not retryable
                throw new StepExecutionException("Step failed", e.getCause(), false);
            }
        }
    }
}
```

No `isNonRetryable()` method needed — the participant's `StepExecutionException.isRetryable()` flag is the single source of truth.

## Persistence via ScalarDB

### What It Does

Stores saga state as **append-only events** using ScalarDB's transaction API. Each state change is a single INSERT into `saga_events`. A small mutable `saga_state` table provides fast status lookups and efficient recovery scanning.

**Why ScalarDB**: Database-agnostic ACID transactions — one persistence implementation works regardless of the user's database choice (Cassandra, DynamoDB, PostgreSQL, MySQL, Cosmos DB, etc.).

The critical features:
- **1 ScalarDB transaction with 1 INSERT per step** — append-only events, no read-modify-write
- **No per-step lease/ownership writes** — recovery uses periodic scan of `saga_state` (Seata-style)

### Where Saga State Lives

> See [Deployment Architecture](#deployment-architecture-cross-service-orchestration) in Part I for the full deployment model, participant relationship, and multi-replica HA diagrams.

### Schema (2 Tables)

```java
// --- persistence/SagaSchema.java ---
public class SagaSchema {
    public static final String NAMESPACE = "saga";

    /**
     * Table 1: saga_events — append-only event log.
     *
     * Partition key: saga_id (TEXT)
     * Clustering key: sequence (INT, ascending)
     *
     * Every state change is a single INSERT. No UPDATEs, no DELETEs.
     * ScalarDB's clustering key ensures efficient ordered scan by saga_id.
     */
    public static TableMetadata sagaEventsTable() {
        return TableMetadata.newBuilder()
            .addColumn("saga_id",    DataType.TEXT)     // PK
            .addColumn("sequence",   DataType.INT)      // CK: monotonically increasing per saga
            .addColumn("event_type", DataType.TEXT)      // SAGA_STARTED, STEP_COMPLETED, etc.
            .addColumn("step_index", DataType.INT)       // step index (for step events; -1 for saga events)
            .addColumn("step_name",  DataType.TEXT)      // step name (for step events; null for saga events)
            .addColumn("payload",    DataType.TEXT)      // JSON: step result, error, input, etc.
            .addColumn("created_at", DataType.TIMESTAMPTZ)
            .addPartitionKey("saga_id")
            .addClusteringKey("sequence", Scan.Ordering.Order.ASC)
            .build();
    }

    /**
     * Table 2: saga_state — mutable status/recovery table.
     *
     * Partition key: bucket (INT)  — hash(saga_id) % NUM_BUCKETS
     * Clustering key: (status (INT), updated_at (TIMESTAMPTZ), saga_id (TEXT))
     * Secondary index: saga_id
     *
     * One row per saga. Written on saga start and on each status transition.
     * Because status and updated_at are part of the clustering key (immutable),
     * status transitions require DELETE old row + INSERT new row in one transaction.
     *
     * Bucket-based partitioning distributes recovery scans across database
     * nodes — each bucket is a separate partition, avoiding hot-partition
     * problems that would occur if status alone were the partition key.
     *
     * Clustering key design enables efficient recovery scans:
     * scan each bucket with status=RUNNING and updated_at <= threshold,
     * reading only stale active sagas. Completed/compensated rows and
     * recently-updated active sagas are skipped at the storage layer.
     *
     * Used for:
     * - Recovery scan: for each bucket, scan status=RUNNING with updated_at <= threshold
     * - Fast status lookup: getStateSnapshot(sagaId) via secondary index on saga_id
     * - Admin API queries: list by status, count by name
     * - Conflict-based claiming: version column incremented on claim
     *
     * Terminal entries (COMPLETED, COMPENSATED, ESCALATED) can be
     * cleaned up after a configurable retention period.
     */
    public static final int NUM_BUCKETS = 16;  // configurable; 16 is a good default

    public static int bucketOf(String sagaId) {
        return (sagaId.hashCode() & 0x7FFFFFFF) % NUM_BUCKETS;
    }

    public static TableMetadata sagaStateTable() {
        return TableMetadata.newBuilder()
            .addColumn("bucket",       DataType.INT)      // PK: hash(saga_id) % NUM_BUCKETS
            .addColumn("status",       DataType.INT)      // CK1: SagaStatus ordinal
            .addColumn("updated_at",   DataType.TIMESTAMPTZ) // CK2: last state change time
            .addColumn("saga_id",      DataType.TEXT)      // CK3: unique identifier
            .addColumn("saga_name",    DataType.TEXT)
            .addColumn("owner_id",     DataType.TEXT)      // ID of the replica processing this saga (informational, for observability)
            .addColumn("version",      DataType.INT)       // incremented on each recovery claim (optimistic concurrency control)
            .addColumn("definition_version", DataType.TEXT) // saga definition version at creation
            .addColumn("created_at",   DataType.TIMESTAMPTZ)
            .addPartitionKey("bucket")
            .addClusteringKey("status", Scan.Ordering.Order.ASC)
            .addClusteringKey("updated_at", Scan.Ordering.Order.ASC)
            .addClusteringKey("saga_id", Scan.Ordering.Order.ASC)
            .addSecondaryIndex("saga_id")
            .build();
    }

    public static TableMetadata sagaDefinitionsTable() {
        return TableMetadata.newBuilder()
            .addColumn("saga_name",          DataType.TEXT)        // PK
            .addColumn("definition_version", DataType.TEXT)        // CK
            .addColumn("definition_json",    DataType.TEXT)        // full definition JSON
            .addColumn("registered_at",      DataType.TIMESTAMPTZ)
            .addPartitionKey("saga_name")
            .addClusteringKey("definition_version", Scan.Ordering.Order.ASC)
            .build();
    }

    /**
     * Create all saga tables using ScalarDB Admin API.
     */
    public static void createAll(Admin admin) throws ExecutionException {
        admin.createNamespace(NAMESPACE, true);
        admin.createTable(NAMESPACE, "saga_events",      sagaEventsTable(),      true);
        admin.createTable(NAMESPACE, "saga_state",      sagaStateTable(),       true);
        admin.createTable(NAMESPACE, "saga_definitions",  sagaDefinitionsTable(),  true);
    }
}
```

### Event Types

```java
// --- store/SagaEvent.java ---
@Immutable
public final class SagaEvent {
    private final String eventType;
    private final int stepIndex;            // -1 for saga-level events
    private final @Nullable String stepName;     // null for saga-level events
    private final @Nullable String payload;      // event-specific data (serialized JSON for step results, plain text for escalation reasons)
    private final @Nullable SagaStatus targetStatus;  // non-null for transition events, null for step-level events
    private final @Nullable Instant timestamp;   // set from saga_events.created_at when loaded from store

    // --- Saga lifecycle (6) ---
    // In-progress states use present participle; terminal states use past participle.
    public static final String SAGA_STARTED      = "SAGA_STARTED";
    public static final String SAGA_CONFIRMING   = "SAGA_CONFIRMING";   // TCC
    public static final String SAGA_COMPENSATING = "SAGA_COMPENSATING";
    public static final String SAGA_COMPLETED    = "SAGA_COMPLETED";
    public static final String SAGA_COMPENSATED  = "SAGA_COMPENSATED";
    public static final String SAGA_ESCALATED    = "SAGA_ESCALATED";

    // --- Step outcomes (4) ---
    public static final String STEP_COMPLETED           = "STEP_COMPLETED";
    public static final String STEP_FAILED              = "STEP_FAILED";
    public static final String STEP_COMPENSATED         = "STEP_COMPENSATED";
    public static final String STEP_COMPENSATION_FAILED = "STEP_COMPENSATION_FAILED";

    public @Nullable SagaStatus getTargetStatus() { return targetStatus; }
    public @Nullable Instant getTimestamp() { return timestamp; }

    // Step-level factory methods (no status transition)
    public static SagaEvent sagaStarted(@Nullable String payload) { ... }
    public static SagaEvent stepCompleted(int stepIndex, String stepName, @Nullable String payload) { ... }
    public static SagaEvent stepFailed(int stepIndex, String stepName, @Nullable String payload) { ... }
    public static SagaEvent stepCompensated(int stepIndex, String stepName) { ... }
    public static SagaEvent stepCompensationFailed(int stepIndex, String stepName, @Nullable String payload) { ... }

    // Saga-level factory methods (each carries its target SagaStatus)
    public static SagaEvent sagaCompensating() { ... }  // → COMPENSATING
    public static SagaEvent sagaConfirming() { ... }    // → CONFIRMING (TCC)
    public static SagaEvent sagaCompleted() { ... }     // → COMPLETED
    public static SagaEvent sagaCompensated() { ... }   // → COMPENSATED
    public static SagaEvent sagaEscalated(String reason) { ... }  // → ESCALATED

    // Package-private — used by store implementation to attach the persisted timestamp
    SagaEvent withTimestamp(Instant timestamp) { ... }

    // Validates that stepIndex >= 0; throws IllegalArgumentException for saga-level events
    private void validateStepIndex(int stepIndex) { ... }
}
```

### Write Amplification Comparison

```
Event stream for a 5-step saga (happy path):

  seq=0  SAGA_STARTED      {name: "transfer", input: {...}}     ← saga start
  seq=1  STEP_COMPLETED    {stepIndex: 0, stepName: "debit"}    ← 1 INSERT per step
  seq=2  STEP_COMPLETED    {stepIndex: 1, stepName: "credit"}
  seq=3  STEP_COMPLETED    {stepIndex: 2, stepName: "validate"}
  seq=4  STEP_COMPLETED    {stepIndex: 3, stepName: "notify"}
  seq=5  STEP_COMPLETED    {stepIndex: 4, stepName: "audit"}
  seq=6  SAGA_COMPLETED    {}                                    ← saga end

Transactions and operations:
  1. createSaga:       1 tx — 2 ops (1 INSERT event + 1 INSERT state)
  2. steps 0-4:        5 tx — 5 ops (1 INSERT event each)
  3. sagaCompleted:    1 tx — 4 ops (1 GET state + 1 INSERT event + 1 DELETE old state + 1 INSERT new state)

Total: 7 transactions, 11 operations
  - saga_events:  7 INSERTs
  - saga_state:  1 GET + 2 INSERTs + 1 DELETE

Note: registerDefinition (1 tx, 1 GET + 1 INSERT) is a one-time cost per definition version, not per saga.
```

| Framework | Transactions | Operations | Model |
|---|---|---|---|
| **This design** | 7 tx | 11 ops (7 events + 4 status) | Append-only events + mutable status |
| **Axon** | ~7 tx | ~7 ops | Append-only event store |
| **Eventuate** | ~7 tx | ~7 ops | Append-only events + CDC |
| **Temporal** | ~7 tx (batched) | ~7 ops | Append-only event history |
| **Seata** | ~7 tx | ~12 ops | Mutable rows (STARTED + COMPLETED) |
| **Previous design** | ~7 tx | ~17 ops | Mutable rows (UPSERT + UPDATE + INSERT) |

The 4 additional operations in this design maintain the `saga_state` table (1 GET existence check + 1 DELETE + 1 INSERT per transition), which enables efficient recovery scans (clustering key prefix scan by status) and prevents stale writes after recovery claiming, without requiring CDC infrastructure or eventual-consistency projections.

### SagaStore Interface

`SagaStore` is an interface, allowing alternative implementations (e.g., `JdbcSagaStore` in the future) and easy mocking in tests. The default implementation, `ScalarDbSagaStore`, uses ScalarDB's transaction API.

```java
// --- store/SagaStore.java ---
public interface SagaStore {
    // Saga lifecycle — writes to both saga_events and saga_state in ONE transaction.
    // Returns the full SagaStateSnapshot so the caller can use it without a read-back.
    // sagaId may be null (UUID generated) or a caller-supplied ID. On collision
    // with an existing saga, throws SagaAlreadyExistsException carrying the existing
    // snapshot (see "Client-Supplied Saga IDs" section).
    SagaStateSnapshot createSaga(@Nullable String sagaId, String sagaName, String ownerId,
                             Map<String, Object> input, String definitionVersion);

    // Definition persistence — called once per definition version at registration time.
    // GET-then-conditional-write: idempotent on restart, fail-fast on version conflict.
    void registerDefinition(SagaDefinition definition);
    Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion);

    // Append-only event write — 1 INSERT to saga_events (no status table update).
    // Accepts only step-level events (targetStatus == null). Throws IllegalArgumentException
    // if a saga-level event is passed — use recordTransition() for those.
    // Caller provides the sequence number (tracked in SagaContext).
    void appendEvent(String sagaId, int sequence, SagaEvent event);

    // Append event + transition saga_state status atomically in ONE transaction.
    // Accepts only saga-level events (targetStatus != null). Throws IllegalArgumentException
    // if a step-level event is passed — use appendEvent() for those.
    // The new status is derived from event.getTargetStatus().
    // Reads the current saga_state row first to verify it still exists at the
    // snapshot's CK (prevents stale writes after recovery claiming):
    // 1 GET (existence check) + 1 INSERT (event) + 1 DELETE (old row) + 1 INSERT (new row).
    // Throws SagaConcurrentModificationException if row not found.
    // Returns the post-transition snapshot.
    SagaStateSnapshot recordTransition(SagaStateSnapshot current, int sequence,
                                        SagaEvent event);

    // Recovery — finds sagas in RUNNING, CONFIRMING, or COMPENSATING status whose
    // updated_at is older than the threshold. Cursor-based pagination hides internal partitioning.
    Recoverables findRecoverable(long recoveryTimeoutMillis, @Nullable RecoverablesCursor cursor);
    Optional<SagaStateSnapshot> claimForRecovery(SagaStateSnapshot saga, String newOwnerId);
    void markForRecovery(String sagaId);  // sets updated_at to epoch 0 for immediate recovery
    List<SagaEvent> getEvents(String sagaId);
    int getEventCount(String sagaId);  // avoids materializing all events

    // Queries
    Optional<SagaStateSnapshot> getStateSnapshot(String sagaId);

    // Admin query methods — deferred to Admin API phase (Phase 5)
    // SagaPage<SagaStateSnapshot> listStateSnapshots(SagaQuery query);
    // Map<SagaStatus, Long> countByStatus();
    // Map<String, Long> countBySagaName();

    // Data retention — deletes all events and state for a terminal saga.
    // Only sagas in COMPLETED, COMPENSATED, or ESCALATED status may be deleted.
    void deleteSaga(String sagaId);

    // Nested types for cursor-based recovery pagination
    record Recoverables(List<SagaStateSnapshot> sagas, @Nullable RecoverablesCursor nextCursor) {}
    interface RecoverablesCursor {}
}
```

### ScalarDbSagaStore (Default Implementation)

```java
// --- store/ScalarDbSagaStore.java ---
public class ScalarDbSagaStore implements SagaStore {
    private final DistributedTransactionManager txManager;
    private final SagaSchema schema;
    private final ScalarDbSagaStoreConfig config;

    // --- Transaction execution helper ---

    /** Action to execute within a transaction. */
    @FunctionalInterface
    interface TransactionAction<T> {
        T execute(DistributedTransaction tx) throws Exception;
    }

    /** Verifier to check whether a transaction was committed after unknown status. */
    @FunctionalInterface
    interface CommitVerifier<T> {
        Optional<T> verify() throws Exception;
    }

    /**
     * Executes the given action in a ScalarDB transaction with retry logic.
     *
     * Retries on CrudConflictException and CommitConflictException (transient
     * conflicts) up to transactionRetryCount attempts with exponential backoff.
     *
     * On UnknownTransactionStatusException:
     * - If commitVerifier is non-null (write operations): uses the verifier
     *   to re-read and confirm whether the transaction was committed.
     *   The verifier has its own retry loop. Outcomes:
     *   (1) Verified committed → return the result
     *   (2) Verified not committed → break and retry the transaction
     *   (3) All verifier retries failed → throw immediately (ambiguous state)
     * - If commitVerifier is null: retry the whole transaction. Used for
     *   read-only operations and best-effort writes (e.g., markForRecovery).
     *
     * RuntimeExceptions from the action propagate as-is (business exceptions
     * like SagaConcurrentModificationException, SagaDefinitionException).
     * RuntimeExceptions from the verifier also propagate immediately, except
     * SagaPersistenceException (infrastructure failure from inner transactions)
     * which is retried.
     * Checked exceptions are wrapped in SagaPersistenceException.
     */
    <T> T runInTransaction(TransactionAction<T> action,
                           @Nullable CommitVerifier<T> commitVerifier,
                           String operationName) {
        return runInTransaction(action, commitVerifier, operationName, true);
    }

    /**
     * 4-arg overload: retryOnCommitConflict=false makes CommitConflictException
     * a permanent failure (used by createSaga to treat conflicts as duplicate IDs).
     */
    <T> T runInTransaction(TransactionAction<T> action,
                           @Nullable CommitVerifier<T> commitVerifier,
                           String operationName,
                           boolean retryOnCommitConflict) {
        int maxAttempts = config.getTransactionRetryCount();
        Exception lastException = null;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) sleepForRetry(attempt - 1);
            DistributedTransaction tx = null;
            try {
                tx = txManager.begin();
                T result = action.execute(tx);
                tx.commit();
                return result;
            } catch (UnknownTransactionStatusException e) {
                if (commitVerifier == null) {
                    lastException = e;
                    continue; // read-only — retry whole transaction
                }
                for (int v = 0; v < maxAttempts; v++) {
                    try {
                        Optional<T> verified = commitVerifier.verify();
                        if (verified.isPresent()) return verified.get();
                        break; // verified not committed — retry transaction
                    } catch (Exception ve) {
                        if (ve instanceof RuntimeException re
                                && !(ve instanceof SagaPersistenceException)) {
                            throw re;  // business-logic / programming errors propagate
                        }
                        e.addSuppressed(ve);
                        if (v < maxAttempts - 1) { sleepForRetry(v); continue; }
                        throw new SagaPersistenceException(
                            "Failed to " + operationName
                            + ": commit status unknown and verification failed", e);
                    }
                }
                lastException = e;
            } catch (CommitConflictException e) {
                abortQuietly(tx);
                if (!retryOnCommitConflict) {
                    throw new SagaPersistenceException("Failed to " + operationName, e);
                }
                lastException = e;
            } catch (CrudConflictException e) {
                abortQuietly(tx);
                lastException = e;
            } catch (Exception e) {
                abortQuietly(tx);
                if (e instanceof RuntimeException re) throw re;
                throw new SagaPersistenceException("Failed to " + operationName, e);
            }
        }
        throw new SagaPersistenceException(
            "Failed to " + operationName + " after " + maxAttempts + " attempts",
            lastException);
    }

    // --- Saga lifecycle ---

    /**
     * Create a new saga. Writes to both saga_events (SAGA_STARTED event)
     * and saga_state (RUNNING status) in one transaction.
     *
     * Duplicate IDs detected via ScalarDB Insert "fail if exists" semantics —
     * no extra read on the happy path.
     *
     * Uses runInTransaction with commitVerifier = loadFromDb(sagaId).
     * On CrudException after retries, checks if the saga already exists and
     * throws SagaAlreadyExistsException with a fresh snapshot.
     */
    public SagaStateSnapshot createSaga(@Nullable String sagaId, String sagaName,
                                         String ownerId, Map<String, Object> input,
                                         String definitionVersion) {
        // ... validate sagaId, prepare payload ...
        String id = sagaId;
        try {
            SagaStateSnapshot snapshot = runInTransaction(
                tx -> {
                    Instant now = Instant.now();
                    tx.insert(/* SAGA_STARTED event */);
                    tx.insert(/* saga_state row with RUNNING status */);
                    return new SagaStateSnapshot(id, sagaName, SagaStatus.RUNNING,
                        ownerId, 0, definitionVersion, now, now);
                },
                () -> loadFromDb(id),  // commitVerifier
                "create saga " + id);
            return snapshot;
        } catch (SagaPersistenceException e) {
            if (e.getCause() instanceof CrudException) {
                // Check for duplicate ID on (rare) collision path
                Optional<SagaStateSnapshot> existing = getStateSnapshot(id);
                if (existing.isPresent()) {
                    throw new SagaAlreadyExistsException(id, existing.get(), e);
                }
            }
            throw e;
        }
    }

    /**
     * Append a single step-level event to saga_events.
     * Hot-path write — 1 INSERT per step.
     *
     * Uses runInTransaction with commitVerifier that re-reads the event by key
     * to confirm whether the INSERT was committed.
     */
    public void appendEvent(String sagaId, int sequence, SagaEvent event) {
        // ... validate step-level event ...
        runInTransaction(
            tx -> {
                tx.insert(/* event INSERT */);
                return Boolean.TRUE;
            },
            () -> {
                // Verify by re-reading the event in a new transaction
                return runInTransaction(
                    tx -> {
                        Optional<Result> result = tx.get(/* GET by sagaId + sequence */);
                        return result.isPresent() ? Optional.of(Boolean.TRUE) : Optional.empty();
                    },
                    null,  // verifier is read-only
                    "verify event " + sagaId + " seq " + sequence);
            },
            "append event for saga " + sagaId);
    }

    /**
     * Atomically: append event + transition saga_state status in ONE transaction.
     *
     * Uses runInTransaction with commitVerifier = loadFromDb filtered by newStatus.
     * Throws SagaConcurrentModificationException if the row at the snapshot's CK
     * no longer exists (another replica claimed or transitioned it).
     */
    public SagaStateSnapshot recordTransition(SagaStateSnapshot current, int sequence,
                                               SagaEvent event) {
        // ... validate saga-level event ...
        String sagaId = current.getSagaId();
        SagaStatus newStatus = event.getTargetStatus();

        try {
            SagaStateSnapshot updated = runInTransaction(
                tx -> {
                    // 1. Verify row still exists at snapshot's CK
                    // 2. Append event
                    // 3. DELETE old row (old status + old updated_at in CK)
                    // 4. INSERT new row (new status + new updated_at in CK)
                    return current.withTransition(newStatus, now);
                },
                () -> {
                    Optional<SagaStateSnapshot> state = loadFromDb(sagaId);
                    if (state.isPresent() && state.get().getStatus() == newStatus) {
                        return state;
                    }
                    return Optional.empty();
                },
                "record transition for saga " + sagaId);
            return updated;
        } catch (SagaConcurrentModificationException e) {
            throw e;
        }
    }

    /**
     * Claim a saga for recovery: verify row at snapshot's CK, then DELETE old +
     * INSERT with updated owner_id and incremented version.
     *
     * Uses runInTransaction with commitVerifier = loadFromDb filtered by newOwnerId.
     * Throws SagaConcurrentModificationException if row not found (caught by
     * caller → returns Optional.empty()), enabling retry on CommitConflictException.
     */
    public Optional<SagaStateSnapshot> claimForRecovery(SagaStateSnapshot saga,
                                                         String newOwnerId) {
        String sagaId = saga.getSagaId();
        try {
            SagaStateSnapshot claimed = runInTransaction(
                tx -> {
                    // 1. Verify row at snapshot's CK → throw if empty
                    // 2. DELETE old row + INSERT with new owner and version+1
                    return new SagaStateSnapshot(/* claimed state */);
                },
                () -> {
                    Optional<SagaStateSnapshot> state = loadFromDb(sagaId);
                    if (state.isPresent() && newOwnerId.equals(state.get().getOwnerId())) {
                        return state;
                    }
                    return Optional.empty();
                },
                "claim saga " + sagaId + " for recovery");
            return Optional.of(claimed);
        } catch (SagaConcurrentModificationException e) {
            return Optional.empty();
        }
    }

    /**
     * Mark a saga for immediate recovery by setting updated_at to epoch 0.
     * Best-effort — conflict with executing thread is expected and harmless.
     *
     * Uses runInTransaction with null verifier (best-effort, no verification).
     * Outer catch swallows all exceptions.
     */
    public void markForRecovery(String sagaId) {
        try {
            runInTransaction(
                tx -> {
                    // 1. Read current state via secondary index
                    // 2. DELETE old row
                    // 3. INSERT with updated_at = EPOCH
                    return Boolean.TRUE;
                },
                null,  // best-effort — no verifier
                "mark for recovery " + sagaId);
        } catch (Exception e) {
            logger.warn("markForRecovery failed for saga {} (best-effort)", sagaId, e);
        }
    }

    /**
     * Read-only methods (getDefinition, getEvents, getEventCount, findRecoverable,
     * findByStatusOlderThan, loadFromDb) all pass null as commitVerifier.
     * On UTSE, the entire transaction is simply retried because read results
     * are only valid after a successful commit.
     */
    public Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion) {
        return runInTransaction(
            tx -> {
                Optional<Result> result = tx.get(/* definition lookup */);
                return result.map(r -> definitionSerializer.deserialize(r.getText("definition_json")));
            },
            null,  // read-only — retry whole transaction on UTSE
            "get definition " + sagaName + " " + definitionVersion);
    }

    public List<SagaEvent> getEvents(String sagaId) {
        return runInTransaction(
            tx -> {
                List<Result> results = tx.scan(/* all events for sagaId */);
                return results.stream().map(this::toSagaEvent).toList();
            },
            null,
            "get events for saga " + sagaId);
    }

    private Optional<SagaStateSnapshot> loadFromDb(String sagaId) {
        return runInTransaction(
            tx -> {
                List<Result> results = tx.scan(/* secondary index on saga_id */);
                return results.stream().findFirst().map(this::toSagaStateSnapshot);
            },
            null,
            "load saga state " + sagaId);
    }

    public void deleteSaga(String sagaId) {
        runInTransaction(
            tx -> {
                // 1. Scan saga_state by index, verify terminal status
                // 2. DELETE state row
                // 3. Scan and DELETE all event rows
                return Boolean.TRUE;
            },
            () -> {
                // Verify deletion: state row should be absent
                Optional<SagaStateSnapshot> state = loadFromDb(sagaId);
                return state.isEmpty() ? Optional.of(Boolean.TRUE) : Optional.empty();
            },
            "delete saga " + sagaId);
    }

    // --- Helpers ---

    private void sleepForRetry(int retryIndex) {
        long delay = Math.min(100L * (1L << retryIndex), 5000L);  // 100, 200, 400, ..., max 5s
        Thread.sleep(delay);
    }

    private void abortQuietly(@Nullable DistributedTransaction tx) {
        if (tx != null) {
            try { tx.abort(); } catch (AbortException ignored) {}
        }
    }
}
```

**Duplicate detection cost profile.** The collision-detection logic in `createSaga()` preserves the pure-write happy path:

- **Happy path (no collision):** 2 writes, 0 reads — unchanged from the original design. Duplicate detection is free because ScalarDB's `Insert` operation already fails atomically if the record exists.
- **Collision path (rare — only on actual duplicates):** 2 attempted writes (one fails) + 1 follow-up read in a fresh transaction to populate the existing snapshot attached to `SagaAlreadyExistsException`.

This assumes the configured ScalarDB storage backend honors `Insert` "fail if exists" semantics across all supported backends (Cassandra, DynamoDB, JDBC, Cosmos DB). The follow-up read on the collision path runs in a separate transaction, so the attached snapshot may be slightly newer than what triggered the collision — that is fine, since the caller wants "tell me what's there now" semantics for retry decisions.

## Crash Recovery

### What It Does

On application startup, scans `saga_state` for sagas stuck in `RUNNING` or `COMPENSATING` status with stale `updated_at` (meaning the process crashed mid-execution). Replays events from `saga_events` to reconstruct state, then resumes execution.

### Recovery Logic

```java
// --- recovery/RecoveryConfig.java ---
record RecoveryConfig(long recoveryTimeoutMillis, long recoveryIntervalSeconds,
                      Duration compensationGracePeriod, Clock clock) {}
```

```java
// --- recovery/SagaRecoveryManager.java ---
public class SagaRecoveryManager {
    private final SagaStore store;
    private final SagaEngine engine;
    private final SagaDefinitionRegistry registry;
    private final String ownerId;
    private final long recoveryTimeoutMillis;
    private final ScheduledExecutorService scheduler;
    private final long recoveryIntervalSeconds;
    private final Duration compensationGracePeriod;
    private final Clock clock;

    public SagaRecoveryManager(SagaStore store, SagaEngine engine,
                                SagaDefinitionRegistry registry,
                                String ownerId, RecoveryConfig config) {
        this.store = store;
        this.engine = engine;
        this.registry = registry;
        this.ownerId = ownerId;
        this.recoveryTimeoutMillis = config.recoveryTimeoutMillis();
        this.recoveryIntervalSeconds = config.recoveryIntervalSeconds();
        this.compensationGracePeriod = config.compensationGracePeriod();
        this.clock = config.clock();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "saga-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start periodic recovery scanning.
     * Runs once immediately (startup recovery), then periodically.
     * Finds stale RUNNING sagas in saga_state and resumes them.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
            this::recover, 0, recoveryIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stop the recovery scheduler (for graceful shutdown).
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * Single recovery pass: scan each bucket independently for stale sagas,
     * claim via transactional write, replay events, and resume.
     * Per-bucket transactions avoid a single long transaction spanning all buckets.
     */
    void recover() {
        RecoverablesCursor cursor = null;
        do {
            SagaStore.Recoverables page = store.findRecoverable(recoveryTimeoutMillis, cursor);
            cursor = page.nextCursor();

            for (SagaStateSnapshot saga : page.sagas()) {
                try {
                    // Claim via read + DELETE + INSERT in one transaction.
                    // If another instance claims it concurrently, returns empty.
                    Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, ownerId);
                    if (claimed.isEmpty()) {
                        continue;
                    }
                    recoverOne(claimed.get());
                } catch (Exception e) {
                    // Log and continue — don't let one stuck saga block others
                    logger.error("Failed to recover saga {}", saga.getSagaId(), e);
                }
            }
        } while (cursor != null);
    }

    private void recoverOne(SagaStateSnapshot saga) {
        // Look up the EXACT definition version the saga was started with.
        // Using a different version during recovery would corrupt business data
        // if steps were added, removed, or reordered.
        SagaDefinition def = registry.resolve(
            saga.getSagaName(), saga.getDefinitionVersion());
        if (def == null) {
            // Escalate — recovering with a wrong definition version would corrupt data.
            // Admin must re-register the required version or manually resolve.
            logger.error("Definition not found for saga: {} version: {} — escalating",
                saga.getSagaName(), saga.getDefinitionVersion());
            List<SagaEvent> events = store.getEvents(saga.getSagaId());
            ExecutionContext context = engine.replayEvents(saga, events);
            store.recordTransition(context.getCurrentState(),
                context.nextSequence(),
                SagaEvent.sagaEscalated("definition version "
                    + saga.getDefinitionVersion() + " not found"));
            return;
        }

        // Replay events to reconstruct state
        List<SagaEvent> events = store.getEvents(saga.getSagaId());
        ExecutionContext context = engine.replayEvents(saga, events);
        SagaStatus status = context.getCurrentState().getStatus();

        if (status == SagaStatus.COMPENSATING) {
            // Was mid-compensation — resume from where compensation left off.
            // Check if compensation has been stuck longer than the grace period.
            if (isStuckLongerThanGracePeriod(saga, events, SagaEvent.STEP_COMPENSATION_FAILED)) {
                store.recordTransition(context.getCurrentState(),
                    context.nextSequence(),
                    SagaEvent.sagaEscalated("compensation stuck for over " + compensationGracePeriod));
                context.advanceSequence();
                return;
            }
            int lastCompensated = events.stream()
                .filter(e -> e.getEventType().equals(SagaEvent.STEP_COMPENSATED))
                .mapToInt(SagaEvent::getStepIndex)
                .min().orElse(Integer.MAX_VALUE);
            engine.compensateFrom(def, context, lastCompensated - 1);

        } else {
            // RUNNING or CONFIRMING — resume forward execution.
            // Both are handled identically: resume from the next step after
            // the last completed one. The expanded plan's pivot index ensures
            // the unified loop applies the correct failure handling.
            //
            // Escalation check: if STEP_FAILED events exist and the first one
            // is older than the grace period, escalate. Sagas with no STEP_FAILED
            // events (crash recovery) skip escalation and proceed to resume.
            if (isStuckLongerThanGracePeriod(saga, events, SagaEvent.STEP_FAILED)) {
                store.recordTransition(context.getCurrentState(),
                    context.nextSequence(),
                    SagaEvent.sagaEscalated("step retry stuck for over " + compensationGracePeriod));
                context.advanceSequence();
                return;
            }
            int lastCompleted = events.stream()
                .filter(e -> e.getEventType().equals(SagaEvent.STEP_COMPLETED))
                .mapToInt(SagaEvent::getStepIndex)
                .max().orElse(-1);
            engine.resumeFrom(def, context, lastCompleted + 1);
        }
    }

    /**
     * Check if a saga has been stuck longer than the grace period by examining
     * step-level failure events (STEP_FAILED, STEP_COMPENSATION_FAILED).
     * Used by all recovery branches (COMPENSATING, RUNNING, CONFIRMING) to decide
     * when to escalate. Returns false if no matching failure events exist
     * (e.g., crash recovery where the saga was interrupted, not failed).
     */
    // Uses Clock (injected via constructor) for testability.
    private boolean isStuckLongerThanGracePeriod(SagaStateSnapshot saga,
            List<SagaEvent> events, String failureEventType) {
        Optional<Instant> firstFailure = events.stream()
            .filter(e -> e.getEventType().equals(failureEventType))
            .map(SagaEvent::getTimestamp)
            .findFirst();
        if (firstFailure.isEmpty()) {
            return false;  // no failure events — crash recovery, not stuck
        }
        return Duration.between(firstFailure.get(), clock.instant())
            .compareTo(compensationGracePeriod) > 0;
    }

```

**Recovery behavior by recovery strategy and status:**

| Recovery Strategy | Status at Recovery | Recovery Action | Escalation |
|---|---|---|---|
| **BACKWARD** | `RUNNING` | Resume from last completed + 1. On failure, compensate backward. | Escalate if stuck > grace period |
| **BACKWARD** | `COMPENSATING` | Resume compensation from last compensated - 1. | Escalate if stuck > grace period |
| **FORWARD** | `RUNNING` | Resume from last completed + 1. On failure, stay `RUNNING` for retry. | Escalate if stuck > grace period |
| **MIXED** | `RUNNING` | Resume from last completed + 1. Failure handling depends on position relative to pivot. | Escalate if stuck > grace period |
| **MIXED** | `COMPENSATING` | Resume compensation (step failed at or before pivot). | Escalate if stuck > grace period |
| **TCC** | `RUNNING` | Resume Try. On failure, Cancel all completed. | Escalate if stuck > grace period |
| **TCC** | `CONFIRMING` | Resume Confirm. Retry aggressively. | Escalate if stuck > grace period |
| **TCC** | `COMPENSATING` | Resume Cancel. | Escalate if stuck > grace period |

All in-progress statuses (`RUNNING`, `CONFIRMING`, `COMPENSATING`) follow the same two-phase retry pattern: immediate retry in the engine → periodic recovery retry → time-based escalation after the grace period. Escalation checks use step-level failure events (`STEP_FAILED`, `STEP_COMPENSATION_FAILED`) — sagas with no failure events (crash recovery) skip the escalation check and proceed directly to resume.

```java
}
```

## Timeout Management

### What It Does

Enforces per-step and per-saga timeouts to prevent indefinite hangs. A step that exceeds its timeout is interrupted and triggers compensation. A saga that exceeds its total timeout triggers compensation from the last completed step.

Without timeouts, a hanging step runs indefinitely — no other replica can know if it's stuck or just slow. Every competitor has timeouts in v1 (Temporal: `startToCloseTimeout`, Seata: `Timeout` property, MicroTx: `@LRA(timeLimit = ...)`).

### Timeout Configuration

```json
{
  "name": "MoneyTransfer",
  "version": "1.0",
  "timeoutMillis": 300000,
  "steps": [
    {
      "name": "debit",
      "timeoutMillis": 60000,
      "stepClass": "com.example.DebitAccountStep"
    },
    {
      "name": "credit",
      "timeoutMillis": 30000,
      "stepClass": "com.example.CreditAccountStep"
    }
  ]
}
```

- **`timeoutMillis` (saga-level)**: Total time allowed for the entire saga. Stored as `deadline` in the `SAGA_STARTED` event payload at creation time.
- **`timeoutMillis` (step-level)**: Maximum time for a single step execution (including all retry attempts). If exceeded, the step is interrupted and compensation begins.

### Timeout Enforcement in executeSagaSteps()

Timeout enforcement is integrated directly into the canonical `executeSagaSteps()` loop (see [Core Engine](#sagaengine-core-execution-logic)). The loop computes `sagaDeadline` via `calculateSagaDeadline()` at entry, checks it via `isSagaTimedOut()` before each step, and passes a per-step `stepDeadline` (the minimum of step timeout and saga deadline) to `executeWithRetry()`. `SagaTimeoutException` and `StepTimeoutException` are caught by the existing pivot-based failure handling — they trigger compensation (at or before pivot) or `STEP_FAILED` (after pivot), same as any other step failure. No separate timeout catch block needed.

All three helpers live in the `engine` package (alongside `SagaEngine`) and accept an explicit `nowMillis` parameter instead of calling `System.currentTimeMillis()` internally. This makes them pure functions that are trivial to unit-test without mocking the clock.

```java
// --- engine/TimeoutPolicy.java (package-private) ---

/** Returns the saga deadline epoch-ms, or 0 if no saga-level timeout is configured. */
static long calculateSagaDeadline(long sagaTimeoutMillis, long nowMillis) {
    return sagaTimeoutMillis > 0 ? nowMillis + sagaTimeoutMillis : 0;
}

/** Returns true when the saga deadline has been reached. Always false when sagaDeadline == 0. */
static boolean isSagaTimedOut(long sagaDeadline, long nowMillis) {
    return sagaDeadline > 0 && nowMillis > sagaDeadline;
}

/**
 * Returns the effective step deadline epoch-ms: the minimum of the step-level deadline
 * and the saga deadline. 0 means no timeout for that dimension; the tighter bound wins.
 */
static long calculateStepDeadline(long stepTimeoutMillis, long sagaDeadline, long nowMillis) {
    if (stepTimeoutMillis > 0 && sagaDeadline > 0) {
        return Math.min(nowMillis + stepTimeoutMillis, sagaDeadline);
    } else if (stepTimeoutMillis > 0) {
        return nowMillis + stepTimeoutMillis;
    } else {
        return sagaDeadline;  // 0 means no timeout
    }
}
```

### Exception Hierarchy

```java
// --- exception/StepTimeoutException.java ---
public class StepTimeoutException extends StepExecutionException {
    public StepTimeoutException(String message) { super(message, false); }  // never retryable
    public StepTimeoutException(String message, Throwable cause) { super(message, cause, false); }
}

// --- exception/SagaTimeoutException.java ---
public class SagaTimeoutException extends RuntimeException {
    public SagaTimeoutException(String message) { super(message); }
    public SagaTimeoutException(String message, Throwable cause) { super(message, cause); }
}

// --- exception/SagaAlreadyExistsException.java ---
// Thrown by SagaStore.createSaga() when a caller-supplied sagaId collides
// with an existing saga. Carries a fresh snapshot of the existing saga so
// the caller can inspect its status and decide whether to resume, query,
// or treat the retry as a no-op. See "Client-Supplied Saga IDs" section.
public class SagaAlreadyExistsException extends RuntimeException {
    private final String sagaId;
    private final SagaStateSnapshot existing;  // non-null; required snapshot of the conflicting saga

    public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing) {
        super("Saga already exists: " + sagaId);
        this.sagaId = Objects.requireNonNull(sagaId, "sagaId");
        this.existing = Objects.requireNonNull(existing, "existing");
    }

    public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing,
                                       Throwable cause) {
        super("Saga already exists: " + sagaId, cause);
        this.sagaId = Objects.requireNonNull(sagaId, "sagaId");
        this.existing = Objects.requireNonNull(existing, "existing");
    }

    public String getSagaId() { return sagaId; }
    public SagaStateSnapshot getExisting() { return existing; }
}

// --- exception/SagaPersistenceException.java ---
public class SagaPersistenceException extends RuntimeException {
    public SagaPersistenceException(String message, Throwable cause) { super(message, cause); }
}

// --- exception/SagaDefinitionException.java ---
public class SagaDefinitionException extends RuntimeException {
    public SagaDefinitionException(String message) { super(message); }
    public SagaDefinitionException(String message, Throwable cause) { super(message, cause); }
}

// --- exception/SagaDefinitionNotFoundException.java ---
public class SagaDefinitionNotFoundException extends RuntimeException {
    private final String sagaName;

    public SagaDefinitionNotFoundException(String sagaName) {
        super("No saga definition registered for: " + sagaName);
        this.sagaName = sagaName;
    }

    public String getSagaName() { return sagaName; }
}

// --- exception/SagaNotFoundException.java ---
public class SagaNotFoundException extends RuntimeException {
    private final String sagaId;

    public SagaNotFoundException(String sagaId) {
        super("Saga not found: " + sagaId);
        this.sagaId = sagaId;
    }

    public String getSagaId() { return sagaId; }
}

// --- exception/SagaConcurrentModificationException.java ---
public class SagaConcurrentModificationException extends RuntimeException {
    private final String sagaId;

    public SagaConcurrentModificationException(String sagaId) {
        super("Saga is being processed by another replica: " + sagaId);
        this.sagaId = sagaId;
    }

    public SagaConcurrentModificationException(String sagaId, Throwable cause) {
        super("Saga is being processed by another replica: " + sagaId, cause);
        this.sagaId = sagaId;
    }

    public String getSagaId() { return sagaId; }
}

// --- exception/StepCompensationException.java ---
public class StepCompensationException extends RuntimeException {
    @Nullable private final String stepName;
    private final int stepIndex;

    public StepCompensationException(String message) {
        super(message);
        this.stepName = null;
        this.stepIndex = -1;
    }

    public StepCompensationException(Throwable cause) {
        super(cause);
        this.stepName = null;
        this.stepIndex = -1;
    }

    public StepCompensationException(String stepName, int stepIndex, Throwable cause) {
        super("Compensation failed for step '" + stepName + "' at index " + stepIndex, cause);
        if (stepIndex < 0) throw new IllegalArgumentException("stepIndex must be >= 0: " + stepIndex);
        this.stepName = stepName;
        this.stepIndex = stepIndex;
    }

    @Nullable public String getStepName() { return stepName; }
    public int getStepIndex() { return stepIndex; }
}
```

### Interaction with Recovery

When a step times out:
1. The virtual thread running `step.execute()` is interrupted via `Future.cancel(true)`
2. Compensation begins immediately — the current replica handles it
3. Other replicas will not pick up this saga because the current replica is still active and updating `saga_state` on status changes

## Graceful Shutdown

### What It Does

When the process shuts down (e.g., Kubernetes pod termination, deployment rolling update), the engine:

1. Stops accepting new sagas
2. Waits for in-flight sagas according to the configured shutdown mode
3. Marks incomplete sagas in `saga_state` for immediate recovery (sets `updated_at = 0`)
4. Stops the recovery scheduler

#### Shutdown Modes

| Mode | Behavior | Default timeout | Best for |
|---|---|---|---|
| `WAIT_CURRENT_STEP` (default) | Complete the in-flight step, then stop between steps | 30s | Kubernetes rolling updates, fast restarts |
| `WAIT_ALL_SAGAS` | Wait for all in-flight sagas to fully complete | 300s | Planned maintenance, zero-recovery shutdown |

With `WAIT_CURRENT_STEP`, incomplete sagas are marked for immediate recovery and resumed by another replica. No step is interrupted mid-execution, so no partial side effects.

With `WAIT_ALL_SAGAS`, the engine waits for every saga to reach a terminal state. If the timeout expires, remaining sagas fall back to the `WAIT_CURRENT_STEP` behavior (mark for recovery).

Without graceful shutdown, the recovery timeout (60s default) means a recovery delay of up to 60s, and the interrupted step may leave partial side effects.

### SagaEngine.shutdown()

```java
// --- engine/SagaEngine.java ---
public class SagaEngine implements AutoCloseable {
    private volatile boolean shuttingDown = false;
    private final Object shutdownLock = new Object();
    private final Set<String> activeSagas = ConcurrentHashMap.newKeySet();
    private final ShutdownMode shutdownMode;   // default: WAIT_CURRENT_STEP
    private final long shutdownTimeoutMillis;

    public enum ShutdownMode { WAIT_CURRENT_STEP, WAIT_ALL_SAGAS }

    public SagaEngine(SagaStore store, ShutdownMode shutdownMode,
                      Long shutdownTimeoutMillis, ...) {
        this.shutdownMode = shutdownMode != null ? shutdownMode : ShutdownMode.WAIT_CURRENT_STEP;
        this.shutdownTimeoutMillis = shutdownTimeoutMillis != null
            ? shutdownTimeoutMillis
            : (this.shutdownMode == ShutdownMode.WAIT_ALL_SAGAS ? 300_000L : 30_000L);
    }

    /**
     * Register a saga as active for shutdown tracking.
     * Returns false if the engine is shutting down (saga should not execute).
     * Synchronized with shutdown() to prevent the race where a saga starts
     * executing after shutdown sees an empty activeSagas set.
     */
    private boolean registerActive(String sagaId) {
        synchronized (shutdownLock) {
            if (shuttingDown) {
                return false;
            }
            activeSagas.add(sagaId);
            return true;
        }
    }

    private void unregisterActive(String sagaId) {
        activeSagas.remove(sagaId);
    }

    /**
     * Initiate graceful shutdown.
     */
    public void shutdown() {
        // Set flag under lock — after this, no new sagas can register
        synchronized (shutdownLock) {
            shuttingDown = true;
        }

        // activeSagas is now frozen (no new additions possible).
        // Wait for in-flight sagas to drain.
        long deadline = System.currentTimeMillis() + shutdownTimeoutMillis;
        while (!activeSagas.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;  // proceed to mark remaining sagas for recovery
            }
        }

        // Mark remaining active sagas for immediate recovery
        //    (update saga_state.updated_at to epoch 0 so recovery picks them up)
        for (String sagaId : activeSagas) {
            try {
                store.markForRecovery(sagaId);  // sets updated_at = 0
            } catch (Exception e) {
                logger.warn("Failed to mark saga {} for recovery", sagaId, e);
            }
        }

        // Stop recovery scheduler
        recoveryManager.stop();
    }

    /**
     * Check if the engine should stop between steps.
     * In WAIT_CURRENT_STEP mode, returns true after the current step completes.
     * In WAIT_ALL_SAGAS mode, returns false — sagas continue to completion.
     */
    private boolean shouldStopBetweenSteps() {
        return shuttingDown && shutdownMode == ShutdownMode.WAIT_CURRENT_STEP;
    }

    @Override
    public void close() { shutdown(); }
}
```

### Integration with JVM Shutdown

```java
// Automatic cleanup via shutdown hook
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    sagaManager.close();
}));

// Or via Spring @PreDestroy / Quarkus @Shutdown
@PreDestroy
void onShutdown() {
    sagaManager.close();
}
```

## Data Retention & Periodic Cleanup

### What It Does

Terminal sagas (COMPLETED, COMPENSATED) remain in `saga_state` and `saga_events` after completion. This allows callers to query saga outcomes (especially important for async and crash recovery scenarios). Over time, these rows accumulate and must be purged.

The retention manager periodically scans `saga_state` for successfully resolved entries (COMPLETED, COMPENSATED) whose `updated_at` is older than a configurable retention period, then deletes both the `saga_state` row and all `saga_events` rows for each expired saga in a single transaction.

ESCALATED sagas are excluded from automatic cleanup — they represent unresolved failures that require manual intervention. Admins delete them explicitly via `deleteSaga()` through the Admin API after investigation.

### Why Not Immediate Deletion?

- **Caller visibility:** Async callers and crash-recovered processes need to query saga status after completion. If the saga and events are deleted immediately on terminal transition, these callers have no way to determine the outcome.
- **OTel is not a database:** OpenTelemetry traces capture execution data during runtime, but querying OTel for saga status is not practical for programmatic lookups.
- **Cleanup efficiency:** `saga_state` uses `status` as the first clustering key, so scanning terminal entries is a CK prefix scan — efficient even with millions of rows. Scanning `saga_events` directly (without `saga_state` as the index) would require scanning the entire table, which ScalarDB is not optimized for.

### Retention Configuration

```java
// --- retention/RetentionConfig.java ---
record RetentionConfig(Duration retentionPeriod, long cleanupIntervalSeconds,
                       int batchSize, Clock clock) {

    /** Default: 7-day retention, cleanup every 60 minutes, 100 sagas per batch. */
    static RetentionConfig defaults() {
        return new RetentionConfig(Duration.ofDays(7), 3600, 100, Clock.systemUTC());
    }
}
```

| Parameter | Default | Description |
|---|---|---|
| `retentionPeriod` | 7 days | How long terminal sagas are retained before purging |
| `cleanupIntervalSeconds` | 3600 (1h) | How often the cleanup job runs |
| `batchSize` | 100 | Max sagas purged per cleanup pass (limits transaction size) |

### Cleanup Logic

```java
// --- retention/SagaRetentionManager.java ---
public class SagaRetentionManager {
    private final ScalarDbSagaStore store;  // package-private access for findByStatusOlderThan
    private final RetentionConfig config;
    private final ScheduledExecutorService scheduler;

    public SagaRetentionManager(ScalarDbSagaStore store, RetentionConfig config) {
        this.store = store;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "saga-retention");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
            this::cleanup, config.cleanupIntervalSeconds(),
            config.cleanupIntervalSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
    }

    /**
     * Single cleanup pass: for each bucket, scan resolved statuses
     * (COMPLETED, COMPENSATED) for entries older than the retention
     * period, then delete both saga_state and saga_events for each
     * expired saga. ESCALATED sagas are excluded — they require
     * manual intervention and explicit admin deletion.
     *
     * Uses CK prefix scan per (bucket, status) — reads only matching
     * entries with ascending updated_at, stopping as soon as entries
     * are newer than the threshold. This is efficient because the
     * clustering key orders by (status, updated_at, saga_id).
     */
    void cleanup() {
        Instant threshold = config.clock().instant().minus(config.retentionPeriod());
        int purged = 0;

        for (int bucket = 0; bucket < SagaSchema.NUM_BUCKETS; bucket++) {
            for (SagaStatus status : Arrays.stream(SagaStatus.values())
                    .filter(SagaStatus::isPurgeable).toList()) {
                purged += purgeTerminalEntries(bucket, status, threshold);
                if (purged >= config.batchSize()) {
                    return;  // batch limit reached — continue in next pass
                }
            }
        }
    }

    /**
     * Scan one (bucket, status) partition for entries with updated_at <= threshold.
     * For each match, call store.deleteSaga(sagaId) which deletes both the
     * saga_state row and all saga_events rows in a single transaction.
     *
     * Uses ScalarDbSagaStore.findByStatusOlderThan (package-private) — not
     * part of the SagaStore interface. Admin query methods will be added in
     * a later phase; the retention manager can switch to those when available.
     */
    private int purgeTerminalEntries(int bucket, SagaStatus status, Instant threshold) {
        int purged = 0;
        List<SagaStateSnapshot> expired = store.findByStatusOlderThan(
            bucket, status, threshold, config.batchSize());
        for (SagaStateSnapshot saga : expired) {
            try {
                store.deleteSaga(saga.getSagaId());
                purged++;
            } catch (Exception e) {
                // Log and continue — one failed purge shouldn't block others
                logger.warn("Failed to purge saga {}", saga.getSagaId(), e);
            }
        }
        return purged;
    }
}
```

**Scan efficiency:** The cleanup scan uses CK prefix `(bucket, status)` with `updated_at ASC` ordering. For a bucket with 1,000 terminal entries and only 10 older than the threshold, the scan reads just those 10 rows — the storage engine stops at the first row where `updated_at > threshold`. This is the key advantage of keeping `status` in the clustering key.

**`deleteSaga()` transaction:** Each `deleteSaga(sagaId)` call runs a single transaction that:
1. Reads the `saga_state` row via the secondary index on `saga_id` (to get the full CK)
2. Deletes the `saga_state` row
3. Scans and deletes all `saga_events` rows for the saga (partitioned by `saga_id`, so this is a single-partition scan)

**Batch limiting:** The `batchSize` parameter limits how many sagas are purged per cleanup pass. This prevents long-running cleanup jobs from competing with normal saga execution. With 100 sagas per batch and hourly cleanup, the system purges up to 2,400 sagas/day — sufficient for most workloads. Increase `batchSize` or decrease `cleanupIntervalSeconds` for busier systems.

### Lifecycle Integration

The retention manager is started and stopped alongside the recovery manager:

```java
// In SagaManager (bootstrap):
SagaRetentionManager retentionManager = new SagaRetentionManager(store, retentionConfig);
retentionManager.start();

// On shutdown:
retentionManager.stop();
recoveryManager.stop();
```

## Async Step Completion (Daemon Mode Only)

### What It Does

Supports steps that complete asynchronously — the step initiates work and returns immediately, but the result arrives later via an external callback. The engine parks the saga (persists `WAITING` status, releases the thread) and resumes when the callback arrives.

**This feature is available only in daemon mode.** The daemon is a server that can receive callback requests (HTTP or gRPC) from participant services. In embedded library mode, the library cannot receive inbound requests on its own. Instead, embedded mode relies on virtual threads — even slow services (seconds to minutes) simply block the virtual thread, which is cheap. Most other saga frameworks (Temporal, Seata, Narayana LRA, MicroTx) also require a standalone coordinator for async callbacks.

### Sequence

```
Daemon (Orchestrator)                     Participant
    │                                         │
    ├── request (sagaId, stepName, callbackInfo) ──►│
    │◄── accepted ────────────────────────────────│
    │                                         │
    │   Engine parks saga (WAITING)           │  [processes async]
    │                                         │
    │◄── callback: completeStep(sagaId, stepName, output) ─│
    │                                         │
    │   Engine resumes saga from next step    │
```

The request and callback can use any transport:
- **HTTP**: `POST /debit` → `202 Accepted`, then participant POSTs to the daemon's callback URL
- **gRPC**: unary RPC returns an operation ID, then participant calls the daemon's gRPC callback service

### Step Implementation

```java
public class AsyncDebitStep implements Step {
    private final HttpClient httpClient;      // or gRPC stub
    private final String serviceUrl;
    private final String callbackBaseUrl;     // daemon's own URL

    @Override
    public String getName() { return "debit"; }

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl + "/debit"))
                .header("Content-Type", "application/json")
                .header("X-Saga-Id", ctx.getSagaId())
                .header("X-Saga-Step", "debit")
                .header("X-Callback-Url",
                    callbackBaseUrl + "/api/sagas/" + ctx.getSagaId()
                    + "/steps/debit/complete")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(Map.of(
                    "accountId", ctx.get("accountId"),
                    "amount", ctx.get("amount")))))
                .build();

            HttpResponse<String> resp = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() == 202) {
                // Participant accepted — will call back when done
                return StepResult.pending();
            } else if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                // Synchronous completion
                return StepResult.of(fromJson(resp.body()));
            } else {
                throw new StepExecutionException("HTTP " + resp.statusCode());
            }

        } catch (IOException | InterruptedException e) {
            throw new StepExecutionException(e);
        }
    }

    @Override
    public void compensate(SagaContext ctx) throws StepCompensationException {
        // Compensation is typically synchronous (POST /debit/compensate)
    }
}
```

### Engine Handling of Pending Steps

```java
// In SagaEngine.executeSteps() — daemon mode only:
StepResult result = executeWithRetry(step, context, policy, stepDeadline);

if (result.isPending()) {
    // Park the saga — release the thread without appending an event.
    // The saga stays RUNNING with step i as the next to execute
    // (lastCompleted + 1). The saga will be resumed either by:
    //   1. completeStep() when the callback arrives, or
    //   2. Recovery, which re-dispatches the step (participant is idempotent).
    return store.getStateSnapshot(sagaId).orElseThrow();
}

// Synchronous completion — continue as before
store.appendEvent(sagaId, context.nextSequence(),
    SagaEvent.stepCompleted(i, stepDef.getName(), result));
context.advanceSequence();
context.merge(result);
completedIndex = i;
```

In embedded mode, `StepResult.pending()` is not supported — steps always block until completion.

### Callback API

The daemon exposes callback endpoints for participant services to report async step completion:

```java
// --- api/SagaManager.java --- (daemon mode only)
/**
 * Complete an async step via external callback.
 * Resumes the parked saga from the next step.
 */
SagaStateSnapshot completeStep(String sagaId, String stepName, Map<String, Object> output);
```

```java
// --- engine/EmbeddedSagaManager.java ---
@Override
public SagaStateSnapshot completeStep(String sagaId, String stepName,
                                  Map<String, Object> output) {
    SagaStateSnapshot saga = store.getStateSnapshot(sagaId)
        .orElseThrow(() -> new SagaNotFoundException(sagaId));
    if (saga.getStatus() != SagaStatus.RUNNING) {
        return saga;  // saga already moved on (compensating, escalated, etc.) — ignore late callback
    }

    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    List<SagaEvent> events = store.getEvents(sagaId);
    ExecutionContext context = engine.replayEvents(saga, events);

    // Resolve step index from definition and validate
    int stepIndex = def.getStepIndex(stepName);
    boolean alreadyCompleted = events.stream()
        .anyMatch(e -> e.getEventType().equals(SagaEvent.STEP_COMPLETED)
                    && e.getStepIndex() == stepIndex);
    if (alreadyCompleted) {
        return saga;  // idempotent: callback already processed
    }

    // Validate output types before persisting (merge throws on invalid types)
    StepResult stepResult = StepResult.of(output);
    context.merge(stepResult);

    // Persist event after validation succeeds
    store.appendEvent(sagaId, context.nextSequence(),
        SagaEvent.stepCompleted(stepIndex, stepName, stepResult));
    context.advanceSequence();

    return engine.resumeFrom(def, context, stepIndex + 1);
}
```

The daemon exposes this via HTTP and/or gRPC depending on configuration:

```java
// HTTP (built into the daemon)
@PostMapping("/api/sagas/{sagaId}/steps/{stepName}/complete")
public ResponseEntity<SagaStateSnapshot> completeStep(...) {
    return ResponseEntity.ok(sagaManager.completeStep(sagaId, stepName, output));
}
```

```protobuf
// gRPC (built into the daemon)
service SagaCallbackService {
    rpc CompleteStep(CompleteStepRequest) returns (SagaStateSnapshotResponse);
}

message CompleteStepRequest {
    string saga_id = 1;
    string step_name = 2;
    map<string, string> output = 3;
}
```

### Timeout and Recovery

The per-step timeout still applies to async steps. If the callback doesn't arrive within the timeout:

1. The step's `deadline` column expires
2. The recovery manager's periodic scan finds the saga with an expired deadline
3. The recovery manager triggers compensation from the WAITING step

When the recovery manager finds a saga with a WAITING step:

- If the step has an expired deadline → compensate (the async operation took too long)
- If the step has no deadline and the saga is stale → re-claim and continue waiting (the orchestrator crashed but the async operation may still complete)

## TCC (Try-Confirm-Cancel) Mode

### What It Does

Adds TCC support as an alternative execution mode alongside the default Saga pattern. TCC is a two-phase protocol where all steps first **reserve** resources (Try), then either **commit** all reservations (Confirm) or **release** all reservations (Cancel) based on the outcome.

The key advantage over Saga: cancelling a *reservation* is cleaner and safer than compensating *committed work*. For example, "release a hold on $100" is simpler than "refund $100 that was already transferred."

### Saga vs TCC Execution Flow

```
SAGA mode (default):
  Step1.execute() ──► Step2.execute() ──► Step3.execute() ──► COMPLETED
       │                   │                   │
       │  on Step3 failure:│                   │
       │                   ▼                   ▼
       │            Step2.compensate()  Step1.compensate()  ──► COMPENSATED
       │
       └── execute() commits REAL work. compensate() undoes committed work.

TCC mode:
  Step1.reserve() ──► Step2.reserve() ──► Step3.reserve()     (Try phase)
       │                      │                      │
       │  all succeeded:      │                      │
       │                      ▼                      ▼
  Step1.confirm() ──► Step2.confirm() ──► Step3.confirm()  ──► COMPLETED
       │
       │  on Step3.reserve() failure:
       │
  Step2.cancel() ──► Step1.cancel()                        ──► COMPENSATED
       │
       └── reserve() only RESERVES. confirm() commits. cancel() releases.
```

### TCC Step Interface

Steps in TCC mode implement `TccStep` instead of `Step`:

```java
public class TccDebitStep implements TccStep {
    private final AccountServiceClient client;

    @Override
    public String getName() { return "debit"; }

    @Override
    public StepResult reserve(SagaContext ctx) throws StepExecutionException {
        // TRY: Place a hold on funds. The money is not yet transferred.
        HoldResponse hold = client.holdFunds(
            ctx.get("accountId", String.class),
            ctx.get("amount", Integer.class));
        return StepResult.of("holdId", hold.getHoldId());
    }

    @Override
    public void confirm(SagaContext ctx) throws StepExecutionException {
        // CONFIRM: Convert the hold into an actual debit.
        // Idempotent — calling confirm twice with the same holdId is a no-op.
        client.confirmHold(ctx.get("holdId", String.class));
    }

    @Override
    public void cancel(SagaContext ctx) throws StepCompensationException {
        // CANCEL: Release the hold. Funds become available again.
        // Idempotent — calling cancel on an already-released hold is a no-op.
        client.releaseHold(ctx.get("holdId", String.class));
    }
}
```

### TCC Definition (JSON / YAML / Java Builder)

```json
{
  "name": "MoneyTransfer",
  "version": "1.0",
  "mode": "TCC",
  "timeoutMillis": 300000,
  "steps": [
    {
      "name": "debit",
      "stepClass": "com.example.TccDebitStep",
      "timeoutMillis": 60000
    },
    {
      "name": "credit",
      "stepClass": "com.example.TccCreditStep",
      "timeoutMillis": 30000
    }
  ]
}
```

The differences from a Saga definition: `"mode": "TCC"`, and no `recoveryStrategy` field. The step classes must implement `TccStep` instead of `Step`. If a step class implements only `Step` (no `reserve()`/`confirm()`/`cancel()`), the engine throws a `SagaDefinitionException` at registration time. If `recoveryStrategy` is set on a TCC definition, the engine throws a `SagaDefinitionException` — the TCC protocol defines fixed recovery behavior per phase (see below).

### Engine Execution Logic (Unified Model)

TCC and Saga share the same pivot-based execution loop (`executeSagaSteps`). For TCC, the engine expands N user-defined TCC steps into a 2N-step plan using `TccReserveStep` and `TccConfirmStep` wrappers before running the unified loop:

```
TCC expansion (3 user steps → 6 plan slots):
  [debit.reserve, credit.reserve, ship.reserve (pivot), debit.confirm, credit.confirm, ship.confirm]
   └── compensatable (cancel on failure) ────────────┘  └── retriable (retry on failure) ──────────┘
```

The expansion in `SagaEngine.executeSteps()`:

```java
List<StepWithPolicy> plan;
if (def.getMode() == SagaMode.TCC) {
    plan = expandTccPlan(def);
} else {
    plan = def.getSteps().stream()
        .map(stepDef -> new StepWithPolicy(instantiate(stepDef), resolveRetryPolicy(stepDef, def),
                                            stepDef.getTimeoutMillis()))
        .toList();
}
executeSagaSteps(plan, def.getPivotPolicy(), context, startIndex, def.getTimeoutMillis());
```

The mode-specific behavior is encapsulated in `PivotPolicy`: for TCC, `getPivotPolicy()` returns a `crossingEvent` of `SagaEvent.sagaConfirming()`; for Saga modes, `crossingEvent` is null. The unified loop handles both modes identically:

```java
// Inside executeSagaSteps() — mode-agnostic pivot crossing:
// All sagas are RUNNING before crossing, so this is idempotent on recovery.
if (pivot.crossingEvent() != null && i == pivot.index() + 1
        && context.getCurrentState().getStatus() == SagaStatus.RUNNING) {
    transition(context, pivot.crossingEvent());
}
```

`TccReserveStep` and `TccConfirmStep` wrap `TccStep` methods into the `Step` interface. `TccReserveStep.execute()` calls `reserve()` and `compensate()` calls `cancel()`. `TccConfirmStep.execute()` calls `confirm()` and `compensate()` is a no-op (confirm steps are always after the pivot, so they are retriable, not compensatable). See the definitions in the [Core Engine](#sagaengine-core-execution-logic) section.

### Confirm Retry Behavior

The confirm phase has a critical property: **it must always succeed**. Resources are already reserved (Try succeeded), so confirmation is just committing the reservation. In the unified model, confirm steps are `TccConfirmStep` instances placed after the pivot. The standard `executeWithRetry()` handles retries, but with a more aggressive confirm retry policy:

```java
// --- engine/RetryPolicy.java ---
public static RetryPolicy confirmDefault() {
    return new RetryPolicy(10, 500, 2.0, 60_000);  // 10 attempts, 500ms initial, 60s max
}
```

Default: **10 attempts** with exponential backoff (more aggressive than the standard 3 attempts for forward steps, because confirm is expected to succeed). If all retries fail, the step throws and the unified loop emits a `STEP_FAILED` event (the saga stays `CONFIRMING` since confirm steps are after the pivot). The recovery manager retries periodically, escalating to `ESCALATED` if stuck longer than the grace period.

### Recovery in TCC Mode

Unlike Saga mode, where `recoveryStrategy` is configurable (BACKWARD, FORWARD, or MIXED), TCC mode has **fixed recovery behavior** defined by the TCC protocol — represented internally by the `PREDEFINED` strategy. You must not set `recoveryStrategy` on a TCC definition; the engine rejects any value other than the implicit `PREDEFINED`.

TCC recovery follows the same **two-phase retry pattern** as Saga mode: immediate retry (in the engine) → periodic recovery retry (recovery manager) → time-based escalation. The phases differ in which retry behavior applies:

| TCC Phase | On failure | Recovery behavior | Rationale |
|---|---|---|---|
| **Try** | Cancel all tried steps | Compensate backward | Cannot leave resources reserved indefinitely |
| **Confirm** | Retry confirm | Retry until success, escalate if stuck | Reservations exist — must commit them |
| **Cancel** | Retry cancel | Retry until success, escalate if stuck | Must release reservations |

The recovery manager handles these scenarios on crash:

| Saga Status at Crash | Recovery Action |
|---|---|
| `RUNNING` (Try phase) | Resume from last completed step via `resumeFrom()`. If a try step fails, the unified loop compensates (cancels) all completed try steps. |
| `CONFIRMING` | Resume from last completed step via `resumeFrom()`. In the expanded plan, confirm steps are after the pivot, so the unified loop retries them. Escalate if stuck longer than grace period. |
| `COMPENSATING` | Continue Cancel (same as Saga mode compensation). Escalate if stuck longer than grace period. |

Note: In the unified model, TCC `CONFIRMING` recovery uses the same `resumeFrom()` as all other modes. The expanded plan places confirm steps after the pivot, so the unified loop's failure handling (emit `STEP_FAILED`, stay in current status for retry) applies naturally. Both TCC confirm and Saga FORWARD/MIXED recovery represent "must succeed" steps that the recovery manager retries periodically. Both escalate to `ESCALATED` after the grace period.

```java
// In SagaRecoveryManager.recoverOne():
// CONFIRMING and RUNNING recovery both use resumeFrom() — no special-casing needed.
// In the expanded TCC plan, confirm steps are after the pivot (indices N..2N-1),
// so the unified loop retries them on failure. No TCC-specific resumeConfirm() needed.
```

### Event Types for TCC

In the unified model, both try and confirm steps emit `STEP_COMPLETED` events — they are all just `Step.execute()` calls from the loop's perspective. The step name suffix (`.reserve` vs `.confirm`) distinguishes phases. On recovery, the event indices map directly to positions in the expanded plan: indices `0..N-1` are try steps, `N..2N-1` are confirm steps.

### When to Use TCC vs Saga

| Aspect | Saga | TCC |
|---|---|---|
| **Step semantics** | Execute commits real work | reserve only reserves |
| **Compensation** | Undoes committed work (harder) | Releases reservation (easier) |
| **Isolation** | Low — intermediate states are visible | Higher — reserves are not visible to other transactions |
| **Participant complexity** | Simpler (action + undo) | More complex (try + confirm + cancel = 3 operations) |
| **Use case** | Long-running workflows, eventual consistency OK | Financial transactions, inventory, anything needing resource reservation |
| **Industry examples** | Seata Saga, Temporal, Eventuate Tram | Seata TCC, ByteTCC, Hmily |

**Guidance:** Use **Saga** when participants are simple services where compensation is straightforward (e.g., send a cancellation email, reverse a shipment). Use **TCC** when you need stronger isolation — the reservation pattern prevents other transactions from seeing or consuming resources that are part of an in-flight transaction (e.g., holding inventory so another order can't claim it).

### Comparison with Seata TCC

Seata's TCC uses annotations (`@TwoPhaseBusinessAction`) with a centralized Transaction Coordinator (TC) server. Our approach differs in:

| Aspect | Seata TCC | This Engine (TCC Mode) |
|---|---|---|
| **Architecture** | Centralized TC server + participant SDK | Embedded library (daemon mode in Phase 3) |
| **Registration** | Annotation-driven (`@TwoPhaseBusinessAction`) | Java builder or JSON/YAML definition + `TccStep` interface |
| **State storage** | Seata Server's internal DB | ScalarDB (any backend) |
| **Try/Confirm/Cancel** | Separate methods via annotation | `TccStep.reserve()` / `confirm()` / `cancel()` |
| **Resource manager** | Branch transaction model | Step-level (each step manages its own resources) |

---

# Part III: Communication & Integration

## Declarative Service Steps and Framework Integration

### Architecture: Layered Step Authoring

The saga engine is framework-agnostic at its core. On top of that core, users choose how much they want to write themselves, from a full `Step` class down to a few lines of declarative configuration:

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 3: Framework Integration (Spring Boot / Quarkus) — future  │
│                                                                   │
│  @SagaStep / @SagaCompensation annotations                       │
│  Build-time (Quarkus) or startup-time (Spring) scanning          │
│  Auto-registers Step / ServiceStep definitions                   │
│  Users keep using framework-native clients                       │
│  (@FeignClient, @GrpcClient, @RestClient, @Autowired, @Inject)  │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2: Declarative Service Steps (framework-agnostic)          │
│                                                                   │
│  ServiceStep + CallSpec in the saga definition — zero Java code  │
│  Realized by DeclarativeBindingStep / DeclarativeBindingTccStep  │
│  over a pluggable TransportAdapter SPI (HTTP today; gRPC future) │
│  Automatic ${...}/$.path mapping, context propagation, error     │
│    classification, SSRF allowlist + body-size limits             │
├──────────────────────────────────────────────────────────────────┤
│  Layer 1: Core Engine + Code Steps (framework-agnostic)          │
│                                                                   │
│  Step / TccStep interfaces, SagaEngine, SagaStore               │
│  SagaRecoveryManager, RetryPolicy                                │
│  SagaHttpClient removes HTTP boilerplate inside code steps       │
│  Works with any transport, any database                          │
└──────────────────────────────────────────────────────────────────┘
```

Users can interact at any layer:
- **Layer 1 (code steps)**: Full control — implement the `Step` / `TccStep` interface directly. Best for complex steps (conditional logic, multi-call orchestration, non-HTTP transports). For the common case of calling one HTTP endpoint, inject a `SagaHttpClient` to remove the transport boilerplate while keeping full programmatic control.
- **Layer 2 (declarative service steps)**: No Java code — define the request/response mapping for each phase directly in the saga definition (Java builder or JSON/YAML). The engine handles transport, marshaling, context propagation, and error classification. Best for straightforward service calls.
- **Layer 3 (framework integration — future)**: Annotate methods on a saga class; the framework scans and auto-registers them. Minimal boilerplate for Spring/Quarkus users.

> **What changed:** earlier revisions of this design had a separate *Layer 2 `ServiceInvoker`* of typed lambdas (`ServiceInvokerRegistry`, `GrpcInvoker`, `HttpInvoker`). That layer has been **removed**. Its two motivations are now served more directly: the *no-code* case by **declarative service steps** (below), and the *typed-but-custom* case by ordinary **code steps** with an injected `SagaHttpClient`. There is no longer a lambda registry to maintain.

### Layer 1: Code Steps with `SagaHttpClient`

A code step is a class implementing `Step` (SAGA mode) or `TccStep` (TCC mode). It runs in the orchestrator process and may do anything inside its methods. When the work is "call one HTTP endpoint," `SagaHttpClient` removes the boilerplate of building a request, propagating saga headers, enforcing the outbound policy, and classifying the response — without giving up programmatic control.

`SagaHttpClient` is injected into the step's constructor. When more than one endpoint is registered, the `@Named` qualifier selects one by name; with a single endpoint the qualifier is optional.

```java
// --- a code step that calls the account service over HTTP ---
public final class DebitStep implements Step {
  private final SagaHttpClient http;

  // @Named matches an endpoint registered on the SagaManager builder.
  public DebitStep(@Named("account-svc") SagaHttpClient http) {
    this.http = http;
  }

  @Override
  public String getName() {
    return "debit";
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    Map<String, Object> body =
        http.post("/debit")
            .jsonBody(Map.of(
                "accountId", context.get("fromAccountId", String.class),
                "amount", context.get("amount", Integer.class)))
            .send() // throws on a non-2xx status, classified retryable/non-retryable
            .bodyJsonObject();
    return StepResult.of("debitId", body.get("debit_id"));
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    http.post("/reverse-debit")
        .jsonBody(Map.of("debitId", context.get("debitId", String.class)))
        .send();
  }
}
```

`SagaHttpClient` is an application-level singleton (thread-safe, shared across sagas). It is a thin fluent wrapper over the same per-endpoint HTTP machinery used by declarative steps — see [Endpoint Configuration](#endpoint-configuration) below.

```java
// --- api/SagaHttpClient.java ---
public interface SagaHttpClient {
  Request get(String path);
  Request post(String path);
  Request put(String path);
  Request patch(String path);
  Request delete(String path);
  Request method(HttpMethod method, String path);

  interface Request {
    Request header(String name, String value);
    Request headers(Map<String, String> headers);
    Request query(String name, String value);
    Request query(Map<String, String> params);
    Request jsonBody(Object value);
    Request stringBody(String body, String contentType);
    Request bytesBody(byte[] body, String contentType);
    Request formBody(Map<String, String> form);

    SagaHttpResponse send();    // throws on a non-2xx status
    SagaHttpResponse sendRaw(); // returns any status without throwing
  }
}

// --- api/SagaHttpResponse.java ---
public interface SagaHttpResponse {
  int status();
  Map<String, List<String>> headers();
  Optional<String> header(String name); // case-insensitive
  Map<String, Object> bodyJsonObject();
  <T> T bodyJson(Class<T> type);
  String bodyString();
  byte[] bodyBytes();
}
```

Every request automatically carries the saga correlation headers `X-Saga-Id` and `X-Saga-Step`, is checked against the endpoint's SSRF allowlist and body-size limit, and has its remaining saga deadline applied as the call timeout.

### Layer 2: Declarative Service Steps

#### The Problem

For straightforward service calls — which are the majority of saga steps — a code step is repetitive boilerplate: read values from context → build a request → make the call → extract response fields → put them back into context. Multiply that by an execute and a compensate (or reserve/confirm/cancel) for every step.

#### Solution: Declarative Communication in the Saga Definition

A **declarative service step** describes that mapping as data instead of code. Each phase of the step (execution + compensation for SAGA; reservation + confirmation + cancellation for TCC) is a `CallSpec` — for the HTTP transport, an `HttpCall` giving the method, path, query, body, and output extraction. The engine resolves the request from the saga context, performs the call, extracts the outputs, propagates the saga context, and classifies errors — no Java code for the step at all.

**JSON (SAGA mode):**

```json
{
  "name": "transferMoney",
  "mode": "SAGA",
  "version": "1.0",
  "recoveryStrategy": "BACKWARD",
  "timeoutMillis": 30000,
  "steps": [
    {
      "name": "debit",
      "service": "account-svc",
      "transport": "HTTP",
      "execution": {
        "method": "POST",
        "path": "/accounts/${fromAccountId}/debit",
        "jsonBody": { "amount": "${amount}" },
        "output": { "debitId": "$.debit_id" }
      },
      "compensation": {
        "method": "POST",
        "path": "/accounts/${fromAccountId}/reverse-debit",
        "jsonBody": { "debitId": "${debitId}" }
      }
    },
    {
      "name": "credit",
      "service": "account-svc",
      "execution": {
        "path": "/accounts/${toAccountId}/credit",
        "jsonBody": { "amount": "${amount}" },
        "output": { "creditId": "$.credit_id" }
      },
      "compensation": {
        "path": "/accounts/${toAccountId}/reverse-credit",
        "jsonBody": { "creditId": "${creditId}" }
      }
    },
    {
      "name": "complex",
      "stepClass": "com.example.ComplexStep"
    }
  ]
}
```

**YAML (TCC mode):**

```yaml
name: reserveBooking
mode: TCC
version: "1.0"
steps:
  - name: reserveFlight
    service: booking-svc
    reservation:
      method: POST
      path: /flights/${flightId}/reserve
      jsonBody:
        passengers: ${numPassengers}
      output:
        reservationId: $.reservation_id
    confirmation:
      method: POST
      path: /flights/${flightId}/confirm
      jsonBody:
        reservationId: ${reservationId}
    cancellation:
      method: POST
      path: /flights/${flightId}/cancel
      jsonBody:
        reservationId: ${reservationId}
```

**Step fields:**

| Field | Meaning |
|---|---|
| `service` | Logical endpoint name; resolved to a base URL + policy via the `SagaManager` builder. Required for a declarative step. |
| `transport` | `HTTP` (default). `GRPC` is reserved but not yet supported. |
| `execution` / `compensation` | The two SAGA-mode phases (both required). |
| `reservation` / `confirmation` / `cancellation` | The three TCC-mode phases (all required). |

A step uses **either** `stepClass` (a code step) **or** `service` + declarative phases — never both, and SAGA and TCC phases must not be mixed. Code steps and declarative steps can be freely mixed within one saga.

**`CallSpec` (HTTP) fields, per phase:**

| Field | Meaning |
|---|---|
| `path` | URL path appended to the endpoint base URL (required). May contain `${...}` expressions. |
| `method` | `GET`, `POST` (default), `PUT`, `PATCH`, `DELETE`. |
| `query` | Map of query-parameter name → value template. |
| `jsonBody` | Flat map serialized as a JSON object body (default `Content-Type: application/json`). |
| `stringBody` | Raw request body template (mutually exclusive with `jsonBody`). |
| `contentType` | Overrides the `Content-Type` header. |
| `output` | Map of context key → extraction expression applied to the response. |

`GET`/`DELETE` must not declare a body. The parser rejects unknown fields and these constraints at registration time.

#### Java Builder API

The same declarative steps can be built type-safely in embedded mode. `serviceStep(name, service).operation()` opens the SAGA-mode phases; `.tccOperation()` opens the TCC-mode phases:

```java
SagaDefinition def = SagaDefinition.newBuilder("transferMoney", SagaMode.SAGA)
    .version("1.0")
    .recoveryStrategy(RecoveryStrategy.BACKWARD)
    .timeoutMillis(30_000)
    .serviceStep("debit", "account-svc")
        .operation()
        .execution(HttpCall.newBuilder("/accounts/${fromAccountId}/debit")
            .method(HttpMethod.POST)
            .jsonBody(Map.of("amount", "${amount}"))
            .output(Map.of("debitId", "$.debit_id"))
            .build())
        .compensation(HttpCall.newBuilder("/accounts/${fromAccountId}/reverse-debit")
            .jsonBody(Map.of("debitId", "${debitId}"))
            .build())
        .add()
    .serviceStep("credit", "account-svc")
        .operation()
        .execution(HttpCall.newBuilder("/accounts/${toAccountId}/credit")
            .jsonBody(Map.of("amount", "${amount}"))
            .output(Map.of("creditId", "$.credit_id"))
            .build())
        .compensation(HttpCall.newBuilder("/accounts/${toAccountId}/reverse-credit")
            .jsonBody(Map.of("creditId", "${creditId}"))
            .build())
        .add()
    .build();
```

For TCC, `.tccOperation().reservation(...).confirmation(...).cancellation(...).add()`. Each `add()` validates that all phases for the mode are present and that they share a single transport.

**Expression syntax:**
- `${key}` — substitutes a value from the saga context. When it is the entire value, the original type is preserved; embedded in a larger string, it is coerced to text. Path segments are percent-encoded to prevent traversal/injection. A missing key is a non-retryable error.
- `$.field` — extracts a field from the JSON response body into the named context key.
- `$body` — binds the entire raw response body as a string (works for non-JSON responses).

#### How It Works

```
Saga definition (builder / JSON / YAML)
    │  parsed by SagaDefinitionParser + CallSpecCodec
    ▼
ServiceStep { service, Map<Phase, CallSpec> }   (sealed StepDefinition)
    │  StepInstantiator + HttpEndpointRegistry
    ▼
┌──────────────────────────────────────────────┐
│  DeclarativeBindingStep   (implements Step)   │
│  DeclarativeBindingTccStep(implements TccStep)│
│                                               │
│  Picks the CallSpec for the current phase,    │
│  delegates to the endpoint's TransportAdapter,│
│  translates TransportException into the        │
│  engine's StepExecution/CompensationException │
└──────────┬───────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│  TransportAdapter (SPI, pluggable per transport)│
│                                               │
│  HttpTransportAdapter:                        │
│  - resolves ${...} in path/query/body via      │
│    DeclarativeExpressions                     │
│  - builds the request (jsonBody/stringBody)   │
│  - calls HttpExchange (shared per endpoint):  │
│      • adds X-Saga-Id / X-Saga-Step           │
│      • enforces SSRF allowlist + body limits  │
│      • applies the remaining saga deadline    │
│      • forbids redirects                      │
│  - classifies the status (2xx success;        │
│    408/429/5xx retryable; other 4xx not),     │
│    honoring an X-Saga-Retryable override      │
│  - extracts $.field / $body into outputs      │
└──────────────────────────────────────────────┘
```

The engine itself never branches on step kind. `DeclarativeBindingStep` and a hand-written `Step` both implement `Step`; the only difference is how `StepInstantiator` builds them (see [Single Dispatch Path](#single-dispatch-path) below).

#### `CallSpec` and `CallSpecCodec`

`CallSpec` is a sealed type tagged by transport. Only `HttpCall` exists today; `GRPC` is a reserved transport value that the parser currently rejects as "not yet supported." The discriminator is persisted with the saga definition so the correct subtype is reconstructed on reload.

```java
// --- api/CallSpec.java ---
public abstract sealed class CallSpec permits HttpCall {
  public enum Transport { HTTP, GRPC } // GRPC not yet supported
  public abstract Transport transport();
}
```

`CallSpecCodec` is the single JSON (de)serializer for every `CallSpec` subtype, used by **both** `SagaDefinitionParser` (user-authored YAML/JSON) and the store's definition serializer (round-trip persistence). Because the two directions are inverses and live in one class, adding a transport or a field is a one-class change and the parse/persist paths cannot drift.

#### `TransportAdapter` SPI

The transport SPI replaces the old `ServiceInvoker`. It executes one phase of a declarative step end to end: resolve the request from context, perform the call, return the extracted outputs.

```java
// --- transport/TransportAdapter.java ---
public interface TransportAdapter {
  /**
   * Execute one declarative phase: resolve the spec's request from the saga
   * context, perform the remote call, and return the extracted output fields.
   *
   * @throws TransportException carrying a retryable / non-retryable classification
   */
  Map<String, Object> call(CallSpec spec, SagaContext context, String stepName)
      throws TransportException;
}
```

`HttpTransportAdapter` is the only implementation today (package-private; created per endpoint). A future `GrpcTransportAdapter` would implement the same SPI against a `GrpcCall` subtype of `CallSpec` — no change to the engine, the binding steps, or the definition model.

#### Endpoint Configuration

A declarative step's `service` name and a code step's `@Named` qualifier both resolve to an **`HttpEndpoint`**: one base URL, one shared `HttpClient`, one `HttpExchange`, and one `OutboundHttpPolicy`. Endpoints are declared on the `SagaManager` builder:

```java
SagaManager manager = SagaManager.newBuilder()
    .storeFactory(ScalarDbSagaStoreFactory.create(props))
    .httpEndpoint("account-svc", "https://account-svc:8443")
        .allowedHosts("account-svc", "account-svc.internal") // SSRF allowlist
        .maxBodyBytes(1_000_000)                              // request/response cap
        .defaultHeader("Authorization", "Bearer …")          // never persisted
        .add()
    .httpEndpoint("booking-svc", "https://booking-svc:8443")
        .add()
    .build();
```

Internally these become `HttpServiceConfig` records held by an `HttpEndpointRegistry`. The registry both hands `SagaHttpClient` instances to code steps (via the `StepResolver`'s resolution context) and builds `DeclarativeBindingStep` / `DeclarativeBindingTccStep` for declarative steps — so both paths ride the exact same exchange, policy, and correlation behavior. Framework-created HTTP clients are closed when the manager closes; caller-supplied clients are left open.

The policy enforcement (SSRF allowlist, body-size limits, no redirects, default per-call timeout) lives in `HttpExchange` and `OutboundHttpPolicy`, applied uniformly to declarative and code-step calls.

#### Single Dispatch Path

`StepDefinition` is a sealed type with two cases, and `StepInstantiator` pattern-matches on it:

```java
return switch (stepDef) {
  case ClassStep classStep -> resolveClassStep(classStep, expectedType);     // Layer 1
  case ServiceStep serviceStep -> resolveServiceStep(serviceStep, expectedType); // Layer 2
};
```

- **`ClassStep`** is resolved to a `Step` / `TccStep` instance through the `StepResolver` (reflective constructor injection by default, or a DI-framework resolver). Constructor parameters typed `SagaHttpClient` are satisfied from the endpoint registry, matched by `@Named`.
- **`ServiceStep`** is wrapped — fast-failing if its `service` is unregistered — in a `DeclarativeBindingStep` (SAGA) or `DeclarativeBindingTccStep` (TCC) over the resolved endpoint's `TransportAdapter`.

From the engine's perspective there is a single `Step` / `TccStep` it executes. The kind of authoring is entirely resolved before execution.

#### Saga Context Propagation

All transports propagate the saga correlation context to participants so they can log, trace, and deduplicate:

| Transport | Propagation mechanism |
|---|---|
| HTTP | `X-Saga-Id`, `X-Saga-Step` request headers (added last, after user/default headers) |
| gRPC (future) | `x-saga-id`, `x-saga-step` metadata keys |

Participants may also signal retryability back to the coordinator with an `X-Saga-Retryable` response header, overriding the default status-code classification.

#### Participant Idempotency Levels

The engine supports different levels of idempotency handling, trading participant simplicity against built-in guarantees:

**Level 1: Basic (industry norm — current)**

The orchestrator propagates the saga ID and step name (`X-Saga-Id`, `X-Saga-Step`); participants implement their own idempotency. This matches Seata, MicroTx, and Narayana LRA.

```
Orchestrator                         Participant
    │                                    │
    ├── X-Saga-Id: abc123 ──────────────►│
    ├── X-Saga-Step: debit ─────────────►│
    │                                    │
    │   Participant must check:          │
    │   "Did I already process           │
    │    debit for saga abc123?"         │
    │   (using its own business data     │
    │    or a dedup table it manages)    │
```

**Level 2: Participant SDK (future enhancement)**

An optional, lightweight SDK for participant services providing a `@SagaParticipant` annotation with an auto-exposed status-check endpoint, an `@Idempotent` annotation with built-in dedup, and automatic saga-context extraction from headers.

```java
// In the participant service — NOT the orchestrator
@SagaParticipant(service = "account-svc")
@RestController
public class AccountEndpoint {

  @SagaAction(name = "debit")
  @PostMapping("/debit")
  public DebitResponse debit(@RequestBody DebitRequest req,
                             @SagaId String sagaId) { // auto-extracted from header
    // Business logic — no idempotency code needed
    return new DebitResponse(debitId);
  }

  @SagaCompensation(name = "debit")
  @PostMapping("/reverse-debit")
  public void reverseDebit(@RequestBody ReverseDebitRequest req,
                           @SagaId String sagaId) {
    // Compensation logic
  }
}
```

| Level | Participant requirement | Extra infrastructure | Idempotency guarantee |
|---|---|---|---|
| **Basic** | Handle dedup on its own | None | Participant's responsibility |
| **SDK** | Add SDK dependency | SDK needs dedup storage | Automatic, reliable |

**Recommendation:** Ship Level 1 (Basic) — it matches industry norm. Level 2 (Participant SDK) is optional and should be built only if customer demand justifies it.

### Layer 3: Framework Integration (Future)

The annotation layer is syntactic sugar over Layers 1–2: a scanner reads annotations and **auto-registers `Step` / `ServiceStep` definitions** at build time (Quarkus) or startup time (Spring). Users keep their framework-native clients (`@Autowired`, `@Inject`, `@GrpcClient`, `@RestClient`). This layer is planned, not yet implemented; the shapes below are indicative.

#### Annotations

```java
// --- api/annotation/SagaStep.java ---
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SagaStep {
  String saga();   // saga definition name
  String name();   // step name within the saga
  int order();     // execution order (1-based)
}

// --- api/annotation/SagaCompensation.java ---
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SagaCompensation {
  String saga();   // must match a @SagaStep's saga
  String name();   // must match a @SagaStep's name
}
```

#### Spring Boot Integration

A Spring auto-configuration would build the `SagaManager` from properties and register a scanner that turns annotated beans into `SagaDefinition`s. The scanner pairs each `@SagaStep` with its `@SagaCompensation`, sorts by `order`, and registers a `Step` per method that invokes the bean — exactly the Layer 1 wiring, generated for the user.

```java
@Component
public class MoneyTransferSaga {

  @Autowired private AccountServiceClient accountClient; // ← Spring-managed

  @SagaStep(saga = "MoneyTransfer", name = "debit", order = 1)
  public StepResult debit(SagaContext ctx) {
    var resp = accountClient.debit(
        ctx.get("accountId", String.class), ctx.get("amount", Integer.class));
    return StepResult.of("debitId", resp.getDebitId());
  }

  @SagaCompensation(saga = "MoneyTransfer", name = "debit")
  public void compensateDebit(SagaContext ctx) {
    accountClient.reverseDebit(ctx.get("debitId", String.class));
  }
}

// Starting a saga — one line:
sagaManager.start("MoneyTransfer", Map.of("accountId", "A001", "amount", 5000));
```

No `Step` classes, no JSON/YAML files, no manual registration — just annotated methods on a framework-managed bean.

#### Quarkus Integration

Quarkus would scan `@SagaStep` at **build time** via Jandex and record the registrations for runtime init (zero reflection at runtime, native-image friendly). The user writes the same annotated bean with `@Inject`-ed clients.

### DX Comparison (Final)

| Engine | What the user writes for a 3-step saga | Lines of user code |
|---|---|---|
| **Seata Saga** | JSON definition (service names + methods) | ~30 (JSON only) |
| **MicroTx / Narayana** | `@LRA`/`@Compensate` on participant endpoints | ~40 (annotations on 3 services) |
| **Ours — Layer 1 (code step)** | 3 `Step` classes + definition | ~90–120 |
| **Ours — Layer 1 (code step + `SagaHttpClient`)** | 3 thin `Step` classes + definition | ~50–70 |
| **Ours — Layer 2 (declarative)** | Definition only (zero Java code) | **~30 (definition only)** |
| **Ours — Layer 3 (Spring/Quarkus — future)** | 1 annotated saga class | **~30–40** |

### Relationship Between Layers and LRA Compatibility

The MicroProfile LRA compatibility section (earlier in this document) describes how to add full LRA protocol support by layering the LRA REST API on top of the Phase 3 coordinator daemon (Phase 6). Internally it uses Layer 1 (code steps) and Layer 2 (declarative service steps), adding LRA-specific endpoints so the coordinator can interoperate with standard LRA participants.

## Step Implementation Patterns

### The Step Interface is Deployment-Agnostic

The `Step` interface is a plain Java interface — `execute(SagaContext)` and `compensate(SagaContext)`. The saga engine calls these methods and records the result. It does **not** constrain what happens inside — the step can make an RPC call, access a database, publish a message, or do anything else.

ScalarDB is used by the **SagaStore** (the engine's internal persistence) — not by the Step implementations. The step's business logic and the engine's state recording are always separate transactions (see [Transaction Boundary Model](#transaction-boundary-model) below).

### Remote Service Steps (Microservices)

The primary use case for sagas is coordinating across services that can't share a single ACID transaction. In this pattern, each step wraps an RPC call to a remote participant:

```java
public class DebitAccountStep implements Step {
    private final AccountServiceGrpc.AccountServiceBlockingStub accountStub;

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        try {
            var resp = accountStub.debit(DebitRequest.newBuilder()
                .setSagaId(ctx.getSagaId())       // for participant-side idempotency
                .setAccountId(ctx.get("accountId", String.class))
                .setAmount(ctx.get("amount", Integer.class))
                .build());
            return StepResult.of("debitId", resp.getDebitId());
        } catch (StatusRuntimeException e) {
            throw new StepExecutionException(e, isRetryable(e.getStatus()));
        }
    }

    @Override
    public void compensate(SagaContext ctx) throws StepCompensationException {
        accountStub.reverseDebit(ReverseDebitRequest.newBuilder()
            .setDebitId(ctx.get("debitId", String.class))
            .build());
    }
}
```

The step itself runs in the orchestrator process. The business logic (debit, reserve inventory, etc.) runs in the participant service. This is also where declarative service steps (Layer 2) and framework annotations (Layer 3) provide higher-level alternatives to writing Step classes by hand — see [Declarative Service Steps and Framework Integration](#declarative-service-steps-and-framework-integration).

### Local Database Steps (Modular-Monolith)

In a modular-monolith deployment, some or all steps may access databases directly in the same process instead of making RPC calls. The `Step` interface supports any database technology — the engine imposes no constraints:

| Interface | When to Use | Key Difference |
|---|---|---|
| ScalarDB CRUD API | Application already uses ScalarDB | Get/Put builders |
| ScalarDB SQL (`SqlSession`) | Prefer SQL over CRUD API | SQL statements |
| ScalarDB JDBC driver | Standard JDBC tooling (JPA, Hibernate, MyBatis) | Standard `java.sql` API via ScalarDB JDBC driver |
| Any other (JDBC, JPA, MongoDB, etc.) | Business data not in ScalarDB | No ScalarDB involvement — use any database or client library |

Example with ScalarDB CRUD API:

```java
public class DebitAccountStep implements Step {
    private final DistributedTransactionManager txManager;

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        DistributedTransaction tx = txManager.begin();
        try {
            Optional<Result> account = tx.get(Get.newBuilder()
                .namespace("bank").table("accounts")
                .partitionKey(Key.ofText("account_id", ctx.get("accountId", String.class)))
                .build());
            // ... business logic ...
            tx.commit();
            return StepResult.of("debitedAmount", amount);
        } catch (Exception e) {
            abortQuietly(tx);
            throw new StepExecutionException(e);
        }
    }
}
```

Because the `Step` interface doesn't constrain the database technology, a modular-monolith can be migrated to microservices incrementally — extract a module into a service, change the step from a local database call to an RPC call. The idempotency requirement and transaction boundary model are identical in both cases, so no correctness logic needs to change.

A single saga can also **mix** remote and local steps:

```json
{
  "name": "PlaceOrder",
  "steps": [
    { "name": "debit",     "stepClass": "com.example.ScalarDbCrudDebitStep" },
    { "name": "shipping",  "stepClass": "com.example.RemoteShippingStep" },
    { "name": "notify",    "stepClass": "com.example.KafkaNotificationStep" }
  ]
}
```

### Transaction Boundary Model

The Step's business transaction and the SagaStore's state transaction are **always separate**:

```
Time ──────────────────────────────────────────────────────►

Step tx:        [step.execute() ── business logic ── commit]
                                                        │
SagaStore tx:                                           └──[appendEvent(STEP_COMPLETED)]
```

If the process crashes between the Step's commit and `appendEvent()`, the step's business effect is committed but the saga engine doesn't know. On recovery, the step will be re-executed. **This is why `execute()` must be idempotent** — regardless of deployment pattern (microservices or modular-monolith) and regardless of database technology.


## Message Broker Extensibility

### Orchestration Model

The Phase 1 design uses **direct dispatch** — the `SagaEngine` calls `step.execute()` synchronously as a plain method call. No message brokers, no queues, no async message passing. This is the same model Seata Saga uses internally (its `DirectEventBus`/`AsyncEventBus` are in-memory, in-process dispatchers — not external brokers).

```
  SagaEngine (Orchestrator)
    │
    ├─► step1.execute(ctx)   ← direct method call
    ├─► step2.execute(ctx)   ← direct method call
    ├─► step3.execute(ctx)   ← direct method call
    │
    (no brokers in the execution path)
```

The engine itself never needs to change to support brokers. The `Step` interface is the extension point.

### Option A: Step Wraps a Producer

The simplest approach. The saga engine stays synchronous. Individual steps can internally publish to a broker — the engine doesn't know or care.

```
  SagaEngine
    │
    ├─► NotificationStep.execute()
    │     │
    │     ├─► producer.send(message) ──► Kafka/RabbitMQ ──► Consumer
    │     ├─► wait for reply (or fire-and-forget)
    │     └─► return StepResult
    │
    (engine is unaware of the broker)
```

Example:

```java
public class KafkaNotificationStep implements Step {
    private final KafkaProducer<String, String> producer;

    @Override
    public String getName() { return "notify"; }

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        String orderId = ctx.get("orderId", String.class);
        try {
            // Fire-and-forget to Kafka — broker is an implementation detail
            producer.send(new ProducerRecord<>("order-events",
                orderId, toJson(Map.of("event", "ORDER_PLACED", "orderId", orderId))))
                .get();  // .get() for synchronous send; omit for async
            return StepResult.of("notified", true);
        } catch (Exception e) {
            throw new StepExecutionException(e);
        }
    }

    @Override
    public void compensate(SagaContext ctx) throws StepCompensationException {
        // Publish a compensating event
        String orderId = ctx.get("orderId", String.class);
        try {
            producer.send(new ProducerRecord<>("order-events",
                orderId, toJson(Map.of("event", "ORDER_CANCELLED", "orderId", orderId))))
                .get();
        } catch (Exception e) {
            throw new StepCompensationException(e);
        }
    }
}
```

**When to use**: One or two steps need to talk to a queue; the rest are direct calls.

### Which to Choose

| Approach | When to Use |
|---|---|
| **No broker** (default) | All participants are in-process or callable via RPC |
| **Step wraps producer** | One or two steps need to talk to a queue; rest are direct calls |

The key design principle: **the engine never changes.** Broker support is purely at the `Step` implementation level — the engine is unaware of message brokers.

---

# Part IV: Developer Experience

## Bootstrapping (DI-Free)

The core engine has zero dependency on any DI framework. All wiring is done via a plain Java builder:

Three step resolution modes:

1. **No dependencies (default):** Steps must have a single public no-arg constructor.
2. **Resource injection:** Register shared resources via `resource()`. Steps must have exactly one public constructor whose parameter types match registered resources. Use `@Named` to disambiguate multiple resources of the same type.
3. **Custom resolver:** Supply a `StepResolver` via `stepResolver()` for full control over step instantiation (e.g., manual lookup, DI framework integration).

`resource()` and `stepResolver()` are mutually exclusive.

```java
// --- engine/SagaManagerBuilder.java ---
public class SagaManagerBuilder {
    private SagaStore store;
    private String ownerId = UUID.randomUUID().toString();
    private SagaEngine.ShutdownMode shutdownMode = SagaEngine.ShutdownMode.WAIT_CURRENT_STEP;
    private long shutdownTimeoutMillis = 30_000;
    private Clock clock = Clock.systemUTC();
    private ResourceRegistry.Builder resourceRegistryBuilder;
    private StepResolver customStepResolver;

    public SagaManagerBuilder store(SagaStore store) { ... }
    public SagaManagerBuilder ownerId(String ownerId) { ... }
    public SagaManagerBuilder shutdownMode(SagaEngine.ShutdownMode shutdownMode) { ... }
    public SagaManagerBuilder shutdownTimeoutMillis(long shutdownTimeoutMillis) { ... }
    public SagaManagerBuilder clock(Clock clock) { ... }

    // Resource injection: register shared resources for constructor injection
    public <T> SagaManagerBuilder resource(Class<T> type, T instance) { ... }
    public <T> SagaManagerBuilder resource(Class<T> type, T instance, String name) { ... }

    // Custom resolver: full control over step instantiation
    public SagaManagerBuilder stepResolver(StepResolver stepResolver) { ... }

    public SagaManager build() {
        if (store == null) {
            throw new IllegalStateException("SagaStore is required — call store() before build()");
        }
        if (resourceRegistryBuilder != null && customStepResolver != null) {
            throw new IllegalStateException(
                "resource() and stepResolver() are mutually exclusive");
        }
        StepResolver resolver = buildStepResolver();
        SagaEngine.ShutdownConfig shutdownConfig =
            new SagaEngine.ShutdownConfig(shutdownMode, shutdownTimeoutMillis);
        SagaEngine engine = new SagaEngine(store, resolver, ownerId, shutdownConfig, clock);
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry(store);
        return new EmbeddedSagaManager(engine, store, registry, shutdownTimeoutMillis);
    }
}

// --- engine/StepResolver.java ---
@FunctionalInterface
public interface StepResolver {
    // Resolve a step by name and FQCN. Must return same instance for repeated calls.
    Object resolve(String stepName, String stepClass);
}
```

Optional DI integration modules wrap this builder:

- **`scalardb-saga-spring`**: `SagaAutoConfiguration` with `@Bean SagaManager` that reads `SagaProperties` and calls the builder
- **`scalardb-saga-quarkus`**: Quarkus extension with `@Produces SagaManager` CDI bean
- **`scalardb-saga-guice`**: `SagaModule extends AbstractModule` that binds via the builder

## Bootstrap and Execute

Given Step implementations (see [Deployment Architecture](#deployment-architecture-cross-service-orchestration)) and a saga definition JSON (see [Saga Definition Format](#saga-definition-format)), here's how to wire and run the engine:

```java
// Bootstrap — no DI framework required
TransactionFactory factory = TransactionFactory.create("database.properties");
DistributedTransactionManager txManager = factory.getTransactionManager();
Admin admin = factory.getTransactionAdmin();

// Create schema
SagaSchema schema = new SagaSchema();
schema.createAll(admin);

// Wire with plain Java builder (no Guice/Spring/CDI needed)
ObjectMapper objectMapper = new ObjectMapper();
ScalarDbSagaStoreConfig storeConfig = ScalarDbSagaStoreConfig.newBuilder().build();
SagaStore store = new ScalarDbSagaStore(txManager, objectMapper, schema, storeConfig);
SagaManager sagaManager = SagaManagerBuilder.newBuilder()
    .store(store)
    .resource(ManagedChannel.class, accountChannel, "account")
    .resource(ManagedChannel.class, shippingChannel, "shipping")
    .build();

// Register saga definition from file
sagaManager.register(Path.of("sagas/money-transfer.json"));

// Start periodic crash recovery (runs immediately, then every 30 seconds)
sagaManager.startRecovery();
```

The above runs once at application startup. After that, `sagaManager.start()` is called per request:

```java
// Per request — e.g., inside an HTTP handler or message consumer
String sagaId = sagaManager.start("MoneyTransfer", Map.of(
    "fromAccountId", "A001",
    "toAccountId",   "B002",
    "amount",        5000
));

SagaStateSnapshot result = sagaManager.getStateSnapshot(sagaId);
System.out.println(result.getStatus());  // COMPLETED or COMPENSATED
```

## Testing Harness (SagaTestHarness)

### What It Does

Provides a lightweight integration testing framework for saga definitions. Uses `ScalarDbSagaStore` backed by in-memory SQLite — tests the real store code path with no external database required. This is a genuine differentiator against Seata/MicroTx/Narayana, none of which provide testing utilities. Temporal's replay-based testing framework is widely cited as their biggest DX win.

### MockStep

```java
// --- testing/MockStep.java ---
public class MockStep implements Step {
    private final String name;
    private final Function<SagaContext, StepResult> action;
    private final Consumer<SagaContext> compensation;
    private final List<SagaContext> executionHistory = new ArrayList<>();
    private final List<SagaContext> compensationHistory = new ArrayList<>();

    public static MockStep succeeding(String name, String outputKey, Object outputValue) {
        return new MockStep(name,
            ctx -> StepResult.of(outputKey, outputValue),
            ctx -> {});
    }

    public static MockStep failing(String name, Exception error) {
        return new MockStep(name,
            ctx -> { throw new StepExecutionException(error); },
            ctx -> {});
    }

    public boolean wasExecuted() { return !executionHistory.isEmpty(); }
    public boolean wasCompensated() { return !compensationHistory.isEmpty(); }
    public int executionCount() { return executionHistory.size(); }
}
```

### SagaTestHarness

```java
// --- testing/SagaTestHarness.java ---
public class SagaTestHarness {
    private final SagaDefinition definition;
    private final ScalarDbSagaStore store;  // backed by in-memory SQLite
    private final Map<String, MockStep> mockSteps;

    public static Builder forDefinition(SagaDefinition definition) {
        return new Builder(definition);
    }

    public static Builder forJson(String jsonDefinition) {
        return new Builder(SagaDefinitionParser.parse(jsonDefinition));
    }

    // Execute the saga with given input
    public SagaStateSnapshot execute(Map<String, Object> input) { ... }

    // Assertions
    public boolean wasExecuted(String stepName) { ... }
    public boolean wasCompensated(String stepName) { ... }
    public List<String> executionOrder() { ... }
    public List<String> compensationOrder() { ... }
    public SagaContext finalContext() { ... }

    public static class Builder {
        public Builder mockStep(String name, Function<SagaContext, StepResult> action) { ... }
        public Builder mockStep(String name, Function<SagaContext, StepResult> action,
                                Consumer<SagaContext> compensation) { ... }
        public Builder failAt(String stepName, Exception error) { ... }
        public SagaTestHarness build() { ... }
    }
}
```

### CrashingStoreDecorator (Crash Simulation)

```java
// --- testing/CrashingStoreDecorator.java ---
// Wraps a SagaStore to simulate crashes at specific step boundaries.
public class CrashingStoreDecorator implements SagaStore {
    private final SagaStore delegate;
    private final Set<String> crashAfterSteps;  // step names to crash after

    @Override
    public void appendEvent(String sagaId, int sequence, SagaEvent event) {
        delegate.appendEvent(sagaId, sequence, event);
        if (event.getEventType().equals(SagaEvent.STEP_COMPLETED)
                && crashAfterSteps.contains(event.getStepName())) {
            throw new SimulatedCrashException("Simulated crash after step: " + event.getStepName());
        }
    }
}
```

### Usage Examples

```java
// 1. Happy path — all steps succeed
SagaTestHarness harness = SagaTestHarness.forDefinition(moneyTransferDef)
    .mockStep("debit", ctx -> StepResult.of("debitId", "D001"))
    .mockStep("credit", ctx -> StepResult.of("creditId", "C001"))
    .build();

SagaStateSnapshot result = harness.execute(Map.of("amount", 100));
assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
assertThat(harness.executionOrder()).containsExactly("debit", "credit");

// 2. Failure triggers compensation in reverse order
SagaTestHarness harness = SagaTestHarness.forDefinition(moneyTransferDef)
    .mockStep("debit", ctx -> StepResult.of("debitId", "D001"))
    .failAt("credit", new InsufficientFundsException())
    .build();

SagaStateSnapshot result = harness.execute(Map.of("amount", 100));
assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
assertThat(harness.wasCompensated("debit")).isTrue();
assertThat(harness.wasCompensated("credit")).isFalse();  // credit failed, not compensated

// 3. Crash recovery — simulate crash after step 1, then recover
SagaTestHarness harness = SagaTestHarness.forDefinition(moneyTransferDef)
    .mockStep("debit", ctx -> StepResult.of("debitId", "D001"))
    .mockStep("credit", ctx -> StepResult.of("creditId", "C001"))
    .crashAfterStep("debit")  // simulate crash after debit
    .build();

// First execution crashes
assertThrows(SimulatedCrashException.class, () -> harness.execute(Map.of("amount", 100)));

// Recovery resumes from credit step
SagaStateSnapshot recovered = harness.recover();
assertThat(recovered.getStatus()).isEqualTo(SagaStatus.COMPLETED);
```

## Local Development Server (SagaDevServer)

### What It Does

Provides a zero-configuration development environment for building and debugging sagas. Uses an in-memory SQLite database as the ScalarDB backend, creates the saga schema on startup, and exposes a lightweight web UI for inspecting and manually triggering sagas — all from a single JAR with no external dependencies.

Temporal's `temporal server start-dev` is the single biggest driver of their developer adoption. Getting started in under 5 minutes matters enormously.

```
 ┌───────────────────────────────────────────────────┐
 │                  SagaDevServer                     │
 │                                                    │
 │  SQLite (in-memory)  ◄──  SagaStore (ScalarDB/JDBC)   │
 │                                                    │
 │  SagaSchema.createIfNotExists()  (on startup)      │
 │                                                    │
 │  Javalin (embedded HTTP)                           │
 │    GET  /api/sagas          → listSagas            │
 │    GET  /api/sagas/{id}     → getSagaDetail        │
 │    POST /api/sagas/{id}/compensate                 │
 │    POST /api/sagas/{id}/retry                      │
 │    POST /api/sagas/{id}/force-complete             │
 │    GET  /api/metrics        → getMetrics           │
 │    GET  /                   → embedded Web UI      │
 └───────────────────────────────────────────────────┘
```

### SagaDevServer Interface

```java
// --- devserver/SagaDevServer.java ---
public class SagaDevServer implements AutoCloseable {
    public static SagaDevServer create(SagaDevServerConfig config);
    public void start();
    public void stop();
    @Override public void close() { stop(); }

    // Return the SagaManager backed by the dev server's in-memory store
    public SagaManager getSagaManager();
    public String getBaseUrl();
}

// --- devserver/SagaDevServerConfig.java ---
public class SagaDevServerConfig {
    private final int port;                       // default: 7070
    private final String sagaDefinitionPath;      // classpath resource dir
    private final boolean autoOpenBrowser;        // default: false

    public static Builder newBuilder() { return new Builder(); }
    public static SagaDevServerConfig defaults() { return newBuilder().build(); }
}
```

### Command-Line Usage

```bash
# Zero-config development — starts with in-memory SQLite, no setup needed
java -jar scalardb-saga-dev-server.jar

# Custom port + auto-register saga definitions
java -jar scalardb-saga-dev-server.jar --port 8080 --definitions classpath:sagas/
```

### Programmatic Use in Integration Tests

```java
class MoneyTransferSagaTest {
    static SagaDevServer devServer;
    static SagaManager sagaManager;

    @BeforeAll
    static void setUp() {
        devServer = SagaDevServer.create(
            SagaDevServerConfig.newBuilder()
                .port(0)                           // random port
                .sagaDefinitionPath("sagas/")
                .build());
        devServer.start();
        sagaManager = devServer.getSagaManager();
    }

    @AfterAll
    static void tearDown() { devServer.close(); }

    @Test
    void transferSucceeds() {
        String sagaId = sagaManager.start("MoneyTransfer",
            Map.of("fromAccountId", "A1", "toAccountId", "A2", "amount", 100));
        SagaStateSnapshot result = sagaManager.getStateSnapshot(sagaId);
        assertEquals(SagaStatus.COMPLETED, result.getStatus());
    }
}
```

### Module Structure

`scalardb-saga-dev-server` is a separate Maven/Gradle artifact that depends on `scalardb-saga-core` plus SQLite, Javalin, and Jackson. It is never a transitive dependency of `scalardb-saga-core` — production applications pay no classpath cost.

---

# Part V: Production Operations

## Admin API

### What It Does

Provides a programmatic interface for production operations on saga instances: listing, inspecting, and manually intervening in saga execution. This is the foundation for any operational tooling — CLI scripts, monitoring dashboards, and the Web UI.

REST endpoint exposure is intentionally left out of the core module. Spring and Quarkus integration modules wrap `SagaAdminService` in a controller/resource and expose it over HTTP.

### SagaAdminService Interface

```java
// --- admin/SagaAdminService.java ---
public interface SagaAdminService {

    // List saga instances matching query criteria (paginated)
    SagaPage<SagaStateSnapshot> listSagas(SagaQuery query);

    // Full execution history: saga instance + all step logs + derived timeline
    SagaDetail getSagaDetail(String sagaId);

    // Manually trigger compensation for a stuck RUNNING saga (e.g., FORWARD saga
    // where admin prefers compensation over continued retry).
    // Requires at least one STEP_FAILED event (saga must be stuck, not actively executing).
    SagaStateSnapshot triggerCompensation(String sagaId);

    // Admin override: mark an ESCALATED saga as COMPLETED.
    SagaStateSnapshot forceComplete(String sagaId);

    // Immediately retry a stuck saga from the last successfully completed step.
    // Requires at least one STEP_FAILED event (saga must be stuck, not actively executing).
    SagaStateSnapshot retrySaga(String sagaId);

    // Reset ESCALATED sagas back to COMPENSATING so recovery retries compensation.
    // Filtered by query (sagaName, fromTime, toTime). Status is forced to ESCALATED
    // regardless of query. Paginates internally to bound memory. Returns the count.
    int resetEscalated(SagaQuery query);

    // Aggregate metrics across all saga instances.
    SagaMetrics getMetrics();
}
```

### SagaQuery Builder

```java
// --- admin/SagaQuery.java ---
public class SagaQuery {
    private final Set<SagaStatus> statuses;
    private final String sagaName;
    private final Instant fromTime;
    private final Instant toTime;
    private final int pageSize;           // default: 50, max: 500
    private final String pageToken;       // opaque cursor; null = first page

    public static Builder newBuilder() { return new Builder(); }
    public static Builder newBuilder(SagaQuery prototype) { return new Builder(prototype); }

    public static class Builder {
        Builder() {}
        Builder(SagaQuery prototype) { /* copy all fields */ }

        public Builder statusIn(SagaStatus... statuses);
        public Builder sagaName(String name);
        public Builder fromTime(Instant from);
        public Builder toTime(Instant to);
        public Builder pageSize(int size);
        public Builder pageToken(String token);
        public SagaQuery build();
    }
}

// Paginated result wrapper
public class SagaPage<T> {
    private final List<T> items;
    private final String nextPageToken;  // null if no more pages
    private final int totalCount;
}
```

### SagaDetail and SagaMetrics

```java
// --- admin/SagaDetail.java ---
public class SagaDetail {
    private final SagaStateSnapshot saga;
    private final List<SagaEvent> events;        // full event stream
    private final List<TimelineEvent> timeline;  // derived from events

    public static class TimelineEvent {
        private final Instant timestamp;
        private final String stepName;
        private final String eventType;   // "STEP_COMPLETED", "STEP_COMPENSATED", etc.
        private final String detail;
    }
}

// --- admin/SagaMetrics.java ---
// Duration percentiles and failure rates are handled by OpenTelemetry
// histograms (saga.duration, saga.step.duration). This class provides
// operational counts for the Admin API.
public class SagaMetrics {
    private final Map<SagaStatus, Long> countByStatus;
    private final Map<String, Long> countBySagaName;
}
```

### DefaultSagaAdminService

All mutating admin operations (`triggerCompensation`, `forceComplete`, `retrySaga`, `resetEscalated`) should log the operator identity (from `SagaIdentity` in daemon mode, or caller-supplied in embedded mode) and the reason for the action.

```java
// --- admin/DefaultSagaAdminService.java ---
public class DefaultSagaAdminService implements SagaAdminService {
    private final SagaStore store;
    private final SagaManager sagaManager;
    private final String ownerId;

    @Override
    public SagaPage<SagaStateSnapshot> listSagas(SagaQuery query) {
        return store.listStateSnapshots(query);
    }

    @Override
    public SagaDetail getSagaDetail(String sagaId) {
        SagaStateSnapshot saga = store.getStateSnapshot(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));
        List<SagaEvent> events = store.getEvents(sagaId);
        List<TimelineEvent> timeline = TimelineBuilder.from(events);
        return new SagaDetail(saga, events, timeline);
    }

    @Override
    public SagaStateSnapshot triggerCompensation(String sagaId) {
        SagaStateSnapshot saga = store.getStateSnapshot(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));
        requireStatus(saga, SagaStatus.RUNNING);
        requireHasEvent(sagaId, SagaEvent.STEP_FAILED);
        SagaStateSnapshot claimed = store.claimForRecovery(saga, ownerId)
            .orElseThrow(() -> new SagaConcurrentModificationException(sagaId));
        return sagaManager.compensate(sagaId);
    }

    @Override
    public SagaStateSnapshot forceComplete(String sagaId) {
        SagaStateSnapshot saga = store.getStateSnapshot(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));
        requireStatus(saga, SagaStatus.ESCALATED);
        SagaStateSnapshot claimed = store.claimForRecovery(saga, ownerId)
            .orElseThrow(() -> new SagaConcurrentModificationException(sagaId));
        int nextSeq = store.getEventCount(sagaId);
        return store.recordTransition(claimed, nextSeq, SagaEvent.sagaCompleted());
    }

    @Override
    public SagaStateSnapshot retrySaga(String sagaId) {
        SagaStateSnapshot saga = store.getStateSnapshot(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));
        requireStatus(saga, SagaStatus.RUNNING);
        requireHasEvent(sagaId, SagaEvent.STEP_FAILED);
        SagaStateSnapshot claimed = store.claimForRecovery(saga, ownerId)
            .orElseThrow(() -> new SagaConcurrentModificationException(sagaId));
        return sagaManager.resume(sagaId);
    }

    @Override
    public int resetEscalated(SagaQuery query) {
        // Force ESCALATED status regardless of what the caller passed
        SagaQuery effectiveQuery = SagaQuery.newBuilder(query)
            .statusIn(SagaStatus.ESCALATED)
            .pageToken(null)
            .build();

        int count = 0;
        SagaQuery pageQuery = effectiveQuery;
        do {
            SagaPage<SagaStateSnapshot> page = store.listStateSnapshots(pageQuery);
            for (SagaStateSnapshot saga : page.getItems()) {
                store.recordTransition(saga,
                    store.getEventCount(saga.getSagaId()),
                    SagaEvent.sagaCompensating());
                count++;
            }
            pageQuery = page.getNextPageToken() != null
                ? SagaQuery.newBuilder(effectiveQuery)
                    .pageToken(page.getNextPageToken()).build()
                : null;
        } while (pageQuery != null);
        return count;
    }

    @Override
    public SagaMetrics getMetrics() {
        return new SagaMetrics(store.countByStatus(), store.countBySagaName());
    }

    private void requireStatus(SagaStateSnapshot saga, SagaStatus expected) {
        if (saga.getStatus() != expected) {
            throw new IllegalStateException(
                "Saga " + saga.getSagaId() + " is " + saga.getStatus()
                    + ", expected " + expected);
        }
    }

    private void requireHasEvent(String sagaId, String eventType) {
        List<SagaEvent> events = store.getEvents(sagaId);
        boolean found = events.stream()
            .anyMatch(e -> e.getEventType().equals(eventType));
        if (!found) {
            throw new IllegalStateException(
                "Saga " + sagaId + " has no " + eventType + " events");
        }
    }
}
```

### REST Exposure (via Integration Modules)

- `scalardb-saga-spring` — a `@RestController` wrapping `SagaAdminService`
- `scalardb-saga-quarkus` — a JAX-RS `@Path` resource wrapping `SagaAdminService`

Both are optional dependencies. Users who do not use Spring or Quarkus consume `SagaAdminService` directly.

### Future Web UI

A read/write web dashboard (planned as near-future work) will consume `SagaAdminService` to render:

- Paginated saga list filterable by status, name, and date range
- Saga detail view with step-by-step timeline as a visual lane diagram
- Action buttons for compensate, retry, and force-complete
- Metrics dashboard with count cards per status and duration charts

The Web UI will be served by `SagaDevServer` (Component 9) in development, and optionally as an embedded servlet in production via Spring/Quarkus integration modules.

## Observability (OpenTelemetry)

### What It Does

Provides tracing, metrics, and lifecycle event hooks for saga execution. Uses OpenTelemetry for tracing and metrics, with a pluggable `SagaEventListener` interface for custom integrations.

### SagaEventListener Interface

```java
// --- observability/SagaEventListener.java ---
public interface SagaEventListener {
    default void onSagaStarted(String sagaId, String sagaName, Map<String, Object> input) {}
    default void onStepStarted(String sagaId, String stepName, int stepIndex) {}
    default void onStepCompleted(String sagaId, String stepName, int stepIndex,
                                  StepResult result, Duration duration) {}
    default void onStepFailed(String sagaId, String stepName, int stepIndex,
                               Exception error, int attemptCount) {}
    default void onSagaCompleted(String sagaId, Duration totalDuration) {}
    default void onCompensationStarted(String sagaId, int fromStep) {}
    default void onStepCompensated(String sagaId, String stepName, int stepIndex) {}
    default void onStepCompensationFailed(String sagaId, String stepName, int stepIndex,
                                           Exception error) {}
    default void onSagaCompensated(String sagaId, Duration totalDuration) {}
    default void onSagaEscalated(String sagaId, String reason) {}
    default void onRecoveryClaimed(String sagaId, String ownerId) {}
}
```

### OpenTelemetrySagaListener (Tracing + Metrics)

```java
// --- observability/OpenTelemetrySagaListener.java ---
public class OpenTelemetrySagaListener implements SagaEventListener {
    private final Tracer tracer;
    private final Meter meter;

    // Metrics instruments — saga-level
    private final LongCounter sagaStartedCounter;
    private final LongCounter sagaCompletedCounter;
    private final LongCounter sagaCompensatedCounter;
    private final LongCounter sagaEscalatedCounter;
    private final DoubleHistogram sagaDurationHistogram;
    private final LongUpDownCounter activeSagaGauge;

    // Metrics instruments — step-level
    private final LongCounter stepCompletedCounter;
    private final LongCounter stepFailedCounter;
    private final LongCounter stepCompensatedCounter;
    private final LongCounter stepCompensationFailedCounter;
    private final DoubleHistogram stepDurationHistogram;

    // Metrics instruments — recovery
    private final LongCounter recoveryClaimCounter;

    // Per-saga span tracking
    private final ConcurrentMap<String, Span> sagaSpans = new ConcurrentHashMap<>();

    @Override
    public void onSagaStarted(String sagaId, String sagaName, Map<String, Object> input) {
        sagaStartedCounter.add(1, Attributes.of(AttributeKey.stringKey("saga.name"), sagaName));
        activeSagaGauge.add(1);

        Span span = tracer.spanBuilder("saga:" + sagaName)
            .setAttribute("saga.id", sagaId)
            .setAttribute("saga.name", sagaName)
            .startSpan();
        sagaSpans.put(sagaId, span);
    }

    @Override
    public void onStepCompleted(String sagaId, String stepName, int stepIndex,
                                 StepResult result, Duration duration) {
        stepCompletedCounter.add(1,
            Attributes.of(AttributeKey.stringKey("saga.step"), stepName));
        stepDurationHistogram.record(duration.toMillis(),
            Attributes.of(
                AttributeKey.stringKey("saga.step"), stepName,
                AttributeKey.stringKey("step.status"), "completed"));

        Span parent = sagaSpans.get(sagaId);
        if (parent != null) {
            Span stepSpan = tracer.spanBuilder("step:" + stepName)
                .setParent(Context.current().with(parent))
                .setAttribute("step.index", stepIndex)
                .startSpan();
            stepSpan.end();
        }
    }

    @Override
    public void onStepFailed(String sagaId, String stepName, int stepIndex,
                              Exception error, int attemptCount) {
        stepFailedCounter.add(1,
            Attributes.of(AttributeKey.stringKey("saga.step"), stepName));
    }

    @Override
    public void onStepCompensated(String sagaId, String stepName, int stepIndex) {
        stepCompensatedCounter.add(1,
            Attributes.of(AttributeKey.stringKey("saga.step"), stepName));
    }

    @Override
    public void onStepCompensationFailed(String sagaId, String stepName, int stepIndex,
                                          Exception error) {
        stepCompensationFailedCounter.add(1,
            Attributes.of(AttributeKey.stringKey("saga.step"), stepName));
    }

    @Override
    public void onSagaCompleted(String sagaId, Duration totalDuration) {
        sagaCompletedCounter.add(1);
        activeSagaGauge.add(-1);
        sagaDurationHistogram.record(totalDuration.toMillis());

        Span span = sagaSpans.remove(sagaId);
        if (span != null) {
            span.setStatus(StatusCode.OK);
            span.end();
        }
    }

    @Override
    public void onSagaEscalated(String sagaId, String reason) {
        sagaEscalatedCounter.add(1);
        activeSagaGauge.add(-1);

        Span span = sagaSpans.remove(sagaId);
        if (span != null) {
            span.setStatus(StatusCode.ERROR, "Escalated: " + reason);
            span.end();
        }
    }

    @Override
    public void onSagaCompensated(String sagaId, Duration totalDuration) {
        sagaCompensatedCounter.add(1);
        activeSagaGauge.add(-1);
        sagaDurationHistogram.record(totalDuration.toMillis());

        Span span = sagaSpans.remove(sagaId);
        if (span != null) {
            span.setStatus(StatusCode.ERROR, "Compensated");
            span.end();
        }
    }

    @Override
    public void onRecoveryClaimed(String sagaId, String ownerId) {
        recoveryClaimCounter.add(1,
            Attributes.of(AttributeKey.stringKey("recovery.owner"), ownerId));
    }
}
```

### Metrics Summary

| Metric | Type | Description |
|---|---|---|
| `saga.started` | Counter | Total sagas started |
| `saga.completed` | Counter | Total sagas completed successfully |
| `saga.compensated` | Counter | Total sagas compensated (step failure) |
| `saga.escalated` | Counter | Total sagas escalated (compensation failure) |
| `saga.duration` | Histogram | End-to-end saga duration (ms) |
| `saga.active` | UpDownCounter | Currently active (in-flight) sagas |
| `saga.step.completed` | Counter | Total step completions |
| `saga.step.failed` | Counter | Total step failures (including retries) |
| `saga.step.compensated` | Counter | Total step compensations |
| `saga.step.compensation_failed` | Counter | Total step compensation failures |
| `saga.step.duration` | Histogram | Per-step execution duration (ms) |
| `saga.recovery.claimed` | Counter | Recovery claims by replica |

### Integration

Register the listener via the builder:

```java
SagaManager sagaManager = SagaManagerBuilder.newBuilder()
    .store(store)
    .addEventListener(new OpenTelemetrySagaListener(tracer, meter))
    .build();
```

The `SagaEngine` calls listener methods at each lifecycle point. Multiple listeners can be registered (e.g., OpenTelemetry + custom audit logger).

## Security

### Design Principle

The core engine (`scalardb-saga-core`) contains no authentication or authorization logic. Security is enforced at the deployment boundary:

- **Embedded mode**: In-process — no network boundary. The host application handles auth before calling saga APIs.
- **Daemon mode**: Coordinator server exposes HTTP/gRPC — security is required for production.

This follows the industry pattern: Temporal, Conductor, and Axon Server all ship with auth disabled by default and delegate enforcement to a pluggable or infrastructure layer.

### Transport Security

TLS termination is **not** the coordinator's responsibility. It is handled by infrastructure:

- **Kubernetes**: Service mesh (Istio, Linkerd) provides mTLS transparently between services
- **Non-Kubernetes**: TLS termination at the load balancer or reverse proxy

The coordinator accepts plaintext connections by default. When deployed behind a service mesh or TLS-terminating proxy, the connection between the proxy and the coordinator is localhost or mesh-internal — no application-level TLS configuration needed.

### Daemon Mode: Token Validation and Authorization

Authentication is performed by an external Identity Provider (IdP) — the coordinator never verifies credentials directly. Instead, it validates tokens issued by the IdP and extracts the caller's identity and roles.

The coordinator uses a pluggable `SagaSecurityProvider` interface:

```java
// --- security/SagaSecurityProvider.java ---
public interface SagaSecurityProvider {
    /**
     * Validate the token and extract the caller's identity and roles.
     * Authentication was already performed by the external IdP.
     * Throws SagaAuthenticationException if the token is invalid or expired.
     */
    SagaIdentity verify(SagaAuthRequest request);
}

public record SagaIdentity(String principal, Set<SagaRole> roles) {}

public record SagaAuthRequest(String authorizationHeader, Map<String, String> metadata) {}
```

This works uniformly across HTTP and gRPC:
- **HTTP**: `authorizationHeader` extracted from the `Authorization` header
- **gRPC**: `authorizationHeader` extracted from gRPC metadata via a `ServerInterceptor`

**Built-in implementations:**

| Implementation | Use Case |
|---|---|
| `NoopSecurityProvider` | Development only — allows all requests, assigns `saga:admin` role (default) |
| `JwtSecurityProvider` | Production — validates JWT signature via JWKS, extracts roles from claims |

**Configuration:**

```properties
# Development (default — logs a warning at startup in non-dev profiles)
scalar.db.saga.security.enabled=false

# Production — JWT validation
scalar.db.saga.security.enabled=true
scalar.db.saga.security.provider=jwt
scalar.db.saga.security.jwt.issuer=https://idp.example.com
scalar.db.saga.security.jwt.jwks-uri=https://idp.example.com/.well-known/jwks.json
scalar.db.saga.security.jwt.roles-claim=roles
```

`JwtSecurityProvider` fetches the IdP's public keys from the JWKS endpoint at startup (and periodically refreshes). For each request, it:
1. Extracts the `Bearer` token from the `Authorization` header
2. Validates the signature against the IdP's public key
3. Checks `iss`, `exp`, and `aud` claims
4. Maps the `roles-claim` values to `SagaRole` enum values

Operators can implement `SagaSecurityProvider` for custom token formats or integration with non-OIDC identity systems.

### Authorization (RBAC)

Three roles, checked before dispatching to `SagaAdminService`:

| Role | Permissions |
|---|---|
| `saga:read` | `listSagas`, `getSagaDetail`, `getMetrics` |
| `saga:write` | `saga:read` + start/cancel sagas |
| `saga:admin` | `saga:write` + `forceComplete`, `triggerCompensation`, `retrySaga`, `resetEscalated` |

Role assignment is the `SagaSecurityProvider`'s responsibility — it returns `SagaIdentity` with the caller's roles extracted from the token claims.

### Async Callback Authentication (Daemon Mode)

The async step completion endpoint (`POST /api/sagas/{sagaId}/steps/{stepName}/complete`) is called by external participants, not by saga clients. Standard JWT auth is insufficient because:
- Participants may not share the same IdP as the coordinator
- A participant should only complete its own step, not arbitrary steps

When a step returns `StepResult.pending()`, the engine generates a step-scoped HMAC callback token:

```java
long issuedAt = Instant.now().getEpochSecond();
String token = HmacUtils.hmacSha256Hex(callbackSecret,
    sagaId + ":" + stepName + ":" + issuedAt);
```

The token and timestamp are included in the callback URL returned to the participant:

```
POST /api/sagas/{sagaId}/steps/{stepName}/complete?token={hmac}&iat={issuedAt}
```

The callback endpoint validates the token before processing:
1. Recompute the HMAC from the URL parameters (`sagaId`, `stepName`, `iat`)
2. Compare using `MessageDigest.isEqual()` (constant-time) to prevent timing attacks
3. Reject if `iat` is older than the step timeout (default: saga-level `timeoutMillis`)

Requests with missing, invalid, or expired tokens are rejected with `401 Unauthorized`. The `callbackSecret` is configured per coordinator instance:

```properties
scalar.db.saga.security.callback-secret=<random-256-bit-key>
```

This is the same pattern as Oracle MicroTx's signed transaction token (`Oracle_Tmm_Tx_Token`), simplified to HMAC-SHA256 with time-bounded validity.

### Embedded Mode: Framework Integration

In embedded mode, `SagaManager` and `SagaAdminService` are in-process Java objects — there is no network boundary. The host application handles security via its framework:

- **Spring**: `SagaAdminController` annotated with `@PreAuthorize("hasRole('SAGA_ADMIN')")` by default
- **Quarkus**: `SagaAdminResource` annotated with `@RolesAllowed("saga-admin")` by default

These annotations are configurable — the host application can override them to match its existing auth model.

---

# Part VI: LRA Compatibility

## MicroProfile LRA Compatibility

### Overview: MicroProfile LRA Spec

MicroProfile LRA (Long Running Actions) is the standard specification for saga-style distributed transactions in the Jakarta EE / MicroProfile ecosystem. Latest version: **2.0.2** (Feb 2026).

LRA defines:
- **Annotations**: `@LRA`, `@Compensate`, `@Complete`, `@Status`, `@Forget`, `@Leave`, `@AfterLRA`
- **A separate LRA Coordinator** service with a REST API
- **HTTP-based participant protocol** (PUT for compensate/complete, GET for status, DELETE for forget)
- **LRA context propagation** via `Long-Running-Action` HTTP header
- **Participant status model**: `Active → Compensating → Compensated` (or `FailedToCompensate`)

The reference implementation is **Narayana LRA** (Red Hat/JBoss), integrated into Quarkus via the `quarkus-narayana-lra` extension.

### Architectural Gap: Our Design vs. LRA

```
Our design (daemon mode):               MicroProfile LRA:

  ┌─────────────────────┐               ┌─────────────────────┐
  │  Saga Coordinator    │               │  LRA Coordinator     │
  │  (SagaEngine)        │               │  (Narayana)          │
  └──────────┬──────────┘               └──────────┬──────────┘
             │ gRPC / HTTP                         │ HTTP PUT /compensate
             │                                     │ HTTP PUT /complete
  ┌──────────┼──────────────┐           ┌──────────┼──────────────┐
  ▼          ▼              ▼           ▼          ▼              ▼
Service A  Service B     Service C   Service A  Service B     Service C
                                     @Compensate @Compensate  @Compensate
                                     @Complete   @Complete    @Complete

Key differences:
  - Step interface (Java) vs. JAX-RS annotations
  - gRPC + HTTP vs. HTTP-only
  - Saga state in ScalarDB vs. Narayana object store
```

### LRA Annotation Reference

| Annotation | HTTP Method | Required? | Purpose |
|---|---|---|---|
| `@LRA` | (on business method) | Yes | Starts/joins an LRA; `Type`: REQUIRED, REQUIRES_NEW, MANDATORY, SUPPORTS, NOT_SUPPORTED, NEVER, NESTED |
| `@Compensate` | PUT | Yes (or @AfterLRA) | Called when LRA is cancelled (failure path); must be idempotent |
| `@Complete` | PUT | No | Called when LRA is closed (success path) |
| `@Status` | GET | Only if async | Returns current participant status (for async compensate/complete) |
| `@Forget` | DELETE | Only if FailedTo* | Tells participant to clean up after FailedToCompensate/FailedToComplete |
| `@Leave` | PUT | No | Removes participant from LRA before method executes |
| `@AfterLRA` | PUT | No (alt to @Compensate) | Notifies participant when LRA reaches terminal state |

### LRA Lifecycle

Every participant service includes the MicroProfile LRA library, which installs a JAX-RS filter. The filter intercepts requests, talks to the coordinator to start/join LRAs, and closes or cancels based on the method's return status. Participants do not call the coordinator explicitly — the filter handles it.

**Success path (close):**

```
Client          Service A                       Coordinator                     Service B
                (LRA library inside)                                            (LRA library inside)
  │                  │                               │                               │
  │                  │                               │                               │
① │── POST /transfer ►│                               │                               │
  │                  │                               │                               │
② │             A's LRA filter intercepts:            │                               │
  │                  ├── POST /start ────────────────►│                               │
  │                  │◄── 201 Created (LRA-1 URI) ──┤                               │
  │                  │                               │                               │
③ │             A's LRA filter enlists A:             │                               │
  │                  ├── PUT /LRA-1 (join) ──────────►│ registers A's callbacks       │
  │                  │   body: compensate=/a/comp     │                               │
  │                  │         complete=/a/done       │                               │
  │                  │◄── 200 OK ───────────────────┤                               │
  │                  │                               │                               │
④ │             A executes debit() (business logic)   │                               │
  │                  │                               │                               │
⑤ │             A calls Service B (LRA-1 propagated via header):                      │
  │                  ├── POST /b/credit ─────────────────────────────────────────────►│
  │                  │   header: Long-Running-Action: LRA-1                           │
  │                  │                               │                               │
⑥ │                  │                          B's LRA filter intercepts, enlists B: │
  │                  │                               │◄── PUT /LRA-1 (join) ─────────┤
  │                  │                               │    body: compensate=/b/comp    │
  │                  │                               │          complete=/b/done      │
  │                  │                               │──► 200 OK ───────────────────►│
  │                  │                               │                               │
⑦ │                  │                          B executes credit() (business logic)  │
  │                  │◄── 200 OK ────────────────────────────────────────────────────┤
  │                  │                               │                               │
⑧ │             A's LRA filter sees 2xx return → close LRA:                           │
  │                  ├── PUT /LRA-1/close ───────────►│                               │
  │                  │                               │                               │
⑨ │                  │  Coordinator calls @Complete on all participants:               │
  │                  │◄── PUT /a/done ───────────────┤                               │
  │                  │    @Complete executed          │                               │
  │                  │──► 200 OK ───────────────────►│                               │
  │                  │                               ├── PUT /b/done ────────────────►│
  │                  │                               │◄── 200 OK (@Complete) ────────┤
  │                  │◄── 200 Closed ───────────────┤                               │
  │                  │                               │                               │
⑩ │◄── 200 OK ──────┤                               │                               │
  │  (transfer complete)                             │                               │
```

**Failure path (cancel):**

```
Client          Service A                       Coordinator                     Service B
  │                  │                               │                               │
  │                  │                               │                               │
① │── POST /transfer ►│                               │                               │
  │                  │                               │                               │
②-④  (same: start LRA, enlist A, debit succeeds)     │                               │
  │                  │                               │                               │
⑤ │             A calls Service B:                    │                               │
  │                  ├── POST /b/credit ─────────────────────────────────────────────►│
  │                  │                               │                               │
⑥ │                  │                          B's LRA filter enlists B              │
  │                  │                               │                               │
⑦ │                  │                          B's credit() fails:                   │
  │                  │◄── 500 Internal Server Error ─────────────────────────────────┤
  │                  │                               │                               │
⑧ │             A's LRA filter sees failure → cancel LRA:                             │
  │                  ├── PUT /LRA-1/cancel ──────────►│                               │
  │                  │                               │                               │
⑨ │                  │  Coordinator calls @Compensate on all participants:             │
  │                  │                               ├── PUT /b/comp ────────────────►│
  │                  │                               │◄── 200 OK (@Compensate) ──────┤
  │                  │                               │                               │
  │                  │◄── PUT /a/comp ───────────────┤                               │
  │                  │    @Compensate (reverse debit) │                               │
  │                  │──► 200 OK ───────────────────►│                               │
  │                  │◄── 200 Cancelled ────────────┤                               │
  │                  │                               │                               │
⑩ │◄── 500 ─────────┤                               │                               │
  │  (transfer failed, debit reversed)               │                               │
```

**Async completion (participant needs time):**

```
Coordinator                     Service A
     │                               │
     ├── PUT /a/comp ───────────────►│
     │◄── 202 Accepted ─────────────┤  "I'm working on it"
     │                               │
     │  (coordinator polls)          │
     ├── GET /a/status ─────────────►│
     │◄── 200 Compensating ─────────┤  "still working"
     │                               │
     │  (coordinator polls again)    │
     ├── GET /a/status ─────────────►│
     │◄── 200 Compensated ──────────┤  "done"
     │                               │
     │  (if it had failed instead:)  │
     ├── GET /a/status ─────────────►│
     │◄── 200 FailedToCompensate ───┤  "I can't undo this"
     │                               │
     │  (later, after manual resolution:)
     ├── DELETE /a/forget ──────────►│  @Forget — "clean up, we're done"
     │◄── 200 OK ───────────────────┤
```

**Key differences from our saga/TCC model** (this is why Phase 6 builds a separate `LraCoordinator` module rather than layering on `SagaEngine`):

| Aspect | SagaEngine (saga/TCC model) | MicroProfile LRA |
|--------|--------------------------|-------------------|
| Who drives execution | Coordinator calls `step.execute()` for each step | Client calls services directly; coordinator only manages lifecycle |
| Step discovery | Declared upfront in saga definition (Java builder or JSON/YAML) | Dynamic — participants self-register at runtime as LRA context propagates |
| Orchestration | Pure orchestration — coordinator drives the entire flow | Hybrid — client drives the call chain, coordinator handles completion/compensation |
| Who calls participants | Coordinator calls each step directly | Client calls next service; coordinator only calls `@Complete`/`@Compensate` callbacks |
| Library on participants | Not required | Required — every participant needs the LRA library for the JAX-RS filter |
| Success callback | None — on success, nothing extra happens | `@Complete` called on all participants |
| Transport | gRPC + HTTP | HTTP only |

### LRA Participant Status Model

```java
public enum ParticipantStatus {
    Active,              // Not yet asked to Complete or Compensate
    Compensating,        // Currently compensating
    Compensated,         // Successfully compensated
    FailedToCompensate,  // Could not compensate (needs @Forget)
    Completing,          // Currently completing
    Completed,           // Successfully completed
    FailedToComplete     // Unable to complete (needs @Forget)
}
```

### Implementation: Add LRA Coordinator to Daemon Mode (Phase 6)

The LRA coordinator is a **separate module** that runs alongside the saga engine in the daemon process. It does NOT layer on top of `SagaEngine` because the execution models are fundamentally different: our saga engine drives step execution (calls `step.execute()` in order), while LRA has the client drive execution (the coordinator only manages participant enlistment and close/cancel callbacks). However, the LRA coordinator **shares infrastructure** with the saga engine — `SagaStore` for persistence and the recovery mechanism for crash handling.

```
┌──────────────────────────────────────────────┐
│  Daemon Process                               │
│                                               │
│  ┌─────────────────┐  ┌────────────────────┐ │
│  │ SagaEngine       │  │ LraCoordinator      │ │  ← separate execution logic
│  │ (drives steps    │  │ (tracks enlisted    │ │
│  │  in order)       │  │  participants,      │ │
│  │                  │  │  manages close/     │ │
│  │                  │  │  cancel callbacks)  │ │
│  └──────┬───────────┘  └──────┬─────────────┘ │
│         │                      │               │
│  ┌──────▼──────────────────────▼─────────────┐ │
│  │  SagaStore (ScalarDB)                      │ │  ← shared persistence
│  │  Recovery mechanism (periodic scan)        │ │  ← shared recovery
│  └────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
         │                        │
         │ gRPC / HTTP            │ HTTP callbacks (LRA protocol)
    ┌────┴────┐              ┌────┴────┬──────────┐
    ▼         ▼              ▼         ▼          ▼
 Step A    Step B         Service X  Service Y  Service Z
 (saga)    (saga)         @LRA       @LRA       @LRA
                          @Compensate @Compensate @Compensate
```

**Why LRA is a separate module, not a layer on SagaEngine:**

| Aspect | SagaEngine (our saga/TCC model) | LraCoordinator (LRA model) |
|---|---|---|
| **Who drives execution** | Coordinator calls `step.execute()` for each step | Client calls services directly; coordinator only calls close/cancel callbacks |
| **Participant discovery** | Static — all steps declared upfront in Java builder or JSON/YAML | Dynamic — participants self-register via `PUT /lra/{id}` (join) during execution |
| **Workflow definition** | Predefined saga definition (step order, compensation mapping) | No predefined workflow — participants enlist as the client's call chain progresses |
| **Success callback** | None (execution = success) | `@Complete` called on all participants |
| **What coordinator stores** | Full saga state (events, step status, context) | Participant registry (callback URLs per LRA) + LRA lifecycle state |

**What they share:**
- `SagaStore` — append-only events work for both saga and LRA state persistence
- Recovery mechanism — periodic scan of stale sagas/LRAs
- Daemon infrastructure — same HTTP server, ScalarDB connection, config

**What needs to be built:**

| Work Item | Est. LoC | Time |
|---|---|---|
| LRA REST API (start/close/cancel/join/recovery per MicroProfile spec) | ~500 | 1.5-2 days |
| LRA Coordinator execution logic (lifecycle management, close/cancel orchestration, participant ordering) | ~250 | 1-1.5 days |
| Participant registry (track callback URLs per LRA) | ~150 | 0.5 day |
| HTTP callback client (call @Compensate/@Complete endpoints) | ~200 | 0.5-1 day |
| Async status polling (@Status + retry loop) | ~150 | 0.5-1 day |
| @Forget / @AfterLRA lifecycle | ~100 | 0.5 day |
| LRA state persistence (adapt SagaStore for LRA events + recovery) | ~200 | 1-1.5 days |
| Quarkus extension (build processor + dev services) | ~400 | 2-3 days |
| Unit tests (all classes) | ~1,200 | 2-2.5 days |
| TCK compliance testing | ~500 | 1.5-2 days |
| **Total** | **~3,650** | **~10.5-14 days** |

See [Phase 6 in the Implementation Plan](#phase-6-lra-compliance) for the consolidated breakdown.

**Pros**: Fully LRA-compliant, passes TCK, interoperable with any LRA participant
**Cons**: Requires Phase 3 daemon mode as a prerequisite, HTTP overhead per callback

### Quarkus Integration Details

The Quarkus extension would follow the existing patterns found in `quarkus-narayana-lra` and `quarkus-scalardb`:

| Pattern | How It Applies |
|---|---|
| **Two-module structure** | `deployment/` (build-time Jandex scanning, @BuildStep) + `runtime/` (CDI producers, recorder) |
| **Recorder pattern** | `ScalarDbSagaRecorder` captures initialization at build time, replays at runtime |
| **CDI Producer** | Produces `SagaManager` for injection, marked `@Unremovable` |
| **Build-time annotation scan** | Jandex scans for `@LRA`, `@Compensate`, `@Complete` — validates at build, not runtime |
| **Dev Services** | Auto-start LRA coordinator in dev/test mode via TestContainers |
| **Extension descriptor** | `META-INF/quarkus-extension.yaml` with metadata, keywords, guide links |

The `quarkus-scalardb` extension already provides `DistributedTransactionManager` as a CDI bean — the saga extension can directly `@Inject` it.

---

# Part VII: Implementation Plan

**Estimation assumptions**: A skilled engineer using AI (Claude Code) for all development. Estimates include comprehensive unit tests and integration tests covering all classes. AI accelerates boilerplate and test scaffolding (~35% faster than manual), but transactional correctness, crash recovery testing, and spec compliance remain human-driven.

### Testing Strategy

| Phase | Unit (Mockito) | Integration (SagaTestHarness) | Transport (WireMock) |
|---|---|---|---|
| **1: Core Engine** | Engine (incl. inline compensation), RetryPolicy, RecoveryManager, etc. | SagaTestHarness (ScalarDB + SQLite + MockStep): saga lifecycle, crash recovery, TCC | N/A — no transport layer |
| **2a: Declarative steps** | DeclarativeBindingStep/TccStep, DeclarativeExpressions, CallSpecCodec | Transport adapters, declarative + code steps end-to-end | HttpTransportAdapter, SagaHttpClient with WireMock (gRPC future) |
| **2b: Spring Boot** | Annotation scanner, auto-configuration | Spring Boot Test: @SagaStep scanning, property binding, controllers | N/A |
| **3: Daemon Mode** | REST/gRPC handlers, RemoteSagaManager | In-process daemon + HTTP client: full REST API lifecycle | Daemon + WireMock: client → REST API → engine → participants |
| **4a: Observability** | SagaEventListener, metrics | OpenTelemetry test SDK: verify spans/metrics | N/A |
| **5: Admin API** | SagaAdminService, SagaQuery | SagaTestHarness: admin operations on real saga state | N/A |

### Sample Application (Incremental)

A money transfer sample application grows with each phase, serving as E2E validation, demo, and user reference.

| Phase | What's added |
|---|---|
| **2a** | Basic sample: orchestrator + debit/credit participant services + HTTP transport + docker-compose |
| **2b** | Spring Boot version of the orchestrator (annotation-based) |
| **3** | Daemon mode variant: daemon + client SDK calling REST API |
| **4a** | OpenTelemetry: Jaeger/Zipkin in docker-compose, trace visualization |
| **4c** | Dev server: local development workflow demo |
| **5** | Admin API: admin dashboard usage, manual retry/compensation demo |

## Phase 1: Core Engine

**Scope:** Core engine + SagaStore + recovery (with status table) + timeouts + virtual threads + graceful shutdown + saga versioning + compensation retry + SagaContext validation + TCC mode + MIXED recovery strategy (pivot transaction).

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **api/** | | | |
| `Step.java` | Interface (3 methods: `getName`, `execute`, `compensate`) | ~15 | Trivial |
| `TccStep.java` | Standalone TCC interface: `reserve()`, `confirm()`, `cancel()` | ~15 | Trivial |
| `StepResult.java` | Simple data class | ~30 | Trivial |
| `SagaContext.java` | Public interface (3 methods: `getSagaId`, `get`, `put`) — what Step implementations see | ~10 | Trivial |
| `SagaStatus.java` | Enum (6 values: RUNNING, CONFIRMING, COMPLETED, COMPENSATING, COMPENSATED, ESCALATED) | ~10 | Trivial |
| `SagaDefinition.java` | POJO + inner `StepDefinition` (with `pivot` flag) + SagaMode + RecoveryStrategy (BACKWARD/FORWARD/MIXED/PREDEFINED) + `getPivotIndex()` + validation | ~80 | Trivial |
| `SagaStateSnapshot.java` | Read-only view of saga state | ~40 | Trivial |
| `SagaManager.java` | Interface (13 methods incl. register ×2, start ×2, startAsync ×4, resume, compensate, getStateSnapshot, completeStep, startRecovery) | ~35 | Trivial |
| `SagaCallback.java` | Callback interface (onCompleted, onCompensated, onEscalated) | ~10 | Trivial |
| `RetryPolicy.java` | Config POJO + default/compensation/confirm factories | ~50 | Trivial |
| `Named.java` | Qualifier annotation for named resource injection | ~5 | Trivial |
| **engine/** | | | |
| `ExecutionContext.java` | Engine-internal: implements `SagaContext`, adds type validation + event sequencing + state tracking + failure tracking | ~70 | Trivial |
| `SagaEngine.java` | createSaga (server- or client-supplied ID) + executeSaga + execute (convenience) + resumeFrom + unified pivot-based loop (`executeSagaSteps`) + `expandTccPlan` (TCC→StepWithPolicy expansion) + plan caching + retry + timeout + inline compensation + error routing | ~310 | **Medium** |
| `StepWithPolicy.java` | Record bundling `Step` + `RetryPolicy` + `stepTimeoutMillis` (internal) | ~5 | Trivial |
| `TccReserveStep.java` | Wraps `TccStep.reserve()`/`cancel()` → `Step.execute()`/`compensate()` | ~20 | Trivial |
| `TccConfirmStep.java` | Wraps `TccStep.confirm()` → `Step.execute()`, compensate = no-op | ~20 | Trivial |
| `SagaDefinitionRegistry.java` | Definition registration + two-tier versioned lookup (in-memory → store fallback) | ~40 | Trivial |
| `EmbeddedSagaManager.java` | Delegates to engine + recovery + startAsync (virtual thread submission + callback dispatch in finally block). completeStep is daemon mode only. | ~140 | Low |
| `SagaManagerBuilder.java` | DI-free builder: .store(), .resource(), .stepResolver(), .ownerId(), .shutdownMode(), .shutdownTimeoutMillis(), .build() | ~60 | Low |
| `StepResolver.java` | FunctionalInterface: resolve step by name + FQCN | ~5 | Trivial |
| `ReflectiveStepResolver.java` | Default StepResolver: reflection + ResourceRegistry constructor injection + singleton caching | ~80 | Low |
| `ResourceRegistry.java` | Type-keyed + optional name-qualified resource registry for constructor injection | ~60 | Low |
| **api/** (continued) | | | |
| `SagaDefinitionParser.java` | Jackson JSON/YAML → SagaDefinition (detects format by file extension; rejects unrecognized extensions) | ~80 | Low |
| **store/** | | | |
| `SagaStore.java` | Interface (13 methods: createSaga, registerDefinition, getDefinition, appendEvent, recordTransition, findRecoverable, claimForRecovery, markForRecovery, getEvents, getEventCount, getStateSnapshot, deleteSaga; + nested Recoverables record + RecoverablesCursor interface) | ~50 | Trivial |
| `SagaEvent.java` | Event types + factory methods (each saga-level event carries its `targetStatus`) | ~90 | Low |
| `SagaSchema.java` | 3 TableMetadata definitions (saga_events, saga_state, saga_definitions) + bucketOf() + createAll | ~100 | Low |
| `ScalarDbSagaStore.java` | Append-only events + bucket-partitioned saga_state (DELETE+INSERT for status transitions) + bucket-parallel recovery scan + conflict-based claiming + definition persistence | ~450 | **Medium** |
| **recovery/** | | | |
| `RecoveryConfig.java` | Record (4 fields: recoveryTimeoutMillis, recoveryIntervalSeconds, compensationGracePeriod, clock) | ~5 | Trivial |
| `SagaRecoveryManager.java` | Periodic scan of saga_state + event replay + resume via `resumeFrom()` (handles all modes uniformly: RUNNING, CONFIRMING, COMPENSATING) + time-based escalation | ~220 | **Medium** |
| **retention/** | | | |
| `RetentionConfig.java` | Record (4 fields: retentionPeriod, cleanupIntervalSeconds, batchSize, clock) | ~5 | Trivial |
| `SagaRetentionManager.java` | Periodic scan of terminal saga_state entries (CK prefix per bucket + status) + delete via `deleteSaga()` | ~100 | Low |
| **timeout/** | | | |
| `TimeoutPolicy.java` | Per-step and per-saga deadline calculation | ~30 | Trivial |
| **exception/** | | | |
| 10 exception classes | StepExecutionException (with `retryable` flag), StepCompensationException, StepTimeoutException, SagaTimeoutException, SagaPersistenceException, SagaAlreadyExistsException, SagaDefinitionException, SagaDefinitionNotFoundException, SagaNotFoundException, SagaConcurrentModificationException | ~80 | Trivial |
| **testing/** | | | |
| `MockStep.java` | Configurable mock with execution/compensation history tracking | ~80 | Low |
| `SagaTestHarness.java` | Builder + execute + assertions (executionOrder, compensationOrder, finalContext). Uses `ScalarDbSagaStore` backed by in-memory SQLite. | ~150 | Medium |
| `CrashingStoreDecorator.java` | Decorator that throws `SimulatedCrashException` at configured step boundaries | ~60 | Low |
| **Tests** | | | |
| Unit tests (all classes: engine, compensation, retry, store, recovery, parser, builder, context, TCC) | | ~1,800 | Medium |
| Integration tests (via SagaTestHarness: Saga mode + TCC mode, crash recovery, timeout) | | ~400 | Medium |
| **Total** | | **~4,235** | |

### Timeline

| Work | Time |
|---|---|
| Scaffolding: module setup, Gradle config, dependencies | 0.25 day |
| API layer: all interfaces, enums, POJOs | 0.25 day |
| SagaSchema + ScalarDbSagaStore | 1-1.5 days |
| SagaEngine (incl. inline compensation) + RetryPolicy (Saga + TCC modes) | 1-1.5 days |
| SagaRecoveryManager + SagaRetentionManager + graceful shutdown | 0.5 day |
| Parser + Builder wiring + timeout enforcement | 0.25 day |
| SagaTestHarness + MockStep + CrashingStoreDecorator | 0.5 day |
| Tests: unit tests for all classes + integration tests (via harness) | 1.5-2.5 days |
| **Total** | **~5.5-7.5 working days** |

### Where the Time Actually Goes

The ~1,750 LoC of production code is not the hard part — AI generates the API layer, parser, builder, and enums quickly. The hard parts are:

1. **SagaStore correctness** (~30% of effort) — Getting the append-only event writes right, ensuring sequence numbering is correct, handling `CommitConflictException` / `AbortException` edge cases in ScalarDB, and ensuring event replay + `saga_state` recovery scan work correctly.
2. **Comprehensive testing** (~40% of effort) — Unit tests for every class (~1,800 LoC) plus integration tests (~400 LoC). AI generates test scaffolding fast, but the edge cases that matter (crash recovery simulation, concurrent conflict handling, timeout mid-step, TCC confirm-after-partial-failure) require careful human reasoning.
3. **Retry + error classification** (~10%) — Straightforward logic, but the interaction between retry exhaustion → compensation → compensation failure → escalation has several paths to test.

**Bottom line: ~1 week for a skilled engineer with AI. Production code is ~1,750 LoC but tests add ~2,200 LoC. The devil is in the transactional edge cases and their test coverage.**

## Phase 2: Communication & Framework Integration

**Scope:** Declarative service steps over HTTP (Layer 2) + the `SagaHttpClient` code-step client, Spring Boot integration, and Quarkus integration. The HTTP path (Phase 2a) is implemented; gRPC transport, framework integration, and the participant SDK are not yet built.

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **Phase 2a: Declarative Service Steps + HTTP Transport** (✅ implemented) | | | |
| **api/** (declarative model + code-step client) | | | |
| `CallSpec.java` / `HttpCall.java` / `HttpMethod.java` | Sealed transport-tagged call spec + HTTP variant/builder | ~220 | Low |
| `CallSpecCodec.java` | Single JSON (de)serializer for every `CallSpec` (parser + store share it) | ~120 | Medium |
| `SagaHttpClient.java` / `SagaHttpResponse.java` / `SagaHttpClientProvider.java` / `Named.java` | Fluent HTTP client for code steps + `@Named` injection | ~180 | Low |
| Updated `SagaDefinition` / `SagaDefinitionParser` | Sealed `ClassStep`/`ServiceStep` + per-phase `CallSpec`; parse `service`/`execution`/`compensation`/`reservation`/`confirmation`/`cancellation` | ~250 | Medium |
| **transport/** | | | |
| `TransportAdapter.java` + `TransportException.java` | SPI: `call(CallSpec, SagaContext, stepName)` + retryable classification | ~40 | Trivial |
| `HttpTransportAdapter.java` | Resolve templates, build request, call, extract output | ~120 | Medium |
| `DeclarativeBindingStep.java` / `DeclarativeBindingTccStep.java` | Bridge a `ServiceStep`'s `CallSpec`s to `Step`/`TccStep` | ~120 | Low |
| `DeclarativeExpressions.java` | `${...}` resolution + `$.path`/`$body` extraction (percent-encoded path segments) | ~150 | Medium |
| `HttpEndpoint` / `HttpExchange` / `HttpServiceConfig` / `OutboundHttpPolicy` / `HttpStatusClassifier` | Per-endpoint HTTP machinery: one client/exchange/policy; SSRF allowlist + body limits + no-redirect + correlation headers + status classification | ~450 | Medium |
| **engine/** | | | |
| `HttpEndpointRegistry` / `ResourceRegistry` / updated `StepInstantiator` | Resolve endpoints by name; dispatch sealed `StepDefinition` (`ClassStep` → resolver, `ServiceStep` → binding step) | ~200 | Medium |
| Tests (declarative unit + transport edge cases + WireMock integration) | | ~900 | Medium |
| **Phase 2a Total** | | **~3,000** | |
| | | | |
| **Phase 2a (gRPC transport)** — future | | | |
| `GrpcCall.java` (new `CallSpec` subtype) + `GrpcTransportAdapter.java` | Protobuf message building from maps + gRPC metadata propagation; plugs into the same SPI, no engine change | ~250 | Medium |
| | | | |
| **Phase 2b: Spring Boot Integration (`scalardb-saga-spring`)** | | | |
| `SagaAutoConfiguration.java` | `@Bean SagaManager` from `SagaProperties` via builder | ~80 | Low |
| `SagaProperties.java` | Spring Boot config properties | ~40 | Trivial |
| `SagaAnnotationScanner.java` | `SmartInitializingSingleton` scanning `@SagaStep`/`@SagaCompensation` | ~120 | Medium |
| `SagaCallbackController.java` | REST endpoint for async step completion (daemon mode only; requires Phase 3) | ~30 | Trivial |
| `SagaAdminController.java` | REST exposure of `SagaAdminService` | ~60 | Low |
| `spring.factories` / `AutoConfiguration.imports` | Auto-configuration registration | ~5 | Trivial |
| Tests (auto-config, annotation scanning, property binding, controller) | | ~400 | Medium |
| **Phase 2b Total** | | **~735** | |
| | | | |
| **Phase 2c: Quarkus Integration (`scalardb-saga-quarkus`)** | | | |
| `SagaBuildStep.java` | Jandex annotation scan at build time | ~120 | Medium |
| `SagaRecorder.java` | Runtime recorder for saga registration | ~60 | Low |
| `SagaCdiProducer.java` | CDI `@Produces SagaManager` | ~40 | Trivial |
| `SagaCallbackResource.java` | JAX-RS resource for async step completion (daemon mode only; requires Phase 3) | ~30 | Trivial |
| `quarkus-extension.yaml` | Extension descriptor | ~10 | Trivial |
| Tests (build-time scan, CDI integration, recorder) | | ~350 | Medium |
| **Phase 2c Total** | | **~610** | |
| | | | |
| **Phase 2d: Participant Protocol & SDK (`scalardb-saga-participant`)** | | | |
| **protocol/** | | | |
| Participant HTTP Protocol spec (documented contract) | Request/response format, error signaling (`X-Saga-Retryable`), correlation headers (`X-Saga-Id`, `X-Saga-Step`) | ~50 | Trivial |
| **participant/** | | | |
| `SagaParticipantServer.java` | Lightweight HTTP server (Javalin) hosting `Step` implementations, implementing the participant protocol | ~120 | Medium |
| `StepEndpoint.java` | HTTP endpoint: deserialize `SagaContext`, call `Step`, serialize `StepResult`, map errors to HTTP | ~100 | Low |
| `ParticipantConfig.java` | Config POJO (port, step registrations) | ~30 | Trivial |
| Tests (participant SDK, protocol compliance, error mapping) | | ~300 | Low |
| **Phase 2d Total** | | **~600** | |
| | | | |
| **Phase 2 Total** | | **~3,370** | |

### Timeline

| Work | Time |
|---|---|
| Phase 2a: Declarative service steps + HTTP transport + code-step client + tests | 2-3 days |
| Phase 2b: Spring Boot auto-config + annotation scanner + tests | 2-3 days |
| Phase 2c: Quarkus extension (build step + recorder) + tests | 2-3 days |
| Phase 2d: Participant protocol spec + Java participant SDK + tests | 1-1.5 days |
| **Total** | **~7-10 working days** |

### Where the Time Actually Goes

1. **Declarative step communication** (~25% of Phase 2a effort) — The expression resolution (`${...}` context lookup, `$.path`/`$body` response extraction in `DeclarativeExpressions`) and the per-endpoint HTTP machinery (SSRF allowlist, body limits, status classification, correlation headers) are the tricky parts. The future gRPC adapter adds protobuf map-to-message conversion.
2. **Annotation scanning** (~30% of Phase 2b/2c effort) — Correctly matching `@SagaStep` to `@SagaCompensation` methods, handling inheritance, and generating `Step` instances from annotated methods requires careful reflection/Jandex work.
3. **Comprehensive testing** (~35% of Phase 2 effort) — Unit tests for all classes in each sub-phase. Framework-specific test setup (`@SpringBootTest`, `@QuarkusTest`) has its own overhead. Transport edge cases (malformed protobuf, HTTP error codes, connection failures) need thorough coverage.

## Phase 3: Daemon Mode

**Scope:** Package the saga engine as a standalone coordinator process with a REST API for external clients to start, monitor, and manage sagas, plus a Java client SDK (`RemoteSagaManager`) that implements the same `SagaManager` interface. The daemon hosts the same `SagaEngine`, `SagaStore`, and `SagaRecoveryManager` from Phase 1 — it simply adds a process boundary and a client-facing API. How steps are invoked (in-process code step, or declarative service step over HTTP/gRPC) is configured per step and per HTTP endpoint (Phase 2) and is independent of the deployment mode.

### What Daemon Mode Adds

In embedded mode, application code calls `sagaManager.start()` or `startAsync()` directly. Daemon mode wraps this in a standalone process so that external clients interact with the engine over HTTP:

```
Embedded mode:                           Daemon mode:

  Application code                         External client (any language)
       │                                        │
       ▼                                        ▼  HTTP
  sagaManager.start(def, ctx)     (sync)   POST /sagas { saga, input }         → 200 (blocks)
  sagaManager.startAsync(def, ctx)(async)  POST /sagas?async=true { ... }      → 202 (immediate)
  sagaManager.start(id, ...)      (idem.)  PUT  /sagas/{id} { saga, input }    → 200 or 409
       │                                        │
       ▼                                        ▼
  ┌────────────────┐                       ┌──────────────────────────┐
  │ SagaEngine     │                       │  Saga Coordinator         │
  │ SagaStore      │  ← same engine        │  ┌────────────────────┐  │
  │ Recovery       │                       │  │ REST API            │  │
  └────────────────┘                       │  └────────┬───────────┘  │
                                           │  ┌────────▼───────────┐  │
                                           │  │ SagaEngine          │  │  ← same engine
                                           │  │ SagaStore           │  │
                                           │  │ Recovery            │  │
                                           │  └────────────────────┘  │
                                           └──────────────────────────┘
```

Step invocation is orthogonal — both modes use the same step resolution and invocation mechanisms:
- A step configured with `stepClass` → direct method call (same JVM)
- A step configured with `service` + declarative phases → a declarative service-step call over the configured transport (HTTP today; gRPC future) to the remote service

Either configuration works in either deployment mode.

### Java Client SDK: RemoteSagaManager

`RemoteSagaManager` implements the same `SagaManager` interface used in embedded mode, delegating to the coordinator REST API. Java users can switch between embedded and daemon mode with zero code changes:

```java
// Embedded mode
SagaManager manager = SagaManagerBuilder.newBuilder().store(store).build();

// Daemon mode — same interface
SagaManager manager = RemoteSagaManager.newBuilder()
    .coordinatorUrl("http://coordinator:8080").build();

// Both use the same API — sync and async work identically
String sagaId = manager.start("transferMoney", input);                // sync, server-generated ID
String sagaId = manager.startAsync("transferMoney", input);           // async, server-generated ID
SagaStateSnapshot result = manager.getStateSnapshot(sagaId);          // poll
manager.completeStep(sagaId, "debit", output);                        // async step callback

// Idempotent variant with client-supplied ID (see "Client-Supplied Saga IDs" section)
try {
    manager.start("order-12345-refund", "RefundSaga", input);
} catch (SagaAlreadyExistsException e) {
    // Retry observed an existing saga — inspect e.getExisting() to decide what to do
}
```

`RemoteSagaManager` maps `startAsync()` to `POST /sagas?async=true`, which returns `202 Accepted` with the saga ID. The `SagaCallback` variant polls `GET /sagas/{id}` on a background thread and invokes the callback when the saga reaches a terminal status. The client-supplied ID overloads map to `PUT /sagas/{id}`, which returns `409 Conflict` (with the existing snapshot in the body) when the ID is already in use — `RemoteSagaManager` translates that response into `SagaAlreadyExistsException` so embedded and daemon callers see identical semantics.

### Coordinator REST API

| Endpoint | Method | Description |
|---|---|---|
| `/sagas` | POST | Start a new saga with a server-generated ID. Default: synchronous (blocks until complete, returns `200`). With `?async=true`: returns `202 Accepted` immediately with saga ID. |
| `/sagas/{id}` | PUT | Start a new saga with a **client-supplied ID** (idempotent). Same sync/async semantics as `POST /sagas` (use `?async=true` for async). Returns `409 Conflict` with the existing saga snapshot in the body if the ID is already in use. See "Client-Supplied Saga IDs". |
| `/sagas` | GET | List sagas (with status filter, pagination) |
| `/sagas/{id}` | GET | Get saga status and step details |
| `/sagas/{id}/cancel` | PUT | Request saga cancellation (triggers compensation) |
| `/sagas/{id}/steps/{stepName}/complete` | POST | Complete an async step via external callback (resumes parked saga) |
| `/health` | GET | Health check (ScalarDB connectivity, recovery manager status) |

**Sync vs async `POST /sagas`:**

```
# Synchronous (default) — blocks until saga completes
POST /sagas
{ "saga": "transferMoney", "input": { "amount": 100 } }

→ 200 OK
{ "sagaId": "abc-123", "status": "COMPLETED", "result": { ... } }

# Asynchronous — returns immediately
POST /sagas?async=true
{ "saga": "transferMoney", "input": { "amount": 100 } }

→ 202 Accepted
{ "sagaId": "abc-123", "status": "RUNNING" }

# Poll for result (existing endpoint)
GET /sagas/abc-123
→ 200 OK
{ "sagaId": "abc-123", "status": "COMPLETED", "result": { ... } }
```

**Idempotent create with client-supplied ID (`PUT /sagas/{id}`):**

```
# First call — creates the saga
PUT /sagas/order-12345-refund
{ "saga": "RefundSaga", "input": { "orderId": 12345 } }

→ 200 OK
{ "sagaId": "order-12345-refund", "status": "COMPLETED", "result": { ... } }

# Retry with the same ID — saga already exists
PUT /sagas/order-12345-refund
{ "saga": "RefundSaga", "input": { "orderId": 12345 } }

→ 409 Conflict
{
  "error": "SAGA_ALREADY_EXISTS",
  "sagaId": "order-12345-refund",
  "existing": { "sagaId": "order-12345-refund", "status": "RUNNING", ... }
}

# Async variant — same endpoint, returns 202 on first call, 409 on retry
PUT /sagas/order-12345-refund?async=true
```

The `409 Conflict` body carries the existing saga snapshot so the caller can inspect its status (`RUNNING`, `CONFIRMING`, `COMPENSATING`, `COMPLETED`, `COMPENSATED`, `ESCALATED`) and decide whether to resume, query, or treat the retry as a no-op without needing a follow-up `GET /sagas/{id}`.

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **coordinator/** | | | |
| `CoordinatorServer.java` | Standalone process: CLI args, config loading, Javalin HTTP server, graceful shutdown hook | ~150 | Medium |
| `CoordinatorConfig.java` | Config POJO (port, ScalarDB config path, saga definitions path) | ~40 | Trivial |
| **coordinator/api/** | | | |
| `SagaResource.java` | REST endpoints: start, list, get, cancel sagas | ~200 | Low |
| `HealthResource.java` | Health/readiness check endpoint | ~30 | Trivial |
| `ErrorMapper.java` | Exception-to-HTTP-response mapping | ~40 | Trivial |
| **client/** | | | |
| `RemoteSagaManager.java` | Implements `SagaManager`, delegates to coordinator REST API via HTTP client | ~150 | Low |
| **Tests** | | | |
| Unit tests (all classes: REST API, config, error mapper, RemoteSagaManager) | | ~400 | Low |
| Integration tests (full lifecycle via REST, async polling, recovery after restart) | | ~350 | Medium |
| **Phase 3 Total** | | **~1,400** | |

### Timeline

| Work | Time |
|---|---|
| CoordinatorServer bootstrap + config + graceful shutdown | 0.5-1 day |
| REST API (saga lifecycle + health) | 1-1.5 days |
| RemoteSagaManager (Java client SDK) | 0.5-1 day |
| Tests: unit tests for all classes + integration tests | 1.5-2 days |
| **Total** | **~3.5-5 working days** |

### Where the Time Actually Goes

1. **Comprehensive testing** (~45% of effort) — Unit tests for all classes (REST resources, error mapper, config, RemoteSagaManager) plus integration tests verifying the full lifecycle via REST: start saga → engine executes steps → check status → recovery after coordinator restart. Must test both in-process code steps and remote steps (via declarative service steps / `SagaHttpClient`).
2. **Graceful shutdown** (~20% of effort) — The coordinator must drain in-flight sagas before stopping, mark them for recovery, and respond to health checks correctly during shutdown. This reuses the Phase 1 graceful shutdown logic but must integrate with the HTTP server lifecycle.

## Phase 4: Developer Experience & Observability

**Scope:** OpenTelemetry observability, in-memory testing harness, and local development server.

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **Phase 4a: Observability (OpenTelemetry)** | | | |
| **observability/** | | | |
| `SagaEventListener.java` | Interface with default no-op methods (10 lifecycle events) | ~25 | Trivial |
| `OpenTelemetrySagaListener.java` | Tracer spans per saga/step + 8 metrics instruments (counters, histograms, gauges) | ~150 | Medium |
| Updated `SagaEngine` | Call listener methods at each lifecycle point | ~40 | Low |
| Updated `SagaManagerBuilder` | `addEventListener()` support | ~10 | Trivial |
| Tests (listener invocation, metrics assertions, span lifecycle) | | ~300 | Low |
| **Phase 4a Total** | | **~525** | |
| | | | |
| **Phase 4b: Testing Harness — moved to Phase 1** | | | |
| Moved to Phase 1. `InMemorySagaStore` removed (ScalarDB + SQLite replaces it). | | | |
| **Phase 4b Total** | | **~0** | |
| | | | |
| **Phase 4c: Local Development Server (`scalardb-saga-dev-server`)** | | | |
| **devserver/** | | | |
| `SagaDevServer.java` | Javalin embedded HTTP + SQLite-backed ScalarDB + auto-schema creation | ~150 | Medium |
| `SagaDevServerConfig.java` | Config POJO (port, definition path, auto-open browser) | ~30 | Trivial |
| REST routes | Admin API routes delegating to `SagaAdminService` + static Web UI | ~80 | Low |
| `main()` entry point | CLI arg parsing (`--port`, `--definitions`) | ~30 | Trivial |
| Embedded Web UI | Minimal HTML/JS dashboard (saga list, detail view, action buttons) | ~300 | Medium |
| Tests (server startup, API routes, config loading, CLI args) | | ~350 | Low |
| **Phase 4c Total** | | **~940** | |
| | | | |
| **Phase 4 Total** | | **~1,465** | |

### Timeline

| Work | Time |
|---|---|
| Phase 4a: OpenTelemetry listener + metrics + integration with engine + tests | 0.5-1 day |
| Phase 4b: (moved to Phase 1) | 0 days |
| Phase 4c: SagaDevServer (Javalin + SQLite + web UI + CLI) + tests | 1-1.5 days |
| **Total** | **~1.5-2.5 working days** |

### Where the Time Actually Goes

1. **Web UI** (~30% of Phase 4c effort) — Even a minimal dashboard (saga list with status badges, step timeline, action buttons) requires frontend work. Consider using a pre-built admin template to save time. AI generates HTML/JS/CSS quickly.
3. **OpenTelemetry integration** is straightforward — the `SagaEventListener` callback interface keeps the engine decoupled from any specific telemetry SDK.

## Phase 5: Admin API

| Phase | Scope | Est. LoC | Est. Time |
|---|---|---|---|
| **Phase 5** | Admin API (`SagaAdminService`, `SagaQuery`, `SagaDetail`, `SagaMetrics`) + Web UI enhancements + unit/integration tests for all classes | ~1,200 | 2.5-4 days |

## Phase 6: LRA Compliance

**Scope:** Build an LRA coordinator module that runs alongside the saga engine in the Phase 3 daemon process. The LRA coordinator is a **separate module** from `SagaEngine` because the execution models differ fundamentally (see [Part VI: LRA Compatibility](#implementation-add-lra-coordinator-to-daemon-mode-phase-6) for details). However, it shares `SagaStore` for persistence and the recovery mechanism for crash handling. This makes the coordinator pass the MicroProfile LRA TCK. Requires Phase 3 (daemon mode) as a prerequisite.

| Work Item | Est. LoC | Time |
|---|---|---|
| LRA REST API (start/close/cancel/join/recovery per MicroProfile spec) | ~500 | 1.5-2 days |
| LRA Coordinator execution logic (lifecycle management, close/cancel orchestration, participant ordering) | ~250 | 1-1.5 days |
| Participant registry (track callback URLs per LRA) | ~150 | 0.5 day |
| HTTP callback client (call @Compensate/@Complete endpoints) | ~200 | 0.5-1 day |
| Async status polling (@Status + retry loop) | ~150 | 0.5-1 day |
| @Forget / @AfterLRA lifecycle | ~100 | 0.5 day |
| LRA state persistence (adapt SagaStore for LRA events + recovery) | ~200 | 1-1.5 days |
| Quarkus extension for LRA coordinator (build processor + dev services) | ~400 | 2-3 days |
| Unit tests (all classes: REST API, coordinator, registry, callback client, poller, persistence) | ~1,200 | 2-2.5 days |
| TCK compliance testing | ~500 | 1.5-2 days |
| **Phase 6 Total** | **~3,650** | **~10.5-14 days** |

**Pros**: Fully LRA-compliant, passes TCK, interoperable with any LRA participant
**Cons**: Requires Phase 3 daemon mode as a prerequisite; LRA coordinator is a separate module from SagaEngine (more code to maintain)

See [MicroProfile LRA Compatibility](#microprofile-lra-compatibility) in Part VI for LRA spec details, annotation reference, and lifecycle.

## Phase 7: Additional Transports (gRPC & TCP/Netty)

**Scope:** Add gRPC and TCP/Netty as alternative transports for both coordinator and participant communication, alongside the HTTP transport established in Phases 2-3.

### 7a: gRPC Transport

Add gRPC as an alternative transport for both coordinator and participant communication. The `.proto` files serve as both the **formal API contract** and the **saga definition format** for daemon mode — polyglot clients use generated types to define and submit sagas with compile-time type safety.

#### Protocol Buffers Schema

```protobuf
syntax = "proto3";
package scalardb.saga.v1;

import "google/protobuf/struct.proto";
import "google/protobuf/empty.proto";

// --- Saga definition ---

message SagaDefinition {
  string name = 1;
  string version = 2;
  SagaMode mode = 3;
  RecoveryStrategy recovery_strategy = 4;
  uint64 timeout_ms = 5;
  RetryPolicy default_retry_policy = 6;
  repeated StepDefinition steps = 7;
}

message StepDefinition {
  string name = 1;
  uint64 timeout_ms = 2;
  RetryPolicy retry_policy = 3;
  bool pivot = 4;
  oneof kind {
    string step_class = 5;         // Layer 1 code step (embedded mode only)
    ServiceStep service_step = 6;  // Layer 2 declarative service step
  }
}

// A declarative service step: a logical endpoint plus one CallSpec per phase.
message ServiceStep {
  string service = 1;              // logical endpoint name (resolved via config)
  Transport transport = 2;         // HTTP (default) or GRPC
  // SAGA phases:
  CallSpec execution = 3;
  CallSpec compensation = 4;
  // TCC phases:
  CallSpec reservation = 5;
  CallSpec confirmation = 6;
  CallSpec cancellation = 7;
}

enum Transport {
  TRANSPORT_UNSPECIFIED = 0;
  HTTP = 1;
  GRPC = 2;                        // reserved; not yet supported
}

// HTTP transport call spec (a gRPC transport would add its own fields).
message CallSpec {
  string method = 1;                  // GET/POST(default)/PUT/PATCH/DELETE
  string path = 2;                    // URL path; supports ${...} expressions
  map<string, string> query = 3;
  map<string, string> json_body = 4;  // flat map serialized as a JSON body
  string string_body = 5;             // raw body (mutually exclusive with json_body)
  string content_type = 6;
  map<string, string> output = 7;     // $.field / $body extraction
}

message RetryPolicy {
  uint32 max_attempts = 1;
  uint64 initial_interval_ms = 2;
  double backoff_multiplier = 3;
  uint64 max_interval_ms = 4;
}

enum SagaMode {
  SAGA_MODE_UNSPECIFIED = 0;
  SAGA = 1;
  TCC = 2;
}

enum RecoveryStrategy {
  RECOVERY_STRATEGY_UNSPECIFIED = 0;
  BACKWARD = 1;
  FORWARD = 2;
  MIXED = 3;
  PREDEFINED = 4;  // reserved for TCC mode (recovery is the Cancel phase)
}

// --- Runtime API ---

message StartSagaRequest {
  string saga_name = 1;
  google.protobuf.Struct input = 2;
}

message StartSagaResponse {
  string saga_id = 1;
}

message GetSagaRequest {
  string saga_id = 1;
}

enum SagaStatus {
  SAGA_STATUS_UNSPECIFIED = 0;
  RUNNING = 1;
  CONFIRMING = 2;      // TCC only
  COMPLETED = 3;
  COMPENSATING = 4;
  COMPENSATED = 5;
  ESCALATED = 6;
}

service SagaService {
  rpc RegisterSaga(SagaDefinition) returns (google.protobuf.Empty);
  rpc StartSaga(StartSagaRequest) returns (StartSagaResponse);
  rpc GetSaga(GetSagaRequest) returns (SagaStateSnapshotResponse);
  rpc CompensateSaga(GetSagaRequest) returns (SagaStateSnapshotResponse);
}
```

In daemon mode, a `StepDefinition` carries either a `step_class` (a code step, embedded mode only) or a declarative `ServiceStep` with per-phase `CallSpec`s — the same model as the Java builder and JSON/YAML (see [Declarative Communication](#solution-declarative-communication-in-the-saga-definition)). The coordinator realizes a `ServiceStep` via `DeclarativeBindingStep`/`DeclarativeBindingTccStep` over the matching `TransportAdapter`, keeping a single dispatch path consistent across embedded and daemon modes.

#### Client Usage Examples

**Java:**

```java
SagaServiceGrpc.SagaServiceBlockingStub stub =
    SagaServiceGrpc.newBlockingStub(channel);

stub.registerSaga(SagaDefinition.newBuilder()
    .setName("MoneyTransfer")
    .setVersion("1.0")
    .setMode(SagaMode.SAGA)
    .setRecoveryStrategy(RecoveryStrategy.BACKWARD)
    .setTimeoutMillis(300000)
    .addSteps(StepDefinition.newBuilder()
        .setName("debit")
        .setTimeoutMillis(60000)
        .setServiceStep(ServiceStep.newBuilder()
            .setService("account-svc")
            .setTransport(Transport.HTTP)
            .setExecution(CallSpec.newBuilder()
                .setMethod("POST")
                .setPath("/accounts/${fromAccountId}/debit")
                .putJsonBody("amount", "${amount}")
                .putOutput("debitId", "$.debit_id"))
            .setCompensation(CallSpec.newBuilder()
                .setMethod("POST")
                .setPath("/accounts/${fromAccountId}/reverse-debit")
                .putJsonBody("debitId", "${debitId}"))))
    .addSteps(StepDefinition.newBuilder()
        .setName("credit")
        .setTimeoutMillis(30000)
        .setServiceStep(ServiceStep.newBuilder()
            .setService("account-svc")
            .setExecution(CallSpec.newBuilder()
                .setPath("/accounts/${toAccountId}/credit")
                .putJsonBody("amount", "${amount}")
                .putOutput("creditId", "$.credit_id"))
            .setCompensation(CallSpec.newBuilder()
                .setPath("/accounts/${toAccountId}/reverse-credit")
                .putJsonBody("creditId", "${creditId}"))))
    .build());

StartSagaResponse response = stub.startSaga(
    StartSagaRequest.newBuilder()
        .setSagaName("MoneyTransfer")
        .setInput(Struct.newBuilder()
            .putFields("fromAccountId", Value.newBuilder()
                .setStringValue("A001").build())
            .putFields("toAccountId", Value.newBuilder()
                .setStringValue("B002").build())
            .putFields("amount", Value.newBuilder()
                .setNumberValue(100).build())
            .build())
        .build());
```

**Python:**

```python
from google.protobuf.struct_pb2 import Struct

stub = saga_pb2_grpc.SagaServiceStub(channel)

stub.RegisterSaga(saga_pb2.SagaDefinition(
    name="MoneyTransfer",
    version="1.0",
    mode=saga_pb2.SAGA,
    recovery_strategy=saga_pb2.BACKWARD,
    timeout_ms=300000,
    steps=[
        saga_pb2.StepDefinition(
            name="debit",
            timeout_ms=60000,
            service_step=saga_pb2.ServiceStep(
                service="account-svc",
                transport=saga_pb2.HTTP,
                execution=saga_pb2.CallSpec(
                    method="POST", path="/accounts/${fromAccountId}/debit",
                    json_body={"amount": "${amount}"},
                    output={"debitId": "$.debit_id"}),
                compensation=saga_pb2.CallSpec(
                    method="POST", path="/accounts/${fromAccountId}/reverse-debit",
                    json_body={"debitId": "${debitId}"}))),
        saga_pb2.StepDefinition(
            name="credit",
            timeout_ms=30000,
            service_step=saga_pb2.ServiceStep(
                service="account-svc",
                execution=saga_pb2.CallSpec(
                    path="/accounts/${toAccountId}/credit",
                    json_body={"amount": "${amount}"},
                    output={"creditId": "$.credit_id"}),
                compensation=saga_pb2.CallSpec(
                    path="/accounts/${toAccountId}/reverse-credit",
                    json_body={"creditId": "${creditId}"}))),
    ]))

input_data = Struct()
input_data.update({"fromAccountId": "A001", "toAccountId": "B002", "amount": 100})
response = stub.StartSaga(saga_pb2.StartSagaRequest(
    saga_name="MoneyTransfer", input=input_data))
```

#### Work Breakdown

| Work Item | Est. LoC | Time |
|---|---|---|
| `.proto` definitions (saga definition schema + coordinator API + participant protocol) | ~150 | 0.5 day |
| gRPC coordinator server (serves saga lifecycle API over gRPC) | ~200 | 1-1.5 days |
| gRPC participant SDK (hosts Steps over gRPC) | ~200 | 1-1.5 days |
| `RemoteSagaManager` gRPC variant | ~100 | 0.5 day |
| Tests (all classes: server, client, participant SDK, error handling) | ~500 | 1.5-2 days |
| **Phase 7a Total** | **~1,150** | **~4.5-6 days** |

### 7b: TCP/Netty Transport

Add a custom binary protocol over TCP using Netty for maximum performance in datacenter environments. Persistent connections with multiplexing, lower overhead than HTTP.

| Work Item | Est. LoC | Time |
|---|---|---|
| Wire protocol design (message framing, request/response correlation, heartbeat) | ~50 | 0.5 day |
| Netty coordinator server (codec, handler, connection management) | ~400 | 1.5-2 days |
| Netty participant SDK (codec, handler) | ~300 | 1-1.5 days |
| `RemoteSagaManager` Netty variant | ~150 | 0.5-1 day |
| Tests (all classes: codec, handler, connection lifecycle, error cases) | ~600 | 1.5-2 days |
| **Phase 7b Total** | **~1,500** | **~5-7 days** |

### Transport Comparison

| Factor | HTTP (Phases 2-3) | gRPC (Phase 7a) | TCP/Netty (Phase 7b) |
|---|---|---|---|
| **Polyglot ease** | Any language, any tool | Needs protobuf toolchain | Needs custom client per language |
| **Debuggability** | curl, browser, logs | grpcurl, binary on wire | Custom tooling needed |
| **Formal contract** | Documented HTTP spec | `.proto` file (compile-time checks) | Custom spec |
| **Performance** | Good | Better (binary, HTTP/2) | Best (minimal overhead, persistent connections) |
| **Infrastructure** | Any proxy/LB | Needs HTTP/2 support | Direct TCP, no proxy |
| **Best for** | Default, external participants | Teams already using gRPC | High-throughput datacenter |

**Phase 7 Total (both sub-phases): ~2,600 LoC, ~9-12.5 days**

**7a and 7b are independent** — implement either or both based on need.

### Cumulative Timeline

| Milestone | Cumulative Time |
|---|---|
| Phase 1 complete (core engine) | ~5-7 days |
| Phase 2 complete (+ communication, frameworks, participant SDK) | ~12-17 days |
| Phase 3 complete (+ daemon mode, RemoteSagaManager) | ~15.5-22 days |
| Phase 4 complete (+ DX & observability) | ~18.5-26.5 days |
| Phase 5 complete (+ admin API) | ~21-30.5 days |
| Phase 6 complete (+ LRA compliance) | ~31.5-44.5 days |
| Phase 7 complete (+ gRPC & TCP/Netty transports) | ~41-57.5 days |

## Enhancement Roadmap (v2+)

The following features are not included in the initial phases but planned for future iterations:

1. **Conditional branching**: Add `ChoiceStep` that evaluates expressions to pick the next step
2. **Parallel step execution**

   Execute independent steps concurrently within a "parallel steps" group. Reduces latency from `sum(step latencies)` to `max(step latencies)` per group — significant for TCC where all Try steps and all Confirm steps are independent.

   **Definition model — parallel groups (not DAG):**

   Parallel groups are simpler to implement, validate, and reason about compensation for than a full DAG-based model. Steps within a group have no ordering guarantees; steps between groups are sequential.

   ```yaml
   # v2 Saga definition with parallel groups
   name: PlaceOrder
   version: "2.0"
   mode: SAGA
   recoveryStrategy: MIXED
   steps:
     - parallel:                        # parallel group
       - name: reserveInventory
         stepClass: com.example.ReserveInventoryStep
       - name: reserveShipping
         stepClass: com.example.ReserveShippingStep
     - name: chargePayment              # sequential (pivot)
       stepClass: com.example.ChargePaymentStep
       pivot: true
     - parallel:                        # parallel group (retriable)
       - name: sendConfirmationEmail
         stepClass: com.example.SendConfirmationEmailStep
       - name: updateAnalytics
         stepClass: com.example.UpdateAnalyticsStep
   ```

   ```java
   // v2 Java builder
   SagaDefinition.newBuilder("PlaceOrder", SagaMode.SAGA)
       .version("2.0")
       .recoveryStrategy(RecoveryStrategy.MIXED)
       .parallelSteps()
           .step("reserveInventory", ReserveInventoryStep.class).add()
           .step("reserveShipping", ReserveShippingStep.class).add()
           .add()
       .step("chargePayment", ChargePaymentStep.class).pivot(true).add()
       .parallelSteps()
           .step("sendConfirmationEmail", SendConfirmationEmailStep.class).add()
           .step("updateAnalytics", UpdateAnalyticsStep.class).add()
           .add()
       .build();
   ```

   **TCC parallel execution:**

   TCC uses the same `parallel:` group syntax as Saga. If steps are declared in a parallel group, their Try executions run in parallel and their Confirm executions also run in parallel (same grouping applies to both phases):

   ```yaml
   # v2 TCC definition with parallel groups — same syntax as Saga
   name: MoneyTransfer
   version: "2.0"
   mode: TCC
   steps:
     - parallel:                        # try + confirm run in parallel
       - name: debit
         stepClass: com.example.TccDebitStep
       - name: credit
         stepClass: com.example.TccCreditStep
     - name: shipping                   # try + confirm run sequentially
       stepClass: com.example.TccShippingStep
   ```

   **API design for forward compatibility (v1 preparation):**

   In v1, `StepDefinition` is a concrete class. In v2, it becomes a sealed interface with two implementations:

   ```java
   // v1: concrete class (current)
   public static class StepDefinition {
       String name;
       String stepClass;
       long timeoutMillis;
       RetryPolicy retryPolicy;
       boolean pivot;
   }

   // v2: sealed interface
   public sealed interface StepDefinition permits SingleStep, ParallelSteps {}

   public record SingleStep(
       String name, String stepClass, long timeoutMillis, RetryPolicy retryPolicy, boolean pivot
   ) implements StepDefinition {}

   public record ParallelSteps(
       List<SingleStep> steps
   ) implements StepDefinition {}
   ```

   v1 JSON/YAML definitions and builder-built definitions work without changes in v2. The parser treats every step object without the `parallel` key as a `SingleStep`. The builder API is unchanged — `parallelSteps()` is purely additive.

   **Key implementation challenges (v2):**

   - **Compensation within parallel groups:** When one step in a parallel group fails, the engine must wait for all in-flight steps to complete (or timeout), then compensate all completed steps. Compensation order within a group is non-deterministic (any order is safe because the steps are independent by definition).
   - **SagaContext concurrent access:** Parallel steps must not write to the same context keys. Use per-step isolated output that merges after the barrier. Reject parallel groups where steps declare overlapping output keys.
   - **Event sequencing:** Batch-append events after the parallel group barrier. If the process crashes mid-parallel-group, all steps in the group are re-executed on recovery (idempotency required, same as today).
   - **Pivot interaction:** A step with `pivot: true` cannot be inside a parallel group — it must be a single step, because the pivot is a single go/no-go point. The engine validates this at registration time.

3. **Event publishing**: Use CDC/Debezium to tail `saga_events` and publish to Kafka/RabbitMQ, or implement a polling publisher that reads events by sequence range
4. **Participant SDK**: Optional participant-side SDK with `@SagaParticipant`, `@SagaAction`, `@SagaCompensation` annotations, built-in idempotency, and anomaly protection (empty rollback, suspension). The anomaly protection uses an INSERT-based barrier mechanism inspired by Seata's TCC Fence and DTM's Sub-Transaction Barrier. Since we don't control participant databases, this is an opt-in library. See "Participant Idempotency Levels" in Declarative Service Steps and Framework Integration, and [scalardb-saga-barrier-sdk-research.md](scalardb-saga-barrier-sdk-research.md) for detailed research.
5. **SubStateMachine (nesting)**: Compose sagas from sub-sagas for complex workflows
6. **Loop execution**: Repeat steps based on collection inputs
7. **ScriptTask**: Lightweight expression evaluation without full Step implementation
8. **Heartbeat for async steps**: Participants periodically ping the coordinator ("I'm still working") during long-running async operations. If heartbeats stop, the coordinator detects participant failure faster than waiting for the full step timeout. Especially valuable when step timeouts are long (e.g., 5 minutes) but participant death should be detected within seconds. Similar to Temporal's `heartbeatTimeout`.
