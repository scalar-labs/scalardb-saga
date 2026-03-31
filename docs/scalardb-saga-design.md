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

4. **No message broker required** — step invocation uses direct method calls (embedded) or HTTP/gRPC (daemon), not message queues. Unlike Eventuate (requires Kafka + Debezium) or MassTransit (requires RabbitMQ/SQS), there is no broker infrastructure to deploy or manage. An optional outbox relay (v2+) can publish events to brokers for downstream consumers, but it is not required for the saga engine itself.

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
| **Outbox built-in** | Yes (events = outbox) | No | N/A (durable execution) | Yes (CDC/polling) | No | No | No | Yes (event store) |
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
  - [Async Step Completion (Daemon Mode Only)](#async-step-completion-daemon-mode-only)
  - [TCC (Try-Confirm-Cancel) Mode](#tcc-try-confirm-cancel-mode)
- [Part III: Communication & Integration](#part-iii-communication--integration)
  - [ServiceInvoker and Framework Integration](#serviceinvoker-and-framework-integration)
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
│   ├── SagaInstance.java
│   ├── SagaContext.java
│   ├── Step.java
│   ├── StepResult.java
│   ├── SagaStatus.java
│   └── StepStatus.java
├── engine/                      # Core execution
│   ├── SagaEngine.java
│   ├── CompensationManager.java
│   ├── RetryPolicy.java
│   └── DefaultSagaManager.java
├── parser/                      # Definition loading
│   └── SagaDefinitionParser.java
├── store/                       # Storage interface + implementations
│   ├── SagaStore.java           # Interface
│   ├── SagaSchema.java
│   └── ScalarDbSagaStore.java   # Default implementation (ScalarDB)
├── recovery/                    # Crash recovery
│   └── SagaRecoveryManager.java
├── exception/                   # Exception types
│   ├── SagaPersistenceException.java
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
│   ├── InMemorySagaStore.java
│   └── MockStep.java
└── devserver/                   # Local development server
    ├── SagaDevServer.java
    └── SagaDevServerConfig.java
```

**Naming convention**: Classes that users import or interact with directly are prefixed with `Saga` (e.g., `SagaManager`, `SagaStore`, `SagaInstance`, `SagaTestHarness`, `SagaAdminService`). Internal engine components use domain-specific names without the prefix (e.g., `CompensationManager`, `RetryPolicy`, `TimeoutPolicy`) since they live inside the engine package and are not part of the public API.

## Component Summary

| # | Component | Responsibility |
|---|-----------|---------------|
| 1 | **SagaEngine** | Owns saga lifecycle: `createSaga()` (persist), `executeSaga()` (run steps), `execute()` (convenience). Walks through steps sequentially, delegates to retry/compensation on failure. Features: per-step/per-saga timeouts, graceful shutdown, TCC mode (`TccStep` with try/confirm/cancel). Async step completion (daemon mode only): parks on `StepResult.pending()`, resumes via callback. |
| 2 | **CompensationManager** | Executes compensations in reverse (LIFO) with retry. Stops on failure — saga stays in `COMPENSATING` for recovery to retry. Escalation after repeated recovery failures. |
| 3 | **RetryPolicy** | Exponential backoff + jitter. Participant-driven error classification (`StepExecutionException.isRetryable()`). Virtual thread execution. |
| 4 | **SagaStore** (interface) | Append-only event persistence: 1 INSERT per step (event row doubles as outbox entry). Bucket-partitioned `saga_index` table with `status` as clustering key for efficient recovery scans and distributed writes. Default implementation: `ScalarDbSagaStore`. |
| 5 | **SagaRecoveryManager** | Scans each `saga_index` bucket for `status=RUNNING` with stale `updated_at` (similar to Seata's model). Bucket partitioning distributes scans across database nodes. Replays events to reconstruct state. Conflict-based claiming via ScalarDB transaction conflict detection. |
| 6 | **ServiceInvoker** | Framework-agnostic typed lambdas for step dispatch. Declarative JSON communication (Layer 2b). |
| 7 | **SagaTestHarness** | In-memory testing: mock steps, crash simulation, assertion helpers. Uses `InMemorySagaStore`. |
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
scalar.db.password=secret
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
                    │   ScalarDB    │  ← saga events + index live here,
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
  in saga_index, claims it via transactional write, and resumes.
```

**Recovery protocol (Seata-style periodic scan):**

1. **Saga registered on creation**: `SagaEngine.execute()` inserts a row into `saga_index` with `status=RUNNING` and `updated_at=now`.
2. **No per-step ownership writes**: The engine does not write any lease/ownership data per step — only appends events to `saga_events`. The `saga_index.updated_at` is updated on saga start and end only.
3. **Periodic recovery scan**: Every replica runs `SagaRecoveryManager` on a background scheduler (default: every 30s). It scans each `saga_index` bucket with clustering key prefix `status=RUNNING` (and `COMPENSATING`), reading only active sagas. Bucket-based partitioning distributes these scans across database nodes.
4. **Conflict-based claiming**: Before recovering a saga, the replica writes a new `saga_index` row with updated `owner_id` and incremented `version` (DELETE + INSERT in one transaction, zero reads — the status and metadata are already known from step 3). If two replicas try to claim the same saga concurrently, ScalarDB's transaction conflict detection causes one to get `CommitConflictException` and back off.
5. **Event replay for state reconstruction**: The claiming replica reads all events from `saga_events` for the saga and replays them to reconstruct current state (e.g., finds the last `STEP_COMPLETED` event at step 3).
6. **Resume forward execution**: The engine resumes from `lastCompleted + 1` (step 4). The step may or may not have executed before the crash — **this is why `execute()` must be idempotent**.
7. **Resume compensation**: If the saga was `COMPENSATING`, the engine resumes compensation from the last compensated step downward. If a compensation fails again, the saga stays in `COMPENSATING` for the next recovery scan. After repeated failures (configurable threshold), the recovery manager escalates the saga to `ESCALATED` for manual intervention.
8. **Index updated on completion**: When a saga reaches a terminal state (`COMPLETED`, `COMPENSATED`, `ESCALATED`), the engine updates `saga_index.status` to the terminal state. Terminal entries can be cleaned up after a configurable retention period.

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
 │    CompensationManager.compensate(Step2, Step1)      │
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
//   4. Steps are registered in the StepRegistry at application startup (via
//      builder, Spring auto-config, or Quarkus extension) and looked up by name.
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
// Extension of Step for TCC (Try-Confirm-Cancel) mode.
// In TCC mode:
//   - execute()    = Try phase (reserve resources, tentative operation)
//   - confirm()    = Confirm phase (make reservation permanent)
//   - compensate() = Cancel phase (release reservation)
//
// confirm() is called only after ALL steps' execute() succeed.
// confirm() MUST be idempotent and MUST eventually succeed (resources are
// already reserved). The engine retries confirm() on failure.
//
// LIFECYCLE: Same as Step — non-static, application-level singletons.
// Thread-safety requirements apply to all three methods.
public interface TccStep extends Step {

    /**
     * Make the tentative operation permanent. Called after all steps succeed.
     * <p>
     * MUST be idempotent — may be called multiple times on crash recovery.
     * MUST eventually succeed — resources are reserved; there's no reason
     * confirmation should permanently fail (unless the reservation expired,
     * which indicates a timeout misconfiguration).
     */
    void confirm(SagaContext context) throws StepExecutionException;
}

// --- api/SagaContext.java ---
// Shared mutable context passed through all steps.
//
// ALLOWED VALUE TYPES: Only primitives, strings, and collections/maps of
// primitives are allowed. This restriction ensures reliable JSON serialization
// and deserialization across crash recovery boundaries. Complex objects (custom
// POJOs, cyclic references, generic types) are explicitly rejected at put-time.
//
// Allowed: String, Integer, Long, Double, Float, Boolean, BigDecimal,
//          List<allowed>, Map<String, allowed>
// Rejected: Custom objects, null values with ambiguous types, Class references
public class SagaContext {
    private static final Set<Class<?>> ALLOWED_TYPES = Set.of(
        String.class, Integer.class, Long.class, Double.class,
        Float.class, Boolean.class, BigDecimal.class);

    private final String sagaId;
    private final Map<String, Object> data;
    private int nextEventSequence;       // tracks next saga_events sequence in memory
    private SagaStatus currentStatus;    // tracks current saga_index status in memory
    private SagaIndexMetadata indexMetadata; // cached saga_index columns (immutable after creation)

    public int getAndIncrementSequence() { return nextEventSequence++; }
    public void setNextEventSequence(int seq) { this.nextEventSequence = seq; }
    public SagaStatus getCurrentStatus() { return currentStatus; }
    public void setCurrentStatus(SagaStatus status) { this.currentStatus = status; }
    public SagaIndexMetadata getIndexMetadata() { return indexMetadata; }
    public void setIndexMetadata(SagaIndexMetadata m) { this.indexMetadata = m; }

    public <T> T get(String key, Class<T> type);

    public void put(String key, Object value) {
        validateType(value);  // throws IllegalArgumentException if not allowed
        data.put(key, value);
    }

    public void merge(StepResult result);  // merge step output into context

    private void validateType(Object value) {
        if (value == null) return;
        if (ALLOWED_TYPES.contains(value.getClass())) return;
        if (value instanceof List) {
            ((List<?>) value).forEach(this::validateType);
            return;
        }
        if (value instanceof Map) {
            ((Map<?, ?>) value).values().forEach(this::validateType);
            return;
        }
        throw new IllegalArgumentException(
            "SagaContext only accepts primitives, strings, and collections thereof. "
            + "Got: " + value.getClass().getName());
    }
}

// --- engine/SagaIndexMetadata.java ---
// Immutable snapshot of saga_index columns that don't change after creation.
// Cached in SagaContext so that recordTransition can write the new index row
// without reading the old one first (eliminates the GET from the transaction).
public record SagaIndexMetadata(
    String sagaName,
    String ownerId,
    int version,
    String definitionVersion,
    String definitionJson,
    Instant createdAt
) {}

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
    FAILED,         // a step failed, awaiting recovery decision
    COMPENSATING,   // executing compensation steps (Saga) or Cancel phase (TCC)
    COMPENSATED,    // all compensations/cancellations completed
    ESCALATED       // compensation(s) failed, needs manual intervention
}

// --- api/StepStatus.java ---
public enum StepStatus {
    WAITING,     // daemon mode only: returned pending(), waiting for external callback
    COMPLETED,   // execute() succeeded (Try succeeded in TCC mode)
    CONFIRMED,   // TCC only: confirm() succeeded
    FAILED
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
  "recoverStrategy": "COMPENSATE",
  "timeoutMs": 300000,
  "defaultRetryPolicy": {
    "maxAttempts": 3,
    "initialIntervalMs": 1000,
    "backoffMultiplier": 2.0,
    "maxIntervalMs": 30000
  },
  "steps": [
    {
      "name": "debit",
      "stepClass": "com.example.DebitAccountStep",
      "timeoutMs": 60000,
      "retryPolicy": { "maxAttempts": 5 }
    },
    {
      "name": "credit",
      "stepClass": "com.example.CreditAccountStep",
      "timeoutMs": 30000
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
recoverStrategy: COMPENSATE
timeoutMs: 300000

defaultRetryPolicy:
  maxAttempts: 3
  initialIntervalMs: 1000
  backoffMultiplier: 2.0
  maxIntervalMs: 30000

steps:
  - name: debit
    stepClass: com.example.DebitAccountStep
    timeoutMs: 60000
    retryPolicy:
      maxAttempts: 5  # more retries for the debit step

  - name: credit
    stepClass: com.example.CreditAccountStep
    timeoutMs: 30000
```

### Saga Definition (Java Builder API)

For embedded mode, a type-safe Java builder provides compile-time checks and IDE auto-completion:

```java
SagaDefinition def = SagaDefinition.builder("MoneyTransfer")
    .version("1.0")
    .mode(SagaMode.SAGA)
    .recoverStrategy(RecoverStrategy.COMPENSATE)
    .timeoutMs(300000)
    .defaultRetryPolicy(RetryPolicy.builder()
        .maxAttempts(3)
        .initialIntervalMs(1000)
        .backoffMultiplier(2.0)
        .maxIntervalMs(30000)
        .build())
    .step("debit")
        .stepClass(DebitAccountStep.class)
        .timeoutMs(60000)
        .retryPolicy(RetryPolicy.builder().maxAttempts(5).build())
        .add()
    .step("credit")
        .stepClass(CreditAccountStep.class)
        .timeoutMs(30000)
        .add()
    .build();

sagaManager.register(def);
```

The Java builder uses `stepClass` references — the developer writes communication logic in the `Step` implementation. For simple steps where no custom logic is needed, use the declarative `call`/`compensate` format in JSON/YAML instead (see [Declarative Communication](#solution-declarative-communication-in-the-saga-definition)).

#### Saga Definition Formats

| Format | `stepClass` (Java code) | Declarative `call` (no code) | Primary use |
|---|---|---|---|
| **Java builder** | Yes | No (just write a Step class) | Embedded mode |
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
    private RecoverStrategy recoverStrategy;  // COMPENSATE or FORWARD
    private RetryPolicy defaultRetryPolicy;

    public enum SagaMode { SAGA, TCC }
    public enum RecoverStrategy { COMPENSATE, FORWARD }

    public static class StepDefinition {
        String name;
        String stepClass;            // FQCN of Step implementation
        RetryPolicy retryPolicy;     // per-step override (nullable)
    }
}
```

### SagaManager (Top-Level API)

```java
// --- api/SagaManager.java ---
public interface SagaManager extends AutoCloseable {
    // Register a saga definition (from JSON, YAML, or programmatic)
    void register(SagaDefinition definition);

    // Load and register all definitions (.json, .yaml, .yml) from a classpath path
    void registerFromClasspath(String resourcePath);

    // Start a new saga instance (synchronous — blocks until saga completes)
    SagaInstance start(String sagaName, Map<String, Object> input);

    // Start a new saga instance (asynchronous — returns immediately with saga ID)
    String startAsync(String sagaName, Map<String, Object> input);

    // Start a new saga instance (asynchronous with completion callback)
    String startAsync(String sagaName, Map<String, Object> input,
                      SagaCallback callback);

    // Resume a failed/crashed saga (crash recovery)
    SagaInstance resume(String sagaInstanceId);

    // Manually trigger compensation for a saga
    SagaInstance compensate(String sagaInstanceId);

    // Query saga instance state
    SagaInstance getInstance(String sagaInstanceId);

    // Daemon mode only: complete an async step via external callback (resumes parked saga)
    SagaInstance completeStep(String sagaId, String stepName, Map<String, Object> output);
}

// --- api/SagaCallback.java ---
public interface SagaCallback {
    void onCompleted(SagaInstance instance);
    void onCompensated(SagaInstance instance);
    void onEscalated(SagaInstance instance);
}
```

### SagaEngine (Core Execution Logic)

```java
// --- engine/SagaEngine.java ---
public class SagaEngine {
    private final SagaStore store;
    private final CompensationManager compensationManager;
    private final String ownerId;          // unique ID for this replica (e.g., UUID)

    /**
     * Persist a new saga (saga_index + SAGA_STARTED event). Returns the saga ID.
     * Stores the full definition JSON snapshot in saga_index so that recovery
     * always uses the exact definition this saga was started with.
     */
    public String createSaga(SagaDefinition def, Map<String, Object> input) {
        return store.createSaga(def.getName(), ownerId, input, def.toJson());
    }

    /**
     * Execute a previously created saga. Loads definition from the store,
     * then runs steps from the beginning.
     */
    public SagaInstance executeSaga(String sagaId) {
        SagaInstance saga = store.getInstance(sagaId);
        SagaDefinition def = SagaDefinitionParser.parse(saga.getDefinitionJson());
        // createSaga wrote 1 event (SAGA_STARTED at seq 0) and set status RUNNING
        SagaContext context = new SagaContext(sagaId, saga.getInput());
        context.setNextEventSequence(1);        // next sequence after SAGA_STARTED
        context.setCurrentStatus(SagaStatus.RUNNING);
        context.setIndexMetadata(new SagaIndexMetadata(
            saga.getSagaName(), ownerId, 0,
            def.getVersion(), saga.getDefinitionJson(), saga.getCreatedAt()));
        return executeSteps(def, context, 0, -1);
    }

    /**
     * Convenience: create + execute in one call (synchronous).
     */
    public SagaInstance execute(SagaDefinition def, Map<String, Object> input) {
        Instant now = Instant.now();  // same timestamp used by createSaga
        String sagaId = createSaga(def, input);
        // createSaga wrote 1 event (SAGA_STARTED at seq 0) and set status RUNNING
        SagaContext context = new SagaContext(sagaId, input);
        context.setNextEventSequence(1);
        context.setCurrentStatus(SagaStatus.RUNNING);
        context.setIndexMetadata(new SagaIndexMetadata(
            def.getName(), ownerId, 0, def.getVersion(), def.toJson(), now));
        return executeSteps(def, context, 0, -1);
    }

    /**
     * Resume execution from a specific step index (used by crash recovery).
     * completedIndex is initialized to fromStep - 1 (steps before fromStep
     * are known to have completed based on persisted step logs).
     */
    public SagaInstance resumeFrom(SagaDefinition def, SagaContext context, int fromStep) {
        return executeSteps(def, context, fromStep, fromStep - 1);
    }

    /**
     * Shared execution loop used by both execute() and resumeFrom().
     */
    private SagaInstance executeSteps(SagaDefinition def, SagaContext context,
                                      int startIndex, int completedIndex) {
        String sagaId = context.getSagaId();
        List<StepDefinition> steps = def.getSteps();

        try {
            for (int i = startIndex; i < steps.size(); i++) {
                StepDefinition stepDef = steps.get(i);
                Step step = instantiate(stepDef);
                RetryPolicy policy = resolveRetryPolicy(stepDef, def);

                StepResult result = executeWithRetry(step, context, policy);

                // Append-only: 1 INSERT per step. The event row doubles as
                // an outbox entry for downstream consumers. No "step started"
                // write, no lease renewal — recovery uses saga_index scan.
                store.appendEvent(sagaId, context.getAndIncrementSequence(),
                    SagaEvent.stepCompleted(i, stepDef.getName(), result));
                context.merge(result);
                completedIndex = i;
            }

            transition(context, SagaEvent.sagaCompleted());

        } catch (StepExecutionException e) {
            // Step failed (retryable exhausted or non-retryable business error)
            // — compensate. Include the current step (completedIndex + 1) because
            // it may have had external side effects before throwing.
            int compensateFrom = completedIndex + 1;
            transition(context, SagaEvent.sagaCompensating());
            try {
                compensationManager.compensate(def, context, compensateFrom);
                transition(context, SagaEvent.sagaCompensated());
            } catch (StepCompensationException ce) {
                // Compensation failed — saga stays in COMPENSATING.
                // Recovery will retry from the failed compensation step.
            }

        } catch (Exception e) {
            // Compensate from current step — it may have had side effects.
            int compensateFrom = completedIndex + 1;
            transition(context, SagaEvent.sagaFailed());
            if (def.getRecoverStrategy() == RecoverStrategy.COMPENSATE) {
                transition(context, SagaEvent.sagaCompensating());
                try {
                    compensationManager.compensate(def, context, compensateFrom);
                    transition(context, SagaEvent.sagaCompensated());
                } catch (StepCompensationException ce) {
                    // Compensation failed — saga stays in COMPENSATING.
                    // Recovery will retry from the failed compensation step.
                }
            }
            // If FORWARD strategy, leave as FAILED for manual/scheduled retry
        }

        return store.getInstance(sagaId);
    }

    /**
     * Trigger compensation starting from a specific step index.
     */
    public void compensateFrom(SagaDefinition def, SagaContext context, int fromStep) {
        transition(context, SagaEvent.sagaCompensating());
        try {
            compensationManager.compensate(def, context, fromStep);
            transition(context, SagaEvent.sagaCompensated());
        } catch (StepCompensationException e) {
            // Compensation failed — saga stays in COMPENSATING.
            // Recovery will retry from the failed compensation step.
        }
    }

    /**
     * Helper: record a status transition and update context tracking.
     * Passes the cached index metadata so recordTransition needs zero reads.
     */
    private void transition(SagaContext context, SagaEvent event) {
        store.recordTransition(context.getSagaId(),
            context.getAndIncrementSequence(),
            context.getCurrentStatus(),
            context.getIndexMetadata(),
            event);
        context.setCurrentStatus(event.getTargetStatus());
    }

    private Step instantiate(StepDefinition stepDef) {
        // Look up pre-registered Step singleton from the StepRegistry.
        // Steps are application-level singletons — shared across all saga
        // executions. They MUST be thread-safe (stateless or internally synchronized).
        // They are typically created once at application startup and hold expensive
        // resources (gRPC channels, connection pools, HTTP clients).
        return stepRegistry.get(stepDef.getStepClass(), stepDef.getName());
    }

    private RetryPolicy resolveRetryPolicy(StepDefinition stepDef, SagaDefinition def) {
        return stepDef.getRetryPolicy() != null
            ? stepDef.getRetryPolicy()
            : def.getDefaultRetryPolicy();
    }
}
```

### DefaultSagaManager

`DefaultSagaManager` implements the `SagaManager` interface. It delegates all saga lifecycle logic (creation, step execution, compensation) to `SagaEngine`, and handles definition registry, async threading, and callbacks.

- **`start()`**: Delegates to `engine.execute()` — synchronous, blocks until the saga completes.
- **`startAsync()`**: Calls `engine.createSaga()` to persist the saga and get the ID, then submits `engine.executeSaga()` to a virtual thread. The caller gets back the saga ID immediately.
- **`resume()`, `compensate()`**: Delegates to corresponding `SagaEngine` methods.
- **`getInstance()`**: Delegates to `SagaStore`.

```java
// --- engine/DefaultSagaManager.java ---
public class DefaultSagaManager implements SagaManager {
    private final SagaEngine engine;
    private final ExecutorService asyncExecutor =
        Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public SagaInstance start(String sagaName, Map<String, Object> input) {
        // Synchronous — blocks until saga completes
        SagaDefinition def = definitions.get(sagaName);
        return engine.execute(def, input);
    }

    @Override
    public String startAsync(String sagaName, Map<String, Object> input) {
        return startAsync(sagaName, input, null);
    }

    @Override
    public String startAsync(String sagaName, Map<String, Object> input,
                             SagaCallback callback) {
        SagaDefinition def = definitions.get(sagaName);

        // 1. Persist saga via engine (saga_index + SAGA_STARTED event).
        //    This guarantees the saga is recoverable even if the process
        //    crashes before the virtual thread starts executing.
        String sagaId = engine.createSaga(def, input);

        // 2. Submit execution to a virtual thread
        asyncExecutor.submit(() -> {
            try {
                SagaInstance result = engine.executeSaga(sagaId);

                if (callback != null) {
                    switch (result.getStatus()) {
                        case COMPLETED    -> callback.onCompleted(result);
                        case COMPENSATED  -> callback.onCompensated(result);
                        case ESCALATED    -> callback.onEscalated(result);
                    }
                }
            } catch (Exception e) {
                // Saga state is persisted — recovery will pick it up.
                // Log the error but don't propagate (no caller to propagate to).
                log.error("Async saga {} failed unexpectedly", sagaId, e);
            }
        });

        // 3. Return saga ID immediately
        return sagaId;
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
SagaInstance result = sagaManager.getInstance(sagaId);  // poll later

// Fire-and-callback
sagaManager.startAsync("transferMoney", input, new SagaCallback() {
    @Override public void onCompleted(SagaInstance instance) { ... }
    @Override public void onCompensated(SagaInstance instance) { ... }
    @Override public void onEscalated(SagaInstance instance) { ... }
});
```

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

- **Stop on failure**: If a compensation fails after retries, the engine stops the compensation loop. The saga remains in `COMPENSATING` status, and the recovery manager retries from the failed step on the next scan. This preserves the reverse execution order — the application designed the compensation order to maintain business invariants, and the engine must respect it.
- **Escalation after repeated recovery failures**: If the recovery manager has retried the same compensation too many times (configurable threshold), it transitions the saga to `ESCALATED` for manual intervention.
- **Idempotency required**: Compensations may be called multiple times (on crash recovery), so they MUST check if already compensated.

```java
// --- engine/CompensationManager.java ---
public class CompensationManager {
    private final SagaStore store;
    private final RetryPolicy compensationRetryPolicy;  // separate from forward step retry

    public CompensationManager(SagaStore store) {
        this(store, RetryPolicy.compensationDefault());
    }

    public CompensationManager(SagaStore store, RetryPolicy compensationRetryPolicy) {
        this.store = store;
        this.compensationRetryPolicy = compensationRetryPolicy;
    }

    /**
     * Compensate steps [completedIndex..0] in reverse order.
     * Each compensation is retried with exponential backoff.
     * If a compensation fails after all retries, throws StepCompensationException
     * to stop the loop — the saga stays in COMPENSATING for recovery to retry.
     */
    public void compensate(SagaDefinition def, SagaContext context, int completedIndex)
            throws StepCompensationException {

        for (int i = completedIndex; i >= 0; i--) {
            StepDefinition stepDef = def.getSteps().get(i);
            compensateWithRetry(stepDef, context, i);
        }
    }

    /**
     * Retry compensation with exponential backoff.
     * Default: 3 attempts with 1s/2s/4s backoff.
     * Transient failures during compensation are common (network blips),
     * so giving up immediately is too aggressive.
     * If all retries fail, throws StepCompensationException.
     */
    private void compensateWithRetry(StepDefinition stepDef, SagaContext context,
                                      int stepIndex) throws StepCompensationException {
        int attempt = 0;
        long interval = compensationRetryPolicy.getInitialIntervalMs();

        while (attempt < compensationRetryPolicy.getMaxAttempts()) {
            try {
                attempt++;
                Step step = instantiate(stepDef);
                step.compensate(context);
                store.appendEvent(context.getSagaId(),
                    context.getAndIncrementSequence(),
                    SagaEvent.stepCompensated(stepIndex));
                return;
            } catch (Exception e) {
                if (attempt >= compensationRetryPolicy.getMaxAttempts()) {
                    store.appendEvent(context.getSagaId(),
                        context.getAndIncrementSequence(),
                        SagaEvent.compensationFailed(stepIndex, e));
                    throw new StepCompensationException(stepDef.getName(), stepIndex, e);
                }
                // Exponential backoff (virtual thread — cheap to sleep)
                try {
                    long jitter = ThreadLocalRandom.current().nextLong(interval / 4);
                    Thread.sleep(interval + jitter);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    store.appendEvent(context.getSagaId(),
                        context.getAndIncrementSequence(),
                        SagaEvent.compensationFailed(stepIndex, e));
                    throw new StepCompensationException(stepDef.getName(), stepIndex, e);
                }
                interval = Math.min(
                    (long) (interval * compensationRetryPolicy.getBackoffMultiplier()),
                    compensationRetryPolicy.getMaxIntervalMs());
            }
        }
    }
}
```

The compensation retry policy is separate from the forward step retry policy. Default: 3 attempts with 1s initial interval and 2.0x backoff multiplier:

```java
// In RetryPolicy.java
public static RetryPolicy compensationDefault() {
    return new RetryPolicy(3, 1000, 2.0, 10000, Set.of());
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

    public boolean isRetryable() { return retryable; }
}
```

**Default is retryable.** If a step throws `new StepExecutionException(e)` without specifying, the engine retries. This is the safe default — transient failures (network blips, connection pool exhaustion, temporary overload) are more common than business errors, and retrying a business error just wastes a few attempts before compensating anyway.

### RetryPolicy

```java
// --- engine/RetryPolicy.java ---
public class RetryPolicy {
    private int maxAttempts;            // default: 3
    private long initialIntervalMs;     // default: 1000
    private double backoffMultiplier;   // default: 2.0
    private long maxIntervalMs;         // default: 30000

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(3, 1000, 2.0, 30000);
    }
}
```

`RetryPolicy` is purely about timing (how many attempts, how long to wait). Error classification is the participant's responsibility.

### Retry Execution Logic (inside SagaEngine)

Uses **virtual threads** (Java 21+) to eliminate thread pool exhaustion during exponential backoff. Virtual threads are cheap to create and block without consuming OS threads, making `Thread.sleep()` in retry loops a non-issue even with many concurrent sagas.

```java
// Virtual thread executor — used for all step executions and retries.
// Virtual threads are cheap (no thread pool sizing needed) and non-blocking on sleep.
private static final ExecutorService STEP_EXECUTOR =
    Executors.newVirtualThreadPerTaskExecutor();

private StepResult executeWithRetry(Step step, SagaContext ctx, RetryPolicy policy,
                                     long stepDeadline)
        throws StepExecutionException, StepTimeoutException {

    int attempt = 0;
    long interval = policy.getInitialIntervalMs();

    while (true) {
        try {
            attempt++;

            // Per-step timeout enforcement via virtual thread + Future
            if (stepDeadline > 0) {
                long remaining = stepDeadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new StepTimeoutException("Step timeout exceeded");
                }
                Future<StepResult> future = STEP_EXECUTOR.submit(() -> step.execute(ctx));
                try {
                    return future.get(remaining, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);  // interrupt the virtual thread
                    throw new StepTimeoutException("Step timed out after " + remaining + "ms");
                }
            } else {
                return step.execute(ctx);
            }

        } catch (StepTimeoutException e) {
            throw e;  // timeouts are not retryable

        } catch (StepExecutionException e) {
            // Participant decided: is this retryable?
            if (!e.isRetryable()) {
                throw e;  // non-retryable → propagate to trigger compensation
            }

            // Retryable, but exhausted attempts?
            if (attempt >= policy.getMaxAttempts()) {
                throw e;  // give up → propagate to trigger compensation
            }

            // Exponential backoff with jitter (virtual thread — cheap to sleep)
            long jitter = ThreadLocalRandom.current().nextLong(interval / 4);
            Thread.sleep(interval + jitter);
            interval = Math.min(
                (long) (interval * policy.getBackoffMultiplier()),
                policy.getMaxIntervalMs()
            );
        }
    }
}
```

No `isNonRetryable()` method needed — the participant's `StepExecutionException.isRetryable()` flag is the single source of truth.

## Persistence via ScalarDB

### What It Does

Stores saga state as **append-only events** using ScalarDB's transaction API. Each state change is a single INSERT into `saga_events`. A small mutable `saga_index` table provides fast status lookups and efficient recovery scanning.

**Why ScalarDB**: Database-agnostic ACID transactions — saga state and outbox writes are atomic in one transaction (no dual-write problem), and one persistence implementation works regardless of the user's database choice (Cassandra, DynamoDB, PostgreSQL, MySQL, Cosmos DB, etc.).

The critical features:
- **1 INSERT per step** — append-only events, no read-modify-write
- **Event rows double as outbox entries** — CDC or polling can tail `saga_events` directly, eliminating the dual-write problem (same approach as Eventuate Tram)
- **No per-step lease/ownership writes** — recovery uses periodic scan of `saga_index` (Seata-style)

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
     * Every state change is a single INSERT. No UPDATEs, no DELETEs
     * during normal operation. Each row doubles as an outbox entry
     * (the `published` column tracks relay progress).
     *
     * This is the same model as Axon's event store and Eventuate's
     * events table. ScalarDB's clustering key ensures efficient
     * ordered scan by saga_id.
     */
    public static TableMetadata sagaEventsTable() {
        return TableMetadata.newBuilder()
            .addColumn("saga_id",    DataType.TEXT)     // PK
            .addColumn("sequence",   DataType.INT)      // CK: monotonically increasing per saga
            .addColumn("event_type", DataType.TEXT)      // SAGA_STARTED, STEP_COMPLETED, etc.
            .addColumn("step_index", DataType.INT)       // step index (for step events; -1 for saga events)
            .addColumn("step_name",  DataType.TEXT)      // step name (for step events; null for saga events)
            .addColumn("payload",    DataType.TEXT)      // JSON: step result, error, input, etc.
            .addColumn("published",  DataType.BOOLEAN)   // false until outbox relay picks it up
            .addColumn("created_at", DataType.TIMESTAMPTZ)
            .addPartitionKey("saga_id")
            .addClusteringKey("sequence", Scan.Ordering.Order.ASC)
            .build();
    }

    /**
     * Table 2: saga_index — mutable lookup/recovery index.
     *
     * Partition key: bucket (INT)  — hash(saga_id) % NUM_BUCKETS
     * Clustering key: (status (INT), saga_id (TEXT))
     * Secondary index: saga_id
     *
     * One row per saga. Written on saga start and on each status transition.
     * Because status is part of the clustering key (immutable), status
     * transitions require DELETE old row + INSERT new row in one transaction.
     *
     * Bucket-based partitioning distributes recovery scans across database
     * nodes — each bucket is a separate partition, avoiding hot-partition
     * problems that would occur if status alone were the partition key.
     *
     * Status as clustering key prefix enables efficient recovery scans:
     * scan each bucket with clustering key prefix status=RUNNING to read
     * only active sagas, skipping all COMPLETED/COMPENSATED/ESCALATED rows
     * at the storage layer.
     *
     * Used for:
     * - Recovery scan: for each bucket, scan status=RUNNING with stale updated_at
     * - Fast status lookup: getInstance(sagaId) via secondary index on saga_id
     * - Admin API queries: list by status, count by name
     * - Conflict-based claiming: version column incremented on claim
     *
     * Terminal entries (COMPLETED, COMPENSATED, ESCALATED) can be
     * cleaned up after a configurable retention period.
     */
    public static final int NUM_BUCKETS = 16;  // configurable; 16 is a good default

    public static int bucketOf(String sagaId) {
        return Math.abs(sagaId.hashCode()) % NUM_BUCKETS;
    }

    public static TableMetadata sagaIndexTable() {
        return TableMetadata.newBuilder()
            .addColumn("bucket",       DataType.INT)      // PK: hash(saga_id) % NUM_BUCKETS
            .addColumn("status",       DataType.INT)      // CK1: SagaStatus ordinal
            .addColumn("saga_id",      DataType.TEXT)      // CK2: unique identifier
            .addColumn("saga_name",    DataType.TEXT)
            .addColumn("owner_id",     DataType.TEXT)      // replica that last claimed this saga
            .addColumn("version",      DataType.INT)       // incremented on each claim
            .addColumn("definition_version", DataType.TEXT) // saga definition version at creation
            .addColumn("definition_json",    DataType.TEXT) // full definition JSON snapshot
            .addColumn("created_at",   DataType.TIMESTAMPTZ)
            .addColumn("updated_at",   DataType.TIMESTAMPTZ)
            .addPartitionKey("bucket")
            .addClusteringKey("status", Scan.Ordering.Order.ASC)
            .addClusteringKey("saga_id", Scan.Ordering.Order.ASC)
            .addSecondaryIndex("saga_id")
            .build();
    }

    /**
     * Create all saga tables using ScalarDB Admin API.
     */
    public static void createAll(Admin admin) throws ExecutionException {
        admin.createNamespace(NAMESPACE, true);
        admin.createTable(NAMESPACE, "saga_events", sagaEventsTable(), true);
        admin.createTable(NAMESPACE, "saga_index",  sagaIndexTable(),  true);
    }
}
```

### Event Types

```java
// --- store/SagaEvent.java ---
public class SagaEvent {
    private final String eventType;
    private final int stepIndex;        // -1 for saga-level events
    private final String stepName;      // null for saga-level events
    private final String payload;       // JSON
    private final SagaStatus targetStatus;  // non-null for transition events, null for step-level events

    // Event type constants
    public static final String SAGA_STARTED        = "SAGA_STARTED";
    public static final String STEP_COMPLETED      = "STEP_COMPLETED";
    public static final String STEP_WAITING        = "STEP_WAITING";
    public static final String STEP_CONFIRMED      = "STEP_CONFIRMED";   // TCC
    public static final String STEP_COMPENSATED    = "STEP_COMPENSATED";
    public static final String COMPENSATION_FAILED = "COMPENSATION_FAILED";
    public static final String SAGA_COMPENSATING   = "SAGA_COMPENSATING";
    public static final String SAGA_CONFIRMING     = "SAGA_CONFIRMING";  // TCC
    public static final String SAGA_FAILED         = "SAGA_FAILED";
    public static final String SAGA_COMPLETED      = "SAGA_COMPLETED";
    public static final String SAGA_COMPENSATED    = "SAGA_COMPENSATED";
    public static final String SAGA_ESCALATED      = "SAGA_ESCALATED";

    public SagaStatus getTargetStatus() { return targetStatus; }

    // Step-level factory methods (no status transition)
    public static SagaEvent sagaStarted(String sagaName, Map<String, Object> input) { ... }
    public static SagaEvent stepCompleted(int stepIndex, String stepName, StepResult result) { ... }
    public static SagaEvent stepWaiting(int stepIndex) { ... }
    public static SagaEvent stepConfirmed(int stepIndex, String stepName) { ... }
    public static SagaEvent stepCompensated(int stepIndex) { ... }
    public static SagaEvent compensationFailed(int stepIndex, Exception e) { ... }

    // Saga-level factory methods (each carries its target SagaStatus)
    public static SagaEvent sagaCompensating() { ... }  // → COMPENSATING
    public static SagaEvent sagaConfirming() { ... }    // → CONFIRMING (TCC)
    public static SagaEvent sagaFailed() { ... }        // → FAILED
    public static SagaEvent sagaCompleted() { ... }     // → COMPLETED
    public static SagaEvent sagaCompensated() { ... }   // → COMPENSATED
    public static SagaEvent sagaEscalated(String reason) { ... }  // → ESCALATED
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

Total: 7 INSERTs to saga_events + 2 writes to saga_index = 9 writes
```

| Framework | Writes per step | Total for 5-step saga | Model |
|---|---|---|---|
| **This design** | 1 INSERT | 9 (7 events + 2 index) | Append-only events + mutable index |
| **Axon** | 1 INSERT | ~7 | Append-only event store |
| **Eventuate** | 1 INSERT | ~7 | Append-only events + CDC |
| **Temporal** | ~1 (batched) | ~7 | Append-only event history |
| **Seata** | 2 (INSERT + UPDATE) | ~12 | Mutable rows (STARTED + COMPLETED) |
| **Previous design** | 3 ops in 1 tx | ~17 | Mutable rows (UPSERT + UPDATE + INSERT) |

### SagaStore Interface

`SagaStore` is an interface, allowing alternative implementations (e.g., `JdbcSagaStore` in the future) and easy mocking in tests. The default implementation, `ScalarDbSagaStore`, uses ScalarDB's transaction API.

```java
// --- store/SagaStore.java ---
public interface SagaStore {
    // Saga lifecycle — writes to both saga_events and saga_index in ONE transaction
    String createSaga(String sagaName, String ownerId,
                       Map<String, Object> input, String definitionJson);

    // Append-only event write — 1 INSERT to saga_events (no index update)
    // Used for step-level events (STEP_COMPLETED, STEP_COMPENSATED, etc.)
    // Caller provides the sequence number (tracked in SagaContext).
    void appendEvent(String sagaId, int sequence, SagaEvent event);

    // Append event + transition saga_index status atomically in ONE transaction.
    // The new status is derived from event.getTargetStatus().
    // The old status and index metadata are provided by the caller (tracked in
    // SagaContext), so no reads are needed — the transaction is 3 pure writes:
    // 1 INSERT (event) + 1 DELETE (old index row) + 1 INSERT (new index row).
    // Used for status transitions (COMPENSATING, COMPLETED, ESCALATED, etc.)
    void recordTransition(String sagaId, int sequence, SagaStatus oldStatus,
                           SagaIndexMetadata metadata, SagaEvent event);

    // Recovery — scan saga_index + replay saga_events
    List<SagaInstance> findRecoverable(long recoveryTimeoutMs);
    boolean claimForRecovery(SagaInstance saga, String newOwnerId);
    List<SagaEvent> getEvents(String sagaId);

    // Queries
    SagaInstance getInstance(String sagaId);
}
```

### ScalarDbSagaStore (Default Implementation)

```java
// --- store/ScalarDbSagaStore.java ---
public class ScalarDbSagaStore implements SagaStore {
    private final DistributedTransactionManager txManager;

    public ScalarDbSagaStore(DistributedTransactionManager txManager) {
        this.txManager = txManager;
    }

    /**
     * Create a new saga. Writes to both saga_events (SAGA_STARTED event)
     * and saga_index (RUNNING status) in one transaction.
     */
    public String createSaga(String sagaName, String ownerId,
                              Map<String, Object> input, String definitionJson) {
        String sagaId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();

            // 1. Append SAGA_STARTED event (sequence = 0)
            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_events")
                .partitionKey(Key.ofText("saga_id", sagaId))
                .clusteringKey(Key.ofInt("sequence", 0))
                .textValue("event_type", SagaEvent.SAGA_STARTED)
                .intValue("step_index", -1)
                .textValue("payload", toJson(Map.of(
                    "sagaName", sagaName, "input", input)))
                .booleanValue("published", false)
                .timestampTZValue("created_at", now)
                .build());

            // 2. Insert saga_index row (bucket + status=RUNNING)
            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .partitionKey(Key.ofInt("bucket", SagaSchema.bucketOf(sagaId)))
                .clusteringKey(Key.of(
                    Key.ofInt("status", SagaStatus.RUNNING.ordinal()),
                    Key.ofText("saga_id", sagaId)))
                .textValue("saga_name", sagaName)
                .textValue("owner_id", ownerId)
                .intValue("version", 0)
                .textValue("definition_version", /* parsed from def */ "1.0")
                .textValue("definition_json", definitionJson)
                .timestampTZValue("created_at", now)
                .timestampTZValue("updated_at", now)
                .build());

            tx.commit();
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to create saga", e);
        }
        return sagaId;
    }

    /**
     * Append a single event to saga_events.
     * This is the hot-path write — 1 INSERT per step.
     *
     * The sequence number is provided by the caller (tracked in SagaContext),
     * avoiding a scan to find the max sequence.
     */
    public void appendEvent(String sagaId, int sequence, SagaEvent event) {
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();

            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_events")
                .partitionKey(Key.ofText("saga_id", sagaId))
                .clusteringKey(Key.ofInt("sequence", sequence))
                .textValue("event_type", event.getEventType())
                .intValue("step_index", event.getStepIndex())
                .textValue("step_name", event.getStepName())
                .textValue("payload", event.getPayload())
                .booleanValue("published", false)
                .timestampTZValue("created_at", Instant.now())
                .build());

            tx.commit();
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to append event", e);
        }
    }

    /**
     * Atomically: append event + transition saga_index status in ONE transaction.
     * Used for status transitions to ensure event and index are always consistent.
     *
     * Because status is part of the clustering key (immutable in ScalarDB),
     * a status transition = DELETE old row + INSERT new row with the new status.
     * Both happen in the same transaction with the event append.
     *
     * The sequence number, old status, and index metadata are all provided by
     * the caller (tracked in SagaContext). This means the transaction needs
     * zero reads — it's 3 pure writes:
     *   1 INSERT (event) + 1 DELETE (old index row) + 1 INSERT (new index row)
     *
     * Reduced from the original 5 ops (2 scans + 2 inserts + 1 delete).
     */
    public void recordTransition(String sagaId, int sequence,
                                  SagaStatus oldStatus, SagaIndexMetadata metadata,
                                  SagaEvent event) {
        SagaStatus newStatus = event.getTargetStatus();
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();
            int bucket = SagaSchema.bucketOf(sagaId);
            Instant now = Instant.now();

            // 1. Append event
            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_events")
                .partitionKey(Key.ofText("saga_id", sagaId))
                .clusteringKey(Key.ofInt("sequence", sequence))
                .textValue("event_type", event.getEventType())
                .intValue("step_index", event.getStepIndex())
                .textValue("step_name", event.getStepName())
                .textValue("payload", event.getPayload())
                .booleanValue("published", false)
                .timestampTZValue("created_at", now)
                .build());

            // 2. DELETE old row (old status in clustering key)
            tx.delete(Delete.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .partitionKey(Key.ofInt("bucket", bucket))
                .clusteringKey(Key.of(
                    Key.ofInt("status", oldStatus.ordinal()),
                    Key.ofText("saga_id", sagaId)))
                .build());

            // 3. INSERT new row (new status in clustering key, columns from metadata)
            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .partitionKey(Key.ofInt("bucket", bucket))
                .clusteringKey(Key.of(
                    Key.ofInt("status", newStatus.ordinal()),
                    Key.ofText("saga_id", sagaId)))
                .textValue("saga_name", metadata.sagaName())
                .textValue("owner_id", metadata.ownerId())
                .intValue("version", metadata.version())
                .textValue("definition_version", metadata.definitionVersion())
                .textValue("definition_json", metadata.definitionJson())
                .timestampTZValue("created_at", metadata.createdAt())
                .timestampTZValue("updated_at", now)
                .build());

            tx.commit();
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to record transition", e);
        }
    }

    /**
     * Find sagas stuck in RUNNING or COMPENSATING with stale updated_at.
     *
     * Scans each bucket with clustering key prefix status=RUNNING (and
     * status=COMPENSATING), reading only active sagas. Completed/compensated
     * rows are skipped entirely at the storage layer.
     *
     * Bucket-based partitioning distributes scans across database nodes —
     * no hot-partition problem.
     */
    public List<SagaInstance> findRecoverable(long recoveryTimeoutMs) {
        List<SagaInstance> result = new ArrayList<>();
        Instant threshold = Instant.now().minusMillis(recoveryTimeoutMs);
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();

            // Scan each bucket for RUNNING and COMPENSATING sagas
            for (int bucket = 0; bucket < SagaSchema.NUM_BUCKETS; bucket++) {
                for (int status : new int[]{
                        SagaStatus.RUNNING.ordinal(),
                        SagaStatus.COMPENSATING.ordinal()}) {
                    List<Result> rows = tx.scan(Scan.newBuilder()
                        .namespace(SagaSchema.NAMESPACE).table("saga_index")
                        .partitionKey(Key.ofInt("bucket", bucket))
                        .start(Key.of(
                            Key.ofInt("status", status),
                            Key.ofText("saga_id", "")))     // scan from start of this status
                        .end(Key.of(
                            Key.ofInt("status", status),
                            Key.ofText("saga_id", "\uffff"))) // to end of this status
                        .build());

                    for (Result r : rows) {
                        Instant updatedAt = r.getTimestampTZ("updated_at");
                        if (updatedAt.isBefore(threshold)) {
                            result.add(toSagaInstance(r));
                        }
                    }
                }
            }

            tx.commit();
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to find recoverable sagas", e);
        }
        return result;
    }

    /**
     * Claim a saga for recovery: DELETE old row + INSERT with updated owner_id
     * and incremented version in one transaction. Zero reads — the caller
     * (findRecoverable) already provides the full SagaInstance.
     *
     * ScalarDB's transaction conflict detection ensures that if two replicas
     * try to claim concurrently, one gets CommitConflictException.
     */
    public boolean claimForRecovery(SagaInstance saga, String newOwnerId) {
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();
            String sagaId = saga.getSagaId();
            int bucket = SagaSchema.bucketOf(sagaId);
            int status = saga.getStatus().ordinal();
            Instant now = Instant.now();

            // DELETE old row + INSERT with updated owner and version
            tx.delete(Delete.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .partitionKey(Key.ofInt("bucket", bucket))
                .clusteringKey(Key.of(
                    Key.ofInt("status", status),
                    Key.ofText("saga_id", sagaId)))
                .build());

            tx.insert(Insert.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .partitionKey(Key.ofInt("bucket", bucket))
                .clusteringKey(Key.of(
                    Key.ofInt("status", status),
                    Key.ofText("saga_id", sagaId)))
                .textValue("saga_name", saga.getSagaName())
                .textValue("owner_id", newOwnerId)
                .intValue("version", saga.getVersion() + 1)
                .textValue("definition_version", saga.getDefinitionVersion())
                .textValue("definition_json", saga.getDefinitionJson())
                .timestampTZValue("created_at", saga.getCreatedAt())
                .timestampTZValue("updated_at", now)
                .build());

            tx.commit();
            return true;

        } catch (CommitConflictException e) {
            // Another replica claimed it concurrently
            abortQuietly(tx);
            return false;
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to claim saga for recovery", e);
        }
    }

    /**
     * Replay all events for a saga — single partition scan, ordered by sequence.
     * Used for state reconstruction during recovery and admin queries.
     */
    public List<SagaEvent> getEvents(String sagaId) {
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();
            List<Result> results = tx.scan(Scan.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_events")
                .partitionKey(Key.ofText("saga_id", sagaId))
                .all()
                .build());
            tx.commit();
            return results.stream().map(this::toSagaEvent).collect(Collectors.toList());
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to get saga events", e);
        }
    }

    /**
     * Get saga status from saga_index — single point-read.
     * For full state (including step results), use getEvents() and replay.
     */
    public SagaInstance getInstance(String sagaId) {
        DistributedTransaction tx = null;
        try {
            tx = txManager.begin();

            // Lookup via secondary index on saga_id
            Optional<Result> result = tx.scan(Scan.newBuilder()
                .namespace(SagaSchema.NAMESPACE).table("saga_index")
                .indexKey(Key.ofText("saga_id", sagaId))
                .build())
                .stream().findFirst();

            tx.commit();
            return result.map(this::toSagaInstance).orElse(null);
        } catch (Exception e) {
            abortQuietly(tx);
            throw new SagaPersistenceException("Failed to get saga instance", e);
        }
    }

    private void abortQuietly(DistributedTransaction tx) {
        if (tx != null) {
            try { tx.abort(); } catch (AbortException ignored) {}
        }
    }

    private String toJson(Object obj) { /* Jackson serialization */ }
}
```

## Crash Recovery

### What It Does

On application startup, scans `saga_index` for sagas stuck in `RUNNING` or `COMPENSATING` status with stale `updated_at` (meaning the process crashed mid-execution). Replays events from `saga_events` to reconstruct state, then resumes execution.

### Recovery Logic

```java
// --- recovery/SagaRecoveryManager.java ---
public class SagaRecoveryManager {
    private final SagaStore store;
    private final SagaEngine engine;
    private final Map<String, SagaDefinition> definitions;
    private final String ownerId;
    private final long recoveryTimeoutMs;          // default: 60000 (1 min)
    private final ScheduledExecutorService scheduler;
    private final long recoveryIntervalSeconds;    // default: 30

    public SagaRecoveryManager(SagaStore store, SagaEngine engine,
                                String ownerId, long recoveryTimeoutMs,
                                long recoveryIntervalSeconds) {
        this.store = store;
        this.engine = engine;
        this.ownerId = ownerId;
        this.recoveryTimeoutMs = recoveryTimeoutMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "saga-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start periodic recovery scanning.
     * Runs once immediately (startup recovery), then periodically.
     * Finds stale RUNNING sagas in saga_index and resumes them.
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
     * Single recovery pass: find stale sagas in saga_index,
     * claim via transactional write, replay events, and resume.
     */
    void recover() {
        List<SagaInstance> stuckSagas = store.findRecoverable(recoveryTimeoutMs);

        for (SagaInstance saga : stuckSagas) {
            try {
                // Claim via DELETE + INSERT in one transaction (zero reads).
                // If another replica claims it concurrently, returns false.
                if (!store.claimForRecovery(saga, ownerId)) {
                    continue;  // Another replica claimed it
                }
                recoverOne(saga);
            } catch (Exception e) {
                // Log and continue — don't let one stuck saga block others
                logger.error("Failed to recover saga {}", saga.getSagaId(), e);
            }
        }
    }

    private void recoverOne(SagaInstance saga) {
        // Use the definition snapshot stored in saga_index at creation time.
        // This ensures recovery uses the exact definition the saga was started with,
        // even if the current definition has been updated (new steps, different order,
        // renamed steps). This is the same approach Temporal uses (workflow versioning)
        // and Camunda (process instance → process definition version binding).
        SagaDefinition def;
        if (saga.getDefinitionJson() != null) {
            def = SagaDefinitionParser.parse(saga.getDefinitionJson());
        } else {
            // Fallback for sagas created before versioning was added
            def = definitions.get(saga.getSagaName());
        }
        if (def == null) {
            logger.error("No definition found for saga: {}", saga.getSagaName());
            return;
        }

        // Replay events to reconstruct state
        List<SagaEvent> events = store.getEvents(saga.getSagaId());
        SagaState state = replayEvents(saga, events);

        if (state.status == SagaStatus.COMPENSATING) {
            // Was mid-compensation — resume from where compensation left off.
            // Count COMPENSATION_FAILED events to detect repeated failures.
            long failureCount = events.stream()
                .filter(e -> e.getEventType().equals(SagaEvent.COMPENSATION_FAILED))
                .count();
            if (failureCount >= maxCompensationRecoveryAttempts) {
                // Too many failures — escalate for manual intervention
                store.recordTransition(saga.getSagaId(),
                    state.context.getAndIncrementSequence(),
                    state.context.getCurrentStatus(),
                    state.context.getIndexMetadata(),
                    SagaEvent.sagaEscalated("exceeded " + failureCount + " compensation attempts"));
                return;
            }
            int compensateFrom = state.lastCompensated - 1;
            engine.compensateFrom(def, state.context, compensateFrom);

        } else {
            // Was mid-execution — resume from the next step after last completed.
            // Step MUST be idempotent (same as the forward path).
            engine.resumeFrom(def, state.context, state.lastCompleted + 1);
        }
    }

    /**
     * Replay events to reconstruct saga state.
     * Folds the event stream into: lastCompleted step index, current status,
     * and accumulated SagaContext. A typical saga has 5-15 events — trivial cost.
     */
    private SagaState replayEvents(SagaInstance saga, List<SagaEvent> events) {
        SagaContext context = new SagaContext(saga.getSagaId(), Map.of());
        int lastCompleted = -1;
        int lastCompensated = Integer.MAX_VALUE;
        SagaStatus status = SagaStatus.RUNNING;

        for (SagaEvent event : events) {
            switch (event.getEventType()) {
                case SagaEvent.SAGA_STARTED:
                    Map<String, Object> input = fromJson(event.getPayload());
                    context = new SagaContext(saga.getSagaId(), input);
                    break;
                case SagaEvent.STEP_COMPLETED:
                    lastCompleted = Math.max(lastCompleted, event.getStepIndex());
                    StepResult result = StepResult.fromJson(event.getPayload());
                    context.merge(result);
                    break;
                case SagaEvent.STEP_COMPENSATED:
                    lastCompensated = Math.min(lastCompensated, event.getStepIndex());
                    break;
                case SagaEvent.SAGA_COMPENSATING:
                case SagaEvent.SAGA_CONFIRMING:
                case SagaEvent.SAGA_FAILED:
                case SagaEvent.SAGA_COMPLETED:
                case SagaEvent.SAGA_COMPENSATED:
                case SagaEvent.SAGA_ESCALATED:
                    status = event.getTargetStatus();
                    break;
                // STEP_WAITING (daemon mode only), STEP_CONFIRMED (TCC), etc. handled similarly
            }
        }

        // Reconstruct in-memory tracking fields from the event stream.
        // nextEventSequence = total events replayed (events are 0-indexed by sequence).
        // currentStatus = final status after replaying all events.
        // indexMetadata = immutable saga_index columns from the SagaInstance.
        context.setNextEventSequence(events.size());
        context.setCurrentStatus(status);
        context.setIndexMetadata(new SagaIndexMetadata(
            saga.getSagaName(), saga.getOwnerId(), saga.getVersion(),
            saga.getDefinitionVersion(), saga.getDefinitionJson(),
            saga.getCreatedAt()));

        return new SagaState(context, lastCompleted, lastCompensated, status);
    }

    private static class SagaState {
        final SagaContext context;
        final int lastCompleted;
        final int lastCompensated;
        final SagaStatus status;

        SagaState(SagaContext context, int lastCompleted,
                  int lastCompensated, SagaStatus status) {
            this.context = context;
            this.lastCompleted = lastCompleted;
            this.lastCompensated = lastCompensated;
            this.status = status;
        }
    }
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
  "timeoutMs": 300000,
  "steps": [
    {
      "name": "debit",
      "timeoutMs": 60000,
      "stepClass": "com.example.DebitAccountStep"
    },
    {
      "name": "credit",
      "timeoutMs": 30000,
      "stepClass": "com.example.CreditAccountStep"
    }
  ]
}
```

- **`timeoutMs` (saga-level)**: Total time allowed for the entire saga. Stored as `deadline` in the `SAGA_STARTED` event payload at creation time.
- **`timeoutMs` (step-level)**: Maximum time for a single step execution (including all retry attempts). If exceeded, the step is interrupted and compensation begins.

### Timeout Enforcement in executeSteps()

```java
private SagaInstance executeSteps(SagaDefinition def, SagaContext context,
                                  int startIndex, int completedIndex) {
    String sagaId = context.getSagaId();
    List<StepDefinition> steps = def.getSteps();
    long sagaDeadline = def.getTimeoutMs() > 0
        ? System.currentTimeMillis() + def.getTimeoutMs() : 0;

    try {
        for (int i = startIndex; i < steps.size(); i++) {
            // Check saga-level deadline before each step
            if (sagaDeadline > 0 && System.currentTimeMillis() > sagaDeadline) {
                throw new SagaTimeoutException(
                    "Saga " + sagaId + " exceeded deadline of " + def.getTimeoutMs() + "ms");
            }

            StepDefinition stepDef = steps.get(i);
            Step step = instantiate(stepDef);
            RetryPolicy policy = resolveRetryPolicy(stepDef, def);

            // Calculate per-step deadline (minimum of step timeout and saga deadline)
            long stepDeadline = calculateStepDeadline(stepDef, sagaDeadline);

            // Execute with retry — passes step deadline for timeout enforcement
            StepResult result = executeWithRetry(step, context, policy, stepDeadline);

            // Append-only: 1 INSERT per step (no lease renewal needed)
            store.appendEvent(sagaId, context.getAndIncrementSequence(),
                SagaEvent.stepCompleted(i, stepDef.getName(), result));
            context.merge(result);
            completedIndex = i;
        }

        transition(context, SagaEvent.sagaCompleted());

    } catch (StepTimeoutException | SagaTimeoutException e) {
        // Timeout → compensate from current step (may have had side effects)
        int compensateFrom = completedIndex + 1;
        transition(context, SagaEvent.sagaCompensating());
        try {
            compensationManager.compensate(def, context, compensateFrom);
            transition(context, SagaEvent.sagaCompensated());
        } catch (StepCompensationException ce) {
            // Compensation failed — saga stays in COMPENSATING.
        }

    } catch (StepExecutionException e) {
        // ... existing compensation logic ...
    } catch (Exception e) {
        // ... existing error handling ...
    }

    return store.getInstance(sagaId);
}

private long calculateStepDeadline(StepDefinition stepDef, long sagaDeadline) {
    long stepTimeout = stepDef.getTimeoutMs();
    if (stepTimeout > 0 && sagaDeadline > 0) {
        return Math.min(System.currentTimeMillis() + stepTimeout, sagaDeadline);
    } else if (stepTimeout > 0) {
        return System.currentTimeMillis() + stepTimeout;
    } else {
        return sagaDeadline;  // 0 means no timeout
    }
}
```

### Exception Hierarchy

```java
// --- exception/StepTimeoutException.java ---
public class StepTimeoutException extends StepExecutionException {
    public StepTimeoutException(String message) { super(message); }
}

// --- exception/SagaTimeoutException.java ---
public class SagaTimeoutException extends RuntimeException {
    public SagaTimeoutException(String message) { super(message); }
}
```

### Interaction with Recovery

When a step times out:
1. The virtual thread running `step.execute()` is interrupted via `Future.cancel(true)`
2. Compensation begins immediately — the current replica handles it
3. Other replicas will not pick up this saga because the current replica is still active and updating `saga_index` on status changes

## Graceful Shutdown

### What It Does

When the process shuts down (e.g., Kubernetes pod termination, deployment rolling update), the engine:

1. Stops accepting new sagas
2. Waits for in-flight sagas to complete their current step
3. Marks incomplete sagas in `saga_index` for immediate recovery (sets `updated_at = 0`)
4. Stops the recovery scheduler

Without graceful shutdown, the recovery timeout (60s default) means a recovery delay of up to 60s, and the interrupted step may leave partial side effects.

### SagaEngine.shutdown()

```java
// --- engine/SagaEngine.java ---
public class SagaEngine implements AutoCloseable {
    private volatile boolean shuttingDown = false;
    private final Set<String> activeSagas = ConcurrentHashMap.newKeySet();
    private final long shutdownTimeoutMs;  // default: 30_000

    /**
     * Initiate graceful shutdown.
     */
    public void shutdown() {
        shuttingDown = true;

        // 1. Wait for in-flight sagas to finish their current step
        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        while (!activeSagas.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);  // virtual thread — cheap
        }

        // 2. Mark active sagas for immediate recovery
        //    (update saga_index.updated_at to epoch 0 so recovery picks them up)
        for (String sagaId : activeSagas) {
            try {
                store.markForRecovery(sagaId);  // sets updated_at = 0
            } catch (Exception e) {
                logger.warn("Failed to mark saga {} for recovery", sagaId, e);
            }
        }

        // 3. Stop recovery scheduler
        recoveryManager.stop();
    }

    // Called at the start of executeSteps()
    private void checkShutdown() {
        if (shuttingDown) {
            throw new SagaShutdownException("Engine is shutting down");
        }
    }

    @Override
    public void close() { shutdown(); }
}
```

### Integration with JVM Shutdown

```java
// Automatic cleanup via shutdown hook
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    sagaManager.shutdown();
}));

// Or via Spring @PreDestroy / Quarkus @Shutdown
@PreDestroy
void onShutdown() {
    sagaManager.shutdown();
}
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
    // Park the saga — append STEP_WAITING event, release the thread.
    // The saga will be resumed by completeStep() when the callback arrives.
    store.appendEvent(sagaId, context.getAndIncrementSequence(),
        SagaEvent.stepWaiting(i));
    return store.getInstance(sagaId);  // returns with status=RUNNING
}

// Synchronous completion — continue as before
store.appendEvent(sagaId, context.getAndIncrementSequence(),
    SagaEvent.stepCompleted(i, stepDef.getName(), result));
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
SagaInstance completeStep(String sagaId, String stepName, Map<String, Object> output);
```

```java
// --- engine/DefaultSagaManager.java ---
@Override
public SagaInstance completeStep(String sagaId, String stepName,
                                  Map<String, Object> output) {
    SagaInstance instance = store.getInstance(sagaId);
    if (instance == null) throw new SagaNotFoundException(sagaId);

    // Replay events to find the WAITING step
    List<SagaEvent> events = store.getEvents(sagaId);
    SagaEvent waitingEvent = events.stream()
        .filter(e -> e.getEventType().equals(SagaEvent.STEP_WAITING)
                  && e.getStepName().equals(stepName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "Step " + stepName + " is not in WAITING status"));

    // Rebuild context from events (also reconstructs nextEventSequence and currentStatus)
    SagaDefinition def = loadDefinition(instance);
    SagaState state = replayEvents(instance, events);

    // Append step completed event with the callback result
    store.appendEvent(sagaId, state.context.getAndIncrementSequence(),
        SagaEvent.stepCompleted(waitingEvent.getStepIndex(), stepName, StepResult.of(output)));
    state.context.merge(StepResult.of(output));

    return engine.resumeFrom(def, state.context, waitingEvent.getStepIndex() + 1);
}
```

The daemon exposes this via HTTP and/or gRPC depending on configuration:

```java
// HTTP (built into the daemon)
@PostMapping("/api/sagas/{sagaId}/steps/{stepName}/complete")
public ResponseEntity<SagaInstance> completeStep(...) {
    return ResponseEntity.ok(sagaManager.completeStep(sagaId, stepName, output));
}
```

```protobuf
// gRPC (built into the daemon)
service SagaCallbackService {
    rpc CompleteStep(CompleteStepRequest) returns (SagaInstanceResponse);
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
  Step1.execute() ──► Step2.execute() ──► Step3.execute()     (Try phase)
       │                   │                   │
       │  all succeeded:   │                   │
       │                   ▼                   ▼
  Step1.confirm() ──► Step2.confirm() ──► Step3.confirm()  ──► COMPLETED
       │
       │  on Step3.execute() failure:
       │
  Step2.compensate() ──► Step1.compensate()                ──► COMPENSATED
       │
       └── execute() only RESERVES. confirm() commits. compensate() releases.
```

### TCC Step Interface

Steps in TCC mode implement `TccStep` instead of `Step`:

```java
public class TccDebitStep implements TccStep {
    private final AccountServiceClient client;

    @Override
    public String getName() { return "debit"; }

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
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
    public void compensate(SagaContext ctx) throws StepCompensationException {
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
  "recoverStrategy": "COMPENSATE",
  "timeoutMs": 300000,
  "steps": [
    {
      "name": "debit",
      "stepClass": "com.example.TccDebitStep",
      "timeoutMs": 60000
    },
    {
      "name": "credit",
      "stepClass": "com.example.TccCreditStep",
      "timeoutMs": 30000
    }
  ]
}
```

The only difference from a Saga definition is `"mode": "TCC"`. The step classes must implement `TccStep` instead of `Step`. If a step class implements only `Step` (no `confirm()`), the engine throws a `SagaDefinitionException` at registration time.

### Engine Execution Logic

```java
// In SagaEngine.executeSteps():

if (definition.getMode() == SagaMode.TCC) {
    // === TCC MODE ===

    // Phase 1: Try — execute all steps (reserve resources)
    for (int i = 0; i < steps.size(); i++) {
        StepResult result = executeWithRetry(steps.get(i), context, policy, stepDeadline);
        store.appendEvent(sagaId, context.getAndIncrementSequence(),
            SagaEvent.stepCompleted(i, steps.get(i).getName(), result));
        context.merge(result);
        completedIndex = i;
    }
    // All Try steps succeeded. Transition to CONFIRMING.
    transition(context, SagaEvent.sagaConfirming());

    // Phase 2: Confirm — commit all reservations
    for (int i = 0; i < steps.size(); i++) {
        TccStep tccStep = (TccStep) steps.get(i);
        confirmWithRetry(tccStep, context, confirmRetryPolicy);
        store.appendEvent(sagaId, context.getAndIncrementSequence(),
            SagaEvent.stepConfirmed(i, steps.get(i).getName()));
    }
    transition(context, SagaEvent.sagaCompleted());

} else {
    // === SAGA MODE (existing behavior) ===
    // ... execute steps, compensate on failure ...
}
```

### Confirm Retry Behavior

The confirm phase has a critical property: **it must always succeed**. Resources are already reserved (Try succeeded), so confirmation is just committing the reservation. Therefore, the confirm retry policy is more aggressive than the standard retry:

```java
// --- engine/SagaEngine.java ---
private void confirmWithRetry(TccStep step, SagaContext context,
                               RetryPolicy policy) {
    int attempt = 0;
    while (true) {
        try {
            step.confirm(context);
            return;  // success
        } catch (Exception e) {
            attempt++;
            if (attempt >= policy.getMaxAttempts()) {
                // Confirm keeps failing — escalate for manual intervention.
                // This should be rare: the reservation exists, so confirmation
                // should eventually succeed unless the participant is permanently down.
                throw new ConfirmationFailedException(step.getName(), attempt, e);
            }
            // Wait and retry — confirm MUST eventually succeed
            sleepWithBackoff(policy, attempt);
        }
    }
}
```

Default confirm retry policy: **10 attempts** with exponential backoff (more aggressive than the standard 3 attempts for forward steps, because confirm is expected to succeed).

```java
// --- engine/RetryPolicy.java ---
public static RetryPolicy confirmDefault() {
    return new RetryPolicy(10, 500, 2.0, 60_000);  // 10 attempts, 500ms initial, 60s max
}
```

### Recovery in TCC Mode

The recovery manager handles two additional scenarios in TCC mode:

| Saga Status at Crash | Recovery Action |
|---|---|
| `RUNNING` (Try phase) | Resume Try from last completed step. If Try fails, Cancel all completed steps. |
| `CONFIRMING` | Resume Confirm from last confirmed step. Confirm must succeed — retry aggressively. |
| `COMPENSATING` | Continue Cancel (same as Saga mode compensation). |

```java
// In SagaRecoveryManager.recoverOne():
if (definition.getMode() == SagaMode.TCC
        && instance.getStatus() == SagaStatus.CONFIRMING) {
    // Crash during confirm phase. Resume confirming from where we left off.
    // Find the last confirmed step index and continue from the next one.
    int lastConfirmed = findLastConfirmedStepIndex(logs);
    engine.resumeConfirm(definition, context, lastConfirmed + 1);
} else {
    // Standard recovery (Try in progress, or Saga mode)
    engine.resumeFrom(definition, context, lastCompletedIndex + 1);
}
```

### Event Types for TCC

The `STEP_CONFIRMED` event type tracks successful `confirm()` calls in TCC mode. On recovery, event replay distinguishes between steps that completed Try (STEP_COMPLETED) and steps that completed Confirm (STEP_CONFIRMED).

### When to Use TCC vs Saga

| Aspect | Saga | TCC |
|---|---|---|
| **Step semantics** | Execute commits real work | Execute only reserves |
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
| **Try/Confirm/Cancel** | Separate methods via annotation | `TccStep.execute()` / `confirm()` / `compensate()` |
| **Resource manager** | Branch transaction model | Step-level (each step manages its own resources) |

---

# Part III: Communication & Integration

## ServiceInvoker and Framework Integration

### Architecture: Three Layers

The saga engine is designed as three layers. Each layer builds on the one below, and users choose which layer to interact with based on their needs:

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 3: Framework Integration (Spring Boot / Quarkus)           │
│                                                                   │
│  @SagaStep, @SagaCompensation annotations                        │
│  Build-time (Quarkus) or startup-time (Spring) scanning          │
│  Auto-generates ServiceInvoker registrations                     │
│  Users keep using framework-native clients                       │
│  (@FeignClient, @GrpcClient, @RestClient, @Autowired, @Inject)  │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2: ServiceInvoker (framework-agnostic)                     │
│                                                                   │
│  ServiceInvokerRegistry                                          │
│  Typed lambdas — no reflection                                   │
│  Built-in invokers: GrpcInvoker, HttpInvoker, SpringBeanInvoker  │
│  Saga context propagation (X-Saga-Id header / gRPC metadata)    │
├──────────────────────────────────────────────────────────────────┤
│  Layer 1: Core Engine (framework-agnostic)                        │
│                                                                   │
│  Step interface, SagaEngine, SagaStore, CompensationManager      │
│  SagaRecoveryManager, RetryPolicy                                │
│  Works with any transport, any database                          │
└──────────────────────────────────────────────────────────────────┘
```

Users can interact at any layer:
- **Layer 1 only**: Full control, implement `Step` interface directly. Best for complex steps or when no framework is used.
- **Layer 2**: Register typed lambdas in `ServiceInvokerRegistry`. Java builder or JSON/YAML-driven saga definitions. No Step classes needed.
- **Layer 3**: Annotate methods in a saga class. Framework scans and auto-registers. Minimal boilerplate.

### Layer 2: ServiceInvoker

#### Why Not Reflection (Like Seata)

Seata's `ServiceInvokerManager` uses reflection to invoke business methods:

```java
// Seata internally does this:
Object bean = applicationContext.getBean(serviceName);
Method method = bean.getClass().getMethod(methodName);
Object result = method.invoke(bean, args);  // ← reflection, type-unsafe
```

This is fragile — type mismatches are runtime errors, parameter mapping is complex, and debugging is hard. Our design uses **typed lambdas** instead:

```java
// Our approach — type-safe, no reflection
registry.register("account-service", GrpcInvoker.builder(accountStub)
    .action("debit", (stub, ctx) -> {
        var resp = stub.debit(DebitRequest.newBuilder()
            .setAccountId(ctx.get("accountId", String.class))
            .setAmount(ctx.get("amount", Integer.class))
            .build());
        return StepResult.of("debitId", resp.getDebitId());
    })
    .compensation("debit", (stub, ctx) -> {
        stub.reverseDebit(ReverseDebitRequest.newBuilder()
            .setDebitId(ctx.get("debitId", String.class))
            .build());
    })
    .build());
```

#### ServiceInvokerRegistry

```java
// --- invoker/ServiceInvokerRegistry.java ---
public class ServiceInvokerRegistry {
    private final Map<String, ServiceInvoker> invokers = new ConcurrentHashMap<>();

    public void register(String serviceName, ServiceInvoker invoker) {
        invokers.put(serviceName, invoker);
    }

    /**
     * Look up an invoker by service name, then execute the named method.
     * Called by SagaEngine when the saga definition uses service/method
     * instead of stepClass.
     */
    public StepResult execute(String serviceName, String method, SagaContext ctx)
            throws StepExecutionException {
        ServiceInvoker invoker = invokers.get(serviceName);
        if (invoker == null) {
            throw new StepExecutionException("No invoker registered for: " + serviceName);
        }
        return invoker.execute(method, ctx);
    }

    public void compensate(String serviceName, String method, SagaContext ctx)
            throws StepCompensationException {
        ServiceInvoker invoker = invokers.get(serviceName);
        if (invoker == null) {
            throw new StepCompensationException("No invoker registered for: " + serviceName);
        }
        invoker.compensate(method, ctx);
    }
}

// --- invoker/ServiceInvoker.java ---
public interface ServiceInvoker {
    StepResult execute(String method, SagaContext ctx) throws StepExecutionException;
    void compensate(String method, SagaContext ctx) throws StepCompensationException;
}
```

#### Built-In Invokers

```java
// --- invoker/GrpcInvoker.java ---
public class GrpcInvoker<S> implements ServiceInvoker {
    private final S stub;
    private final Map<String, BiFunction<S, SagaContext, StepResult>> actions;
    private final Map<String, BiConsumer<S, SagaContext>> compensations;

    @Override
    public StepResult execute(String method, SagaContext ctx) throws StepExecutionException {
        BiFunction<S, SagaContext, StepResult> action = actions.get(method);
        if (action == null) {
            throw new StepExecutionException("No action registered: " + method);
        }
        try {
            return action.apply(stub, ctx);
        } catch (Exception e) {
            throw new StepExecutionException(e);
        }
    }

    @Override
    public void compensate(String method, SagaContext ctx) throws StepCompensationException {
        BiConsumer<S, SagaContext> compensation = compensations.get(method);
        if (compensation == null) {
            throw new StepCompensationException("No compensation registered: " + method);
        }
        try {
            compensation.accept(stub, ctx);
        } catch (Exception e) {
            throw new StepCompensationException(e);
        }
    }

    // Builder pattern (see examples above)
    public static <S> Builder<S> builder(S stub) { return new Builder<>(stub); }
}
```

`HttpInvoker` follows the same pattern but wraps an HTTP client, automatically propagates `X-Saga-Id` header, and classifies HTTP status codes as retryable/non-retryable.

#### Proto Ownership

**The participant service defines the `.proto` — it is their API contract.** The orchestrator includes the participant's generated stubs as a compile-time dependency.

| Layer | Who defines proto | Who generates stubs | How stubs are used |
|-------|------------------|--------------------|--------------------|
| **Layer 1** (Step) | Participant service | Orchestrator adds generated jar to classpath | Step author calls stub directly in `execute()` |
| **Layer 2** (GrpcInvoker) | Participant service | Orchestrator adds generated jar to classpath | Passed to `GrpcInvoker.builder(stub)` at startup |
| **Layer 2b** (Declarative) | Participant service | Orchestrator adds generated jar to classpath | `GrpcTransportAdapter` uses generated message types and `MethodDescriptor` via reflection |
| **Phase 7** (saga protocol) | Saga engine project | Both coordinator and participant | Coordinator API + participant protocol — published by the saga engine project |

For Layers 1-2b, this is standard gRPC practice: the service owner publishes a `.proto` file (or a generated-stubs jar), and all callers depend on it. The saga engine itself is transport-agnostic — it never sees protobuf types.

Phase 7 is different: the saga engine project defines the `.proto` for the coordinator↔participant protocol itself (saga lifecycle API, participant callbacks). Both sides generate stubs from it.

#### Updated Saga Definition Format

The JSON format now supports **both** `stepClass` (Layer 1) and `service`/`method` (Layer 2):

```json
{
  "name": "PlaceOrder",
  "steps": [
    {
      "name": "debit",
      "service": "account-service",
      "method": "debit"
    },
    {
      "name": "ship",
      "service": "shipping-service",
      "method": "ship"
    },
    {
      "name": "complex",
      "stepClass": "com.example.ComplexStep"
    }
  ]
}
```

When `service`/`method` is specified, the engine uses `ServiceInvokerRegistry` to dispatch. When `stepClass` is specified, the engine instantiates the `Step` class directly (existing behavior). Both can be mixed in the same saga.

### Layer 2b: Declarative Step Communication

#### The Problem

Even with Layer 2 (ServiceInvoker) and Layer 3 (annotations), the user still writes the **actual RPC call logic** inside each step method:

```java
@SagaStep(saga = "MoneyTransfer", name = "debit", order = 1)
public StepResult debit(SagaContext ctx) {
    // User still writes all of this:
    var resp = accountStub.debit(DebitRequest.newBuilder()
        .setAccountId(ctx.get("accountId", String.class))
        .setAmount(ctx.get("amount", Integer.class))
        .build());
    return StepResult.of("debitId", resp.getDebitId());
}
```

For straightforward service calls (which are the majority of saga steps), this is repetitive boilerplate: extract values from context → build request → make call → extract response → put into context.

#### Solution: Declarative Communication in the Saga Definition

Define the request/response mapping directly in the saga JSON. The engine handles transport, marshaling, context propagation, and error classification — no Java code needed for simple steps.

```json
{
  "name": "MoneyTransfer",
  "steps": [
    {
      "name": "debit",
      "call": {
        "service": "account-service",
        "method": "debit",
        "transport": "grpc",
        "request": {
          "account_id": "${accountId}",
          "amount": "${amount}"
        },
        "output": {
          "debitId": "$.debit_id"
        }
      },
      "compensate": {
        "method": "reverseDebit",
        "request": {
          "debit_id": "${debitId}"
        }
      }
    },
    {
      "name": "credit",
      "call": {
        "service": "account-service",
        "method": "credit",
        "transport": "grpc",
        "request": {
          "account_id": "${toAccountId}",
          "amount": "${amount}"
        },
        "output": {
          "creditId": "$.credit_id"
        }
      },
      "compensate": {
        "method": "reverseCredit",
        "request": {
          "credit_id": "${creditId}"
        }
      }
    },
    {
      "name": "complex",
      "stepClass": "com.example.ComplexStep"
    }
  ]
}
```

**Expression syntax:**
- `${key}` — reads a value from the saga context
- `$.field` — extracts a field from the service response (JSON path)

#### How It Works

```
Saga definition (JSON/YAML)
    │
    ▼
┌──────────────────────────────────────────────┐
│  DeclarativeStepAdapter                       │
│                                               │
│  1. Read request mapping from JSON            │
│  2. Resolve ${...} expressions from context   │
│  3. Build request object                      │
│  4. Delegate to transport adapter             │
│  5. Extract output via $.path from response   │
│  6. Return StepResult with extracted values   │
└──────────┬───────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│  Transport Adapter (pluggable)                │
│                                               │
│  GrpcTransportAdapter:                        │
│  - Builds protobuf message from request map   │
│  - Calls stub method via reflection on        │
│    generated gRPC stub                        │
│  - Propagates X-Saga-Id via gRPC metadata     │
│  - Maps gRPC status codes to retryable/       │
│    non-retryable                              │
│                                               │
│  HttpTransportAdapter:                        │
│  - Builds JSON request body from request map  │
│  - Calls HTTP endpoint (POST/PUT)             │
│  - Propagates X-Saga-Id via HTTP header       │
│  - Maps HTTP status codes:                    │
│    - 2xx → success                            │
│    - 408, 429, 502, 503, 504 → retryable     │
│    - 4xx → non-retryable                      │
│    - 5xx → retryable (default)                │
└──────────────────────────────────────────────┘
```

#### DeclarativeStepAdapter Implementation

```java
// --- invoker/DeclarativeStepAdapter.java ---
public class DeclarativeStepAdapter implements Step {
    private final String name;
    private final TransportAdapter transport;
    private final CallDefinition actionCall;
    private final CallDefinition compensateCall;

    @Override
    public String getName() { return name; }

    @Override
    public StepResult execute(SagaContext ctx) throws StepExecutionException {
        try {
            // 1. Resolve request parameters from context
            Map<String, Object> request = resolveExpressions(actionCall.getRequest(), ctx);

            // 2. Call the service via transport adapter
            Map<String, Object> response = transport.call(
                actionCall.getService(), actionCall.getMethod(), request, ctx.getSagaId());

            // 3. Extract output fields from response
            Map<String, Object> output = extractOutput(actionCall.getOutput(), response);

            return StepResult.of(output);
        } catch (TransportException e) {
            throw new StepExecutionException(e);
        }
    }

    @Override
    public void compensate(SagaContext ctx) throws StepCompensationException {
        try {
            Map<String, Object> request = resolveExpressions(compensateCall.getRequest(), ctx);
            transport.call(compensateCall.getService(),
                           compensateCall.getMethod(), request, ctx.getSagaId());
        } catch (TransportException e) {
            throw new StepCompensationException(e);
        }
    }

    /**
     * Resolve ${...} expressions by looking up values in the saga context.
     * E.g., "${accountId}" → ctx.get("accountId")
     */
    private Map<String, Object> resolveExpressions(Map<String, String> template,
                                                     SagaContext ctx) {
        Map<String, Object> resolved = new LinkedHashMap<>();
        for (var entry : template.entrySet()) {
            String value = entry.getValue();
            if (value.startsWith("${") && value.endsWith("}")) {
                String key = value.substring(2, value.length() - 1);
                resolved.put(entry.getKey(), ctx.get(key, Object.class));
            } else {
                resolved.put(entry.getKey(), value);  // literal value
            }
        }
        return resolved;
    }

    /**
     * Extract fields from the service response using $.path expressions.
     * E.g., "$.debit_id" → response.get("debit_id")
     */
    private Map<String, Object> extractOutput(Map<String, String> outputMapping,
                                                Map<String, Object> response) {
        Map<String, Object> output = new LinkedHashMap<>();
        for (var entry : outputMapping.entrySet()) {
            String path = entry.getValue();
            if (path.startsWith("$.")) {
                String field = path.substring(2);
                output.put(entry.getKey(), response.get(field));
            }
        }
        return output;
    }
}
```

#### TransportAdapter Interface

```java
// --- invoker/TransportAdapter.java ---
public interface TransportAdapter {
    /**
     * Call a remote service method.
     *
     * @param service   Service name (resolved to endpoint via configuration)
     * @param method    Method/operation name
     * @param request   Request parameters (key-value pairs)
     * @param sagaId    Saga ID for context propagation
     * @return Response as key-value pairs
     * @throws TransportException with retryable/non-retryable classification
     */
    Map<String, Object> call(String service, String method,
                              Map<String, Object> request, String sagaId)
        throws TransportException;
}

// --- invoker/TransportException.java ---
public class TransportException extends Exception {
    private final boolean retryable;

    public TransportException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() { return retryable; }
}
```

#### GrpcTransportAdapter

**Requires the participant's generated stubs on the orchestrator's classpath** — `buildProtobufMessage()` needs the generated message types and `lookupMethod()` needs the `MethodDescriptor`, both from the participant's `.proto` (see [Proto Ownership](#proto-ownership) above).

```java
// --- invoker/GrpcTransportAdapter.java ---
public class GrpcTransportAdapter implements TransportAdapter {
    private final Map<String, ManagedChannel> channels;       // service → gRPC channel
    private final Map<String, MethodDescriptor<?, ?>> methods; // service.method → descriptor

    @Override
    public Map<String, Object> call(String service, String method,
                                      Map<String, Object> request, String sagaId)
            throws TransportException {
        try {
            ManagedChannel channel = channels.get(service);
            if (channel == null) {
                throw new TransportException(
                    "No gRPC channel for service: " + service, null, false);
            }

            // Propagate saga context via gRPC metadata
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("x-saga-id", Metadata.ASCII_STRING_MARSHALLER), sagaId);

            // Build protobuf message from request map and invoke
            // (uses protobuf JSON format for map → message conversion)
            Message requestMsg = buildProtobufMessage(service, method, request);
            Message responseMsg = ClientCalls.blockingUnaryCall(
                channel, lookupMethod(service, method), CallOptions.DEFAULT
                    .withDeadlineAfter(30, TimeUnit.SECONDS), requestMsg);

            // Convert response protobuf to map
            return protobufToMap(responseMsg);

        } catch (StatusRuntimeException e) {
            boolean retryable = isRetryableStatus(e.getStatus().getCode());
            throw new TransportException(e.getMessage(), e, retryable);
        }
    }

    private boolean isRetryableStatus(Status.Code code) {
        return code == Status.Code.UNAVAILABLE
            || code == Status.Code.DEADLINE_EXCEEDED
            || code == Status.Code.RESOURCE_EXHAUSTED
            || code == Status.Code.ABORTED;
    }
}
```

#### HttpTransportAdapter

```java
// --- invoker/HttpTransportAdapter.java ---
public class HttpTransportAdapter implements TransportAdapter {
    private final HttpClient httpClient;
    private final Map<String, String> serviceBaseUrls;  // service → base URL

    @Override
    public Map<String, Object> call(String service, String method,
                                      Map<String, Object> request, String sagaId)
            throws TransportException {
        String baseUrl = serviceBaseUrls.get(service);
        if (baseUrl == null) {
            throw new TransportException(
                "No base URL for service: " + service, null, false);
        }

        try {
            HttpRequest httpReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + method))
                .header("Content-Type", "application/json")
                .header("X-Saga-Id", sagaId)
                .POST(HttpRequest.BodyPublishers.ofString(toJson(request)))
                .timeout(Duration.ofSeconds(30))
                .build();

            HttpResponse<String> resp = httpClient.send(httpReq,
                HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                return fromJson(resp.body());
            }

            boolean retryable = isRetryableHttpStatus(resp.statusCode());
            throw new TransportException(
                "HTTP " + resp.statusCode() + ": " + resp.body(), null, retryable);

        } catch (IOException | InterruptedException e) {
            throw new TransportException(e.getMessage(), e, true);  // network errors are retryable
        }
    }

    private boolean isRetryableHttpStatus(int status) {
        return status == 408 || status == 429
            || status == 502 || status == 503 || status == 504;
    }
}
```

#### Service Endpoint Configuration

Transport adapters resolve service names to endpoints via configuration:

```properties
# Service endpoint configuration (orchestrator's application.properties)
saga.services.account-service.transport=grpc
saga.services.account-service.endpoint=account-service:50051

saga.services.shipping-service.transport=http
saga.services.shipping-service.endpoint=http://shipping-service:8080
```

#### How It Fits the Layer Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 3: Framework Integration (Spring Boot / Quarkus)           │
│  @SagaStep, @SagaCompensation annotations                        │
│  User writes method body with injected clients                   │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2b: Declarative Communication (NEW)                        │
│                                                                   │
│  JSON-defined request/response mapping                           │
│  Built-in transport adapters: GrpcTransportAdapter,              │
│    HttpTransportAdapter                                          │
│  Automatic context propagation (X-Saga-Id)                       │
│  Automatic error classification (retryable vs non-retryable)     │
│  User writes ZERO Java code for simple steps                     │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2: ServiceInvoker (framework-agnostic)                     │
│  Typed lambdas — no reflection                                   │
│  Built-in invokers: GrpcInvoker, HttpInvoker, SpringBeanInvoker  │
├──────────────────────────────────────────────────────────────────┤
│  Layer 1: Core Engine (framework-agnostic)                        │
│  Step interface, SagaEngine, SagaStore, CompensationManager      │
└──────────────────────────────────────────────────────────────────┘
```

**When to use which layer:**

| Layer | When to use | User writes |
|---|---|---|
| **Layer 1** (Step) | Complex steps with conditional logic, multi-call orchestration, or non-standard transports | Full Java Step class |
| **Layer 2** (ServiceInvoker) | Programmatic registration with type-safe lambdas | Lambda per action/compensation |
| **Layer 2b** (Declarative) | Straightforward service calls where request/response is a simple field mapping | **Zero Java code** — only JSON/YAML definition |
| **Layer 3** (Annotations) | Framework users (Spring/Quarkus) who prefer annotations and injected clients | Annotated method with call logic |

Layer 2b and Layer 3 solve different problems:
- **Layer 2b** eliminates the **communication boilerplate** — the engine handles the RPC call, request building, and response extraction.
- **Layer 3** eliminates the **orchestration boilerplate** — no Step classes, no JSON files, no manual registration. But the user still writes the call logic inside the method body.

They can be combined: a saga can mix declarative steps (Layer 2b) for simple calls with annotated methods (Layer 3) or custom Step classes (Layer 1) for complex logic.

#### Saga Context Propagation

All transport adapters automatically propagate the saga context to participants:

| Transport | Propagation mechanism |
|---|---|
| gRPC | `x-saga-id` metadata key |
| HTTP | `X-Saga-Id` HTTP header |

Participants can read this header for logging, tracing, or idempotency purposes. See "Participant Idempotency Levels" below.

#### Participant Idempotency Levels

The saga engine supports three levels of idempotency handling, reflecting different trade-offs between participant simplicity and reliability guarantees:

**Level 1: Basic (Industry Norm — Phase 1)**

The orchestrator propagates the saga ID and step name to participants via headers (`X-Saga-Id`, `X-Saga-Step`). Participants are responsible for implementing their own idempotency logic. This is the same model used by all current competitors (Seata, MicroTx, Narayana LRA).

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

**Level 2: Participant SDK (Future Enhancement)**

An optional, lightweight SDK for participant services that provides:
- `@SagaParticipant` annotation with auto-exposed status check endpoint
- `@Idempotent` annotation with built-in dedup (the SDK manages the dedup state, so the participant doesn't need to)
- Automatic saga context extraction from headers

```java
// In the participant service — NOT the orchestrator
@SagaParticipant(service = "account-service")
@RestController
public class AccountEndpoint {

    @SagaAction(name = "debit")
    @PostMapping("/debit")
    public DebitResponse debit(@RequestBody DebitRequest req,
                               @SagaId String sagaId) {   // auto-extracted from header
        // Business logic — no idempotency code needed
        return new DebitResponse(debitId);
    }

    @SagaCompensation(name = "debit")
    @PostMapping("/debit/compensate")
    public void reverseDebit(@RequestBody ReverseDebitRequest req,
                              @SagaId String sagaId) {
        // Compensation logic
    }
}
```

The SDK auto-generates the status check endpoint and manages idempotency state internally. However, this requires participants to depend on the SDK and requires the SDK to store dedup state somewhere (participant's own database or an in-process cache).

**Comparison:**

| Level | Participant requirement | Extra infrastructure | Idempotency guarantee |
|---|---|---|---|
| **Basic** | Handle dedup on its own | None | Participant's responsibility |
| **SDK** | Add SDK dependency | SDK needs dedup storage | Automatic, reliable |

**Recommendation:** Ship Level 1 (Basic) in Phase 1 — this matches industry norm. Level 2 (Participant SDK) is optional and should only be built if customer demand justifies it.

### Layer 3: Framework Integration

The annotation layer is syntactic sugar — the scanner reads annotations and auto-generates `ServiceInvoker` registrations (or `Step` instances) at build/startup time.

#### Annotations

```java
// --- api/annotation/SagaStep.java ---
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SagaStep {
    String saga();                  // saga definition name
    String name();                  // step name within the saga
    int order();                    // execution order (1-based)
}

// --- api/annotation/SagaCompensation.java ---
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SagaCompensation {
    String saga();                  // must match a @SagaStep's saga
    String name();                  // must match a @SagaStep's name
}
```

#### Spring Boot Integration

```java
// --- spring/SagaAutoConfiguration.java ---
@Configuration
@ConditionalOnClass(SagaManager.class)
@EnableConfigurationProperties(SagaProperties.class)
public class SagaAutoConfiguration {

    @Bean
    public SagaManager sagaManager(DistributedTransactionManager txManager) {
        return new DefaultSagaManager(txManager);
    }

    @Bean
    public SagaAnnotationScanner sagaAnnotationScanner(
            ApplicationContext appContext, SagaManager sagaManager) {
        return new SagaAnnotationScanner(appContext, sagaManager);
    }
}

// --- spring/SagaAnnotationScanner.java ---
public class SagaAnnotationScanner implements SmartInitializingSingleton {

    @Override
    public void afterSingletonsInstantiated() {
        // 1. Scan all beans for methods annotated with @SagaStep
        Map<String, List<StepBinding>> sagaMap = new HashMap<>();

        for (String beanName : appContext.getBeanDefinitionNames()) {
            Object bean = appContext.getBean(beanName);
            for (Method method : bean.getClass().getDeclaredMethods()) {
                SagaStep stepAnn = method.getAnnotation(SagaStep.class);
                if (stepAnn != null) {
                    // Find matching @SagaCompensation method
                    Method compMethod = findCompensationMethod(
                        bean.getClass(), stepAnn.saga(), stepAnn.name());

                    sagaMap.computeIfAbsent(stepAnn.saga(), k -> new ArrayList<>())
                        .add(new StepBinding(stepAnn, method, compMethod, bean));
                }
            }
        }

        // 2. For each saga, sort by order and build SagaDefinition
        for (var entry : sagaMap.entrySet()) {
            List<StepBinding> bindings = entry.getValue();
            bindings.sort(Comparator.comparingInt(b -> b.annotation.order()));

            List<Step> steps = bindings.stream()
                .map(this::createStep)
                .collect(Collectors.toList());

            SagaDefinition def = new SagaDefinition(entry.getKey(), steps);
            sagaManager.register(def);
        }
    }

    private Step createStep(StepBinding binding) {
        return new Step() {
            @Override
            public String getName() { return binding.annotation.name(); }

            @Override
            public StepResult execute(SagaContext ctx) throws StepExecutionException {
                try {
                    return (StepResult) binding.actionMethod.invoke(binding.bean, ctx);
                } catch (Exception e) {
                    throw new StepExecutionException(e);
                }
            }

            @Override
            public void compensate(SagaContext ctx) throws StepCompensationException {
                try {
                    binding.compensationMethod.invoke(binding.bean, ctx);
                } catch (Exception e) {
                    throw new StepCompensationException(e);
                }
            }
        };
    }
}
```

**What the user writes:**

```java
@Component
public class MoneyTransferSaga {

    @Autowired                                          // ← Spring-managed
    private AccountServiceClient accountClient;

    @Autowired                                          // ← Spring-managed
    private ShippingServiceClient shippingClient;

    @SagaStep(saga = "MoneyTransfer", name = "debit", order = 1)
    public StepResult debit(SagaContext ctx) {
        var resp = accountClient.debit(
            ctx.get("accountId", String.class),
            ctx.get("amount", Integer.class));
        return StepResult.of("debitId", resp.getDebitId());
    }

    @SagaCompensation(saga = "MoneyTransfer", name = "debit")
    public void compensateDebit(SagaContext ctx) {
        accountClient.reverseDebit(ctx.get("debitId", String.class));
    }

    @SagaStep(saga = "MoneyTransfer", name = "ship", order = 2)
    public StepResult ship(SagaContext ctx) {
        var resp = shippingClient.ship(ctx.get("orderId", String.class));
        return StepResult.of("shipmentId", resp.getShipmentId());
    }

    @SagaCompensation(saga = "MoneyTransfer", name = "ship")
    public void compensateShip(SagaContext ctx) {
        shippingClient.cancel(ctx.get("shipmentId", String.class));
    }
}

// Starting a saga — one line:
sagaManager.start("MoneyTransfer", Map.of(
    "accountId", "A001", "amount", 5000, "orderId", "ORD-123"));
```

No Step classes. No JSON/YAML definition files. No ServiceInvoker registration. Just annotated methods on a Spring bean with framework-injected clients.

#### Quarkus Integration

```java
// --- quarkus/deployment/SagaBuildStep.java ---
public class SagaBuildStep {

    @BuildStep
    void scanSagaAnnotations(CombinedIndexBuildItem index,
                              BuildProducer<SagaDefinitionBuildItem> producer) {
        IndexView idx = index.getIndex();

        // Jandex scan for @SagaStep at build time
        for (AnnotationInstance ann : idx.getAnnotations(DotName.createSimple(
                "com.scalar.db.saga.api.annotation.SagaStep"))) {
            MethodInfo method = ann.target().asMethod();
            String saga = ann.value("saga").asString();
            String name = ann.value("name").asString();
            int order = ann.value("order").asInt();

            // Find matching @SagaCompensation
            MethodInfo compMethod = findCompensationMethod(
                method.declaringClass(), saga, name, idx);

            producer.produce(new SagaDefinitionBuildItem(
                saga, name, order,
                method.declaringClass().name().toString(),
                method.name(), compMethod.name()));
        }
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerSagas(SagaRecorder recorder,
                        List<SagaDefinitionBuildItem> definitions,
                        BeanContainerBuildItem beanContainer) {
        recorder.registerSagaDefinitions(beanContainer.getValue(), definitions);
    }
}
```

**What the Quarkus user writes:**

```java
@ApplicationScoped
public class MoneyTransferSaga {

    @Inject @GrpcClient("account-service")               // ← Quarkus-managed
    AccountServiceBlockingStub accountStub;

    @Inject @RestClient                                    // ← Quarkus-managed
    ShippingService shippingClient;

    @SagaStep(saga = "MoneyTransfer", name = "debit", order = 1)
    public StepResult debit(SagaContext ctx) {
        var resp = accountStub.debit(DebitRequest.newBuilder()
            .setAccountId(ctx.get("accountId", String.class))
            .setAmount(ctx.get("amount", Integer.class))
            .build());
        return StepResult.of("debitId", resp.getDebitId());
    }

    @SagaCompensation(saga = "MoneyTransfer", name = "debit")
    public void compensateDebit(SagaContext ctx) {
        accountStub.reverseDebit(ReverseDebitRequest.newBuilder()
            .setDebitId(ctx.get("debitId", String.class))
            .build());
    }
}
```

### DX Comparison (Final)

| Engine | What the user writes for a 3-step saga | Lines of user code |
|---|---|---|
| **Seata Saga** | JSON definition (service names + methods) | ~30 (JSON only) |
| **MicroTx / Narayana** | `@LRA`/`@Compensate` on participant endpoints | ~40 (annotations on 3 services) |
| **Ours — Layer 1 (Step)** | 3 Step classes + JSON/YAML definition | ~90-120 |
| **Ours — Layer 2 (ServiceInvoker)** | Lambda registry + JSON/YAML definition | ~40-60 |
| **Ours — Layer 2b (Declarative)** | JSON/YAML definition only (zero Java code) | **~30 (definition only)** |
| **Ours — Layer 3 (Spring/Quarkus)** | 1 annotated saga class | **~30-40** |

### Relationship Between Layers and LRA Compatibility

The MicroProfile LRA compatibility section (earlier in this document) describes how to add full LRA protocol support by layering the LRA REST API on top of the Phase 3 coordinator daemon (Phase 6). This uses Layer 1 + Layer 2 internally, adding LRA-specific endpoints so the coordinator can interoperate with standard LRA participants.

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

The step itself runs in the orchestrator process. The business logic (debit, reserve inventory, etc.) runs in the participant service. This is also where `ServiceInvoker` (Layer 2) and annotations (Layer 3) provide higher-level alternatives to writing Step classes by hand — see [ServiceInvoker and Framework Integration](#serviceinvoker-and-framework-integration).

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

### Option B: Outbox Relay (Guaranteed Delivery)

The `saga_events` table doubles as an outbox — each event row has a `published` column. A separate background relay process polls unpublished rows and forwards them to an external broker. This is the same approach Eventuate Tram uses (CDC/Debezium can also tail the events table directly).

```
  SagaEngine                       Outbox Relay (background)      Consumer
    │                                   │                            │
    ├─► execute step                    │                            │
    ├─► appendEvent(saga_events) ─────┐│                            │
    │                                 ││                            │
    │                    ┌────────────┘│                            │
    │                    ▼             │                            │
    │              saga_events table   │                            │
    │              (published=false)   │                            │
    │                    │             │                            │
    │                    └──► poll ────┤                            │
    │                                 ├─► publish to Kafka ────────►│
    │                                 ├─► mark published=true      │
```

Because the event is written as the saga state update itself (not a separate outbox INSERT), there is **no dual-write problem by design** — the event IS the state update.

The relay is a small background component (~50 LoC):

```java
public class OutboxRelay implements Runnable {
    private final DistributedTransactionManager txManager;
    private final KafkaProducer<String, String> producer;

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            DistributedTransaction tx = null;
            try {
                tx = txManager.begin();

                // Scan for unpublished events
                // In production, partition by saga_id or use a secondary
                // index to avoid full table scan
                List<Result> events = tx.scan(Scan.newBuilder()
                    .namespace(SagaSchema.NAMESPACE).table("saga_events")
                    // ... filter published=false ...
                    .build());

                for (Result event : events) {
                    String topic = event.getText("event_type");
                    String payload = event.getText("payload");
                    String sagaId = event.getText("saga_id");
                    int sequence = event.getInt("sequence");

                    // Publish to broker
                    producer.send(new ProducerRecord<>(topic, payload)).get();

                    // Mark as published
                    tx.update(Update.newBuilder()
                        .namespace(SagaSchema.NAMESPACE).table("saga_events")
                        .partitionKey(Key.ofText("saga_id", sagaId))
                        .clusteringKey(Key.ofInt("sequence", sequence))
                        .booleanValue("published", true)
                        .build());
                }

                tx.commit();
            } catch (Exception e) {
                abortQuietly(tx);
                // Log and retry on next poll cycle
            }

            Thread.sleep(pollIntervalMs);
        }
    }
}
```

**When to use**: You need guaranteed event delivery to external systems, decoupled from saga execution.

### Option C: Both Combined

Steps do direct calls for orchestration (payments, inventory); the outbox emits events for downstream consumers who don't participate in the saga (analytics, notifications).

```
  SagaEngine
    │
    ├─► DebitStep.execute()          ← direct call (participates in saga)
    │     └─► outbox: "DEBITED"      ← event for external consumers
    │
    ├─► CreditStep.execute()         ← direct call (participates in saga)
    │     └─► outbox: "CREDITED"     ← event for external consumers
    │
    └─► done
         └─► outbox: "SAGA_COMPLETED"

  Meanwhile, OutboxRelay ──► Kafka ──► Analytics service (not part of the saga)
```

### Which to Choose

| Approach | When to Use |
|---|---|
| **No broker** (Phase 1 default) | All participants are in-process or callable via RPC |
| **Option A** (Step wraps producer) | One or two steps need to talk to a queue; rest are direct calls |
| **Option B** (Outbox relay) | Guaranteed event delivery to external systems, decoupled from saga execution |
| **Both combined** | Steps do direct calls for orchestration; outbox emits events for non-participating consumers |

The key design principle: **the engine never changes.** Broker support is purely at the `Step` implementation level or the outbox relay level.

---

# Part IV: Developer Experience

## Bootstrapping (DI-Free)

The core engine has zero dependency on any DI framework. All wiring is done via a plain Java builder:

```java
// --- engine/SagaManagerBuilder.java ---
public class SagaManagerBuilder {
    private SagaStore store;
    private String ownerId = UUID.randomUUID().toString();
    private long recoveryTimeoutMs = 60_000;       // stale saga threshold (1 min)
    private long recoveryIntervalSeconds = 30;
    private List<SagaEventListener> eventListeners = new ArrayList<>();

    public SagaManagerBuilder store(SagaStore store) {
        this.store = store;
        return this;
    }

    public SagaManagerBuilder ownerId(String ownerId) {
        this.ownerId = ownerId;
        return this;
    }

    public SagaManagerBuilder recoveryTimeoutMs(long recoveryTimeoutMs) {
        this.recoveryTimeoutMs = recoveryTimeoutMs;
        return this;
    }

    public SagaManagerBuilder recoveryIntervalSeconds(long seconds) {
        this.recoveryIntervalSeconds = seconds;
        return this;
    }

    public SagaManagerBuilder addEventListener(SagaEventListener listener) {
        this.eventListeners.add(listener);
        return this;
    }

    public SagaManager build() {
        Objects.requireNonNull(store, "SagaStore is required");
        CompensationManager compensationManager = new CompensationManager(store);
        SagaEngine engine = new SagaEngine(store, compensationManager,
                                            ownerId, eventListeners);
        SagaRecoveryManager recovery = new SagaRecoveryManager(
            store, engine, ownerId, recoveryTimeoutMs, recoveryIntervalSeconds);
        return new DefaultSagaManager(engine, store, recovery);
    }
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
SagaSchema.createAll(admin);

// Wire with plain Java builder (no Guice/Spring/CDI needed)
SagaStore store = new ScalarDbSagaStore(txManager);
SagaManager sagaManager = new SagaManagerBuilder()
    .store(store)
    .recoveryTimeoutMs(60_000)     // stale saga threshold (1 min)
    .recoveryIntervalSeconds(30)
    .build();

// Register saga definition
sagaManager.registerFromClasspath("sagas/*.json");

// Start periodic crash recovery (runs immediately, then every 30 seconds)
sagaManager.startRecovery();
```

The above runs once at application startup. After that, `sagaManager.start()` is called per request:

```java
// Per request — e.g., inside an HTTP handler or message consumer
SagaInstance result = sagaManager.start("MoneyTransfer", Map.of(
    "fromAccountId", "A001",
    "toAccountId",   "B002",
    "amount",        5000
));

System.out.println(result.getStatus());  // COMPLETED or COMPENSATED
```

## Testing Harness (SagaTestHarness)

### What It Does

Provides a lightweight, in-memory testing framework for saga definitions — no database required. This is a genuine differentiator against Seata/MicroTx/Narayana, none of which provide testing utilities. Temporal's replay-based testing framework is widely cited as their biggest DX win.

### InMemorySagaStore

```java
// --- testing/InMemorySagaStore.java ---
// In-memory implementation of SagaStore for testing. No database, no ScalarDB.
public class InMemorySagaStore implements SagaStore {
    private final Map<String, SagaInstance> index = new ConcurrentHashMap<>();
    private final Map<String, List<SagaEvent>> events = new ConcurrentHashMap<>();

    // All SagaStore methods implemented with simple in-memory maps.
    // Supports full saga lifecycle: create, append events, recovery scan, event replay.
}
```

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
    private final InMemorySagaStore store;
    private final Map<String, MockStep> mockSteps;

    public static Builder forDefinition(SagaDefinition definition) {
        return new Builder(definition);
    }

    public static Builder forJson(String jsonDefinition) {
        return new Builder(SagaDefinitionParser.parse(jsonDefinition));
    }

    // Execute the saga with given input
    public SagaInstance execute(Map<String, Object> input) { ... }

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
    public void appendEvent(String sagaId, SagaEvent event) {
        delegate.appendEvent(sagaId, event);
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

SagaInstance result = harness.execute(Map.of("amount", 100));
assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
assertThat(harness.executionOrder()).containsExactly("debit", "credit");

// 2. Failure triggers compensation in reverse order
SagaTestHarness harness = SagaTestHarness.forDefinition(moneyTransferDef)
    .mockStep("debit", ctx -> StepResult.of("debitId", "D001"))
    .failAt("credit", new InsufficientFundsException())
    .build();

SagaInstance result = harness.execute(Map.of("amount", 100));
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
SagaInstance recovered = harness.recover();
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

    public static Builder builder() { return new Builder(); }
    public static SagaDevServerConfig defaults() { return builder().build(); }
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
            SagaDevServerConfig.builder()
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
        SagaInstance instance = sagaManager.start("MoneyTransfer",
            Map.of("fromAccountId", "A1", "toAccountId", "A2", "amount", 100));
        assertEquals(SagaStatus.COMPLETED, instance.getStatus());
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
    SagaPage<SagaInstance> listSagas(SagaQuery query);

    // Full execution history: saga instance + all step logs + derived timeline
    SagaDetail getSagaDetail(String sagaId);

    // Manually trigger compensation for a FAILED saga.
    SagaInstance triggerCompensation(String sagaId);

    // Admin override: mark an ESCALATED saga as COMPLETED.
    SagaInstance forceComplete(String sagaId);

    // Re-execute a FAILED saga from the last successfully completed step.
    SagaInstance retrySaga(String sagaId);

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

    public static Builder builder() { return new Builder(); }

    public static class Builder {
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
    private final SagaInstance instance;
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
public class SagaMetrics {
    private final Map<SagaStatus, Long> countByStatus;
    private final Map<String, Long> countBySagaName;
    private final Duration avgDuration;
    private final Duration p50Duration;
    private final Duration p99Duration;
    private final double failureRate;
    private final double compensationFailureRate;
}
```

### DefaultSagaAdminService

```java
// --- admin/DefaultSagaAdminService.java ---
public class DefaultSagaAdminService implements SagaAdminService {
    private final SagaStore store;
    private final SagaManager sagaManager;

    @Override
    public SagaPage<SagaInstance> listSagas(SagaQuery query) {
        return store.listInstances(query);
    }

    @Override
    public SagaDetail getSagaDetail(String sagaId) {
        SagaInstance instance = store.getInstance(sagaId);
        if (instance == null) throw new SagaNotFoundException(sagaId);
        List<SagaEvent> events = store.getEvents(sagaId);
        List<TimelineEvent> timeline = TimelineBuilder.from(events);
        return new SagaDetail(instance, events, timeline);
    }

    @Override
    public SagaInstance triggerCompensation(String sagaId) {
        requireStatus(sagaId, SagaStatus.FAILED);
        return sagaManager.compensate(sagaId);
    }

    @Override
    public SagaInstance forceComplete(String sagaId) {
        requireStatus(sagaId, SagaStatus.ESCALATED);
        // Replay events to reconstruct sequence; build metadata from instance
        SagaInstance saga = store.getInstance(sagaId);
        List<SagaEvent> events = store.getEvents(sagaId);
        SagaIndexMetadata metadata = new SagaIndexMetadata(
            saga.getSagaName(), saga.getOwnerId(), saga.getVersion(),
            saga.getDefinitionVersion(), saga.getDefinitionJson(),
            saga.getCreatedAt());
        store.recordTransition(sagaId, events.size(),
            SagaStatus.ESCALATED, metadata, SagaEvent.sagaCompleted());
        return store.getInstance(sagaId);
    }

    @Override
    public SagaInstance retrySaga(String sagaId) {
        requireStatus(sagaId, SagaStatus.FAILED);
        return sagaManager.resume(sagaId);
    }

    @Override
    public SagaMetrics getMetrics() {
        return store.computeMetrics();
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

    // Metrics instruments
    private final LongCounter sagaStartedCounter;
    private final LongCounter sagaCompletedCounter;
    private final LongCounter sagaFailedCounter;
    private final LongCounter sagaEscalatedCounter;
    private final DoubleHistogram sagaDurationHistogram;
    private final DoubleHistogram stepDurationHistogram;
    private final LongUpDownCounter activeSagaGauge;
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
| `saga.failed` | Counter | Total sagas that failed |
| `saga.escalated` | Counter | Total sagas escalated (compensation failure) |
| `saga.duration` | Histogram | End-to-end saga duration (ms) |
| `saga.step.duration` | Histogram | Per-step execution duration (ms) |
| `saga.active` | UpDownCounter | Currently active (in-flight) sagas |
| `saga.recovery.claimed` | Counter | Recovery claims by replica |

### Integration

Register the listener via the builder:

```java
SagaManager sagaManager = new SagaManagerBuilder()
    .store(store)
    .addEventListener(new OpenTelemetrySagaListener(tracer, meter))
    .build();
```

The `SagaEngine` calls listener methods at each lifecycle point. Multiple listeners can be registered (e.g., OpenTelemetry + custom audit logger).

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

## Phase 1: Core Engine

**Scope:** Core engine + SagaStore + recovery (with index table) + timeouts + virtual threads + graceful shutdown + saga versioning + compensation retry + SagaContext validation + TCC mode.

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **api/** | | | |
| `Step.java` | Interface (2 methods) | ~15 | Trivial |
| `TccStep.java` | TCC extension of Step (1 method) | ~10 | Trivial |
| `StepResult.java` | Simple data class | ~30 | Trivial |
| `SagaContext.java` | Map wrapper with typed getters + type validation + in-memory tracking fields (`nextEventSequence`, `currentStatus`, `indexMetadata`) | ~80 | Trivial |
| `SagaStatus.java` | Enum (7 values: RUNNING, CONFIRMING, COMPLETED, FAILED, COMPENSATING, COMPENSATED, ESCALATED) | ~10 | Trivial |
| `StepStatus.java` | Enum (4 values: WAITING (daemon mode only), COMPLETED, CONFIRMED, FAILED) | ~5 | Trivial |
| `SagaDefinition.java` | POJO + inner `StepDefinition` + SagaMode + RecoverStrategy | ~60 | Trivial |
| `SagaInstance.java` | Read-only view of saga state | ~40 | Trivial |
| `SagaManager.java` | Interface (8 methods incl. start, startAsync ×2; completeStep is daemon mode only) | ~25 | Trivial |
| `SagaCallback.java` | Callback interface (onCompleted, onCompensated, onEscalated) | ~10 | Trivial |
| `SagaIndexMetadata.java` | Record caching immutable saga_index columns (sagaName, ownerId, version, etc.) for zero-read `recordTransition` | ~15 | Trivial |
| **engine/** | | | |
| `RetryPolicy.java` | Config POJO + default/compensation/confirm factories | ~50 | Trivial |
| `CompensationManager.java` | Reverse-loop + retry + stop on failure (throws `StepCompensationException`) | ~70 | Low |
| `SagaEngine.java` | createSaga + executeSaga + execute (convenience) + resumeFrom + core step loop + Saga/TCC branching + retry + timeout + async step handling + error routing | ~270 | **Medium** |
| `DefaultSagaManager.java` | Delegates to engine + recovery + startAsync (virtual thread submission + callback dispatch). completeStep is daemon mode only. | ~140 | Low |
| `SagaManagerBuilder.java` | DI-free builder for wiring | ~60 | Low |
| **parser/** | | | |
| `SagaDefinitionParser.java` | Jackson JSON/YAML → SagaDefinition (detects format by file extension; uses `jackson-dataformat-yaml`) | ~80 | Low |
| **store/** | | | |
| `SagaStore.java` | Interface (appendEvent, recordTransition, findRecoverable, claimForRecovery, getEvents) | ~30 | Trivial |
| `SagaEvent.java` | Event types + factory methods (each saga-level event carries its `targetStatus`) | ~90 | Low |
| `SagaSchema.java` | 2 TableMetadata definitions (saga_events, saga_index with bucket partitioning) + bucketOf() + createAll | ~80 | Low |
| `ScalarDbSagaStore.java` | Append-only events + bucket-partitioned saga_index (DELETE+INSERT for status transitions) + bucket-parallel recovery scan + conflict-based claiming | ~400 | **Medium** |
| **recovery/** | | | |
| `SagaRecoveryManager.java` | Periodic scan of saga_index + event replay + resume (with versioned definitions, TCC CONFIRMING handling) | ~200 | **Medium** |
| **timeout/** | | | |
| `TimeoutPolicy.java` | Per-step and per-saga deadline calculation | ~30 | Trivial |
| **exception/** | | | |
| 5 exception classes | StepExecutionException (with `retryable` flag), StepCompensationException, StepTimeoutException, SagaTimeoutException, SagaPersistenceException | ~50 | Trivial |
| **Tests** | | | |
| Unit tests (all classes: engine, compensation, retry, store, recovery, parser, builder, context, TCC) | | ~1,800 | Medium |
| Integration tests (end-to-end: Saga mode + TCC mode, crash recovery, timeout) | | ~400 | Medium |
| **Total** | | **~3,945** | |

### Timeline

| Work | Time |
|---|---|
| Scaffolding: module setup, Gradle config, dependencies | 0.25 day |
| API layer: all interfaces, enums, POJOs | 0.25 day |
| SagaSchema + ScalarDbSagaStore | 1-1.5 days |
| SagaEngine + CompensationManager + RetryPolicy (Saga + TCC modes) | 1-1.5 days |
| SagaRecoveryManager + graceful shutdown | 0.5 day |
| Parser + Builder wiring + timeout enforcement | 0.25 day |
| Tests: unit tests for all classes + integration tests | 1.5-2.5 days |
| **Total** | **~5-7 working days** |

### Where the Time Actually Goes

The ~1,750 LoC of production code is not the hard part — AI generates the API layer, parser, builder, and enums quickly. The hard parts are:

1. **SagaStore correctness** (~30% of effort) — Getting the append-only event writes right, ensuring sequence numbering is correct, handling `CommitConflictException` / `AbortException` edge cases in ScalarDB, and ensuring event replay + `saga_index` recovery scan work correctly.
2. **Comprehensive testing** (~40% of effort) — Unit tests for every class (~1,800 LoC) plus integration tests (~400 LoC). AI generates test scaffolding fast, but the edge cases that matter (crash recovery simulation, concurrent conflict handling, timeout mid-step, TCC confirm-after-partial-failure) require careful human reasoning.
3. **Retry + error classification** (~10%) — Straightforward logic, but the interaction between retry exhaustion → compensation → compensation failure → escalation has several paths to test.

**Bottom line: ~1 week for a skilled engineer with AI. Production code is ~1,750 LoC but tests add ~2,200 LoC. The devil is in the transactional edge cases and their test coverage.**

## Phase 2: Communication & Framework Integration

**Scope:** ServiceInvoker layer (Layer 2), declarative step communication (Layer 2b), Spring Boot integration, and Quarkus integration.

### File-by-File Breakdown

| File | What to Build | Est. LoC | Complexity |
|---|---|---|---|
| **Phase 2a: ServiceInvoker + Declarative Communication** | | | |
| **invoker/** | | | |
| `ServiceInvoker.java` | Interface (execute + compensate) | ~15 | Trivial |
| `ServiceInvokerRegistry.java` | Concurrent map lookup + dispatch | ~40 | Low |
| `GrpcInvoker.java` | Typed lambda wrapper for gRPC stubs (builder pattern) | ~80 | Low |
| `HttpInvoker.java` | HTTP client wrapper with status code classification | ~80 | Low |
| `DeclarativeStepAdapter.java` | JSON expression resolution (${...}) + output extraction ($.path) | ~120 | **Medium** |
| `TransportAdapter.java` | Interface + TransportException | ~30 | Trivial |
| `GrpcTransportAdapter.java` | Protobuf message building from maps + gRPC metadata propagation | ~100 | Medium |
| `HttpTransportAdapter.java` | JSON body building + X-Saga-Id propagation + status code mapping | ~80 | Low |
| Updated `SagaEngine` | Support `service`/`method` in addition to `stepClass` | ~30 | Low |
| Updated `SagaDefinitionParser` | Parse `call`, `compensate` blocks | ~50 | Low |
| Tests (all classes: invoker unit + declarative integration + transport edge cases) | | ~800 | Medium |
| **Phase 2a Total** | | **~1,425** | |
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
| Participant HTTP Protocol spec (documented contract) | Request/response format, error signaling (`X-Saga-Retryable`), correlation headers (`X-Saga-Id`) | ~50 | Trivial |
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
| Phase 2a: ServiceInvoker + declarative communication + tests | 2-3 days |
| Phase 2b: Spring Boot auto-config + annotation scanner + tests | 2-3 days |
| Phase 2c: Quarkus extension (build step + recorder) + tests | 2-3 days |
| Phase 2d: Participant protocol spec + Java participant SDK + tests | 1-1.5 days |
| **Total** | **~7-10 working days** |

### Where the Time Actually Goes

1. **Declarative step communication** (~25% of Phase 2a effort) — The expression resolution (`${...}` context lookup, `$.path` response extraction) and protobuf map-to-message conversion in `GrpcTransportAdapter` are the tricky parts.
2. **Annotation scanning** (~30% of Phase 2b/2c effort) — Correctly matching `@SagaStep` to `@SagaCompensation` methods, handling inheritance, and generating `Step` instances from annotated methods requires careful reflection/Jandex work.
3. **Comprehensive testing** (~35% of Phase 2 effort) — Unit tests for all classes in each sub-phase. Framework-specific test setup (`@SpringBootTest`, `@QuarkusTest`) has its own overhead. Transport edge cases (malformed protobuf, HTTP error codes, connection failures) need thorough coverage.

## Phase 3: Daemon Mode

**Scope:** Package the saga engine as a standalone coordinator process with a REST API for external clients to start, monitor, and manage sagas, plus a Java client SDK (`RemoteSagaManager`) that implements the same `SagaManager` interface. The daemon hosts the same `SagaEngine`, `SagaStore`, and `SagaRecoveryManager` from Phase 1 — it simply adds a process boundary and a client-facing API. How steps are invoked (direct call, HTTP, gRPC) is configured via `ServiceInvoker` (Phase 2) and is independent of the deployment mode.

### What Daemon Mode Adds

In embedded mode, application code calls `sagaManager.start()` or `startAsync()` directly. Daemon mode wraps this in a standalone process so that external clients interact with the engine over HTTP:

```
Embedded mode:                           Daemon mode:

  Application code                         External client (any language)
       │                                        │
       ▼                                        ▼  HTTP
  sagaManager.start(def, ctx)     (sync)   POST /sagas { saga, input }      → 200 (blocks)
  sagaManager.startAsync(def, ctx)(async)  POST /sagas?async=true { ... }   → 202 (immediate)
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
- A step configured with `service`/`method` → `ServiceInvoker` call (HTTP/gRPC to remote service)

Either configuration works in either deployment mode.

### Java Client SDK: RemoteSagaManager

`RemoteSagaManager` implements the same `SagaManager` interface used in embedded mode, delegating to the coordinator REST API. Java users can switch between embedded and daemon mode with zero code changes:

```java
// Embedded mode
SagaManager manager = SagaManagerBuilder.newBuilder().store(store).build();

// Daemon mode — same interface
SagaManager manager = RemoteSagaManager.builder()
    .coordinatorUrl("http://coordinator:8080").build();

// Both use the same API — sync and async work identically
SagaInstance result = manager.start("transferMoney", input);          // sync
String sagaId = manager.startAsync("transferMoney", input);          // async
SagaInstance status = manager.getInstance(sagaId);                    // poll
```

`RemoteSagaManager` maps `startAsync()` to `POST /sagas?async=true`, which returns `202 Accepted` with the saga ID. The `SagaCallback` variant polls `GET /sagas/{id}` on a background thread and invokes the callback when the saga reaches a terminal status.

### Coordinator REST API

| Endpoint | Method | Description |
|---|---|---|
| `/sagas` | POST | Start a new saga. Default: synchronous (blocks until complete, returns `200`). With `?async=true`: returns `202 Accepted` immediately with saga ID. |
| `/sagas` | GET | List sagas (with status filter, pagination) |
| `/sagas/{id}` | GET | Get saga status and step details |
| `/sagas/{id}/cancel` | PUT | Request saga cancellation (triggers compensation) |
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

1. **Comprehensive testing** (~45% of effort) — Unit tests for all classes (REST resources, error mapper, config, RemoteSagaManager) plus integration tests verifying the full lifecycle via REST: start saga → engine executes steps → check status → recovery after coordinator restart. Must test both in-process steps and remote steps (via ServiceInvoker).
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
| **Phase 4b: Testing Harness (`scalardb-saga-testing`)** | | | |
| **testing/** | | | |
| `InMemorySagaStore.java` | Full `SagaStore` implementation with `ConcurrentHashMap` | ~200 | Low |
| `MockStep.java` | Configurable mock with execution/compensation history tracking | ~80 | Low |
| `SagaTestHarness.java` | Builder + execute + assertions (executionOrder, compensationOrder, finalContext) | ~150 | Medium |
| `CrashingStoreDecorator.java` | Decorator that throws `SimulatedCrashException` at configured step boundaries | ~60 | Low |
| Tests (all classes: InMemorySagaStore semantics, MockStep, harness assertions, crash simulation) | | ~500 | Medium |
| **Phase 4b Total** | | **~990** | |
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
| **Phase 4 Total** | | **~2,455** | |

### Timeline

| Work | Time |
|---|---|
| Phase 4a: OpenTelemetry listener + metrics + integration with engine + tests | 0.5-1 day |
| Phase 4b: InMemorySagaStore + MockStep + SagaTestHarness + CrashingStoreDecorator + tests | 1.5-2 days |
| Phase 4c: SagaDevServer (Javalin + SQLite + web UI + CLI) + tests | 1-1.5 days |
| **Total** | **~3-4.5 working days** |

### Where the Time Actually Goes

1. **InMemorySagaStore + its tests** (~35% of Phase 4b effort) — Must faithfully replicate all `SagaStore` semantics (append-only events, saga_index, recovery scan, conflict-based claiming) without ScalarDB. The store itself and its comprehensive tests (verifying semantic parity with `ScalarDbSagaStore`) are the tricky parts.
2. **Web UI** (~30% of Phase 4c effort) — Even a minimal dashboard (saga list with status badges, step timeline, action buttons) requires frontend work. Consider using a pre-built admin template to save time. AI generates HTML/JS/CSS quickly.
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
  RecoverStrategy recover_strategy = 4;
  uint64 timeout_ms = 5;
  RetryPolicy default_retry_policy = 6;
  repeated StepDefinition steps = 7;
}

message StepDefinition {
  string name = 1;
  uint64 timeout_ms = 2;
  RetryPolicy retry_policy = 3;
  CallDefinition call = 4;             // forward action
  CallDefinition compensate = 5;       // compensation
}

message CallDefinition {
  string service = 1;                  // logical service name (resolved via config)
  string method = 2;                   // method/operation name
  string transport = 3;                // "grpc" or "http"
  google.protobuf.Struct request = 4;  // parameter mapping (${...} expressions)
  map<string, string> output = 5;      // response extraction ($.path expressions)
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

enum RecoverStrategy {
  RECOVER_STRATEGY_UNSPECIFIED = 0;
  COMPENSATE = 1;
  FORWARD = 2;
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
  COMPLETED = 2;
  COMPENSATING = 3;
  COMPENSATED = 4;
  ESCALATED = 5;
}

service SagaService {
  rpc RegisterSaga(SagaDefinition) returns (google.protobuf.Empty);
  rpc StartSaga(StartSagaRequest) returns (StartSagaResponse);
  rpc GetSaga(GetSagaRequest) returns (SagaInstanceResponse);
  rpc CompensateSaga(GetSagaRequest) returns (SagaInstanceResponse);
}
```

In daemon mode, `StepDefinition` uses declarative `call`/`compensate` blocks — the same format as JSON/YAML declarative communication (see [Declarative Communication](#solution-declarative-communication-in-the-saga-definition)). No `stepClass` field because daemon-mode clients don't run Java Step classes.

#### Client Usage Examples

**Java:**

```java
SagaServiceGrpc.SagaServiceBlockingStub stub =
    SagaServiceGrpc.newBlockingStub(channel);

stub.registerSaga(SagaDefinition.newBuilder()
    .setName("MoneyTransfer")
    .setVersion("1.0")
    .setMode(SagaMode.SAGA)
    .setRecoverStrategy(RecoverStrategy.COMPENSATE)
    .setTimeoutMs(300000)
    .addSteps(StepDefinition.newBuilder()
        .setName("debit")
        .setCall(CallDefinition.newBuilder()
            .setService("account-service")
            .setMethod("debit")
            .setTransport("grpc")
            .setRequest(Struct.newBuilder()
                .putFields("account_id", Value.newBuilder()
                    .setStringValue("${accountId}").build())
                .putFields("amount", Value.newBuilder()
                    .setStringValue("${amount}").build())
                .build())
            .putOutput("debitId", "$.debit_id")
            .build())
        .setCompensate(CallDefinition.newBuilder()
            .setService("account-service")
            .setMethod("reverseDebit")
            .setTransport("grpc")
            .setRequest(Struct.newBuilder()
                .putFields("debit_id", Value.newBuilder()
                    .setStringValue("${debitId}").build())
                .build())
            .build())
        .setTimeoutMs(60000)
        .build())
    .addSteps(StepDefinition.newBuilder()
        .setName("credit")
        .setCall(CallDefinition.newBuilder()
            .setService("account-service")
            .setMethod("credit")
            .setTransport("grpc")
            .setRequest(Struct.newBuilder()
                .putFields("account_id", Value.newBuilder()
                    .setStringValue("${toAccountId}").build())
                .putFields("amount", Value.newBuilder()
                    .setStringValue("${amount}").build())
                .build())
            .putOutput("creditId", "$.credit_id")
            .build())
        .setCompensate(CallDefinition.newBuilder()
            .setService("account-service")
            .setMethod("reverseCredit")
            .setTransport("grpc")
            .setRequest(Struct.newBuilder()
                .putFields("credit_id", Value.newBuilder()
                    .setStringValue("${creditId}").build())
                .build())
            .build())
        .setTimeoutMs(30000)
        .build())
    .build());

StartSagaResponse response = stub.startSaga(
    StartSagaRequest.newBuilder()
        .setSagaName("MoneyTransfer")
        .setInput(Struct.newBuilder()
            .putFields("accountId", Value.newBuilder()
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

debit_req = Struct()
debit_req.update({"account_id": "${accountId}", "amount": "${amount}"})
debit_comp_req = Struct()
debit_comp_req.update({"debit_id": "${debitId}"})

credit_req = Struct()
credit_req.update({"account_id": "${toAccountId}", "amount": "${amount}"})
credit_comp_req = Struct()
credit_comp_req.update({"credit_id": "${creditId}"})

stub.RegisterSaga(saga_pb2.SagaDefinition(
    name="MoneyTransfer",
    version="1.0",
    mode=saga_pb2.SAGA,
    recover_strategy=saga_pb2.COMPENSATE,
    timeout_ms=300000,
    steps=[
        saga_pb2.StepDefinition(
            name="debit",
            call=saga_pb2.CallDefinition(
                service="account-service", method="debit", transport="grpc",
                request=debit_req, output={"debitId": "$.debit_id"}),
            compensate=saga_pb2.CallDefinition(
                service="account-service", method="reverseDebit",
                transport="grpc", request=debit_comp_req),
            timeout_ms=60000),
        saga_pb2.StepDefinition(
            name="credit",
            call=saga_pb2.CallDefinition(
                service="account-service", method="credit", transport="grpc",
                request=credit_req, output={"creditId": "$.credit_id"}),
            compensate=saga_pb2.CallDefinition(
                service="account-service", method="reverseCredit",
                transport="grpc", request=credit_comp_req),
            timeout_ms=30000),
    ]))

input_data = Struct()
input_data.update({"accountId": "A001", "toAccountId": "B002", "amount": 100})
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
2. **Parallel steps**: Execute independent steps concurrently with configurable parallelism
3. **Outbox relay**: Background thread/process that reads unpublished events from `saga_events` and publishes to Kafka/RabbitMQ (or use CDC/Debezium to tail the table directly)
4. **Participant SDK**: Optional participant-side SDK with `@SagaParticipant`, `@SagaAction`, `@SagaCompensation` annotations, built-in idempotency, and anomaly protection (empty rollback, suspension). The anomaly protection uses an INSERT-based barrier mechanism inspired by Seata's TCC Fence and DTM's Sub-Transaction Barrier. Since we don't control participant databases, this is an opt-in library. See "Participant Idempotency Levels" in ServiceInvoker and Framework Integration, and [scalardb-saga-barrier-sdk-research.md](scalardb-saga-barrier-sdk-research.md) for detailed research.
5. **SubStateMachine (nesting)**: Compose sagas from sub-sagas for complex workflows
6. **Loop execution**: Repeat steps based on collection inputs
7. **ScriptTask**: Lightweight expression evaluation without full Step implementation
8. **Heartbeat for async steps**: Participants periodically ping the coordinator ("I'm still working") during long-running async operations. If heartbeats stop, the coordinator detects participant failure faster than waiting for the full step timeout. Especially valuable when step timeouts are long (e.g., 5 minutes) but participant death should be detected within seconds. Similar to Temporal's `heartbeatTimeout`.
