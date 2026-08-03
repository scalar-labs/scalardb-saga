# ScalarDB Saga

[![CI](https://github.com/scalar-labs/scalardb-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/scalar-labs/scalardb-saga/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

ScalarDB Saga is a modern saga orchestration engine for microservices. It coordinates eventually
consistent distributed transactions across services using the Saga pattern (steps with compensations)
and TCC (try/confirm/cancel), keeping saga state durable in a database of your choice — with no
message broker to operate.

A saga is a sequence of steps, each with a compensation that undoes it. ScalarDB Saga drives the
steps in order; if one fails, it runs the compensations for everything already attempted, in
reverse. Every transition is persisted before it happens, so a coordinator that crashes mid-saga
does not leave the operation half-applied — another replica picks it up and finishes it.

### ScalarDB Saga and ScalarDB

[ScalarDB](https://github.com/scalar-labs/scalardb) provides strongly consistent ACID transactions
across databases. ScalarDB Saga coordinates operations across **services**, where a single ACID
transaction is not possible — it trades strong consistency for compensation-based rollback and
eventual convergence.

Use ScalarDB transactions where correctness requires immediate consistency, and ScalarDB Saga where
eventual consistency with compensation is sufficient. ScalarDB Saga uses ScalarDB as its state store,
which is what lets it run on the database you already have.

## Features

- **Saga and TCC.** Steps with compensations, or two-phase reservations where every step is reserved
  before any is confirmed.
- **Server or embedded.** Run the engine as a server exposing REST and gRPC, or as a library inside
  your application. Only embedded mode supports **code steps** — steps implemented as Java classes
  rather than service calls — because an operator cannot add classes to a server's image.
- **Runs on your database.** Saga state is stored through ScalarDB, so it lives in any database
  ScalarDB supports — PostgreSQL, MySQL, Oracle, SQL Server, Cassandra, DynamoDB, Cosmos DB — on any
  cloud or on-premises. No message broker, no dedicated coordinator datastore.
- **Durable by construction.** Saga state and its event history are written as an append-only log
  before each action, so recovery has an exact record of what was attempted.
- **Crash recovery.** Any replica can pick up a saga stranded by a failed one; there is no leader to
  elect and no standby to run.
- **Retries that know what is retryable.** Per-step retry policies with exponential backoff, applied
  to transient failures (5xx, transport errors) and not to permanent ones (4xx).
- **Synchronous or asynchronous.** Block until the saga reaches a terminal state, or start it and
  poll. Steps whose work outlives the request can park the saga and resume it from a callback.
- **Declarative definitions.** Define sagas in JSON or YAML, versioned and registered without
  recompiling — or implement steps in Java when they need in-process logic.
- **Operable.** An admin API to list, recover, force-complete, and reset sagas that need manual
  intervention, gated by API-key or JWT authentication with role-based authorization.

## Getting started

**[Getting started](getting-started/README.md)** runs the saga server, a database, and three
participant services with Docker Compose, and drives a saga with `curl` — nothing to build. The
walkthrough covers a saga that completes, one that fails and is compensated, asynchronous starts,
and querying a saga's history.

For running the server for real — configuration, authentication, health checks — see
[server/docker/README.md](server/docker/README.md).

## Saga definitions

A definition names the steps, the call each one makes, and the call that undoes it. Values flow
between steps through the saga context: `${...}` reads from it, and `output` captures fields from a
response back into it.

```json
{
  "name": "order-saga",
  "mode": "SAGA",
  "steps": [
    {
      "name": "charge",
      "service": "payment",
      "execution": {
        "method": "POST", "path": "/charge",
        "jsonBody": { "orderId": "${orderId}", "amount": "${amount}" },
        "output": { "paymentId": "$.payment_id" }
      },
      "compensation": {
        "method": "POST", "path": "/refund",
        "jsonBody": { "orderId": "${orderId}" }
      }
    },
    {
      "name": "ship",
      "service": "shipping",
      "execution": {
        "method": "POST", "path": "/ship",
        "jsonBody": { "orderId": "${orderId}", "paymentId": "${paymentId}" }
      },
      "compensation": {
        "method": "POST", "path": "/cancel",
        "jsonBody": { "orderId": "${orderId}" }
      }
    }
  ]
}
```

If `ship` fails, `charge` is refunded and the saga ends `COMPENSATED`.

Setting `"mode": "TCC"` switches to two-phase steps — `reservation`, `confirmation`, `cancellation`.
Every step reserves before any confirms; a failed reservation cancels the earlier ones, while a
failure after that point can only roll forward, so confirmations are retried until they succeed.
Definitions are equally valid in YAML.

## Modules

| Module | Artifact | Java | Distribution |
| --- | --- | --- | --- |
| `server` | — | 21 | `ghcr.io/scalar-labs/scalardb-saga-server` |
| `client` | `com.scalar-labs:scalardb-saga-java-client-sdk` | 8 | Maven Central |
| `core` | `com.scalar-labs:scalardb-saga-core` | 21 | Maven Central |
| `api` | `com.scalar-labs:scalardb-saga-api` | 8 | Maven Central |
| `rpc` | `com.scalar-labs:scalardb-saga-rpc` | 8 | Maven Central |
| `bom` | `com.scalar-labs:scalardb-saga-bom` | — | Maven Central |

The server is not published to Maven Central: it ships as a container image, which is how it is
meant to be deployed. `api` and `rpc` are published because `core` and `client` expose them, not
because you normally declare them yourself. The client SDK and the types it exposes are compiled
for Java 8, so applications that cannot move off it can still use server mode.

## Install

Import the BOM to pin every artifact to one version, then declare the artifact for the mode you run
in: `scalardb-saga-java-client-sdk` to call a server, or `scalardb-saga-core` to embed the engine.
The two are alternatives, not a pair — the SDK never depends on `core`, so declaring both puts the
whole engine into an application that only wanted a client.

Gradle:

```kotlin
dependencies {
    implementation(platform("com.scalar-labs:scalardb-saga-bom:VERSION"))

    implementation("com.scalar-labs:scalardb-saga-java-client-sdk")
}
```

Maven:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.scalar-labs</groupId>
      <artifactId>scalardb-saga-bom</artifactId>
      <version>VERSION</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.scalar-labs</groupId>
    <artifactId>scalardb-saga-java-client-sdk</artifactId>
  </dependency>
</dependencies>
```

Snapshots built from `main` are published to the Central snapshot repository.

## Contributing

Bug reports and feature suggestions are welcome as GitHub issues.

Building requires JDK 21. Before opening a pull request:

```bash
./gradlew spotlessApply   # format
./gradlew check           # tests, formatting, and static analysis
```

Code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html),
enforced by Spotless, and is checked by Error Prone, NullAway, and SpotBugs. See
[RELEASING.md](RELEASING.md) for how releases are cut.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
