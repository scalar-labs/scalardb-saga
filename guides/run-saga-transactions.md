# Run Saga Transactions

This page explains how an application runs sagas against a ScalarDB Saga server: starting them,
learning their outcome, and handling failures. It uses the Java client SDK, which is the recommended
interface for production applications, and the `order-saga` definition from
[Model Your Workflow](model-your-workflow.md).

Before you start, you need a running ScalarDB Saga server with at least one definition registered.
To run one locally, follow the [getting started guide](../getting-started/README.md). The SDK
connects to the server's [gRPC port](configurations.md#grpc), `12051` by default, and uses TLS when
the server is [configured for it](configurations.md#tls).

## Server or embedded?

ScalarDB Saga runs in one of two ways. The engine runs in a server that your application calls, or
it runs inside your application as a library, which is called embedded mode. Either way, saga state
is stored in your database through ScalarDB.

| Choose | When |
| --- | --- |
| The server | Your application does not run on Java 25, or is not written in Java. |
| The server | Several applications or languages start sagas from the same definitions. |
| The server | A step waits for a slow participant to call back. The server receives the callbacks; an embedded engine has nowhere to receive them unless you build that yourself. |
| The server | Operators change definitions, services, or certificates without redeploying your application. The server reloads them from its files without a restart. |
| The server | You want the database credentials for saga state in one place, not in every application. |
| Embedded mode | A step has to run as Java code inside your application, such as a step that updates your own tables or calls a library instead of a service. Only embedded mode supports these code steps. |
| Embedded mode | One Java 25 application owns its sagas, and you would rather not deploy and secure a separate server. Each instance of the application can recover sagas that another left unfinished, once it calls `startBackgroundTasks()`. |

Start with the server. Choose embedded mode when you need code steps, or cannot run a server. The
rest of this page uses the server; for embedded mode, see
[Run the engine in your application](#run-the-engine-in-your-application).

## Choose an interface

| Interface | Use when |
| --- | --- |
| Java client SDK (recommended) | Your application runs on Java 8 or later. The SDK talks to the server over gRPC, absorbs transient network failures for you, and reports failures as typed exceptions. |
| REST API | Your application is not written in Java. See [Use another language](#use-another-language). |
| gRPC API | Your application is not written in Java and you prefer gRPC. See [Use another language](#use-another-language). |
| Embedded mode | You need steps written in Java, or want to run without a server, and your application runs on Java 25. See [Server or embedded?](#server-or-embedded) and [Run the engine in your application](#run-the-engine-in-your-application). |

The rest of this page uses the Java client SDK.

## Add the SDK to your project

Import the BOM to pin every ScalarDB Saga artifact to one version, then add the client SDK. Replace
`VERSION` with the ScalarDB Saga version you use.

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

The SDK brings the gRPC transport it needs. Do not add `scalardb-saga-core` as well: that is the
engine itself, for embedded mode, and the SDK does not need it.

The classes on this page are in three packages: the clients in `com.scalar.db.saga.grpc`, the
exceptions and `SagaErrorCode` in `com.scalar.db.saga.exception`, and everything else, such as
`SagaStatus` and `SagaCallback`, in `com.scalar.db.saga.api`.

## Connect to the server

`GrpcSagaOrchestratorClient` is the application's client. It is thread-safe and holds one
connection that all calls share, so create one instance when your application starts, share it,
and close it when your application stops.

For a server on a trusted network without TLS:

```java
GrpcSagaOrchestratorClient client = GrpcSagaOrchestratorClient.create("saga-server:12051");

// When your application stops:
client.close();
```

For anything else, use the builder:

```java
GrpcSagaOrchestratorClient client =
    GrpcSagaOrchestratorClient.newBuilder()
        .target("saga-server:12051")
        .useTransportSecurity()
        .trustCaCertificate(Paths.get("/etc/saga/ca.crt"))
        .defaultDeadlineMillis(30_000)
        .build();
```

| Builder method | Description |
| --- | --- |
| `target(String)` | The server's gRPC address, as `host:port`. Required. |
| `usePlaintext()` | Connect without TLS. This is the default. |
| `useTransportSecurity()` | Connect with TLS, validating the server's certificate against the JVM's default trust store. |
| `trustCaCertificate(Path)` | Trust only the CA certificate in this PEM file, for a server whose certificate a private CA issued. Requires `useTransportSecurity()`. |
| `overrideAuthority(String)` | Validate the server's certificate against this name instead of the address you connect to, such as when you connect through a port-forward. |
| `callCredentials(CallCredentials)` | The credential sent on every call. See [Authenticate](#authenticate). |
| `defaultDeadlineMillis(long)` | How long a call may take. For a waiting `start`, this bounds the whole wait; for `startAsync`, the start and its retries. It does not bound the watch behind a completion callback. `0`, the default, means no limit. See [Limit how long to wait](#limit-how-long-to-wait). |

`build()` fails right away on a configuration that cannot work, such as a CA certificate with
plaintext or a CA file that cannot be read, rather than on the first call.

> [!NOTE]
> On Java 8, use Java 8u252 or later for TLS. The SDK's default gRPC transport also supports TLS on
> older Java 8 updates.

### Authenticate

When the server runs with an [authentication provider](configurations.md#security) (`apikey` or
`jwt`), every call must carry a credential, and the caller needs the role for the operation:
`saga:write` to start sagas, and `saga:read` to check them. `saga:write` includes `saga:read`.
Set the credential on the builder:

```java
GrpcSagaOrchestratorClient client =
    GrpcSagaOrchestratorClient.newBuilder()
        .target("saga-server:12051")
        .useTransportSecurity()
        .callCredentials(
            GrpcSagaOrchestratorClient.staticHeaderCredentials(
                "X-API-Key", System.getenv("SAGA_API_KEY")))
        .build();
```

`staticHeaderCredentials` sends a fixed header value on every call:

- For the `apikey` provider, use the header the server is configured to read
  (`security.apikey.header`, `X-API-Key` by default) and the key's secret.
- For the `jwt` provider, use `Authorization` and `"Bearer " + token`.

A token that expires needs a `CallCredentials` of your own that fetches a fresh token for each call.
The credential also covers the calls the client makes for a
[completion callback](#get-notified-when-a-saga-ends).

A call without a valid credential throws `SagaUnauthenticatedException`, and one whose credential
lacks the role throws `SagaPermissionDeniedException`. Send credentials over TLS, since a header
sent over plaintext can be read on the network.

## Start a saga

`start` runs a saga and waits until it reaches a final status. Pass the definition name and the
saga's input:

```java
Map<String, Object> input = new HashMap<>();
input.put("orderId", "o-1001");
input.put("amount", 100);
input.put("item", "widget");
input.put("quantity", 2);

String sagaId = client.start("order-saga", input);
SagaStatus status = client.getStateSnapshot(sagaId).getStatus();
// Act on status, as the table below describes.
```

`start` waits until the saga is final, but returns only the saga's ID, not its outcome. To learn
whether the saga succeeded, read its status with `getStateSnapshot`:

| Status | Meaning | What the application does |
| --- | --- | --- |
| `COMPLETED` | Every step succeeded. | Continue, such as confirming the order to the customer. |
| `COMPENSATED` | A step failed, and the completed steps were undone. | Report the failure, such as telling the customer the order could not be placed. |
| `ESCALATED` | The engine could not finish the saga, and an operator has to resolve it. | Treat the outcome as unknown until an operator resolves it. |

A saga that ends `COMPENSATED` is a normal outcome, not an exception: `start` returns as usual.
`start` throws only when the saga could not be run, or its deadline passed first, as described in
[Handle errors](#handle-errors).

Input values can be strings, numbers, booleans, `BigDecimal`, and lists and maps of those. A
definition reads them with `${...}`, as described in
[Pass data between steps](model-your-workflow.md#pass-data-between-steps).

### Use your own saga ID

Pass an ID of your own to make starting a saga safe to repeat:

```java
String sagaId = "order-" + orderId;
try {
  client.start(sagaId, "order-saga", input);
} catch (SagaAlreadyExistsException e) {
  // An earlier attempt already started this saga, which may still be running.
}
SagaStateSnapshot snapshot = client.getStateSnapshot(sagaId);
while (!snapshot.getStatus().isTerminal()) {
  Thread.sleep(1000);
  snapshot = client.getStateSnapshot(sagaId);
}
// Act on snapshot.getStatus() as in Start a saga, whichever attempt started it.
```

`SagaAlreadyExistsException` is thrown as soon as the ID is found taken, without waiting for that
saga to finish, so the example reads the status until it is final.

If your application crashes or loses its connection without learning whether a saga started, it can
call `start` again with the same ID. Either the saga starts, or `SagaAlreadyExistsException` reports
the one that already exists, so the same work never runs twice while the saga is kept.
[Retention](configurations.md#retention) deletes a finished saga after a while, and a `start` with
its ID after that runs the saga again, so if a retry can come later than that, also deduplicate in
your application.

Derive the ID from your business key, such as the order ID, so that a retry produces the same ID. An
ID can be up to 128 characters of letters, digits, `.`, `_`, and `-`.

### Start a specific version

A saga started by name uses the newest registered version of the definition. To use a specific one,
pass a `SagaDefinitionId`:

```java
String sagaId = client.start(new SagaDefinitionId("order-saga", "1.0"), input);
```

## Start a saga without waiting

`startAsync` returns as soon as the server has saved the saga, and the saga runs on the server.
Use it for sagas that take long, such as one with an asynchronous step that waits for a person's
approval, so your application does not hold a thread for the whole saga.

```java
String sagaId = client.startAsync("order-saga", input);

// Later, or from another process:
SagaStateSnapshot snapshot = client.getStateSnapshot(sagaId);
if (snapshot.getStatus().isTerminal()) {
  // COMPLETED, COMPENSATED, or ESCALATED
}
```

Like `start`, `startAsync` retries on its own when the connection to the server fails, under the same
saga ID, so a start whose answer was lost does not start a second saga. Pass your own ID, as in
[Use your own saga ID](#use-your-own-saga-id), when your application must be able to find the saga
again after a crash.

### Get notified when a saga ends

Pass a `SagaCallback` to `startAsync` to be called when the saga reaches a final status:

```java
client.startAsync(
    sagaId,
    "order-saga",
    input,
    new SagaCallback() {
      @Override
      public void onCompleted(SagaStateSnapshot saga) {
        executor.execute(() -> confirmOrder(saga.getSagaId()));
      }

      @Override
      public void onCompensated(SagaStateSnapshot saga) {
        executor.execute(() -> rejectOrder(saga.getSagaId()));
      }

      @Override
      public void onEscalated(SagaStateSnapshot saga) {
        executor.execute(() -> flagForOperator(saga.getSagaId()));
      }
    });
```

`executor` is an `Executor` of your own, and `confirmOrder`, `rejectOrder`, and `flagForOperator`
stand for your application's code.

After the start, the client watches the saga on the server and calls exactly one of the three
methods, matching the status table in [Start a saga](#start-a-saga). It keeps watching while the
saga is parked on an asynchronous step, one whose participant finishes later and calls back (see
[Wait for slow participants](model-your-workflow.md#wait-for-slow-participants)), so it never calls
`onParked`; that method is for embedded mode.

This callback is the application's. It is unrelated to the callbacks that participants send to the
server for asynchronous steps.

- **Hand work off quickly.** A callback runs on one of the client's gRPC threads. Pass anything
  slow, such as a database write, to your own executor. When the start finds the saga already
  finished, which can happen when the start had to be retried, the callback runs on the calling
  thread before `startAsync` returns.
- **A callback is not guaranteed.** The watch lives in your application's process. No callback
  comes if the process exits, if you close the client, if retention removes the saga first, or if
  the watch hits a failure that retrying cannot fix, which the client logs. If your application acts on the outcome, keep the
  saga ID, and check `getStateSnapshot` for sagas whose callback has not arrived within the time
  you expect, for example after a restart.
- **A dropped connection can stop a watch silently.** If something on the network drops the
  connection without telling either end, the watch waits on it and no callback comes. The same
  check by saga ID covers this.

Each watched saga keeps one call open to the server. The server answers it when the saga finishes,
or when its wait window (`sync.max_wait_millis`, 60 seconds by default) ends, and the client then
asks again. A saga that stays unfinished for a long time therefore costs calls and store reads for
as long as it is watched; for how the window trades that cost against how soon a callback comes,
see [Synchronous starts](configurations.md#synchronous-starts).

> [!NOTE]
> All calls from one client share one connection. A proxy or load balancer that caps the calls open
> on one connection, as many do at about 100, makes calls beyond the cap wait for a free slot. A
> client that watches more sagas than that through such a proxy can see its other calls delayed by
> up to a wait window.

## Limit how long to wait

With `defaultDeadlineMillis` set to `0`, `start` waits as long as the saga runs. A saga with an
asynchronous step and no timeout can wait indefinitely, and so can the thread that started it. In
production, set `defaultDeadlineMillis` to the longest your application can wait for an answer.

When the deadline passes, `start` throws `SagaTimeoutException`, and the outcome is unknown: the
deadline can pass while `start` is still trying to reach the server, before the saga exists. Get
the saga's ID from `getSagaId()` and check it. If no saga has that ID, start it again with the same
ID:

```java
try {
  client.start(sagaId, "order-saga", input);
} catch (SagaTimeoutException e) {
  // The outcome is unknown. Check e.getSagaId(), and reuse that ID for any retry.
}
```

While it waits, `start` retries on its own when the connection to the server fails, because
repeating the call is safe. The deadline bounds those retries, too. The same goes for `startAsync`:
with no deadline, a `startAsync` against a server that is down keeps retrying until the server is
back.

## Check a saga

| Method | Returns |
| --- | --- |
| `getStateSnapshot(sagaId)` | The saga's current state: ID, definition name and version, status, and when it was created and last updated. |
| `getSagaDetail(sagaId)` | The current state plus the saga's timeline, the durable record of what the engine did. |

```java
SagaDetail detail = client.getSagaDetail(sagaId);
for (TimelineEvent event : detail.getTimeline()) {
  System.out.println(event.getTimestamp() + " " + event.getType());
}
```

A timeline holds event types, step names, and failure details, never the step's request or response
data. A long timeline is cut to the newest events, and `isTruncated()` reports whether it was.

A saga can be checked while it runs and after it ends, until the server's
[retention period](configurations.md#retention) removes it. After that, `getStateSnapshot` throws `SagaNotFoundException`.

## Handle errors

Every exception the SDK throws for a failed call extends `SagaRuntimeException`, and carries an
error code from `getErrorCode()`. A few mistakes in the calling code are reported with standard Java
exceptions instead: `IllegalStateException` for a start on a closed client, and
`IllegalArgumentException` for input that cannot be sent as JSON. A read on a closed client throws
`SagaUnavailableException`.

| Exception | Cause | What to do |
| --- | --- | --- |
| `SagaDefinitionNotFoundException` | No definition has this name, or this name and version. | Fix the name or version. |
| `SagaDefinitionNotServedException` | The definition exists, but this server does not serve it: it was retired, or was added so recently that this server has not loaded it yet. | Check the server's definitions. For a new definition, retry later. |
| `SagaAlreadyExistsException` | A saga with this ID already exists. | Treat it as the result of an earlier attempt. `getExisting()` returns its state, which may not be final yet. |
| `SagaIllegalArgumentException` | The saga ID or an input value is invalid. | Fix the request. |
| `SagaOverloadedException` | The server is at its [limit of concurrent saga starts](configurations.md#admission-control). Nothing was started. | Retry later, or reject the request in your application. The SDK retries only connection failures, not this, so your application can decide. |
| `SagaRuntimeException` with `RATE_LIMIT_EXCEEDED` | The server's [limit on new sagas per minute](configurations.md#admission-control) was reached. Nothing was started. | Retry after a short delay. |
| `SagaTimeoutException` | The call's deadline passed. | Check the saga by `getSagaId()`, if it is set, before starting it again. |
| `SagaUnavailableException` | The server could not be reached while reading a saga, or the client is closed. A start normally does not throw it: it retries until its deadline, then throws `SagaTimeoutException`. | Retry the read, unless the client is closed. |
| `SagaUnauthenticatedException`, `SagaPermissionDeniedException` | The credential is missing or invalid, or lacks the required role. | Fix the credential. |
| `SagaNotFoundException` | No saga has this ID, or it was removed after the retention period. | Check the ID. |

To decide whether to retry, check the error code's category rather than the exception type. The
category is also the first digit after `DB-SAGA-` in the code, such as the `2` in `DB-SAGA-20006`:

| Digit | Category | What to do |
| --- | --- | --- |
| `1` | `USER_ERROR` | Fix the request. Retrying it unchanged fails the same way. |
| `2` | `RETRYABLE_SERVER_ERROR` | Retry with the same saga ID. The SDK has already retried connection failures; these are left to your application. |
| `3` | `NON_RETRYABLE_SERVER_ERROR` | Escalate to an operator. Retrying does not help. |
| `4` | `CLIENT_ERROR` | The call failed in the client before the server answered, such as on a deadline. Check the saga before you retry. |

For example:

```java
try {
  client.start(sagaId, "order-saga", input);
} catch (SagaRuntimeException e) {
  if (e.getErrorCode().category() == SagaErrorCode.Category.RETRYABLE_SERVER_ERROR) {
    // Transient. Retry with the same saga ID.
  } else {
    throw e;
  }
}
```

A retry is only safe with your own saga ID. With a generated ID, a retry after a failure that
reached the server starts a second saga.

## Operate sagas

`GrpcSagaAdminClient` is a separate client for operators. Its calls require the `saga:admin` role.
It lists sagas and resolves ones that need a person, such as an `ESCALATED` saga:

| Method | Description |
| --- | --- |
| `listSagas(SagaQuery)` | Lists sagas, filtered by status and update time, a page at a time. |
| `recoverSaga(sagaId, reason)` | Drives a stuck `RUNNING` or `COMPENSATING` saga now, instead of waiting for background recovery. The engine decides the direction from the pivot, as recovery does. |
| `forceComplete(sagaId, reason)` | Marks an `ESCALATED` saga `COMPLETED` without running its remaining steps, after an operator has finished the work by hand. |
| `resetEscalated(sagaId, reason)` | Returns an `ESCALATED` saga to the engine, so it tries again. |
| `resetEscalated(SagaQuery, reason)` | Returns one page of `ESCALATED` sagas to the engine, such as after an outage of a participant. The result counts the sagas reset and skipped, and gives the token for the next page. |

Every change is recorded on the saga's timeline with the operator's identity and the reason. Build
the client the same way as the application client:

```java
GrpcSagaAdminClient admin =
    GrpcSagaAdminClient.newBuilder()
        .target("saga-server:12051")
        .useTransportSecurity()
        .callCredentials(
            GrpcSagaAdminClient.staticHeaderCredentials(
                "X-API-Key", System.getenv("SAGA_ADMIN_KEY")))
        .build();
```

## Use another language

Applications in other languages call the server directly, over REST on port `12080` or gRPC on port
`12051` by default.

| Operation | REST | gRPC (`SagaService`) |
| --- | --- | --- |
| Start a saga and wait | `POST /sagas` | `StartSaga` |
| Start a saga with your own ID | `PUT /sagas/{id}` | `StartSaga` with `saga_id` set |
| Start a saga without waiting | `POST /sagas?async=true` or `PUT /sagas/{id}?async=true` | `StartSaga` with `async` set |
| Wait for a saga to finish | | `AwaitSaga` |
| Get a saga's state | `GET /sagas/{id}` | `GetSaga` |
| Get a saga's state and timeline | `GET /sagas/{id}/detail` | `GetSagaDetail` |

A REST request to start a saga sends the definition name and the input as JSON:

```console
curl -X PUT localhost:12080/sagas/order-1001 \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-1001","amount":100,"item":"widget","quantity":2}}'
```

A waiting start returns when the saga reaches a final status or when the server's
[wait limit](configurations.md#synchronous-starts) passes, whichever comes first. In the second
case, the server answers `202` with the saga's current state while the saga keeps running, and the
application polls `GET /sagas/{id}` until it ends. The
gRPC contract is in the [`rpc` module](../rpc/src/main/proto/saga.proto). Error responses carry an
error code, such as `DB-SAGA-20004`, whose first digit after `DB-SAGA-` gives the same category the
Java SDK reports, as listed in [Handle errors](#handle-errors).

For every route, its request and response, and its status codes, see the
[REST API reference](rest-api.md). The getting started guide walks through the REST API end to end.

## Run the engine in your application

In embedded mode, the engine runs inside your application instead of in a server, and saga state
goes straight to your database through ScalarDB. `DefaultSagaOrchestrator`, in `scalardb-saga-core`,
implements the same `SagaOrchestrator` interface as the client SDK, so the code on this page
compiles against either, but an embedded engine waits less:

- **`start` returns after one pass over the saga.** If a compensation fails, the saga is left
  `COMPENSATING` for recovery to finish, and `start` returns anyway, where the client SDK keeps
  waiting until the saga is final. Check the status after `start`, and if it is not final, poll
  `getStateSnapshot` until it is.
- **An embedded completion callback stops at a parked saga.** It reports a saga that waits on an
  asynchronous step through `onParked` and then reports nothing more, where the client SDK keeps
  watching it.

Embedded mode requires Java 25, and lets a step be a Java class instead of a service call. For
details, see the [root README](../README.md).

Call `startBackgroundTasks()` once, after you build the engine. It starts recovery, which finishes
sagas that a stopped instance left unfinished, and retention, which removes finished sagas. Without
that call, neither runs.

Asynchronous steps need the server. An embedded engine rejects a definition with an `async` call
unless you give it a callback URL provider and route the participants' callbacks to it yourself.

## Next steps

- [Model Your Workflow](model-your-workflow.md): design the definitions your application starts.
- [ScalarDB Saga Configurations](configurations.md): configure the server, including
  authentication and TLS.
