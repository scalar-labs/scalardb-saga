# Getting Started with ScalarDB Saga

This getting started tutorial explains how to run ScalarDB Saga as a server and illustrates the
process of running a sample order-placement flow, where placing an order charges a payment service,
reserves stock in an inventory service, and hands the parcel to a shipping service. The sample shows
how ScalarDB Saga completes the flow when every service succeeds, and how it automatically undoes the
completed steps when one of them fails.

> [!WARNING]
> Since the focus of the sample is to demonstrate using ScalarDB Saga, the participant services are
> stand-ins that return canned responses, and the server and the dashboard run with authentication
> disabled. For details about running the server in production, see
> [server/docker/README.md](../server/docker/README.md).

## Prerequisites for this sample application

- [Docker](https://www.docker.com/get-started/) 20.10 or later with
  [Docker Compose](https://docs.docker.com/compose/install/) V2 or later
- `curl`, or any other HTTP client
- A web browser, for the live dashboard

No JDK is required. The saga server runs as a container and you drive it over HTTP.

## Clone the ScalarDB Saga repository

Open **Terminal**, then clone the ScalarDB Saga repository by running the following command:

```console
git clone https://github.com/scalar-labs/scalardb-saga
```

Then, go to the directory that contains this sample by running the following command:

```console
cd scalardb-saga/getting-started
```

## Start the sample environment

Start the database, the participant services, and the saga server by running the following command:

```console
docker compose up -d --wait
```

`--wait` holds the command until the saga server and the dashboard report healthy, so the requests
below cannot race their startup. That starts six containers:

| Container | Purpose |
| --- | --- |
| `postgres` | The database where the saga server keeps saga state |
| `payment`, `inventory`, `shipping` | The three services a saga calls |
| `saga-server` | ScalarDB Saga, serving REST on `12080` and gRPC on `12051` |
| `dashboard` | A live web view of the sagas, on [http://localhost:12090](http://localhost:12090) |

The saga server creates its tables in the database on first start, so there is no schema to load, and
registers the saga definitions in `conf/definitions`.

### Watch the participant services

The services print every request they receive and every response they return. Keep their logs in a
second terminal while you run sagas, so you can watch a saga drive them:

```console
docker compose logs -f payment inventory shipping
```

If you use tmux, [`tmux-demo.sh`](tmux-demo.sh) opens a ready-made cockpit instead: the three
services' logs across the top, and a control pane for the curl commands below — next to the saga
server's own log, so you can watch the engine and the participants react to each command you run.

### Watch sagas in your browser

The dashboard shows the same thing visually. Open
[http://localhost:12090](http://localhost:12090) and keep it open while you run the commands
below: every saga you start appears there live, with its steps, its status, and the durable
timeline the engine writes. The dashboard only reads the saga server's REST API, so everything in
this tutorial works the same without it. It can also run sagas itself; see
[Drive sagas from the dashboard](#drive-sagas-from-the-dashboard).

### Use a different database

This sample uses PostgreSQL because Compose can start it in one step, but saga state can live in any
database ScalarDB supports. To use another one, change the `scalar.db.*` properties in
`conf/server.properties`. For the list of supported databases, see
[Databases](https://scalardb.scalar-labs.com/docs/latest/requirements#databases).

## Saga definition details

The `conf/definitions` directory registers the two saga definitions this walkthrough uses — plus
seven demo variants of the same flow, four in saga mode and three in TCC mode, described in
[Drive sagas from the dashboard](#drive-sagas-from-the-dashboard). Each names its steps, the call
each step makes, and the call that undoes it:

- [`order-saga.json`](conf/definitions/order-saga.json): the order flow, where every service succeeds

  | Step | Service | Execution | Compensation |
  | --- | --- | --- | --- |
  | `charge` | `payment` | `POST /charge` | `POST /refund` |
  | `reserve` | `inventory` | `POST /reserve` | `POST /release` |
  | `ship` | `shipping` | `POST /ship` | `POST /cancel` |

- [`order-saga-failing.json`](conf/definitions/order-saga-failing.json): the same flow, except that
  `ship` calls `POST /ship-fail`, which the shipping service rejects with `422`

Values flow between the steps through the saga's context. A step's `jsonBody` reads from the context
with `${...}`, and its `output` captures fields from the service's response back into it, so a later
step can use them:

```json
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
}
```

Note that the compensation keys on `orderId`, which came from the request that started the saga,
rather than on the step's own output — a step that failed may not have produced one.

## Execute sagas and check their state in the sample application

The following sections describe how to run sagas and inspect them in the sample application.

### Place an order

Start with placing an order for two widgets by running the following command:

```console
curl -X POST localhost:12080/sagas \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-1001","amount":"100","item":"widget","quantity":"2"}}'
```

You should see a similar output as below, with a different UUID for `sagaId` and different
timestamps, where `COMPLETED` confirms that every step succeeded:

```console
{"sagaId":"7f9c2a41-...","sagaName":"order-saga","status":"COMPLETED","definitionVersion":"1.0","createdAt":"2026-07-30T01:22:03.914Z","updatedAt":"2026-07-30T01:22:04.512Z"}
```

In the service logs, you should see the three steps running in order, each receiving values the saga
passed to it:

```console
[payment] POST /charge <- {"orderId":"o-1001","amount":"100"}
[payment] POST /charge -> 200 {"payment_id": "payment-o-1001"}
[inventory] POST /reserve <- {"orderId":"o-1001","item":"widget","quantity":"2"}
[inventory] POST /reserve -> 200 {"inventory_id": "inventory-o-1001"}
[shipping] POST /ship <- {"orderId":"o-1001","paymentId":"payment-o-1001","reservationId":"inventory-o-1001"}
[shipping] POST /ship -> 200 {"shipping_id": "shipping-o-1001"}
```

Note what the `ship` step received: `paymentId` is the value the payment service returned two steps
earlier, captured by that step's `output` and read back with `${paymentId}`. The fields are shown in
the order the definition declares them; the order they arrive in is not significant.

### Check the saga state

Get the current state of a saga by running the following command, replacing `<SAGA_ID_UUID>` with the
UUID for the `sagaId` that was shown after running the previous command:

```console
curl localhost:12080/sagas/<SAGA_ID_UUID>
```

You should see the same state as above. A saga is queryable while it is running and after it has
finished.

### Place an order that cannot be shipped

This is what a saga exists for. Place an order with the definition whose shipping step fails, by
running the following command:

```console
curl -X POST localhost:12080/sagas \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga-failing","input":{"orderId":"o-1002","amount":"100","item":"widget","quantity":"2"}}'
```

You should see a similar output as below, where `COMPENSATED` shows that the failure was handled and
nothing was left half-applied:

```console
{"sagaId":"c41b8e07-...","sagaName":"order-saga-failing","status":"COMPENSATED","definitionVersion":"1.0","createdAt":"2026-07-30T01:24:11.208Z","updatedAt":"2026-07-30T01:24:12.377Z"}
```

In the service logs, you should see the shipping step rejected, and then every step compensated in
reverse order, starting with `ship` itself:

```console
[shipping] POST /ship-fail -> 422 {"error": "shipping rejected /ship-fail"}
[shipping] POST /cancel -> 200 {"shipping_id": "shipping-o-1002"}
[inventory] POST /release -> 200 {"inventory_id": "inventory-o-1002"}
[payment] POST /refund -> 200 {"payment_id": "payment-o-1002"}
```

Note that `ship` is compensated even though it failed. A step that reports a failure may still have
applied its side effect: the service can commit the change and then fail to answer, or the response
can be lost on the way back. The engine cannot tell that case apart from one where nothing happened,
so it compensates the failed step rather than assuming it did nothing. Compensations are required to
be idempotent, which is what makes the harmless case harmless; cancelling a shipment that was never
created must succeed.

A `422` is a permanent failure, so the step is not retried. A `503` or a connection error would be,
according to the step's retry policy.

### Check what the engine did

Get a saga's timeline — the durable record the engine writes as the saga progresses — by running
the following command:

```console
curl localhost:12080/sagas/<SAGA_ID_UUID>/detail
```

You should see the saga's state followed by its timeline, abridged here for readability:

```console
{"saga":{"sagaId":"c41b8e07-...","status":"COMPENSATED",...},
 "timeline":[{"timestamp":"...","type":"SAGA_STARTED"},
             {"timestamp":"...","type":"STEP_COMPLETED","stepIndex":0,"stepName":"charge"},
             {"timestamp":"...","type":"STEP_COMPLETED","stepIndex":1,"stepName":"reserve"},
             {"timestamp":"...","type":"STEP_FAILED","stepIndex":2,"stepName":"ship","detail":"..."},
             {"timestamp":"...","type":"SAGA_COMPENSATING"},
             {"timestamp":"...","type":"STEP_COMPENSATED","stepIndex":2,"stepName":"ship"},
             {"timestamp":"...","type":"STEP_COMPENSATED","stepIndex":1,"stepName":"reserve"},
             {"timestamp":"...","type":"STEP_COMPENSATED","stepIndex":0,"stepName":"charge"},
             {"timestamp":"...","type":"SAGA_COMPENSATED","resultingStatus":"COMPENSATED"}]}
```

This record is what lets another server pick up a saga whose coordinator died mid-flight and finish
it. A step interrupted between running and being recorded runs again on recovery, which is why steps
must be idempotent. The timeline carries metadata and failure details only; raw step payloads are
never returned.

### Start a saga without waiting for it to finish

The commands above blocked until the saga reached a terminal state. Add `async=true` to get the saga
ID back immediately by running the following command:

```console
curl -X POST 'localhost:12080/sagas?async=true' \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-1003","amount":"100","item":"widget","quantity":"2"}}'
```

You should see a similar output as below, with `RUNNING` and a `202` status code, returned before the
saga has finished:

```console
{"sagaId":"9b3d5f18-...","sagaName":"order-saga","status":"RUNNING","definitionVersion":"1.0","createdAt":"2026-07-30T01:26:40.115Z","updatedAt":"2026-07-30T01:26:40.115Z"}
```

Then poll for the outcome by running the following command:

```console
curl localhost:12080/sagas/<SAGA_ID_UUID>
```

> [!NOTE]
> The stand-in services answer instantly, so an asynchronous saga is usually already `COMPLETED` by
> the time you poll. To watch one in progress, set `DELAY_SECONDS: 3` on a service in
> `docker-compose.yaml` and run `docker compose up -d` again — or start a saga from
> [the dashboard](#drive-sagas-from-the-dashboard), whose pace slider slows the services for that
> saga alone.

### Start a saga with your own ID

Supply your own saga ID instead of having one generated by running the following command:

```console
curl -X PUT localhost:12080/sagas/order-1001 \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-1004","amount":"100","item":"widget","quantity":"2"}}'
```

This makes starting a saga idempotent: if your application crashes without learning the outcome, it
can retry with the same ID instead of starting a second saga. Reusing an ID that already exists
returns `409`.

## Drive sagas from the dashboard

The launcher at the top of [the dashboard](http://localhost:12090) starts sagas through the same
REST API you just used with curl. Every definition registered from `conf/definitions` can be
started — including any you add yourself — and the input fields are derived from the definition:
one for each `${...}` value its steps read from the saga input (values captured from an earlier
step's `output` are excluded automatically). Four extra definitions ship alongside the two above:

- `order-saga-flaky` — the same flow, except that `ship` answers `503` on its first two attempts,
  so the step's retry policy retries it with growing waits before it succeeds.
- `order-saga-retry` — the same transient-failure scenario with a tight retry backoff; when paced,
  the attempts are spaced by the pace itself.
- `order-saga-retry-failing` — `ship` never stops failing (`503` every time), so its three
  attempts exhaust and the saga rolls back to `COMPENSATED`.
- `order-saga-pivot` — the counterpart to `order-saga-retry-failing`: `reserve` is the saga's
  *pivot*, and once it completes the saga only rolls forward. Its `ship` step exhausts its two
  attempts the same way, but instead of compensating, the saga stays `RUNNING` — parked past the
  pivot, nothing rolled back — until background recovery re-drives the step 20–30 seconds later
  (the sample's `server.properties` tunes the recovery scan for the demo) and the third attempt
  succeeds. Watch the two timelines side by side: the same retry exhaustion
  ends in `STEP_COMPENSATED` walking backward in one, and in `STEP_FAILED` followed by
  `STEP_COMPLETED` for the same step in the other.

Three more run the same order flow in **TCC mode**, where every step is *reserved* before any step
is *confirmed* — the dashboard shows each service moving through reserved → confirmed:

- `order-tcc` — reserve payment, inventory, and shipping, then confirm all three → `COMPLETED`.
- `order-tcc-try-fail` — shipping's reservation is rejected (`422`), so the reservations already
  held are cancelled right-to-left → `COMPENSATED`. Nothing is ever confirmed.
- `order-tcc-confirm-retry` — shipping's confirmation answers `503` twice. A confirmation is past
  the point of no return, so it is only ever retried forward — never cancelled — and the saga
  ends `COMPLETED`, with the attempts visible under the shipping step.

The pace slider applies to every saga the dashboard starts, whichever definition it uses: before
starting the saga, the dashboard tells each participant service how long to hold that saga's
calls, so every step — and every compensation, which runs at the same pace in reverse — takes
long enough to watch. Sagas started with curl are never paced, which is why the walkthrough above
stays instant while the dashboard is running.

While a saga runs, the dashboard shows the step pipeline — execution walking forward,
compensation walking backward, each service over its own database with the calls it received
beneath it — next to the orchestrator's state timeline, the durable record you saw on
`GET /sagas/<SAGA_ID_UUID>/detail`, with every participant call in the panel at the bottom. The
calls are where retries are visible: the timeline records step outcomes, not attempts, so the
flaky saga's three `ship` calls appear under its step and in the calls panel, nowhere else. When
several sagas run at once, the participant calls — in that panel and in the service logs —
interleave.

### What happens when an input is missing

Definitions read their inputs with `${...}`, and a missing value fails the step while its call is
still being built, before anything is sent. Try leaving out `item`:

```console
curl -X POST localhost:12080/sagas \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-2001","amount":"100"}}'
```

`charge` completes, then `reserve` fails resolving `${item}` — and the engine, knowing that a
call it never sent cannot have applied a side effect, compensates `charge`, skips `reserve`, and
settles the saga as `COMPENSATED`, with the reason recorded on the timeline's `STEP_FAILED`
event. Watch it in the dashboard: a definition-input mistake is handled as cleanly as a business
rejection.

## Stop the sample environment

To stop the sample, stop the Docker containers and remove the database volume by running the following
command:

```console
docker compose down -v
```

## Reference

| Path | Purpose |
| --- | --- |
| [`docker-compose.yaml`](docker-compose.yaml) | The database, the three services, the saga server, and the dashboard |
| [`conf/server.properties`](conf/server.properties) | Server configuration: the store, where definitions live, and each service's base URL |
| [`conf/definitions/`](conf/definitions) | The saga definitions: the two used in the walkthrough, plus seven demo variants (four saga, three TCC) |
| [`services/service.py`](services/service.py) | The stand-in participant service; all three containers run it with a different name |
| [`dashboard/`](dashboard) | The live dashboard: a dependency-free Python server and a plain-JavaScript page |
| [`tmux-demo.sh`](tmux-demo.sh) | A tmux cockpit: service logs on top, curl control pane and server log below |
| [`COMMANDS.md`](COMMANDS.md) | A copy-paste cheat sheet of the curl commands for starting, listing, and unsticking sagas |
| [`client-example/`](client-example) | A minimal Java program driving the same flows through the client SDK over gRPC |

To go further:

- Add a step, or a `retryPolicy`, to a definition, bump its `version`, then restart the server with
  `docker compose restart saga-server`. Definitions are immutable once registered, so editing one in
  place without bumping the version stops the server on its next start. The dashboard picks the
  change up within a few seconds and can start the new definition.
- Drive sagas from Java instead of curl: [`client-example/`](client-example) runs the same flows
  through the `scalardb-saga-java-client-sdk` over gRPC — with the stack up, run
  `../../gradlew run` from that directory (this one needs a JDK).
- [server/docker/README.md](../server/docker/README.md) — running the server for real: configuration,
  authentication, health checks, and deployment.
- Embedded mode, listed in the [root README](../README.md), runs the engine as a library inside your
  application, where steps can be Java code rather than service calls.

The Compose file pulls `ghcr.io/scalar-labs/scalardb-saga-server`, which is published from the first
release onward. To run against a locally built image instead, run `./gradlew :server:dockerBuild` from
the repository root, then run `docker compose up -d --wait` with `SAGA_VERSION` set to the `version`
in `gradle.properties`, which is the tag `dockerBuild` applies.
