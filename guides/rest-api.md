# ScalarDB Saga REST API

This page lists every route of the ScalarDB Saga server's REST API, with its request, its response,
and the status codes it answers with. The REST API lets applications in any language start and check
sagas without an SDK. Java applications usually use the client SDK instead, as described in
[Run Saga Transactions](run-saga-transactions.md).

| Route | Description | Access |
| --- | --- | --- |
| [`POST /sagas`](#start-a-saga) | Start a saga with an ID the server generates. | `saga:write` |
| [`PUT /sagas/{id}`](#start-a-saga) | Start a saga with your own ID. | `saga:write` |
| [`GET /sagas/{id}`](#get-a-saga) | Get a saga's state. | `saga:read` |
| [`GET /sagas/{id}/detail`](#get-a-sagas-detail) | Get a saga's state and timeline. | `saga:read` |
| [`GET /sagas`](#list-sagas) | List sagas. | `saga:admin` |
| [`POST /sagas/{id}/recover`](#recover-a-saga) | Drive a stuck saga now. | `saga:admin` |
| [`POST /sagas/{id}/force-complete`](#force-complete-a-saga) | Mark an escalated saga completed. | `saga:admin` |
| [`POST /sagas/{id}/reset`](#reset-an-escalated-saga) | Return an escalated saga to the engine. | `saga:admin` |
| [`POST /admin/reset-escalated`](#reset-escalated-sagas-in-bulk) | Return a page of escalated sagas to the engine. | `saga:admin` |
| [`POST /sagas/{id}/steps/{stepName}/complete`](#participant-callbacks) | Report that an asynchronous step succeeded. | Callback token |
| [`POST /sagas/{id}/steps/{stepName}/fail`](#participant-callbacks) | Report that an asynchronous step failed. | Callback token |
| [`GET /health`](#check-health) | Check that the server is up. | None |

## Conventions

The server listens on port `12080` by default, set by
[`scalar.db.saga.server.http.port`](configurations.md#rest). With [TLS](configurations.md#tls)
enabled, use `https`.

Request and response bodies are JSON. Send `Content-Type: application/json` with a body. The server
accepts a request body of up to about 1 MB. A larger body gets `400` with `DB-SAGA-10001` on
the start and operator routes, and `413` with the same code on the callback routes.

Timestamps are ISO-8601 instants in UTC, such as `2026-10-26T09:15:30.123Z`. A field with no value
is sent as `null`, not left out.

### Authentication

With the `apikey` or `jwt` [security provider](configurations.md#security), every route except the
callback routes and `/health` needs a credential:

- **API key:** the key in the `X-API-Key` header, or in the header that
  [`security.apikey.header`](configurations.md#api-key) names.
- **JWT:** `Authorization: Bearer <token>`.

The credential must carry the route's role. Each role includes the ones before it:
`saga:read`, then `saga:write`, then `saga:admin`. A missing or invalid credential gets `401`, and a
credential without the role gets `403`.

With the `noop` provider, the server does not check credentials.

### The saga object

Most routes return a saga's state as this object:

```json
{
  "sagaId": "order-1001",
  "sagaName": "order-saga",
  "status": "COMPLETED",
  "definitionVersion": "1",
  "createdAt": "2026-10-26T09:15:30.123Z",
  "updatedAt": "2026-10-26T09:15:31.456Z"
}
```

| Field | Description |
| --- | --- |
| `sagaId` | The saga's ID. |
| `sagaName` | The name of the definition the saga runs. |
| `status` | `RUNNING`, `WAITING`, `COMPENSATING`, `COMPLETED`, `COMPENSATED`, or `ESCALATED`. The last three are final. |
| `definitionVersion` | The version of the definition the saga runs. |
| `createdAt` | When the saga was created. |
| `updatedAt` | When the saga's state last changed. |

For what each status means, see [Concepts](model-your-workflow.md#concepts).

### Errors

An error response has a JSON body with an error code:

```json
{
  "errorCode": "DB-SAGA-10201",
  "message": "DB-SAGA-10201: Saga not found [saga_id=order-1001]",
  "metadata": { "saga_id": "order-1001" }
}
```

| Field | Description |
| --- | --- |
| `errorCode` | The error code. The first digit after `DB-SAGA-` says whether retrying can help. |
| `message` | A description for people to read. Do not parse it; it can change. |
| `metadata` | Values that identify what the error is about, such as the saga ID. The keys depend on the code. |

Decide what to do from `errorCode`, not from the HTTP status: two codes can share a status and need
different reactions. `DB-SAGA-20004`, `DB-SAGA-20006`, and `DB-SAGA-20007` carry a `Retry-After`
header, in seconds; wait at least that long before you retry. The other retryable codes carry none,
so retry them with backoff.

For every code, its cause, and what to do, see
[ScalarDB Saga Status Codes](scalardb-saga-status-codes.md).

Any route can answer with the following codes, so the tables for each route leave them out:

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `DB-SAGA-10001` | The request is malformed, such as a body that is not valid JSON. |
| `401` | `DB-SAGA-10101` | The credential is missing or invalid. |
| `403` | `DB-SAGA-10102` | The credential lacks the route's role. |
| `404` | `DB-SAGA-10204` | No route has this path and method. |
| `500` | `DB-SAGA-30001`, `DB-SAGA-30002`, `DB-SAGA-39999` | The server failed. Retrying does not help. |
| `503` | `DB-SAGA-20002`, `DB-SAGA-20003`, `DB-SAGA-20005` | The store or the identity provider is unavailable, or the server stopped the operation while shutting down. Retry with backoff. |

A `HEAD` request to a route that serves only `GET` gets `405` with an `Allow: GET` header and no
body.

## Start a saga

```http
POST /sagas
PUT /sagas/{id}
```

`POST` starts a saga with an ID the server generates. `PUT` starts one with the ID in the path,
which makes the request safe to retry: a second `PUT` with the same ID never starts a second saga.
Use `PUT` whenever your application may send the request again, such as after a timeout. A retry
that gets `409` means the earlier attempt created the saga; get its state with
[`GET /sagas/{id}`](#get-a-saga).

| Query parameter | Description |
| --- | --- |
| `async` | `true` to answer as soon as the saga is created, without waiting for it. `false`, the default, waits. |

The body names the definition and gives the saga's input:

```json
{
  "sagaName": "order-saga",
  "input": { "orderId": "o-1001", "amount": 100 }
}
```

| Field | Description |
| --- | --- |
| `sagaName` | The name of the definition to run. Required. |
| `input` | A JSON object that becomes the saga's initial context, which steps read with `${...}`. Optional. |

The response body is the [saga object](#the-saga-object).

Without `async=true`, the server waits until the saga reaches a final status, until its
[wait limit](configurations.md#synchronous-starts) passes, or until the server begins shutting down,
whichever comes first:

- **`200`** means the saga reached a final status. It does not mean the saga succeeded: check
  `status`, which is `COMPLETED` when it did, and `COMPENSATED` or `ESCALATED` when it did not.
- **`202`** means the wait ended while the saga was still running, such as on a step waiting for
  a slow participant. The saga keeps running. Poll [`GET /sagas/{id}`](#get-a-saga) until its
  status is final.

With `async=true`, the server answers `202` as soon as the saga is created, with `status`
`RUNNING`.

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The saga reached a final status. |
| `202` | | The saga is still running. |
| `400` | `DB-SAGA-10001` | `sagaName` is missing, the body is not a JSON object, or `async` is not `true` or `false`. |
| `400` | `DB-SAGA-10002` | The saga ID or an input value is invalid. |
| `404` | `DB-SAGA-10202` | No definition has this name. |
| `409` | `DB-SAGA-10301` | A saga with this ID already exists. The body does not include its state; get it with [`GET /sagas/{id}`](#get-a-saga). |
| `422` | `DB-SAGA-10403` | The definition exists, but this server does not serve it. |
| `429` | `DB-SAGA-20004` | The caller reached the [limit on starts per minute](configurations.md#admission-control). Nothing was started. |
| `503` | `DB-SAGA-20006` | The server is at its [limit of concurrent starts](configurations.md#admission-control). Nothing was started. |

A failure after the saga is created, such as a step that fails, is not an error response. It shows
in the saga's `status`, and in its [detail](#get-a-sagas-detail).

For example:

```console
curl -X PUT localhost:12080/sagas/order-1001 \
  -H 'Content-Type: application/json' \
  -d '{"sagaName":"order-saga","input":{"orderId":"o-1001","amount":100}}'
```

## Get a saga

```http
GET /sagas/{id}
```

Returns the [saga object](#the-saga-object). A saga can be read while it runs and after it ends,
until the server's [retention period](configurations.md#retention) removes it.

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The saga's state. |
| `404` | `DB-SAGA-10201` | No saga has this ID, or retention removed it. |

## Get a saga's detail

```http
GET /sagas/{id}/detail
```

Returns the saga's state and its timeline, the record of what the engine did:

```json
{
  "saga": { "sagaId": "order-1001", "sagaName": "order-saga", "status": "COMPENSATED", ... },
  "timeline": [
    {
      "timestamp": "2026-10-26T09:15:30.789Z",
      "type": "STEP_FAILED",
      "stepIndex": 1,
      "stepName": "charge-payment",
      "resultingStatus": null,
      "detail": "<why the step failed>",
      "operator": null
    }
  ],
  "truncated": false
}
```

| Field | Description |
| --- | --- |
| `saga` | The [saga object](#the-saga-object). |
| `timeline` | The saga's events, oldest first. |
| `timeline[].timestamp` | When the event was recorded. |
| `timeline[].type` | The event type, such as `STEP_FAILED`. |
| `timeline[].stepIndex`, `timeline[].stepName` | The step the event is about, or `null` for an event about the whole saga. |
| `timeline[].resultingStatus` | The saga status the event moved to, or `null` for an event about a step. |
| `timeline[].detail` | The failure, or the reason an operator gave, or `null`. |
| `timeline[].operator` | Who performed an operator action, or `null` for any other event. |
| `truncated` | `true` when the timeline holds only the newest events, because the saga has more than [`detail.max_timeline_events`](configurations.md#saga-detail). |

The timeline never includes the data that steps sent or received.

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The saga's detail. |
| `404` | `DB-SAGA-10201` | No saga has this ID, or retention removed it. |

## List sagas

```http
GET /sagas
```

Lists sagas a page at a time.

| Query parameter | Description |
| --- | --- |
| `status` | Only sagas with this status, such as `ESCALATED`. Case-sensitive. |
| `updatedAfter` | Only sagas last updated at or after this instant. |
| `updatedBefore` | Only sagas last updated at or before this instant. |
| `pageSize` | How many sagas to return, from `1` to `1000`. A page can hold a few more, when several sagas share the last `updatedAt`. Default `100`. |
| `pageToken` | The `nextPageToken` of the previous page, to get the next one. |

```json
{
  "sagas": [ { "sagaId": "order-1001", "status": "ESCALATED", ... } ],
  "nextPageToken": "<token>"
}
```

| Field | Description |
| --- | --- |
| `sagas` | The page's [saga objects](#the-saga-object). |
| `nextPageToken` | Pass it as `pageToken` to get the next page, or `null` on the last page. Treat it as opaque. |

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | A page of sagas. |
| `400` | `DB-SAGA-10001` | `status` is not a saga status, `pageSize` is not a number, or a time is not an ISO-8601 instant. |
| `400` | `DB-SAGA-10002` | `pageSize` is out of range, `updatedAfter` is after `updatedBefore`, or `pageToken` is invalid. |

## Operator actions

The next four routes change a saga for an operator. Each takes the operator's reason in the body:

```json
{ "reason": "payment-service outage resolved, INC-2041" }
```

The reason is required, and can be up to 1024 characters. It is recorded on the saga's timeline
with the identity of the caller, taken from the credential. A missing, blank, or longer reason gets
`400` with `DB-SAGA-10002`.

Recover, force-complete, and reset can also answer:

| Status | Code | Meaning |
| --- | --- | --- |
| `404` | `DB-SAGA-10201` | No saga has this ID. |
| `409` | `DB-SAGA-20001` | Another server or operator changed the saga at the same time. Get the saga and decide again. |
| `422` | `DB-SAGA-10401` | The saga's status does not allow the action, such as a reset of a saga that is not `ESCALATED`. |

### Recover a saga

```http
POST /sagas/{id}/recover
```

Drives a stuck `RUNNING` or `COMPENSATING` saga now, instead of waiting for background recovery.
The engine decides whether to go forward or compensate, as recovery does.

The server drives the saga within the request, for a bounded time, and returns the
[saga object](#the-saga-object): `200` when the saga reached a final status, or `202` when it is
still running, in which case recovery finishes it.

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The saga reached a final status. |
| `202` | | The saga is still running. |
| `400` | `DB-SAGA-10009` | The saga's definition version names a service the server no longer has. The recovery is already recorded on the saga's timeline, so get the saga before you act again. |
| `404` | `DB-SAGA-10203` | The saga's definition version is not registered. Nothing was changed. |
| `422` | `DB-SAGA-10402` | The saga is waiting for an asynchronous step, which continues on its own. |

### Force-complete a saga

```http
POST /sagas/{id}/force-complete
```

Marks an `ESCALATED` saga `COMPLETED` without running its remaining steps, after an operator has
finished the work by hand. Returns `200` with the [saga object](#the-saga-object).

### Reset an escalated saga

```http
POST /sagas/{id}/reset
```

Returns an `ESCALATED` saga to the engine, so that it tries again. Like
[recover](#recover-a-saga), the server drives the saga within the request for a bounded time, and
answers `200` or `202` with the [saga object](#the-saga-object).

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The saga reached a final status. |
| `202` | | The saga is still running. |
| `400` | `DB-SAGA-10009` | The saga's definition version names a service the server no longer has. The saga has already left `ESCALATED`, so get it before you act again. |
| `404` | `DB-SAGA-10203` | The saga's definition version is not registered. Nothing was changed. |

### Reset escalated sagas in bulk

```http
POST /admin/reset-escalated
```

Returns one page of `ESCALATED` sagas to the engine, such as after an outage of a participant.
Recovery then drives them in the background.

```json
{
  "reason": "payment-service outage resolved, INC-2041",
  "updatedAfter": "2026-10-26T08:00:00Z",
  "updatedBefore": "2026-10-26T09:00:00Z",
  "pageSize": 100
}
```

| Field | Description |
| --- | --- |
| `reason` | Why the sagas are reset. Required. |
| `updatedAfter`, `updatedBefore` | Only sagas last updated within this window. Optional. |
| `pageSize` | How many sagas to reset in this call, as for [listing](#list-sagas). Optional. |
| `pageToken` | The `nextPageToken` of the previous call, to continue. Optional. |

```json
{
  "resetCount": 98,
  "skipped": [
    { "sagaId": "order-0997", "reason": "CONCURRENT_MODIFICATION", "detail": null }
  ],
  "nextPageToken": null
}
```

| Field | Description |
| --- | --- |
| `resetCount` | How many sagas were reset. |
| `skipped` | The sagas in the page that were not reset, each with a `reason` and a `detail`, which can be `null`. See the reasons below. |
| `nextPageToken` | Pass it as `pageToken` to reset the next page, or `null` when no escalated sagas are left in the window. |

| Skip reason | Meaning |
| --- | --- |
| `CONCURRENT_MODIFICATION` | Something else changed the saga at the same time. A later call usually resets it. |
| `DEFINITION_NOT_FOUND` | The saga's definition version is not registered. Register it, then call again. |
| `CORRUPT_EVENT_STREAM` | The saga's stored history cannot be read. Calling again fails the same way. |

| Status | Code | Meaning |
| --- | --- | --- |
| `200` | | The page was processed. |
| `400` | `DB-SAGA-10001` | The body is malformed, `pageSize` is not a number, or a time is not an ISO-8601 instant. |
| `400` | `DB-SAGA-10002` | `reason` is missing or too long, `pageSize` is out of range, `updatedAfter` is after `updatedBefore`, or `pageToken` is invalid. |
| `503` | `DB-SAGA-20002` | The store failed partway through. Sagas reset before the failure stay reset; call again to reset the rest. |

## Participant callbacks

```http
POST /sagas/{id}/steps/{stepName}/complete?token=...&iat=...
POST /sagas/{id}/steps/{stepName}/fail?token=...&iat=...
```

A participant that runs an asynchronous step reports its result on these routes. It does not build
the URLs itself: the engine sends them with the step's call, in the `X-Saga-Callback-Url` and
`X-Saga-Failure-Url` headers, and their signed token is their only credential. For the bodies, the
`X-Saga-Retryable` header, and the status codes, see
[Wait for slow participants](model-your-workflow.md#wait-for-slow-participants).

The routes exist only when the server has a
[callback secret](configurations.md#participant-callbacks); without one, they answer `404`.

Both routes return `200` with the [saga object](#the-saga-object), including for a report that
repeats an earlier one.

## Check health

```http
GET /health
HEAD /health
```

Returns `200` with `{"status":"UP"}` while the server is running. It needs no credential, so load
balancers and container probes can call it. It does not check the store, so it says the process is
up, not that sagas can run.

## Next steps

- [Run Saga Transactions](run-saga-transactions.md): start and check sagas from Java.
- [ScalarDB Saga Status Codes](scalardb-saga-status-codes.md): every error code, its cause, and
  what to do.
- [ScalarDB Saga Configurations](configurations.md): ports, authentication, and the limits on
  starts.
