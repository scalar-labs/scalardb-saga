# Model Your Workflow

This page explains how to model a business workflow as a ScalarDB Saga definition: which steps to
create, what each one calls, how to order them, and how they behave when something fails. It
assumes you have run the [getting started guide](../getting-started/README.md), and uses the same
order-placement flow as its example: charge a payment, reserve stock, and ship the parcel. To start
sagas from an application once a definition is registered, see
[Run Saga Transactions](run-saga-transactions.md).

## Concepts

A **saga** coordinates a workflow that spans several services, each with its own database, without
a distributed transaction across them. ScalarDB Saga calls each service in turn and records every
outcome durably. If a step fails, it undoes the steps that already completed, so the workflow either
finishes or leaves nothing half-applied.

| Term | Meaning |
| --- | --- |
| Definition | The workflow's blueprint: its name, version, mode, and steps. Registered with the server from a JSON or YAML file. |
| Step | One unit of work in a participant service, such as charging a payment. |
| Participant | The service a step calls. A definition refers to it by a logical service name, not a URL. |
| Execution | The call that performs a step's work. |
| Compensation | The call that undoes a step's work after a later failure. |
| Saga context | The values a saga carries from step to step: the input it was started with, plus the values its steps captured from responses. |
| Pivot | The step after which a saga stops undoing work and only moves forward. |

A saga started from a definition moves through these statuses:

| Status | Meaning |
| --- | --- |
| `RUNNING` | Executing its steps. |
| `WAITING` | Parked on an asynchronous step until the participant calls back. |
| `COMPLETED` | Every step succeeded. |
| `COMPENSATING` | Undoing completed steps after a failure. |
| `COMPENSATED` | Every completed step was undone. |
| `ESCALATED` | The engine could not finish the saga on its own, and an operator has to resolve it. |

`COMPLETED`, `COMPENSATED`, and `ESCALATED` are final.

Some of a saga's work happens after the request that started it:

- **Background recovery** runs in every server. It picks up sagas that stopped making progress, such
  as after a server crash, after a step used up its retries, or after an asynchronous step's callback
  timed out, and drives them on. For its settings, see [Recovery](configurations.md#recovery).
- **Parking** is how a saga waits for an asynchronous step, one whose participant finishes later and
  calls back, as described in [Wait for slow participants](#wait-for-slow-participants). The saga is
  stored as `WAITING` and holds no thread until the participant calls back.
- **The grace period** bounds how long recovery keeps trying, counted from the step's first failure,
  or for an asynchronous step from its first park. A saga still stuck after the server's
  `recovery.escalation_grace_period_seconds` (4 hours by default) is escalated, or, for an
  asynchronous step at or before the pivot, compensated.
- **Escalation** moves a saga that the engine gave up on to `ESCALATED`. The engine no longer touches
  it, and an operator resolves it, as described in
  [Operate sagas](run-saga-transactions.md#operate-sagas).

## Choose a mode

A definition runs in one of two modes, set by `mode`.

- **`SAGA` (default):** each step executes immediately, and a failure runs the compensations of the
  completed steps in reverse order. Between a step and its compensation, other transactions can see
  the step's effect: the payment is charged before the shipment is known to succeed. Choose this mode
  when each service can undo its work after the fact, such as refunding a payment or releasing stock.
- **`TCC` (Try-Confirm-Cancel):** every step first makes a *reservation*, such as holding funds
  rather than charging them. Only when every reservation succeeds are they all *confirmed*. If any
  reservation fails, the ones already made are *cancelled*. Choose this mode when your services can
  hold a resource provisionally and you do not want other transactions to see a partial workflow.

TCC needs more from each participant (three calls instead of two, and a notion of a reservation that
expires or is cancelled), so start with `SAGA` mode unless your services already support
reservations.

## Define the steps

A definition is a JSON or YAML file. The file extension (`.json`, `.yaml`, or `.yml`) selects the
format, and the server registers every definition in its definitions directory when it starts. The
directory is set by [`definitions_path`](configurations.md#general). To check your definitions
before you deploy them, run the server with
[`--validate-config`](configurations.md#check-a-configuration-before-you-deploy-it).

```json
{
  "name": "order-saga",
  "version": "1.0",
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
      "name": "reserve",
      "service": "inventory",
      "execution": {
        "method": "POST", "path": "/reserve",
        "jsonBody": { "orderId": "${orderId}", "item": "${item}", "quantity": "${quantity}" },
        "output": { "reservationId": "$.inventory_id" }
      },
      "compensation": {
        "method": "POST", "path": "/release",
        "jsonBody": { "orderId": "${orderId}" }
      }
    },
    {
      "name": "ship",
      "service": "shipping",
      "execution": {
        "method": "POST", "path": "/ship",
        "jsonBody": {
          "orderId": "${orderId}",
          "paymentId": "${paymentId}",
          "reservationId": "${reservationId}"
        }
      },
      "compensation": {
        "method": "POST", "path": "/cancel",
        "jsonBody": { "orderId": "${orderId}" }
      }
    }
  ]
}
```

The first step of the same definition in YAML:

```yaml
name: order-saga
version: "1.0"
mode: SAGA
steps:
  - name: charge
    service: payment
    execution:
      method: POST
      path: /charge
      jsonBody:
        orderId: ${orderId}
        amount: ${amount}
      output:
        paymentId: $.payment_id
    compensation:
      method: POST
      path: /refund
      jsonBody:
        orderId: ${orderId}
```

Keep the following rules in mind when you define steps:

- **One step per participant action.** A step calls exactly one service, once per attempt. If a
  service has to do two things that can fail independently, model them as two steps so that each
  can be undone on its own.
- **Every step in `SAGA` mode has both `execution` and `compensation`.** This is required even for a
  step that, by its position after the pivot, will never be compensated.
- **Step names are unique within a definition.** They appear in the saga's timeline, and
  participants receive them to identify the call.
- **`service` is a logical name.** The server resolves it to a base URL through a service file named
  `<service>.properties` in its services directory, which also holds per-service settings such as
  headers and allowed hosts. Keeping URLs out of the definition lets the same definition run in
  every environment. For the service file format, see
  [Service files](configurations.md#service-files).
- **Unknown fields are rejected.** A misspelled field fails registration instead of being ignored.
- **Values such as `POST`, `SAGA`, and `MIXED` are case-sensitive.** Write them in capitals, as
  shown on this page.

### Describe a call

Each phase of a step (`execution`, `compensation`, and in TCC mode `reservation`, `confirmation`,
and `cancellation`) is an HTTP call:

| Field | Description | Default |
| --- | --- | --- |
| `method` | `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`. `GET` and `DELETE` must not have a body. | `POST` |
| `path` | The path appended to the service's base URL. Can contain `${...}`. Required. | |
| `query` | Query parameters, as a map of name to value. Values can contain `${...}`. | None |
| `jsonBody` | The request body, as a map of field to value, sent as a JSON object. Numbers, booleans, `null`, and nested objects and arrays are sent as written; numbers are kept exactly, including decimals. Strings, at any depth, can contain `${...}`. | None |
| `stringBody` | A raw request body template, sent as is. Use it for a non-JSON body. Can contain `${...}`. Cannot be combined with `jsonBody`. | None |
| `contentType` | The `Content-Type` of the request body. | `application/json` |
| `output` | Values to capture from the response into the saga context, as a map of context key to expression. | None |
| `async` | `true` if the participant completes the call later through a callback. Forward phases only. See [Wait for slow participants](#wait-for-slow-participants). | `false` |
| `callbackTimeoutMillis` | How long to wait for the callback of an `async` call. `0` means no limit other than the saga's timeout. Only valid with `async`. | `0` |

## Pass data between steps

A saga starts with an input, a map of values supplied by the application that starts it. The input
and every value captured by a step's `output` make up the saga context, which later steps read.

- **Read** a value with `${key}` in `path`, `query`, `jsonBody`, or `stringBody`. A value that is
  exactly one `${key}` keeps the type of the context value, so a number stays a number in the JSON
  body. A value with `${key}` embedded in other text becomes a string. A value substituted into
  `path` is encoded as a single path segment, so it cannot change the URL's structure. A map or list
  in the context can only be used as a whole `jsonBody` value, written as exactly one `${key}`.
  Inserting one into `path`, `query`, `stringBody`, or a string with other text fails the step
  before its call is sent.
- **Capture** a value with `output`. `$.field` reads a field from a JSON response, and `$.a.b` reads
  a nested one; array elements cannot be addressed. `$body` captures the whole response body as a
  string, which also works for a non-JSON response. A response that lacks a field named in `output`,
  or has `null` there, fails the step even though its status was `2xx`. At or before the pivot, the
  step is compensated too, because the participant may have applied the change.

A `${key}` with no value in the context fails the step before its call is sent. Because the call was
never sent, the engine knows the step changed nothing: at or before the pivot, it compensates only
the steps before it.

A captured value overwrites a context value with the same key, including one from the input. Give
captured values names that describe where they came from, such as `paymentId` rather than `id`.

The saga ID is not a context value. Participants receive it in the `X-Saga-Id` header of every
call, as described in [Make participants safe to retry](#make-participants-safe-to-retry).

## Design compensations

A compensation is the most important call in a step, because it runs when something has already gone
wrong. Design it with these rules:

- **Compensate the failed step, too.** When a step fails, the engine cannot always tell whether the
  participant applied the change: the service can commit and then fail to answer, or the response
  can be lost. So the engine compensates the failed step as well as the completed ones, unless the
  call was never sent. A compensation for a change that was never made must therefore succeed and do
  nothing; cancelling a shipment that was never created must return success.
- **Key the compensation on data that existed before the step ran.** A failed step captured no
  output, so its compensation cannot rely on its own output. Use the saga input (such as `orderId`)
  or the output of an earlier step. The server rejects a definition whose compensation reads its own
  step's output.
- **Make it succeed eventually.** A compensation that fails is retried, first in place and then by
  background recovery. One that still fails after the recovery grace period sends the saga to
  `ESCALATED`, which needs an operator. A compensation should only fail for a reason that goes away,
  such as the service being down, never for a business rule.
- **Do not capture data from it.** A compensation's output is not added to the saga context, so
  do not give it an `output` mapping: a response that lacks a field named there fails the
  compensation. A compensation must also answer within the request; a `202 Accepted` fails it.

## Order the steps

The order of steps decides how much has to be undone when a step fails, and whether it can be undone
at all. Steps fall into three kinds:

- **Compensatable steps** can be undone, such as charging a payment that can be refunded.
- **The pivot** is the point of no return, such as handing the parcel to the carrier. Once it
  succeeds, the saga has to finish.
- **Retriable steps** come after the pivot. They cannot be skipped once the pivot succeeds, so they
  must be able to succeed eventually, such as notifying the customer.

Order your steps compensatable first, then the pivot, then retriable, and follow these guidelines:

- **Put the step most likely to fail first.** If a step that often rejects requests runs early, a
  rejection has fewer steps to undo. Validating an order, for example, should come before charging
  for it.
- **Put steps that are hard to undo last.** Sending an email or calling a partner without a cancel
  API cannot be compensated cleanly, so place it after the pivot, where it is only ever retried.
- **Keep the forward path short.** Every step adds latency to the saga and work to its compensation.

Tell the engine how your steps divide with `recoveryStrategy`:

| `recoveryStrategy` | When a step fails | Use when |
| --- | --- | --- |
| `BACKWARD` (default) | Every completed step is compensated, and the saga ends `COMPENSATED`. | Every step can be undone. |
| `FORWARD` | Nothing is compensated. The saga stays `RUNNING`, and background recovery retries the failed step until it succeeds or the grace period passes. | No step can be undone, but every step will succeed eventually. |
| `MIXED` | A failure at or before the pivot compensates the steps up to that point. A failure after the pivot is retried forward, as in `FORWARD`. | The workflow has a point of no return in the middle. |

With `MIXED`, mark exactly one step with `"pivot": true`. The pivot cannot be the first step (that is
`FORWARD`) or the last (that is `BACKWARD`), and `BACKWARD` and `FORWARD` definitions must not mark
one.

The following version of `order-saga` adds a `notify` step after `ship`, and makes handing the
parcel to the carrier the pivot. It is version `2.0` because a registered version cannot change. The
example is abbreviated: `charge`, `reserve`, and `ship` keep the fields they have in version `1.0`,
and the `notification` service needs its own service file.

```yaml
name: order-saga
version: "2.0"
mode: SAGA
recoveryStrategy: MIXED
steps:
  - name: charge
    # service, execution, and compensation as in version 1.0
  - name: reserve
    # service, execution, and compensation as in version 1.0
  - name: ship
    pivot: true
    # service, execution, and compensation as in version 1.0
  - name: notify
    service: notification
    execution:
      method: POST
      path: /notify
      jsonBody:
        orderId: ${orderId}
    compensation:
      method: POST
      path: /notify/cancel
      jsonBody:
        orderId: ${orderId}
```

A failure in `charge`, `reserve`, or `ship` compensates the steps up to it. A failure in `notify`
is only retried, because the parcel is already on its way. If it still fails after the
[grace period](#concepts), the saga is escalated, so a forward-only step should fail only for
reasons that go away.

## Handle failures and retries

### How the engine classifies a failure

When a participant responds, the status code decides what happens next:

| Response | Treated as |
| --- | --- |
| `2xx` | Success |
| `408`, `429`, and every `5xx` | Transient: retried according to the step's retry policy |
| Any other `4xx`, and every `3xx` | Permanent: the step fails without a retry. Redirects are not followed. |
| Connection error or timeout before a response | Transient |

A participant can override the classification of a failed response by setting the
`X-Saga-Retryable` response header to `true` or `false`. For example, a `409` that is safe to retry
can return `X-Saga-Retryable: true`. A `2xx` response is always a success.

A call to a host outside the service's `allowed_hosts`, and a response larger than its
`max_body_bytes`, fail permanently too. For these settings, see
[Service files](configurations.md#service-files).

Use `4xx` codes for business rejections, such as insufficient funds, so the saga fails fast and
compensates. Use `5xx` codes for problems that go away, so the step is retried.

### Set a retry policy

A retry policy controls how many times a step's call is attempted and how long the engine waits
between attempts. The interval between attempts grows exponentially, and each wait is a random time
between half and all of the current interval, so that sagas retrying at the same time do not call
the participant in sync.

```json
{
  "name": "ship",
  "service": "shipping",
  "retryPolicy": {
    "maxAttempts": 5,
    "initialIntervalMillis": 500,
    "backoffMultiplier": 2.0,
    "maxIntervalMillis": 10000
  },
  ...
}
```

| Field | Description | Default |
| --- | --- | --- |
| `maxAttempts` | Total attempts, including the first. At least `1`. | `3` |
| `initialIntervalMillis` | The interval before the second attempt. Greater than `0`. | `1000` |
| `backoffMultiplier` | The factor the interval grows by after each attempt. At least `1.0`. | `2.0` |
| `maxIntervalMillis` | The longest interval between two attempts. At least `initialIntervalMillis`. | `30000` |

Set `defaultRetryPolicy` at the top level of a definition to apply a policy to every step that does
not set its own `retryPolicy`.

A retry policy applies to a step's forward call: `execution` in `SAGA` mode and `reservation` in
`TCC` mode. Other phases use fixed policies, with the same random wait:

| Phase | Attempts | Wait |
| --- | --- | --- |
| `compensation`, `cancellation` | 3 | 1 second, doubling up to 10 seconds |
| `confirmation` | 10 | 500 milliseconds, doubling up to 60 seconds |

When these attempts run out, the saga does not give up: background recovery tries again later,
within the [grace period](#concepts).

### Set timeouts

| Field | Where | Description | Default |
| --- | --- | --- | --- |
| `timeoutMillis` | Definition | The longest one run of a saga may take, checked before each step. A run is the start, or a resume after a callback, a re-issued call, or recovery, and each run starts the timeout again, so it does not bound the saga's total time. | `0`, which falls back to the server's [`default_saga_timeout_millis`](configurations.md#general) (no limit unless set) |
| `timeoutMillis` | Step | The longest a step's forward call may take, across all of its attempts together. The step's compensation is bounded by the same time. | `0`, meaning only the saga's timeout applies. Without a step or saga timeout, each attempt still times out after 30 seconds and is retried. |

A step whose timeout expires fails without further attempts, and is then handled like any other
failure of that step. A saga whose timeout expires before or at the pivot is compensated. A saga
past the pivot is never rolled back by a timeout, because its remaining steps can only move forward.

Set a step timeout longer than the participant's slowest normal response. A step that times out may
still complete in the participant after the engine has given up on it, which the compensation then
has to undo.

## Make participants safe to retry

The engine retries calls and, after a server crash, re-runs a step whose outcome it had not yet
recorded. A participant can therefore receive the same call more than once, and must handle that:

- **Make every call idempotent.** Use the `X-Saga-Id` and `X-Saga-Step` request headers, which stay
  the same across retries and recovery, to detect a call you have already processed, and return the
  original result. `X-Saga-Step` carries the step's name for both its execution and its
  compensation, so key on the endpoint as well. In `TCC` mode it carries `<step>.reserve`,
  `<step>.confirm`, or `<step>.cancel`.
- **Return a status code that matches the failure,** as described in
  [How the engine classifies a failure](#how-the-engine-classifies-a-failure).
- **Accept a compensation for a call you never received,** and return success.
- **Accept a compensation that arrives before the call it undoes.** Under retries and recovery, a
  delayed execution can reach the participant after its compensation. Record the compensation so
  that the late execution does nothing.

## Wait for slow participants

Some participants cannot finish a step within one HTTP request, such as a payment that needs a
person's approval. Mark the call `async`:

```json
"execution": {
  "method": "POST", "path": "/approve",
  "jsonBody": { "orderId": "${orderId}" },
  "async": true,
  "callbackTimeoutMillis": 3600000
}
```

The participant answers `202 Accepted` right away, and finishes the work later. Any other `2xx`
answer completes the step at once, with its `output` read from the response, so a participant can also
finish quickly when it can. A `202` from a call that is not `async` fails the step permanently.

The engine sends a
callback URL in the `X-Saga-Callback-Url` request header and a failure URL in `X-Saga-Failure-Url`,
parks the saga in `WAITING`, and frees its resources. When the work is done, the participant sends
a `POST` to the callback URL, and the saga resumes:

- **The body is the step's output.** A JSON object in the callback body is merged into the saga
  context as is, so later steps read its fields with `${...}`; the call's `output` mapping does not
  apply to it. An empty body completes the step with no output.
- **The URL is the credential.** It carries a signed token, so the participant needs no other
  credential to call it, and the URL must be kept as secret as one. A token older than the server's
  [`callback.max_age_seconds`](configurations.md#participant-callbacks) is rejected. That check is
  off by default; when you set it, make it longer than your longest `callbackTimeoutMillis`.
- **A repeated callback is harmless.** A callback for a step that has already resumed returns
  `200` with the saga's current state, so the participant can retry its callback until it gets an
  answer.

If the work fails, the participant sends a `POST` to the failure URL instead:

```http
POST {failure URL}
X-Saga-Retryable: true

{"error": "approver unavailable"}
```

- **Without `X-Saga-Retryable: true`, the step fails at once.** It is compensated if it is at or
  before the pivot, and the saga is `ESCALATED` if it is after. The failed step's own compensation
  runs too, since the participant may have done part of the work.
- **With `X-Saga-Retryable: true`, the engine sends the call again,** as it does after a callback
  timeout, within the step's `maxAttempts` and the recovery grace period. When those run out, the
  step fails as above.
- **The body is optional.** Its `error` text is flattened to one line, cut to 1024 characters, and
  recorded with what happens next, the re-issue or the step's failure, where [`getSagaDetail`](run-saga-transactions.md#check-a-saga) shows
  it. A missing body, or a missing or `null` `error`, records only that the participant reported a
  failure.
- **The same rules apply as for the callback.** The URL carries the same kind of signed token, and a
  report for a step that has already resumed or failed returns `200` with the saga's current state.

Both URLs can also answer:

| Status | Meaning | Retry? |
| --- | --- | --- |
| `503` with `Retry-After` | The report arrived before the saga finished parking, such as a callback sent right after the `202`. | Yes, after `Retry-After`. |
| `401` | The token is missing, invalid, or too old. | No |
| `400` | The body is malformed, `error` is not a string, or the step is not the one the URL names. | No |
| `404` | No saga has this ID. | No |

If no callback arrives within `callbackTimeoutMillis` (or the saga's timeout, if that is sooner), the
engine sends the call again, up to the step's `maxAttempts` (10 for a `confirmation`) and within the
grace period. When those run out, the step fails: it is compensated if it is at or before the
pivot, and the saga is `ESCALATED` if it is after.

With neither `callbackTimeoutMillis` nor a saga timeout set, which is the default, a parked step has
no deadline. If its callback never arrives, the saga stays `WAITING` and is never escalated, so set
one of them for every asynchronous step.

Only forward phases (`execution`, `reservation`, and `confirmation`) can be `async`. Compensation and
cancellation calls always complete within the request. Asynchronous steps also need the server's
[participant callback settings](configurations.md#participant-callbacks); without them, a
definition with an `async` call is rejected at registration.

## Model a TCC workflow

In `TCC` mode, each step has three calls instead of two:

| Phase | Purpose | Retried with |
| --- | --- | --- |
| `reservation` | Hold the resource, such as authorizing a payment without capturing it. | The step's retry policy |
| `confirmation` | Make the reservation permanent. | A fixed policy of 10 attempts, then background recovery |
| `cancellation` | Release the reservation. | A fixed policy of 3 attempts, then background recovery |

```json
{
  "name": "order-tcc",
  "mode": "TCC",
  "steps": [
    {
      "name": "payment",
      "service": "payment",
      "reservation": {
        "method": "POST", "path": "/reserve",
        "jsonBody": { "orderId": "${orderId}", "amount": "${amount}" }
      },
      "confirmation": {
        "method": "POST", "path": "/confirm",
        "jsonBody": { "orderId": "${orderId}" }
      },
      "cancellation": {
        "method": "POST", "path": "/cancel",
        "jsonBody": { "orderId": "${orderId}" }
      }
    }
  ]
}
```

The engine makes every reservation first, in step order. If one fails, it cancels the reservations
already made, in reverse order, including the one that failed. If every reservation succeeds, it
confirms them in step order. A confirmation is past the point of no return, so it is only ever
retried, never cancelled.

The recovery behavior of TCC is fixed, so a `TCC` definition must not set `recoveryStrategy` or
`pivot`. As with compensations in `SAGA` mode, a cancellation must succeed for a reservation that was
never made, and must not read its own step's output. A confirmation must succeed eventually, so make
sure a reservation stays valid until it is confirmed or cancelled.

## Change a definition

A definition is identified by its `name` and `version`. Once a version is registered, it cannot
change, because sagas already running under it rely on it, including after a server restart.

- **To change a workflow, change `version`.** The server refuses a definition whose content changed
  under an existing version.
- **New sagas use the newest registered version.** An application can also start a saga on a
  specific version.
- **Running sagas keep the version they started with,** so an old version stays usable until its
  sagas finish.
- **To roll back a change, register the earlier content under a new version,** such as `2.1`. A
  version that already exists cannot take the content back, since its content cannot change.
- **To retire a workflow, delete every definition file for its name.** The server stops accepting new
  sagas for it, while the running ones finish. Deleting only the newest version's file does not
  bring back an older version, because new sagas use the newest version ever registered.

With [configuration reload](configurations.md#reload) enabled, the server applies added, changed, and
deleted definition files without a restart. For how reload behaves in a deployment, see
[server/docker/README.md](../server/docker/README.md#configuration-reload).

The `name` and `version` cannot contain `:`.

## Definition reference

### Definition fields

| Field | Description | Default |
| --- | --- | --- |
| `name` | The workflow's name, used to start sagas. Required. | |
| `version` | The definition's version. | `"1.0"` |
| `mode` | `SAGA` or `TCC`. | `SAGA` |
| `recoveryStrategy` | `BACKWARD`, `FORWARD`, or `MIXED`. `SAGA` mode only. | `BACKWARD` |
| `timeoutMillis` | The saga timeout. `0` uses the server default. | `0` |
| `defaultRetryPolicy` | The retry policy for steps that set none. | 3 attempts, 1 second doubling up to 30 seconds |
| `steps` | The steps, in execution order. At least one. Required. | |

### Step fields

| Field | Description | Default |
| --- | --- | --- |
| `name` | The step's name, unique within the definition. Required. | |
| `service` | The logical name of the participant service. Required. | |
| `transport` | How the step's calls are made. `HTTP` is the only value. | `HTTP` |
| `execution`, `compensation` | The step's calls in `SAGA` mode. Both required. | |
| `reservation`, `confirmation`, `cancellation` | The step's calls in `TCC` mode. All required. | |
| `retryPolicy` | The retry policy for the forward call. | The definition's `defaultRetryPolicy` |
| `timeoutMillis` | The step timeout. `0` means only the saga's timeout applies. | `0` |
| `pivot` | `true` on the pivot step of a `MIXED` definition. | `false` |

For the fields of a call, see [Describe a call](#describe-a-call).

> [!NOTE]
> When you embed the engine in a Java application instead of running the server, a step can also be
> a Java class: set `stepClass` to the class name instead of `service` and the phase calls. The
> server runs only service steps.

## Next steps

- [Run Saga Transactions](run-saga-transactions.md): start sagas from an application and act on
  their outcome.
- [ScalarDB Saga Configurations](configurations.md): configure the server that runs your
  definitions, including service files and callbacks.
