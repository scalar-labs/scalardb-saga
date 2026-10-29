# Command cheat sheet

Copy-paste commands for driving the getting-started stack — handy in the control pane that
[`tmux-demo.sh`](tmux-demo.sh) opens. Everything talks to the saga server on `12080`, except the
paced launches, which go through the dashboard proxy on `12090`. Every saga started here appears
live in [the dashboard](http://localhost:12090).

## Start sagas

Synchronous — the response carries the final status:

```console
# success flow -> COMPLETED
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga","input":{"orderId":"o-1001","amount":"100","item":"widget","quantity":"2"}}'

# ship fails with 422 -> compensation walks backward -> COMPENSATED
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga-failing","input":{"orderId":"o-1002","amount":"100","item":"widget","quantity":"2"}}'

# ship answers 503 twice, the step's retry policy rides it out -> COMPLETED
# (order-saga-retry is the same scenario with a tighter backoff)
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga-flaky","input":{"orderId":"o-1003","amount":"100","item":"widget","quantity":"2"}}'

# ship always answers 503 -> the three retries exhaust -> compensation walks backward
# -> COMPENSATED (compare with order-saga-pivot below: same exhaustion, opposite resolution)
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga-retry-failing","input":{"orderId":"o-1009","amount":"100","item":"widget","quantity":"2"}}'

# reserve is the pivot; ship exhausts its 2 attempts -> the saga stays RUNNING with nothing
# rolled back, and background recovery rolls it forward to COMPLETED 20-30 seconds later
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga-pivot","input":{"orderId":"o-1004","amount":"100","item":"widget","quantity":"2"}}'

# missing input: reserve fails resolving ${item} before its call is sent -> charge is
# compensated, reserve is skipped (known not committed) -> COMPENSATED
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga","input":{"orderId":"o-1005","amount":"100"}}'

# TCC: reserve all three services, then confirm all three -> COMPLETED
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-tcc","input":{"orderId":"o-1011","amount":"100","item":"widget","quantity":"2"}}'

# TCC: shipping's reservation is rejected (422) -> the held reservations are cancelled -> COMPENSATED
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-tcc-try-fail","input":{"orderId":"o-1012","amount":"100","item":"widget","quantity":"2"}}'

# TCC: shipping's confirmation answers 503 twice; a confirmation only rolls forward, so the
# retries ride it out -> COMPLETED (never cancelled)
curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-tcc-confirm-retry","input":{"orderId":"o-1013","amount":"100","item":"widget","quantity":"2"}}'
```

Variants of starting:

```console
# async: get the sagaId back immediately (202), then poll for the outcome
curl -X POST 'localhost:12080/sagas?async=true' -H 'Content-Type: application/json' -d '{"sagaName":"order-saga","input":{"orderId":"o-1006","amount":"100","item":"widget","quantity":"2"}}'

# your own saga ID (idempotent: retrying with the same ID cannot start a second saga; 409 on reuse)
curl -X PUT localhost:12080/sagas/order-1001 -H 'Content-Type: application/json' -d '{"sagaName":"order-saga","input":{"orderId":"o-1007","amount":"100","item":"widget","quantity":"2"}}'

# paced via the dashboard proxy: each step (and each compensation) takes ~paceMs, so the log
# panes and the dashboard show the saga progressing step by step
curl -X POST localhost:12090/api/sagas -H 'Content-Type: application/json' -d '{"sagaName":"order-saga","input":{"orderId":"o-1008","amount":"100","item":"widget","quantity":"2"},"paceMs":1500}'
```

## Inspect and list

```console
curl localhost:12080/sagas/<SAGA_ID>              # current state
curl localhost:12080/sagas/<SAGA_ID>/detail       # state + the durable event timeline

curl 'localhost:12080/sagas?pageSize=100'         # list (pageSize max 1000)
curl 'localhost:12080/sagas?status=RUNNING'       # filter by status: RUNNING, COMPLETED,
                                                  #   COMPENSATING, COMPENSATED, ESCALATED, WAITING
curl 'localhost:12080/sagas?updatedAfter=2026-08-07T00:00:00Z&pageSize=50'
```

A listing response carries `nextPageToken` when there is another page; pass it back verbatim as
`&pageToken=...`.

## Operator verbs

For a stuck saga (all of these require a reason, which lands on the timeline attributed to the
operator):

```console
curl -X POST localhost:12080/sagas/<SAGA_ID>/recover \
  -H 'Content-Type: application/json' -d '{"reason":"manual nudge"}'

curl -X POST localhost:12080/sagas/<SAGA_ID>/reset \
  -H 'Content-Type: application/json' -d '{"reason":"participant fixed"}'

curl -X POST localhost:12080/sagas/<SAGA_ID>/force-complete \
  -H 'Content-Type: application/json' -d '{"reason":"nothing left to undo"}'

curl -X POST localhost:12080/admin/reset-escalated \
  -H 'Content-Type: application/json' -d '{"reason":"bulk sweep"}'
```

`reset` and `force-complete` apply to `ESCALATED` sagas: `reset` retries the compensation,
`force-complete` marks the saga done when the operator has cleaned up by hand.
