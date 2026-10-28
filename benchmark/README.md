# ScalarDB Saga benchmark

A load-generation harness that drives the `SagaOrchestrator` interface, so the identical workload
runs against either implementation:

| `--mode` | Implementation under test | Infrastructure |
|---|---|---|
| `embedded` | `DefaultSagaOrchestrator` in-process | ScalarDB store only (throwaway SQLite by default) |
| `server` | `GrpcSagaOrchestratorClient` → a real `SagaServer` booted **in this process** on ephemeral loopback ports | store + a built-in fake HTTP participant |
| `grpc` | `GrpcSagaOrchestratorClient` → an external daemon at `--target` | yours (register the definition first — see `--print-definition`) |

The benchmark saga is `--steps` sequential no-op steps (`step-0..N-1`), each optionally sleeping
`--step-delay-ms` to emulate participant latency. Embedded mode runs them as class steps
(`BenchmarkStep`); the server modes run them as declarative service steps against the built-in
participant.

## Running

```
./gradlew :benchmark:installDist
benchmark/build/install/scalardb-saga-benchmark/bin/scalardb-saga-benchmark --mode embedded \
    --concurrency 64 --requests 5000 --steps 3
```

(or `./gradlew :benchmark:run --args="--mode embedded ..."`.)

Typical experiments:

```bash
# Baseline: embedded engine, synchronous starts.
scalardb-saga-benchmark --mode embedded --start-mode sync --concurrency 32 --requests 2000

# The concurrency collapse: slow participants + high concurrency. Once in-flight sagas
# outlive the recovery staleness threshold (default 60s), watch duplicates and
# SagaConcurrentModificationException counts climb.
scalardb-saga-benchmark --mode embedded --start-mode async-fire --concurrency 256 \
    --requests 5000 --step-delay-ms 2000 --drain-seconds 300

# Same threshold reached quickly, without waiting a minute: shrink the recovery window.
# Works in embedded and server modes alike.
scalardb-saga-benchmark --mode embedded --start-mode async-fire --concurrency 128 \
    --requests 2000 --step-delay-ms 500 --recovery-staleness-threshold-ms 5000 --recovery-interval-seconds 2

# Full daemon round-trip over gRPC, in one process.
scalardb-saga-benchmark --mode server --start-mode sync --concurrency 100 --requests 2000 \
    --step-delay-ms 100

# Against a real deployment.
scalardb-saga-benchmark --print-definition --steps 3 > bench.json   # register on the daemon
scalardb-saga-benchmark --mode grpc --target saga-host:12051 --op-timeout-ms 60000 \
    --concurrency 100 --duration-seconds 120
```

`--properties FILE` points the embedded/server store at a real ScalarDB backend;
`-D key=value` (repeatable) layers extra properties on top — store keys in both modes, plus the
`scalar.db.saga.server.*` keys in server mode (e.g.
`-D scalar.db.saga.server.recovery.timeout_millis=5000`,
`-D scalar.db.saga.server.sync.timeout_millis=10000`).

## Reading the output

Progress lines print every `--report-interval-seconds`:

```
[ 15.0s] issued=812 resolved=800 inFlight=12 oldestInFlight=3.1s
[ 20.0s] issued=812 resolved=800 inFlight=12 oldestInFlight=8.1s  ** NO PROGRESS for 5.0s **
```

`** NO PROGRESS **` intervals are the hang, seen from the client. If nothing completes for
`--stall-abort-seconds` (default 180) the run aborts, reports how many workers were still blocked,
and exits with code 3.

The final report separates:

- **start-call latency** (accepting the request) from **end-to-end latency** (to a terminal state);
- **terminal by op / drained / stillPending** — `stillPending > 0` after the drain means sagas
  never finished;
- **statuses** — `COMPENSATED`/`ESCALATED` on a no-failure workload means the system rolled back
  work it should have completed;
- **errors by exception class** — `SagaConcurrentModificationException` under pure load is the
  recovery sweeper fighting live drives;
- **duplicate step executions** — the same saga executing a step twice: a saga was re-driven by
  recovery while (or after) its original drive ran. Embedded mode counts inside `BenchmarkStep`;
  server mode counts on the participant via the `X-Saga-Id` header.

## Diagnosing virtual-thread pinning

The engine executes sagas on virtual threads; blocking JDBC calls made inside `synchronized`
(e.g. the SQLite driver) pin carrier threads and can stall the whole scheduler on Java 21. Run
with:

```
JAVA_OPTS="-Djdk.tracePinnedThreads=full" scalardb-saga-benchmark --mode embedded ...
```

and correlate pinning stack traces with `** NO PROGRESS **` intervals. Note the trace only fires
when a pinned thread *parks*; a virtual thread blocked on `monitorenter` or spinning inside a
native JDBC call (e.g. SQLite's `busy_timeout` wait in `NativeDB.step`) pins its carrier
silently. To see those, dump the hung JVM with virtual-thread stacks included:

```
jcmd <pid> Thread.print                                  # platform threads + carrier ownership
jcmd <pid> Thread.dump_to_file -format=json threads.json # every virtual thread's stack
```

A freeze where `Thread.print` shows carriers "Carrying virtual thread #N" with no pinned-park
trace, and the JSON shows those threads inside native JDBC frames, is carrier exhaustion: the
lock's holder is an unmounted virtual thread that cannot get a carrier back.
