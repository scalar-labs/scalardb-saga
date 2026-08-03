# ScalarDB Saga server image

`ghcr.io/scalar-labs/scalardb-saga-server` runs the saga engine as a service, exposing it over REST
(`12080`) and gRPC (`12051`). Built for `linux/amd64` and `linux/arm64`.

Build it yourself with `./gradlew :server:dockerBuild`: that assembles the context from the
`Dockerfile` in this directory plus the server distribution, and loads a single-architecture image
tagged with the project version. See [RELEASING.md](../../RELEASING.md) for how the published image
is built and signed, and how to verify its signature.

## Running it

The image ships a configuration *template* and does not start as-is: the daemon needs a reachable
ScalarDB database, at least one saga definition, and a security provider — uncomment either the
`jwt` or the `apikey` block in the template and fill it in. It refuses to start if any is missing: a
healthy process that can run no saga, or that lets anyone start one, is worse than a failure at boot.
Every value the template cannot guess is marked `REPLACE_ME`. Fill those in as well: a `REPLACE_ME`
is a syntactically valid value, so it is accepted at startup and fails only later, once a caller
first presents a credential.

```bash
docker run --rm \
  --publish 12080:12080 --publish 12051:12051 \
  --volume "$PWD/conf:/scalardb-saga/conf:ro" \
  --env SCALAR_DB_USERNAME=saga --env SCALAR_DB_PASSWORD=... \
  ghcr.io/scalar-labs/scalardb-saga-server:<version>
```

Mount over `/scalardb-saga/conf` with your own `server.properties` and `definitions/`. Start from the
template in this directory — every key is documented there and on `SagaServerConfig`, or on
`JwtConfig` and `ApiKeyConfig` for the security keys of each provider.

Daemon mode is **declarative-only**: a definition naming a code step (`stepClass`) is rejected at
startup, because an operator cannot add classes to this image. Use a declarative service step, or embed
`scalardb-saga-core` in your own application for code steps.

## Configuration

Point the daemon at a different file by overriding the command:

```bash
docker run ... ghcr.io/scalar-labs/scalardb-saga-server:<version> --config /etc/saga/other.properties
```

Secrets do not have to be baked in. Any value under `scalar.db.saga.*` accepts `${env:NAME}` or
`${file:UTF-8:/path}`, the latter reading a mounted Kubernetes Secret. Keys under plain `scalar.db.*`
are resolved by ScalarDB, which supports `${env:...}` but **not** `${file:...}`.

| Variable | Effect |
| --- | --- |
| `SCALAR_DB_SAGA_LOG_LEVEL` | Root log level; defaults to `INFO`. Covers gRPC too — its `java.util.logging` output is bridged into Logback, so everything the process emits shares one format and one level |
| `JAVA_OPTS` | Appended after the image's own JVM flags, so it overrides them |
| `SCALARDB_SAGA_SERVER_OPTS` | Same, applied after `JAVA_OPTS` |

Setting the level to `DEBUG` turns gRPC up as well, which is the point of bridging it — Logback's
`DEBUG` reaches JUL as `FINE`, and `io.grpc` logs per RPC at that level, so expect substantial output
from a busy daemon. gRPC's `FINER` and `FINEST` records stay declined until `TRACE`, which maps to JUL
`FINEST`.

To raise the daemon's own level without gRPC following it, replace the configuration wholesale and set
levels per logger: `JAVA_OPTS=-Dlogback.configurationFile=/etc/saga/logback.xml`. Keep the
`LevelChangePropagator` `contextListener` in any replacement. Per-logger levels at or above `INFO` work
without it, but JUL then keeps its own `INFO` default: it builds a `LogRecord` for every disabled gRPC
call before the bridge can discard it, and a `<logger name="io.grpc" level="DEBUG"/>` has no effect at
all, because JUL declines those records before the bridge ever sees them.

The image already sets `-XX:MaxRAMPercentage=75.0` (heap sized from the cgroup limit, not the host) and
`-XX:+ExitOnOutOfMemoryError` (die on heap exhaustion so the orchestrator restarts it, rather than hold
saga leases while making no progress). Override either through `JAVA_OPTS`.

## Health checks

The image carries no `grpc_health_probe` binary: Kubernetes has had native gRPC probes since 1.24 (GA
in 1.27), and each transport carries its own check, so whichever one you run stays probeable.

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 12080
readinessProbe:
  grpc:
    port: 12051
```

`GET /health` and the `grpc.health.v1.Health` service are both reachable without a credential, by
design — a probe cannot present one. Every other route is governed by the configured security
provider: `jwt` and `apikey` authenticate them, `noop` does not, which is why the daemon refuses to
start under `noop` on a non-loopback interface.

## Security

- Runs as uid/gid `201:201`, so `runAsNonRoot` admission passes with no passwd lookup.
- `readOnlyRootFilesystem: true` works, but the daemon needs a writable **and executable** temp
  directory. Several dependencies — the SQLite and other JDBC drivers, and Netty's epoll transport —
  ship their native libraries inside their jars and extract them to `java.io.tmpdir` at startup before
  loading them. A `noexec` mount there fails the load with `NativeLibraryNotFoundException`, and the
  daemon exits before serving. A Kubernetes `emptyDir` at `/tmp` is executable by default and works;
  Docker's `--tmpfs /tmp` defaults to `noexec` and does not, so use `--tmpfs /tmp:rw,exec`.

  ```yaml
  securityContext:
    readOnlyRootFilesystem: true
    runAsNonRoot: true
  volumeMounts:
    - { name: tmp, mountPath: /tmp }
  volumes:
    - { name: tmp, emptyDir: {} }
  ```
- Serves **plaintext** on both ports — there is no TLS listener. Terminate TLS at an ingress or a
  service mesh.
- The default `noop` security provider authenticates nothing, and the daemon refuses to start under it
  on a non-loopback interface unless `insecure_mode.enabled=true` is set. Configure the `jwt` or
  `apikey` provider instead of setting that flag.

## Graceful shutdown

The JVM is PID 1 and receives `SIGTERM` directly, which triggers a drain rather than dropping
in-flight work. The daemon drains in two windows, one after the other, so budget for their sum:

- **gRPC call drain** — `max(30s, sync.max_wait_millis + 5s)`, so 65s at the default
  `sync.max_wait_millis` of 60s. It tracks that setting: raise it to `300000` and this window
  becomes 305s.
- **Saga engine drain** — `shutdown.timeout_millis`, 30s by default. Under the default
  `shutdown.mode=WAIT_CURRENT_STEP` the engine only finishes each running step and leaves the saga
  for recovery, so this window is rarely spent in full; `WAIT_ALL_SAGAS` instead waits for in-flight
  sagas to reach a terminal state, which needs a window sized to your longest saga. Setting it to
  `0` skips this window entirely, cancelling in-flight work at once and leaving all of it to the
  recovery scan.

At defaults that totals 95s. Set `terminationGracePeriodSeconds` above the sum; below it, the daemon
is `SIGKILL`ed mid-drain.

Being cut short costs latency, not integrity: whatever was interrupted is reclaimed by the recovery
scan on the next boot.
