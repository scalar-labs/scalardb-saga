# ScalarDB Saga daemon image

`ghcr.io/scalar-labs/scalardb-saga-daemon` runs the saga engine as a service, exposing it over REST
(`8080`) and gRPC (`50051`). Built for `linux/amd64` and `linux/arm64`.

Build it yourself with `./gradlew :daemon:dockerBuild`: that assembles the context from the
`Dockerfile` in this directory plus the daemon distribution, and loads a single-architecture image
tagged with the project version.

## Running it

The image ships a configuration *template* and does not start as-is: the daemon needs a reachable
ScalarDB database, at least one saga definition, and — since the template selects the `jwt` security
provider — the JWKS URL, issuer, and audience of your IdP. It refuses to start if any is missing: a
healthy process that can run no saga, or that lets anyone start one, is worse than a failure at boot.
Every value the template cannot guess is marked `REPLACE_ME`.

```bash
docker run --rm \
  --publish 8080:8080 --publish 50051:50051 \
  --volume "$PWD/conf:/scalardb-saga/conf:ro" \
  --env SCALAR_DB_USERNAME=saga --env SCALAR_DB_PASSWORD=... \
  ghcr.io/scalar-labs/scalardb-saga-daemon:1.0.0
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
docker run ... ghcr.io/scalar-labs/scalardb-saga-daemon:1.0.0 /etc/saga/other.properties
```

Secrets do not have to be baked in. Any value under `scalar.db.saga.*` accepts `${env:NAME}` or
`${file:UTF-8:/path}`, the latter reading a mounted Kubernetes Secret. Keys under plain `scalar.db.*`
are resolved by ScalarDB, which supports `${env:...}` but **not** `${file:...}`.

| Variable | Effect |
| --- | --- |
| `SCALAR_DB_SAGA_LOG_LEVEL` | Root log level; defaults to `INFO` |
| `JAVA_OPTS` | Appended after the image's own JVM flags, so it overrides them |
| `SCALARDB_SAGA_DAEMON_OPTS` | Same, applied after `JAVA_OPTS` |

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
    port: 8080
readinessProbe:
  grpc:
    port: 50051
```

`GET /health` and the `grpc.health.v1.Health` service are both reachable without a credential, by
design — a probe cannot present one. Every other route requires authentication.

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

The JVM is PID 1 and receives `SIGTERM` directly, which triggers a drain of in-flight sagas rather than
dropping them. Allow time for it: the gRPC drain window is `max(30s, sync_max_wait_millis + 5s)` — 65s
at the default `sync_max_wait_millis` of 60s — and the engine then drains running sagas. A
`terminationGracePeriodSeconds` below that will `SIGKILL` the daemon mid-drain.
