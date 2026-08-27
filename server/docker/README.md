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

Mount over `/scalardb-saga/conf` with your own `server.properties`, `definitions/`, and — when you
configure downstream services — `services/`. Start from the template in this directory — every key is
documented there and on `SagaServerConfig`, or on `JwtConfig` and `ApiKeyConfig` for the security
keys of each provider.

Daemon mode is **declarative-only**: a definition naming a code step (`stepClass`) is rejected at
startup, because an operator cannot add classes to this image. Use a declarative service step, or embed
`scalardb-saga-core` in your own application for code steps.

## Configuration

Point the daemon at a different file by overriding the command:

```bash
docker run ... ghcr.io/scalar-labs/scalardb-saga-server:<version> --config /etc/saga/other.properties
```

Secrets do not have to be baked in. In `server.properties`, any value under `scalar.db.saga.*`
accepts `${env:NAME}` or `${file:UTF-8:/path}`, the latter reading a mounted Kubernetes Secret. In
service files the same references work, but `${file:...}` must resolve inside `secrets_root`
(default `/run/secrets`) — service files are reloadable input, so what they may read is confined to
the secrets mounted for that purpose. Keys under plain `scalar.db.*` are resolved by ScalarDB, which
supports `${env:...}` but **not** `${file:...}`.

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

With TLS enabled (see below), probing changes shape:

- `httpGet` probes need `scheme: HTTPS`. The kubelet skips certificate verification, so a
  private-CA or self-signed certificate works unmodified.
- Kubernetes-native `grpc` probes speak **plaintext only** — they cannot probe a TLS listener. A
  gRPC-only deployment therefore loses its probe under TLS: either keep the HTTP transport enabled
  for probing (its `/health` needs no credential), or use an `exec` probe with a
  `grpc_health_probe` binary you provide (`-tls -tls-ca-cert ...`), which this image deliberately
  does not ship.

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
- Serves **plaintext** on both ports by default. Terminating TLS at an ingress or a service mesh
  remains the recommended setup; where no such layer exists, enable native TLS (below).
- The default `noop` security provider authenticates nothing, and the daemon refuses to start under it
  on a non-loopback interface unless `insecure_mode.enabled=true` is set. Configure the `jwt` or
  `apikey` provider instead of setting that flag. TLS does not relax this guard: encrypting the
  transport is confidentiality, not authentication.

## TLS

Native TLS covers **both** transports from one certificate, all-or-nothing (the two listeners share
`host`, so one certificate's SANs cover both). Three keys, documented in the template and on
`SagaServerConfig`:

```properties
scalar.db.saga.server.tls.enabled=true
scalar.db.saga.server.tls.cert_chain_path=/scalardb-saga/tls/tls.crt
scalar.db.saga.server.tls.private_key_path=/scalardb-saga/tls/tls.key
```

Both files are PEM; the paths are read and validated at startup, before either port binds, and every
misconfiguration (missing file, unreadable file, malformed PEM, key/cert mismatch, wrong key format)
fails boot with an error naming the key (the configured value is never echoed — like any value, it
could be a mis-pasted secret). Protocols default to TLS 1.3 and 1.2 with nothing to
tune; cipher policy is each stack's hardened default — Jetty applies its standard exclusions over
the JDK list, gRPC restricts to the HTTP/2-approved suites — rather than a knob. The rare
compliance need is served through `JAVA_OPTS` (e.g. `-Djdk.tls.server.protocols=TLSv1.3`). There is no mTLS, and no plaintext→HTTPS redirect listener:
with TLS on, nothing serves plaintext.

Mounting the material in Kubernetes — the mounted files must be readable by uid `201`, so set an
explicit permissive `defaultMode` (a root-owned `0600` Secret is invisible to the daemon and fails
boot with a permissions hint):

```yaml
volumeMounts:
  - { name: tls, mountPath: /scalardb-saga/tls, readOnly: true }
volumes:
  - name: tls
    secret:
      secretName: saga-server-tls
      defaultMode: 0444
```

The private key must be **unencrypted PKCS#8** (`BEGIN PRIVATE KEY`), RSA or EC. That is *not* what
the common issuers emit by default:

- **cert-manager** emits PKCS#1 unless the Certificate sets `spec.privateKey.encoding: PKCS8`
- **Vault PKI** emits traditional encoding unless the request passes `private_key_format=pkcs8`
- anything else converts with `openssl pkcs8 -topk8 -nocrypt`

Certificate **rotation currently requires a restart** — the files are read once at startup. Hot
reload is planned alongside the configuration hot-reload feature.

Two operational notes:

- **Async callbacks**: with TLS on, `callback.base_url` should be an `https` URL — participants dial
  it, and a plain `http` URL pointing back at this server dies at the handshake on the first async
  step. The daemon warns at startup about that combination.
- **Outbound calls are unaffected**: participant calls (each service file's `base_url`) and JWKS fetches
  verify against the JVM's default trust store. A participant behind a private CA needs that CA in
  the daemon's trust store (`JAVA_OPTS=-Djavax.net.ssl.trustStore=...`); there is deliberately no
  per-service trust knob.

Java clients of the SDK enable TLS with `useTransportSecurity()`; against a private CA, add
`trustCaCertificate(path)` (and `overrideAuthority(name)` when dialing by IP or through a
port-forward). Non-Java REST consumers pass their CA the usual way (`curl --cacert ...`).

## Configuration reload

With `reload.interval_seconds` > 0 (default 30), the daemon re-reads `services_path` and
`definitions_path` on that interval, validates the **complete** candidate set, and only then
applies it — services first, then definition registrations. A set that fails **validation** changes
nothing at all: the previously applied configuration keeps serving, the rejection is logged once at
WARN (repeats at DEBUG until it changes), and the next pass retries. A failure while **applying**
(the store is unreachable, say) can leave part of the set live — the swapped endpoints, or the
definitions registered before the failure; those are named in an `INFO` apply line of their own,
and the next pass retries only what is left. The applied INFO line carries the changed names and a
SHA-256 over the raw file bytes — grep it across replicas to tell a lagging replica from a
rejecting one. Secret **values** never appear in any log line.

Operational notes, learned from how Kubernetes actually delivers files:

- **Mount whole ConfigMaps/Secrets, never `subPath`**: a `subPath` mount pins the file's inode, so
  updates never arrive. The kubelet's atomic-symlink layout (`..data`) is fully supported — the
  daemon re-opens files through their symlink paths every pass.
- **Multi-part credentials (cert + key) belong in ONE Secret volume**: kubelet updates are atomic
  per volume, so splitting a pair across two Secrets invites a torn rotation.
- **Rotate with dual validity**: a rotated downstream credential propagates within kubelet sync
  plus one reload interval, and replicas do not rotate in lockstep — the downstream service must
  accept old and new credentials for at least that window.
- **Retire a saga by deleting its definition file.** A daemon serves the sagas its own definition
  files describe, so removing the file stops new starts: they are refused with `422` /
  `FAILED_PRECONDITION` and error code `SAGA_DEFINITION_NOT_SERVED`. Starts pinned to a specific
  version are refused too — being served is a property of the name, so pinning is not a way around
  it. Sagas already running finish normally and admin recovery on them keeps working, because they
  resume by the version recorded at their start rather than through the start check. Bring the saga
  back by restoring the file.

  The registration itself is never deleted: the store is append-only, so the definition stays there
  for the sagas still running under it. That is why a `404` and this `422` mean different things —
  `404` is a saga nobody ever registered (check the name), `422` is one this daemon is not serving.

  Three things to know:

  - **Retirement is per replica, and applies as each one syncs.** There is no fleet-wide switch;
    starts keep succeeding on replicas whose files have not caught up, for up to one sync period.
  - **A newly ADDED saga can be refused the same way, for the same window.** A replica that has not
    yet seen the new file answers `SAGA_DEFINITION_NOT_SERVED` for it, because from where it stands
    "registered but not in my configuration" looks identical to a retirement. Retry, or wait a sync
    period.
  - **Do not delete a retired saga's services while any of its sagas are still running.** A
    declarative step resolves its service on every call, compensation and recovery included, so a
    saga still in flight would fail to resolve an endpoint mid-way. The daemon warns when you remove
    a service that a vanished definition still names — let the in-flight sagas drain first.

  Deleting the **last** definition file is rejected while the daemon runs: an empty candidate set
  reads as a failed mount rather than a deliberate wind-down. Leave one definition in place, or wind
  the daemon down through a restart.
- **Definition rollback is roll-forward only**: `helm rollback` reverts service files, but
  re-registering an old definition version is an idempotent no-op — the store's latest version
  keeps winning. "Latest" means the version registered most recently, not the highest-numbered one;
  versions are opaque strings and nothing compares them. To revert a definition, register the old
  content as a NEW version, which then serves because it was registered last. The
  reload now says so rather than letting it pass quietly: a definition file naming an
  already-registered version that is not the one serving draws a WARN naming both versions, and the
  pass goes on validating the version that actually serves. It is not rejected — the rollback has
  already failed to take by the time the daemon sees it, and refusing the only configuration it can
  run would stop a replica from starting over a disagreement no restart can settle. Validating what
  serves is also what keeps the service checks honest: removing a service the serving version still
  needs is caught, even while the file names an older version that does not.
- **Changing a service's `base_url` mid-saga is safe only if the endpoints are compatible**: an
  in-flight step finishes against the endpoint it resolved; the saga's next step (or a TCC
  confirm/cancel) resolves the new one.
- **Trust model**: whoever can write the watched directories reshapes the daemon's egress within
  one interval, no restart — treat write access to them as operator-equivalent. `${file:...}`
  references in service files resolve only inside `secrets_root`, and
  `egress.allowed_hosts_ceiling` bounds what any service file can authorize. Set the ceiling if
  service files and `server.properties` have different authors: it is the only egress bound that
  holds no matter what sequence of edits arrives. Without it, the reload rejects a service whose
  `allowed_hosts` goes from restricted to empty — which catches the edit that loses the line, but
  not a service deleted in one interval and recreated allow-all in the next.
- **`max_body_bytes` is capped at 64 MiB per service**, against a 1 MiB default. The coordinator
  buffers a whole response before a step sees it and holds one per in-flight call, so this is the
  daemon's memory rather than the service's. A participant with more to hand back should write it
  somewhere and return a reference.

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
