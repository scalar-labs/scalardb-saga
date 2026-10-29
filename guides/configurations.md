# ScalarDB Saga Configurations

This page describes the parameters you can set to configure the ScalarDB Saga server. For how the
settings fit into a deployment, see [server/docker/README.md](../server/docker/README.md).

## Configure the server

The server reads one Java properties file, given with `--config` (or its alias `--properties`). The
container image starts the server with `--config /scalardb-saga/conf/server.properties`.

The file holds two kinds of keys:

- **`scalar.db.saga.*` keys,** described on this page, configure ScalarDB Saga.
- **`scalar.db.*` keys** configure the ScalarDB connection to the database where the server keeps
  saga state. For these, see [ScalarDB Core Configurations](https://scalardb.scalar-labs.com/docs/latest/configurations).
  ScalarDB Saga requires the Consensus Commit transaction manager.

The following rules apply to every `scalar.db.saga.server.*` key unless its description says
otherwise:

- **An unknown key stops the server.** A misspelled key fails startup instead of being ignored.
  This also applies to `scalar.db.saga.store.*` keys.
- **A blank value means the default.** Values are trimmed.
- **A boolean is `true` or `false`,** in any case. Other values, such as `yes` or `1`, are
  rejected.
- **Every key is read once, at startup.** To change one, restart the server. The files these keys
  point to, such as definitions, service files, and certificates, can be reloaded without a restart;
  see [Reload](#reload).

### Keep secrets out of the file

Any `scalar.db.saga.*` value can be read from somewhere else when the server starts:

| Syntax | Reads |
| --- | --- |
| `${env:NAME}` | The environment variable `NAME`. An undefined variable is left in the value as is. |
| `${file:UTF-8:/path}` | The contents of the file at `/path`, such as a mounted Kubernetes Secret. A file that cannot be read stops the server. |

`${env:NAME:-fallback}` gives a fallback value, and `$$` writes a literal `$`. These references
apply to `scalar.db.saga.*` keys only.

### Check a configuration before you deploy it

Run the server with `--validate-config` to check a configuration without starting the server:

```console
scalardb-saga-server --config server.properties --validate-config
```

It validates the `scalar.db.saga.server.*` keys, the definitions, and the service files, and opens no
database connection, binds no port, and calls no service, so it is safe to run in CI. It does not
check `scalar.db.*` or `scalar.db.saga.store.*` keys: a misspelled store key is caught only when the
server starts. It exits with one of these codes:

| Code | Meaning |
| --- | --- |
| `0` | The configuration is valid. |
| `1` | The configuration is invalid. The problems found are listed on standard output. A bad server setting stops the check at that setting, so fix it and run the check again. |
| `2` | The command line is wrong, such as an unknown option. |
| `3` | The check did not happen: the properties file could not be read, or the command failed while running. An unreadable definition or service file is reported as a problem, with `1`. |

A CI step that gates a deployment should require `0`: `2` and `3` mean the check did not run, so
they prove nothing about the configuration. Test for `1` only in a test that deliberately supplies
an invalid configuration. For what it can and cannot check, see
[server/docker/README.md](../server/docker/README.md#checking-a-configuration-before-it-ships).

## General

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.host` | The network interface that both the REST and gRPC listeners bind to. | `0.0.0.0` |
| `scalar.db.saga.server.owner_id` | The identity this server stamps on the sagas it recovers. Each running server must have a unique value, of up to 128 letters, digits, `.`, `_`, and `-`. Setting it to `${env:HOSTNAME}` keeps it stable across restarts of the same pod. | A random UUID for each process |
| `scalar.db.saga.server.definitions_path` | The saga definition file, or the directory of definition files, to register. The server does not start without at least one definition. For the file format, see [Model Your Workflow](model-your-workflow.md). | `/scalardb-saga/conf/definitions`, if that directory exists |
| `scalar.db.saga.server.default_saga_timeout_millis` | The saga timeout for definitions that set none, in milliseconds. `0` means no timeout. It applies each time a saga is driven, so a change also reaches running sagas when they next move. | `0` |

## Admission control

These keys protect the server from more saga starts than it can handle. Both are off by default.
A blank value is rejected instead of meaning the default, so that a protection cannot be switched
off by accident.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.max_concurrent_saga_starts` | The most sagas that can be starting at once. A start beyond it is refused with `DB-SAGA-20006` (HTTP `503`, gRPC `UNAVAILABLE`), and nothing is persisted. Recovery, resumed sagas, and operator actions are never refused. `0` means no limit. | `0` |
| `scalar.db.saga.server.max_start_requests_per_minute` | The most saga starts each caller can make per minute. A start beyond it is refused with `DB-SAGA-20004` (HTTP `429`, gRPC `RESOURCE_EXHAUSTED`). The caller is the authenticated principal; without authentication, all callers share one limit. `0` means no limit. | `0` |

## REST

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.http.enabled` | Whether to serve the REST API. REST and gRPC cannot both be disabled. | `true` |
| `scalar.db.saga.server.http.port` | The REST port. `0` picks a free port. Must differ from the gRPC port. | `12080` |
| `scalar.db.saga.server.http.max_threads` | The most REST requests dispatched at once. Requests run on virtual threads, so this does not limit how many are in flight or how many sagas run. | `200` |
| `scalar.db.saga.server.http.min_threads` | The fewest threads kept for accepting REST requests. Must not exceed `max_threads`. | `8` |
| `scalar.db.saga.server.http.max_queued_requests` | The most REST requests that can wait to be accepted. | Twice `max_threads` |

## gRPC

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.grpc.enabled` | Whether to serve the gRPC API. REST and gRPC cannot both be disabled. | `true` |
| `scalar.db.saga.server.grpc.port` | The gRPC port. `0` picks a free port. Must differ from the REST port. | `12051` |
| `scalar.db.saga.server.grpc.max_inbound_metadata_bytes` | The largest total size of a call's metadata, in bytes. Raise it if your JWTs are larger. | `8192` |

The largest gRPC message the server accepts equals
[`scalar.db.saga.store.max_event_payload_bytes`](#store).

## TLS

These keys enable TLS on both the REST and gRPC listeners, with one certificate. TLS is all or
nothing: the server cannot serve one transport with TLS and the other without it.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.tls.enabled` | Whether to serve with TLS. Must be set explicitly whenever a certificate or key is configured or present at its default path, so a mounted certificate never takes effect, or is ignored, without your say. A blank value is rejected. | `false` when no certificate or key is present |
| `scalar.db.saga.server.tls.cert_chain_path` | The server's certificate chain, in PEM format, leaf first. | `/scalardb-saga/conf/tls/server/tls.crt`, if that file exists |
| `scalar.db.saga.server.tls.private_key_path` | The private key for the certificate: an unencrypted PKCS#8 PEM file (`BEGIN PRIVATE KEY`), RSA or EC. | `/scalardb-saga/conf/tls/server/tls.key`, if that file exists |

The server accepts TLS 1.3 and 1.2. It re-reads the certificate and key on every
[reload](#reload) pass, so a rotated certificate takes effect without a restart.

## Security

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.security.provider` | How callers are authenticated: `noop` (no authentication), `apikey`, or `jwt`. | `noop` |
| `scalar.db.saga.server.security.insecure_mode.enabled` | Allows `noop` on an interface other than loopback. Without it, the server refuses to start unauthenticated where other machines can reach it. Use it only on a network nobody else can reach. | `false` |

Callers need a role for each operation: `saga:read` to check sagas, `saga:write` to start them, and
`saga:admin` for operator actions. Each role includes the ones before it.

### API key

Used when `security.provider` is `apikey`. Define one or more keys, each under a name of your
choice, `<name>`:

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.security.apikey.header` | The request header callers send the key in. | `X-API-Key` |
| `scalar.db.saga.server.security.apikey.key.<name>.secret` | The key's value. Must be a secret reference, such as `${file:UTF-8:/run/secrets/writer-key}`; a key written inline is rejected. Unlike other values, it is not trimmed, so write the secret file without a trailing newline: a key that ends in one never matches the header. | Required, for at least one key |
| `scalar.db.saga.server.security.apikey.key.<name>.roles` | The key's roles, separated by commas, such as `saga:write`. | Required |
| `scalar.db.saga.server.security.apikey.key.<name>.principal` | The identity recorded for the key's operator actions. | `<name>` |

### JWT

Used when `security.provider` is `jwt`.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.security.jwt.jwks_url` | The JWKS endpoint that publishes the keys to verify tokens with. Must be `https`, except on loopback. | Required |
| `scalar.db.saga.server.security.jwt.issuer` | The `iss` value tokens must carry. | Required |
| `scalar.db.saga.server.security.jwt.audience` | The `aud` value tokens must carry, which identifies this server. | Required |
| `scalar.db.saga.server.security.jwt.token_type` | The `typ` header value tokens must carry. Unset means no check. | None |
| `scalar.db.saga.server.security.jwt.principal_claim` | The claim that holds the caller's identity. | `sub` |
| `scalar.db.saga.server.security.jwt.roles_claim` | The claim that holds the caller's roles, as a space-separated string or an array. Values other than the three roles are ignored. | `scope` |
| `scalar.db.saga.server.security.jwt.connect_timeout_millis` | The connect timeout for fetching the JWKS, in milliseconds. | `2000` |
| `scalar.db.saga.server.security.jwt.read_timeout_millis` | The read timeout for fetching the JWKS, in milliseconds. | `2000` |

## Synchronous starts

A synchronous start waits for the saga to finish before answering. These keys bound how long the
server waits; when the bound passes, the server answers with the saga's current state while the
saga keeps running. The Java client SDK keeps waiting across that answer on its own; see
[Limit how long to wait](run-saga-transactions.md#limit-how-long-to-wait).

The same bound paces the SDK's completion callbacks, which wait on a saga one window at a time,
while the server checks the saga several times within each window. A longer window means fewer
calls and store reads for each saga being watched, but a later callback for a saga that another
server finishes; a shorter one means the reverse. See
[Get notified when a saga ends](run-saga-transactions.md#get-notified-when-a-saga-ends).

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.sync.max_wait_millis` | The longest the server waits in one request, in milliseconds. | `60000` |

## Saga detail

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.detail.max_timeline_events` | The most timeline events a saga's detail returns. A longer timeline is cut to the newest events and marked as truncated. | `1000` |

## Recovery

Background recovery finishes sagas whose server stopped while running them, and retries failures
that the engine could not finish in place. Every server runs it.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.recovery.staleness_threshold_millis` | How long a saga can go without progress before recovery takes it over, in milliseconds. Set it above your longest step timeout. It is also roughly how long a saga waits to resume after its server crashes. | `60000` |
| `scalar.db.saga.server.recovery.interval_seconds` | How often recovery looks for sagas to take over, in seconds. | `30` |
| `scalar.db.saga.server.recovery.escalation_grace_period_seconds` | How long recovery keeps retrying a stuck saga before it gives up, in seconds. A step whose retries keep failing, or a compensation that keeps failing, is escalated to `ESCALATED` for an operator. An asynchronous step that keeps timing out is failed: at or before the pivot the saga compensates, and after it the saga is escalated. The period runs from the first step failure, or for an asynchronous step from its first park. | `14400` (4 hours) |
| `scalar.db.saga.server.recovery.max_recoveries_per_sweep` | The most sagas one recovery scan takes over. Each pass runs two scans, one for abandoned sagas and one for asynchronous steps whose wait expired. Keep it well above `200`. | `1000` |
| `scalar.db.saga.server.recovery.max_concurrent_recoveries` | The most sagas recovery drives at once. | `10` |

## Retention

Retention deletes finished sagas after a while, so the database does not grow without bound.
`ESCALATED` sagas are never deleted, because an operator has to resolve them first.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.retention.period_seconds` | How long a finished saga is kept, in seconds. After that, it can no longer be checked. | `604800` (7 days) |
| `scalar.db.saga.server.retention.interval_seconds` | How often retention runs, in seconds. | `60` |
| `scalar.db.saga.server.retention.max_purges_per_pass` | The most sagas one retention pass deletes. | `10000` |
| `scalar.db.saga.server.retention.max_concurrent_purges` | The most sagas retention deletes at once. | `10` |

## Participant callbacks

Asynchronous steps need these keys: they let participants report a step's completion or failure
back to the server, as described in
[Wait for slow participants](model-your-workflow.md#wait-for-slow-participants). They do not affect
the completion callbacks that the Java client SDK delivers to an application; those are paced by
[Synchronous starts](#synchronous-starts).

Set both `base_url` and `secret`, or neither. Without them, a definition with an `async` call is
rejected at registration. Participants call back on the REST API, so `base_url` must reach this
server's REST port, and `http.enabled` must stay `true`.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.callback.base_url` | The URL participants reach this server at, used to build callback URLs. With TLS enabled, use an `https` URL. | None |
| `scalar.db.saga.server.callback.secret` | The secret callback URLs are signed with. Use a secret reference. Unlike other values, it is not trimmed. | None |
| `scalar.db.saga.server.callback.max_age_seconds` | How long a callback URL stays valid, in seconds. Set it longer than your longest callback timeout. `0` means no limit. A blank value is rejected. | `0` |

## Services and outbound calls

A step's `service` names a participant service. The server finds each service's settings in a
service file.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.services_path` | The directory of service files. | `/scalardb-saga/conf/services`, if that directory exists |
| `scalar.db.saga.server.secrets_root` | The directory that `${file:...}` references in service files must stay inside. It does not apply to the properties file. | `/run/secrets` |
| `scalar.db.saga.server.egress.allowed_hosts_ceiling` | The hosts, separated by commas, that service files may allow. When set, every service file must set `allowed_hosts` to some of these hosts. A blank value is rejected. | None |
| `scalar.db.saga.server.egress.ca_cert_path` | Extra CA certificates, in PEM format, to trust for outbound HTTPS calls to participants and to the JWKS endpoint, in addition to the JVM's defaults. | `/scalardb-saga/conf/tls/egress/ca.crt`, if that file exists |

### Service files

Each service has one file in `services_path`, named `<service>.properties`, where `<service>` is the
name definitions use. The keys in a service file have no prefix:

```properties
base_url=https://payment.internal:8443
allowed_hosts=payment.internal
header.Authorization=${file:UTF-8:/run/secrets/payment/authorization}
```

| Name | Description | Default |
| --- | --- | --- |
| `base_url` | The service's base URL, `http` or `https`. A step's `path` is appended to it. | Required |
| `allowed_hosts` | The hosts, separated by commas, that calls to this service may reach. A call to any other host fails before it is sent. Unset means any host. | None |
| `max_body_bytes` | The largest request or response body for this service, in bytes, up to 64 MiB. | `1048576` (1 MiB) |
| `header.<name>` | A header sent with every call to this service, such as the credential the service requires. The names `X-Saga-Id`, `X-Saga-Step`, `X-Saga-Callback-Url`, and `X-Saga-Failure-Url` are reserved, and so are `Connection`, `Content-Length`, `Expect`, `Host`, and `Upgrade`, which the HTTP client sets itself. Two names that differ only in case are rejected. | None |

Values in a service file can use `${file:...}`, which must resolve inside `secrets_root`, and
`${env:...}`, which logs a warning because an environment variable cannot be rotated without a
restart. Unknown keys are rejected, and so is a blank `base_url`, `allowed_hosts`, or `header.*`
value. Service names can use up to 128 letters, digits, `.`, `_`, and `-`.

## Reload

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.reload.interval_seconds` | How often the server re-reads its definitions, service files, TLS certificate and key, and egress CA certificates, in seconds. `0` disables reload, so those files are read only at startup. | `30` |

Each pass validates the definitions and service files as a whole, and applies them only if all of
them are valid. The properties file itself is never reloaded. For how reload behaves in a
deployment, see [server/docker/README.md](../server/docker/README.md#configuration-reload).

## Shutdown

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.server.shutdown.mode` | What a stopping server waits for: `WAIT_CURRENT_STEP` stops each saga after its running step, and `WAIT_ALL_SAGAS` waits for sagas to finish. Sagas left unfinished are resumed by recovery on another server. | `WAIT_CURRENT_STEP` |
| `scalar.db.saga.server.shutdown.timeout_millis` | The budget for the drain, in milliseconds. Shutdown takes a few seconds more, so set the termination grace period above it, as described in [server/docker/README.md](../server/docker/README.md#graceful-shutdown). `0` skips the waits, and hands running sagas to recovery at once. | `30000` |

## Store

These keys configure how the server keeps saga state through ScalarDB. Saga state is stored in the
`saga` namespace.

| Name | Description | Default |
| --- | --- | --- |
| `scalar.db.saga.store.max_event_payload_bytes` | The largest data a single saga event can hold, such as a saga's input or a step's output, in bytes. It also sets the largest gRPC message the server accepts. `0` means no limit. | `1048576` (1 MiB) |
| `scalar.db.saga.store.transaction_max_attempts` | How many times a store transaction is attempted, including the first, before it fails. Conflicts, an unknown commit status, and other transaction failures are attempted again. At least `1`. | `3` |
| `scalar.db.saga.store.scalardb.num_buckets` | How many partitions the sweep table, which recovery, retention, and saga listing scan, is spread across. Cannot change once the table holds data. | `16` |
| `scalar.db.saga.store.scalardb.creation_options.<name>` | A ScalarDB creation option, passed as is when the server creates its namespace and tables on first start, such as `replication-factor` for Cassandra. It has no effect on tables that already exist. | None |

> [!WARNING]
> On Cassandra, ScalarDB creates the namespace with `SimpleStrategy` and a replication factor of `1`
> unless told otherwise. In production, set the `replication-factor` creation option before the
> server's first start.

## Environment variables

The container image reads these environment variables:

| Name | Description | Default |
| --- | --- | --- |
| `SCALAR_DB_SAGA_LOG_LEVEL` | The log level of the server, including gRPC. | `INFO` |
| `JAVA_OPTS` | JVM options, added after the image's own, so they override them. | None |
| `SCALARDB_SAGA_SERVER_OPTS` | JVM options, added after `JAVA_OPTS`. | None |

## Next steps

- [Model Your Workflow](model-your-workflow.md): write the definitions the server registers.
- [Run Saga Transactions](run-saga-transactions.md): start sagas from an application.
