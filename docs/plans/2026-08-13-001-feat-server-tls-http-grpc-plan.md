---
title: "feat: Support TLS in SagaServer for HTTP and gRPC"
type: feat
status: active
date: 2026-08-13
origin: docs/brainstorms/2026-08-13-server-tls-brainstorm.md
---

# feat: Support TLS in SagaServer for HTTP and gRPC

## Overview

Add native server-side TLS to SagaServer on both transports — HTTPS on Javalin/Jetty, TLS on grpc-java/Netty — configured from the properties file as one shared block of PEM file paths, using JDK SSL (no new native libraries). Ship the matching client-side trust surface in the Java 8 SDK (custom CA + authority override), mirroring ScalarDB Cluster. Infrastructure TLS termination (mesh/ingress/LB) remains the recommended default; native TLS serves deployments with nothing to terminate at.

All WHAT decisions were settled in the brainstorm (see docs/brainstorms/2026-08-13-server-tls-brainstorm.md); this plan is the HOW, incorporating the SpecFlow gap analysis.

## Problem Statement

SagaServer serves plaintext on both ports by design (`server/docker/README.md:110-111`; design doc "Transport Security"). That position assumes a mesh/ingress exists to terminate TLS. Deployments without one — bare VMs, docker-compose, small clusters — currently have no way to encrypt traffic to the coordinator, even though API keys and JWT bearers transit these connections. The client SDK already exposes `useTransportSecurity()` with nothing on the server to point it at.

## Proposed Solution

### Config surface (server)

Three new keys in the `scalar.db.saga.server.` namespace, names mirroring ScalarDB Cluster (`ClusterNodeConfig`: `tls.enabled`, `tls.cert_chain_path`, `tls.private_key_path`):

| Key | Default | Meaning |
|---|---|---|
| `scalar.db.saga.server.tls.enabled` | `false` | Serve TLS on **both** enabled transports (all-or-nothing) |
| `scalar.db.saga.server.tls.cert_chain_path` | — | Path to PEM certificate chain (leaf first) |
| `scalar.db.saga.server.tls.private_key_path` | — | Path to **unencrypted PKCS#8** PEM private key |

Paths, never contents: no `${file:}`-style inlining, no keystore support, no inline-PEM key. Paths keep future cert hot reload possible (re-read the same path). *(Amended post-review: path values are **not** redaction-safe to name in errors after all — a secret reference mis-pasted onto a path key resolves to the secret itself — so errors and logs name config keys only; see the error-matrix amendment below.)*

**Validation truth table** (every cell gets a `SagaServerConfigTest` case):

| `tls.enabled` | paths | Result |
|---|---|---|
| `true` | both set | TLS on; file-level checks at server start |
| `true` | either missing or blank | **Fatal at parse** — names the missing key(s) (paired-keys precedent: callback URL/secret) |
| explicit `false` | any set | OK, paths ignored — supports the operational "toggle TLS off, leave Secret mounted" move |
| absent | any set | **Fatal at parse** — "`tls.cert_chain_path` is set but `tls.enabled` is not; set `tls.enabled=true` to serve TLS or `tls.enabled=false` to disable it explicitly." Closes the forgot-the-switch silent-plaintext hole. |
| blank | — | **Fatal** — `tls.enabled` sits on the reject-blank line (its default leaves a protection off), via `requireNonBlankIfSet` |

Two-phase validation: pairing/blank/known-key rules at config-parse time in `SagaServerConfig`; file existence, readability, PEM parse, and key/cert match at server-start time in the TLS material component — **before either port binds**, so a bad cert can never strand a listening plaintext socket or a half-up server.

### One TLS material component (server-internal)

`TlsMaterial` (working name), package-private in `com.scalar.db.saga.server`: loads and parses the two PEM files **once**, and both transports consume the same parsed `PrivateKey` + `X509Certificate[]`. This is the single-parser guarantee (acceptance can't diverge between stacks), the seam where future hot reload swaps in (brainstorm decision 8), and the one place the misconfiguration error matrix lives.

Parsing is JDK-only — **no BouncyCastle at runtime**:
- Cert chain: `CertificateFactory.getInstance("X.509").generateCertificates(...)` (handles concatenated PEM). Gotchas: an input with no certificates returns an *empty collection*, not an exception — check it; pre-extract the `BEGIN/END CERTIFICATE` blocks so openssl's `subject=`/`issuer=` prose lines between blocks can't trip the parser.
- Private key: strip `BEGIN/END PRIVATE KEY` armor, decode with `Base64.getMimeDecoder()` (plain `getDecoder()` throws on line wraps), `PKCS8EncodedKeySpec`, try `KeyFactory` RSA → EC.
- **PKCS#8 only, RSA and EC keys only** (this pins the brainstorm's ambiguity). PKCS#8 is what `openssl genpkey` emits — but **not** what cert-manager or Vault emit by default: cert-manager's `privateKey.encoding` defaults to `PKCS1` and Vault to traditional encoding, so the error text and docs must carry the one-line issuer-side fixes (`spec.privateKey.encoding: PKCS8`; `private_key_format=pkcs8`) alongside `openssl pkcs8 -topk8 -nocrypt`. `BEGIN RSA PRIVATE KEY` (PKCS#1), `BEGIN EC PRIVATE KEY` (SEC1), and `BEGIN ENCRYPTED PRIVATE KEY` each get a dedicated, actionable error (see matrix) rather than BouncyCastle support. Ed25519 is excluded deliberately: Netty's own PKCS#8 parser (used by the gRPC path) tries only RSA/DSA/EC, so accepting Ed25519 here would pass our validation and then fail inside Netty. Diverges from Cluster's parser in exchange for zero new runtime dependency; revisit only on real operator demand.
- **Invariant: `TlsMaterial`'s acceptance set is a strict subset of what both stacks accept**, enforced by validating before either stack touches the files and proven by integration test 1. This also makes behavior classpath-independent: with BouncyCastle on the classpath, Netty silently starts accepting PKCS#1 — but our validator has already rejected it (BC stays test-only, see Phase 2).

**Startup error matrix** — every message names the config key, never a byte of file content **and never the configured path value** (amended post-review: a secret reference mis-pasted onto a path key resolves to the secret itself, so path values fall under the PR #95 redaction doctrine like any other value; causes are dropped, so each class needs its own message):

| Failure | Message must convey |
|---|---|
| file missing | which key, which path, not found |
| file unreadable | distinct from missing; hint at permissions/uid — the root-owned `0600` K8s Secret mount at uid 201 is the #1 real-world trip |
| malformed PEM | which file failed to parse as PEM (no snippet) |
| PKCS#1 key | "convert with `openssl pkcs8 -topk8 -nocrypt`" |
| encrypted key | unencrypted keys only; decrypt and re-mount |
| wrong content type (cert given as key / vice versa) | which key points at the wrong kind of material |
| key/cert mismatch | **eagerly checked** by comparing the leaf cert's public key against the private key — neither Jetty nor Netty checks this at startup; without it, it surfaces as runtime handshake failures |

Doctrine ruling (recorded here deliberately): cert **metadata** (validity dates) may be logged — it is public material presented to every client on handshake, not a resolved config value. `TlsMaterial` WARNs at startup if the cert is expired or not yet valid, dates only. File contents and key material remain absolutely unloggable.

### gRPC wiring

In `buildGrpcServer` (`server/src/main/java/com/scalar/db/saga/server/SagaServer.java:164-178`), when TLS is enabled — after `TlsMaterial` has validated the files:

```java
builder.useTransportSecurity(tls.certChainPemStream(), tls.privateKeyPemStream());
```

*(Amended post-review: the `InputStream` overload fed from `TlsMaterial`'s validated material
re-encoded as PEM, not the `File` overload — Netty then never re-reads the files, closing the
rotation-during-boot window where gRPC could serve unvetted bytes or diverge from Jetty.)* This is
the **stable** API (`GrpcSslContexts` is still `@ExperimentalApi` in grpc 1.82.1). Verified against grpc 1.82.1 / Netty 4.1.136.Final: the JDK provider is selected automatically and deterministically when tcnative is absent (reflective ALPN probe, always true on Java 21), the fallback logs nothing above DEBUG/FINE, ALPN h2 is configured automatically, and the defaults land on TLS 1.3 + 1.2 (brainstorm decision 5 — BoringSSL later is a dependency-line change). Netty re-parses the PEM files here; that is safe because `TlsMaterial` validated first and its acceptance set is a strict subset of Netty's (invariant above). Follows the `applyGrpcTransportSettings` seam pattern (SagaServer.java:189-193) with its mock-builder test.

### HTTP wiring

In `createHttpServer` (SagaServer.java:359-372), when TLS is enabled: build an in-memory PKCS12 `KeyStore` from the parsed material with a throwaway generated `char[]` password — and hand that same password to `SslContextFactory.Server.setKeyStorePassword(...)`, because Jetty initializes its `KeyManagerFactory` with it (key retrieval fails on `null`). Register the connector via `cfg.jetty.addConnector((server, httpConfig) -> ...)` with the `SslConnectionFactory("http/1.1")` → `HttpConnectionFactory` chain.

Semantics verified in Javalin 6.7.0 source (Jetty 11.0.25 underneath):

- **Registering any custom connector suppresses Javalin's default plaintext connector** — the default is created only when the connector list is empty, so TLS-on cannot leak a plaintext listener by construction (still pinned by integration test).
- **`app.start(host, port)` silently ignores its arguments once a custom connector exists.** `SagaServer.start()` (SagaServer.java:488) must call `app.start()` with no args on the TLS path, with host/port set on the connector we build.
- `app.port()` reads `connectors[0]`; with our single custom connector it keeps working (used by `SagaServer.port()`, SagaServer.java:518-520), including ephemeral port 0.
- Copy the callback's `HttpConfiguration` (`new HttpConfiguration(base)`) before mutating it — Javalin hands the same instance to every connector callback.
- Add `new SecureRequestCustomizer(false)` to the copied config: it populates `request.isSecure()`/the `https` scheme, and the `false` disables `sniHostCheck`, whose default `true` returns **HTTP 400 "Invalid SNI"** to clients that dial by IP — exactly what K8s probes, the smoke test's `curl https://127.0.0.1/...`, and port-forwards do.
- Jetty 11.0.25 has `SslContextFactory.reload(Consumer)`, confirming the future hot-reload seam.

The community `ssl-plugin` (`io.javalin.community.ssl:ssl-plugin:6.7.0`) was considered and rejected: it drags in SSLContext-Kickstart, **BouncyCastle (required by its PEM module), Conscrypt (a native uber-jar), and Jetty http2/ALPN modules** — a large tree to buy PEM parsing and reload we build in ~one class. What it would add (encrypted-key support, one-call reload, h2-over-TLS on the REST side, mTLS helpers) is all out of scope or deferred.

### Runtime policy

- **Handshake-failure logging: DEBUG or below, decided up front.** Plaintext clients hitting a TLS port, LB TCP health checks, and bare-TCP probes are handshake-EOF generators; per-connection WARNs would flood logs and break the smoke test's log-cleanliness assertion. Pinned by integration test.
- **One INFO line at startup**: `TLS enabled for HTTP and gRPC` — gives operators boot-time confirmation and the smoke test a positive assertion. *(Amended post-review: the line carries no cert path — configured values, paths included, are never logged; see the redaction amendment above.)*
- **`callback.base_url` scheme check**: with `tls.enabled=true` and an `http://` callback base URL on a non-loopback host, every async completion would dial plaintext into a TLS port and die at the first async step in production. Startup **WARN** (not fatal — the callback endpoint may legitimately live behind separate plaintext infra).
- **TLS does not relax the noop-on-non-loopback guard** (`ensureSecureBindingOrAcknowledged`, SagaServer.java:433-453): TLS is confidentiality, the guard is authentication. No code change — pinned with a test so nobody "improves" the guard later.

### Client SDK (Java 8)

`GrpcClientSupport.openChannel` (client/.../GrpcClientSupport.java:33-46) uses generic `ManagedChannelBuilder`, which has **no trust-manager hook** — the CA path forces a channel-construction change:

- With a CA configured: `Grpc.newChannelBuilder(target, TlsChannelCredentials.newBuilder().trustManager(caFile).build())`. Verified on grpc 1.82.1: `TlsChannelCredentials` is **stable** (no `@ExperimentalApi`; the builder's `trustManager(File)` exists since grpc 1.37.0), reads the file **eagerly at builder time** (`IOException`), and accepts concatenated PEM. Wrap so the failure at `build()` names the path (never contents).
- Without a CA: existing paths unchanged (`useTransportSecurity()` / `usePlaintext()`); the ALPN fail-fast guard (GrpcClientSupport.java:101-111) **must be carried onto the new credentials path**. Java 8 nuance for the docs: TLS 1.3 arrives only at 8u261/8u272 — older 8u252+ clients negotiate TLS 1.2, which the server keeps enabled, so nothing breaks.

Builder surface, added to **both** `GrpcSagaOrchestratorClient.Builder` and `GrpcSagaAdminClient.Builder` (they share `openChannel`):

- `trustCaCertificate(Path caPemPath)` — custom CA; **requires TLS**: set together with `usePlaintext()` → `IllegalStateException` at `build()` (Cluster precedent: trust settings only meaningful under TLS; failing beats silently ignoring).
- `overrideAuthority(String authority)` — legitimate with or without TLS (dialing by IP/port-forward while the cert names the Service DNS name); plain delegation to the channel builder.

Client-side failures (CA file missing/malformed) fail at `build()`, not first RPC — lazy failure surfaces as an opaque `UNAVAILABLE` deep in application code. SDK stays Java 8-clean; no new client dependencies.

## System-Wide Impact

- **Interaction graph**: TLS wraps the transport below everything — security provider, rate limiter, routes, and interceptors are unchanged and unaware. The gRPC health service stays credential-free and unintercepted; under TLS it is reachable only through a TLS handshake (see probe guidance below). `start()`'s partial-failure path (SagaServer.java:494-504) already `close()`s on either bind failure; TLS validation errors thrown before binding ride the same path.
- **Error propagation**: all TLS misconfiguration errors are `IllegalArgumentException`-family at startup, surfaced through `SagaServerCommand`'s existing exit path — which prints cause chains, hence the no-causes/no-content rule on every new message. Runtime handshake errors never reach application code; they die in the transport at DEBUG.
- **State lifecycle risks**: none — TLS holds no persistent state. The only lifecycle rule is validate-before-bind.
- **API surface parity**: both transports gain TLS together by design (single shared block). The two wire mappers and error-code surfaces are untouched. Client parity: both SDK clients get identical new builder methods.
- **Ports/probes**: K8s `httpGet` probes need `scheme: HTTPS` (kubelet skips cert verification, so self-signed works); **K8s-native gRPC probes do not support TLS at all**, so a gRPC-only TLS deployment loses its probe — docs must steer that topology to an exec probe or to keeping HTTP enabled for probing.

## Implementation Phases

Tests are co-located with each phase (CLAUDE.md), named `methodName_condition_expectedResult`, Arrange/Act/Assert.

### Phase 1 — Config surface

- `SagaServerConfig`: three key constants, `KNOWN_KEYS` entries, parsing (`Optional<Path>` getters), the full truth table in `validateCombinations`, `tls.enabled` on the reject-blank list.
- `SagaServerConfigTest`: every truth-table cell, including the blank-path edges where the two doctrines compose — a blank path is unset, so blank `cert_chain_path` + absent `enabled` is a no-op (not the forgot-the-switch fatal), while blank path + `enabled=true` reports as the missing-pair error; redaction assertions on every new error message (no resolved values).
- Update `server/docker/conf/server.properties` template + `SagaServerConfig` class javadoc.

### Phase 2 — TLS material component

- `TlsMaterial`: PEM parsing (JDK-only, PKCS#8-only), full error matrix, eager key/cert public-key match, expired/not-yet-valid WARN (dates only), in-memory PKCS12 builder for Jetty.
- `TlsMaterialTest`: every matrix row; a **redaction sweep** asserting no error message or log line contains `BEGIN`, base64 runs, or key bytes; RSA + ECDSA keys (cert-manager commonly emits ECDSA).
- Test certs **generated at test runtime** (never checked in — expiry time bombs and committed-key scanners), SAN `localhost` + `127.0.0.1`; BouncyCastle `bcpkix` as a **test-only** dependency (catalog: `[versions]` + `[libraries]` entries so Dependabot maps it; server `testImplementation`/`integrationTestImplementation` only — the Java 8 client must not grow it).

### Phase 3 — gRPC transport wiring

- `buildGrpcServer`: apply `sslContext(...)` when enabled, via the `applyGrpcTransportSettings`-style seam; mock-builder test.
- Verify JDK-provider selection (no tcnative) and note any Netty fallback log line so the smoke test can tolerate/assert it.

### Phase 4 — HTTP transport wiring

- Wire the TLS connector per the verified semantics above: no-arg `start()` on the TLS path, host/port on the connector, copied `HttpConfiguration`, `SecureRequestCustomizer(false)`, keystore password handed to the factory.
- Tests: TLS serves on the configured port; **no plaintext listener exists when TLS is on**; ephemeral port 0 still reports the bound port; an HTTPS request dialed by IP succeeds (pins the `sniHostCheck=false` decision).

### Phase 5 — Startup sequencing & runtime policy

- Load/validate `TlsMaterial` in the constructor path before any bind; startup INFO line; `callback.base_url` scheme WARN; handshake-log policy.
- Tests: guard non-interaction (TLS + noop + non-loopback + no insecure_mode → still refuses); WARN present/absent per callback scheme; validation failure leaves no bound socket.

### Phase 6 — Client SDK

- `GrpcClientSupport.openChannel` rework (channel credentials path + ALPN guard carried over); `trustCaCertificate` + `overrideAuthority` on both builders; eager `build()`-time validation naming the path.
- Tests: builder matrix (trust-without-TLS fails; authority-without-TLS OK), missing/malformed CA file, Java 8 compatibility (`./gradlew :client:check` under the toolchain).

### Phase 7 — Integration tests (`server/src/integrationTest`)

`SagaServerTlsIntegrationTest` with runtime-generated certs, covering the six SpecFlow scenarios:
1. Full TLS round trip, both transports, one cert: HTTPS `/health` with custom truststore **and** SDK client with `trustCaCertificate` + `overrideAuthority` runs a saga — the only test proving Jetty's PKCS12 conversion and Netty's path consume identical material.
2. Plaintext client vs TLS port (both transports): assert client-visible failure shape and server logs stay ≤ DEBUG.
3. Bare TCP connect-and-close on the TLS gRPC port (LB-probe simulation): server keeps serving, no log noise.
4. Startup-failure redaction sweep: mismatched key/cert, encrypted key, malformed PEM → exit before any port binds; no PEM content in output.
5. Guard non-interaction (also unit-tested in Phase 5; kept here cross-layer).
6. Ephemeral ports + TLS (the test-harness path future work will rely on).

### Phase 8 — CI image smoke variant

- Extend `image-smoke-test` (`.github/workflows/ci.yml`) with a TLS boot: generate a cert with `openssl` in the workflow step (nothing committed; SAN `DNS:localhost,IP:127.0.0.1`), mount it plus a TLS properties fixture (uid-201-readable), assert: `curl --cacert https://127.0.0.1:12080/health`, gRPC health via downloaded `grpc-health-probe -tls -tls-ca-cert ... -tls-server-name localhost` (the bare TCP probe is meaningless under TLS), the startup INFO TLS line, log cleanliness, clean SIGTERM drain, epoll assertion unchanged.
- The plaintext smoke job stays as-is. The arm64 QEMU job stays plaintext-only: JDK SSL adds no arch-specific artifact, so there is nothing arm64-specific to assert.

### Phase 9 — Documentation

- `server/docker/README.md`: replace the plaintext-only statement (110-111) with TLS config + "infra termination still recommended by default"; K8s probe guidance (httpGet `scheme: HTTPS`; the gRPC-probe TLS gap and its exec-probe/keep-HTTP workarounds); cert Secret mount example readable by uid 201.
- Document once, as decisions: no mTLS; **outbound** TLS trust (participant `service.<name>.base_url`, JWKS fetch) uses the JVM default truststore — private-CA participants need `-Djavax.net.ssl.trustStore` (consistent with no-knobs); no plaintext→HTTPS redirect listener; typo'd TLS keys are caught by unknown-keys-fatal.
- Client SDK javadoc: one worked private-CA + Java 8 example (trust CA, overrideAuthority for port-forward/IP dialing, the pre-8u252 ALPN remedy).
- Getting-started walkthrough: stays plaintext (it is a quickstart); add one pointer to the docker README's TLS section so readers know the option exists.
- **Design doc** (`~/git/scalardb-saga-design/.../scalardb-saga-design.md` "Transport Security", ~5320): amend the "no application-level TLS" position. Separate repo — flag as a companion change (design-doc staleness is already tracked in todos/052); not editable from this working directory.

## Acceptance Criteria

### Functional
- [x] `tls.enabled=true` + both paths serves TLS on both transports; certificate verifiable by clients trusting the issuing CA
- [x] Every truth-table cell behaves and errors exactly as specified, with tests
- [x] Every startup error-matrix row produces its dedicated, redaction-safe message; no PEM content in any error/log at any level
- [x] Key/cert mismatch fails eagerly, before any port binds
- [x] No plaintext listener on either port when TLS is enabled
- [x] HTTPS requests dialed by IP succeed (no SNI-check 400); probes and port-forwards work
- [x] Handshake failures log at ≤ DEBUG
- [x] `callback.base_url` http-scheme WARN fires per spec
- [x] noop-guard behavior unchanged under TLS (test-pinned)
- [x] SDK: `trustCaCertificate` + `overrideAuthority` on both clients; trust-without-TLS fails at `build()`; CA file errors fail at `build()` naming the path; ALPN guard preserved
- [x] Ephemeral port 0 works under TLS; `port()`/`grpcPort()` report bound ports

### Quality gates
- [x] `./gradlew spotlessApply` → `check` → `clean compileTestJava --no-build-cache` all clean
- [x] All six integration scenarios green (guard non-interaction is pinned at unit level with real TLS material); no checked-in key material anywhere
- [ ] TLS smoke job green on amd64; plaintext smoke and arm64 jobs untouched and green — *verifiable only when CI runs on the pushed branch*
- [x] Client module remains Java 8-clean with no new dependencies
- [x] ~~BC test-only dependency has catalog entries~~ **Superseded, better than planned**: no BouncyCastle anywhere — the test-certificate fixture (`TlsTestCerts`) shells out to the JDK's own `keytool`, which also keeps the integration-test classpath on the production Netty parse path (BC would silently widen it)

## Dependencies & Risks

| Risk | Mitigation |
|---|---|
| **Jetty 11 is EOL** (community releases ceased January 2026; 11.0.25 is effectively last; Javalin 6 cannot move to Jetty 12) | Pre-existing exposure, not created by this feature — but TLS enlarges the surface riding on it. Note it in the docs' "infra termination remains recommended" framing; the real remedy is the eventual Javalin 7/Jetty 12 migration |
| PKCS#8-only rejects **cert-manager's and Vault's default key encodings** (PKCS#1/traditional) | Error text + docs carry the one-line issuer-side fixes (`spec.privateKey.encoding: PKCS8`, `private_key_format=pkcs8`, `openssl pkcs8 -topk8 -nocrypt`); revisit BC only on real demand |
| BC appearing on the runtime classpath later would silently widen Netty's key acceptance (PKCS#1 starts working, then breaks if BC leaves) | `TlsMaterial` validates first, so acceptance is classpath-independent; BC pinned test-only |
| FindSecBugs findings on in-memory PKCS12 password handling | Throwaway generated `char[]`, cleared after use; suppress with justification only if a false positive |
| Smoke-test TLS variant flakiness (openssl generation, probe download) | Pin `grpc-health-probe` version; generation is 2 openssl invocations; fixture shared via `.github/smoke/` like the existing one |

## Out of Scope (confirmed in brainstorm)

mTLS; cert hot reload (rides the config hot-reload plan — `TlsMaterial` is the seam); `tls.min_version`/cipher knobs (JVM flags are the escape hatch); BoringSSL (one dependency line if ever profiled as needed); outbound-TLS trust knobs; HTTP client SDK.

## Sources & References

### Origin
- **Brainstorm:** [docs/brainstorms/2026-08-13-server-tls-brainstorm.md](../brainstorms/2026-08-13-server-tls-brainstorm.md) — decisions carried forward: server-side TLS only; PEM paths only, single shared block; JDK SSL, no knobs; client trust API now; hot reload deferred behind the `TlsMaterial` seam.

### Internal
- Transport seams: `server/src/main/java/com/scalar/db/saga/server/SagaServer.java:164-193` (gRPC), `:359-372` (HTTP), `:482-510` (start), `:433-453` (security guard)
- Config doctrine: `server/src/main/java/com/scalar/db/saga/server/SagaServerConfig.java` (KNOWN_KEYS :348, validateCombinations :641, requireNonBlankIfSet :1449); redaction: todos/050 + PR #95
- Client seam: `client/src/main/java/com/scalar/db/saga/grpc/GrpcClientSupport.java:33-46,101-111`
- Smoke tests: `.github/workflows/ci.yml` (REST curl :168, TCP probe :208, epoll :190), `.github/smoke/server.properties`
- ScalarDB Cluster precedent (naming + client trust surface): `~/git/scalardb-cluster` — `ClusterConfig.java:79-90`, `RemoteClusterNode.java:63-76`, `ClusterNodeConfig.java:63-66`, `TlsUtils.java`

### Related
- Config hot-reload plan (future cert reload rides it): `docs/plans/2026-08-10-001-feat-config-hot-reload-services-definitions-plan.md`
- Design-doc staleness tracking: `todos/052`
