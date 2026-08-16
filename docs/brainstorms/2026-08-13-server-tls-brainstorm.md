# Server TLS (HTTP + gRPC) — Brainstorm

**Date:** 2026-08-13
**Status:** Decided — ready for planning

## What We're Building

Native TLS support in SagaServer for both transports — HTTPS on the Javalin/Jetty side and TLS on the gRPC/Netty side — configured from the properties file (the operator's only channel) as one shared block:

- `scalar.db.saga.server.tls.enabled` (default `false`)
- `scalar.db.saga.server.tls.cert_chain_path` — PEM certificate chain
- `scalar.db.saga.server.tls.private_key_path` — PEM private key (unencrypted PKCS#8 only —
  *amended during planning*: PKCS#1/SEC1 are rejected with conversion guidance)

Plus client-side trust convenience in the Java 8 client SDK: builder methods for a custom CA certificate (PEM) and authority override, mirroring ScalarDB Cluster's day-one client surface (`tls.ca_root_cert_path` / `tls.override_authority` semantics).

This amends the recorded plaintext-only position (design doc "Transport Security" section; `server/docker/README.md:110-111`): infrastructure termination (mesh/ingress/LB) remains the recommended default, and native TLS becomes available for deployments with nothing to terminate at.

## Why This Approach

- **PEM paths, not keystores or inline content** — what cert-manager/Vault/Let's Encrypt emit and what K8s Secret mounts contain; matches ScalarDB Cluster's `cert_chain_path`/`private_key_path` names exactly; paths keep future hot reload possible (re-readable). *(Amended post-review: "a path is not a secret" proved wrong — a secret reference mis-pasted onto a path key resolves to the secret — so errors and logs name config keys only.)*
- **One shared block** — both ports share one host, so one cert's SANs cover both; smallest surface; all-or-nothing avoids a half-secure server ambiguity.
- **JDK SSL, no tuning knobs** — Java 21 defaults (TLS 1.2+/1.3, maintained cipher list) are correct without us; every knob costs parse/validate/redact/test on two stacks; JVM flags remain the compliance escape hatch; adding knobs or BoringSSL later is backward-compatible, removing them is not.
- **Family consistency** — ScalarDB Cluster verified as precedent for key naming, client trust surface, and the PEM→PKCS12 conversion pattern Jetty needs (`TlsUtils` with BouncyCastle in scalardb-cluster).

## Key Decisions

1. **Server-side TLS only; no mTLS.** Client authentication stays with the security-provider seam (JWT/API key).
2. **PEM file paths only.** No keystore support, no inline PEM values. Mirror Cluster key names under `scalar.db.saga.server.tls.*`.
3. **Single shared TLS block** covering both enabled transports; one cert/key pair.
4. **No protocol/cipher knobs.** TLS 1.2+/1.3 with JDK defaults. `tls.min_version` can be added compatibly if a real compliance need appears.
5. **JDK SSL engine on the server** — no netty-tcnative natives, no new CI alignment burden. gRPC auto-selects JDK SSL when tcnative is absent, so a BoringSSL upgrade later is a dependency-line change. (Cluster's BoringSSL is a side effect of grpc-netty-shaded, not an explicit choice to copy.)
6. **Client SDK trust API ships now**: trust-CA PEM + `overrideAuthority` builder methods on the gRPC client builders, matching Cluster's client trust surface. Default remains plaintext; `useTransportSecurity()` unchanged.
7. **Unencrypted private keys only.** Key protection is file permissions / K8s Secret mounts, not a passphrase. (*Amended during planning*: PKCS#8-only, stricter than Cluster's BouncyCastle-backed parser, so no crypto dependency is needed.)
8. **Cert hot reload deferred** to the config hot-reload feature. Kept cheap by two guardrails in this iteration: path-based config keys, and a single internal cert-material component both transports consume (later: Jetty `SslContextFactory.reload()` / gRPC delegating key manager — an internal swap, no config-surface change).

## Constraints & Facts (from repo research)

- Two unrelated stacks get TLS: Javalin/Jetty (`cfg.jetty` block in `createHttpServer`, SagaServer.java:359-372) and grpc-java `NettyServerBuilder` (`buildGrpcServer` / the `applyGrpcTransportSettings` seam and its mock-builder test pattern, SagaServer.java:164-193).
- Config doctrine applies in full: unknown keys fatal, blank-is-unset policy (an "enabled"-style key sits on the reject-blank line), paired-keys-fail-at-startup (callback URL/secret precedent → `enabled` requires both paths; paths without `enabled` also fail), PR #95 redaction doctrine (no resolved values or exception causes in errors — TLS errors must name the path, never file contents).
- Jetty needs PEM→in-memory-PKCS12 conversion (Cluster's `TlsUtils` is the in-family pattern); gRPC takes PEM natively via `useTransportSecurity(certFile, keyFile)`.
- CI smoke tests: the REST `curl http://…/health` probe breaks under TLS; the gRPC probe is a bare TCP connect that survives but proves nothing. TLS needs its own boot coverage; mounted cert fixtures must be readable by uid 201.
- Client SDK already has `useTransportSecurity()` (default plaintext) and a Java 8 ALPN fail-fast advising `netty-tcnative-boringssl-static` — unchanged by this work.
- Docs to amend: design doc "Transport Security" (~line 5320), `server/docker/README.md:110-111`, `server/docker/conf/server.properties` template, class javadoc of `SagaServerConfig`.

## Resolved Questions

- **Scope?** Server-side TLS only; mTLS out of scope; cert hot reload rides the upcoming hot-reload feature (both Jetty and gRPC support live reload, so deferring costs no rework given the two guardrails).
- **Cert format?** PEM only.
- **Config shape?** Single shared block, both transports together.
- **Tuning knobs?** None; JDK defaults; JVM flags as escape hatch.
- **SSL engine?** JDK SSL (server is Java 21; client-side engine is independent and already handled by the ALPN guard).
- **Client trust config?** Ship now — ScalarDB Cluster precedent (`ClusterConfig.java:79-90`, `RemoteClusterNode.java:63-76`) settled this.

## Out of Scope / Future

- mTLS (client certificates)
- Certificate hot reload (config hot-reload plan)
- `tls.min_version` / cipher knobs (add on demand)
- BoringSSL server engine (perf-driven, one dependency line)
- HTTP client SDK (none exists or is planned; REST consumers use their platform's TLS)
