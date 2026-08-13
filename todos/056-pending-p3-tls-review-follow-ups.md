---
status: pending
priority: p3
issue_id: 056
tags: [code-review, security, quality, tls, server, client]
dependencies: []
---

# TLS review follow-ups: findings remaining after the P1/P2 fixes

## Problem Statement

The five-way review of the feat/server-tls branch (security-sentinel, architecture-strategist,
pattern-recognition-specialist, code-simplicity-reviewer, plus /code-review at high effort) produced
two merge-blockers that were fixed on the branch immediately — the resolved-path redaction leak
(errors/logs now name keys only, never path values) and the CI HTTPS wait loop dying on its first
poll under `set -euo pipefail` — plus the findings below, which remain open. One is P2; the rest are
P3/P4 polish. Recording them all so nothing is relearned. Findings independently reported by
multiple reviewers are marked with their vote count.

## Status after the 2026-08-13 follow-up pass

The recommended pass (option 1) ran the same day and **fixed findings 1–6, 13, and 14–19** on the
branch (full check chain green). Notes on the fixes: 1+2 were solved together — `TlsMaterial` now
retains the validated material re-encoded as PEM and gRPC consumes it via
`useTransportSecurity(InputStream, InputStream)`, so Netty never re-reads the files, and the class
javadoc now states honestly what a future reload still needs (retain Jetty's factory for
`reload(...)`; a delegating key manager or rebuild for gRPC); 6 became the shared `LogCapture`
testFixture (the pre-existing `SagaServerJulBridgeTest` copy was deliberately left — its capture
needs differ); 4 pins sha256 `4b818d…45a96` for the v0.4.28 linux-amd64 probe.

**Still open: findings 11–12 and 20** (test-overlap and doc-copy judgment calls, plus PR #95's
`redacted()` duplication, which is not this branch's debt). Finding 10 was resolved by **removal**
after a careful usage check: the P1 redaction fix had made `certChainPath()` production-dead too
(its consumers were the since-removed INFO-line and WARN path echoes), both accessors' only readers
were tautological getter asserts, and reload cannot use them — the authoritative path holder is
`SagaServerConfig`, which any reloader re-reads, and the redaction rule forbids logging paths
anyway. `TlsMaterial` now carries only the validated material; re-adding provenance later is a
two-line change if a self-reloading design ever wants it. Findings 7–9 were fixed in a second same-day pass: 7's catch-all
message now owns both readings ("the block is corrupt, or its algorithm ... is not supported"), 8
was **verified real** (the base64-only block pattern indeed skips RFC 1421-encrypted PKCS#1 keys)
and now gets a dedicated encrypted-legacy-key message, and 9 was **verified real** (readAllBytes
blocks to process exit, making the timeout unreachable) and fixed by waiting before draining — all
three pinned by tests.

## Findings

### P2

1. **TlsMaterial's hot-reload javadoc promises a seam that reaches neither transport.**
   `TlsMaterial.java` class javadoc says "reloading is re-running `load` and swapping the result" —
   but swapping the field would be a no-op: Jetty's material lives in an `SslContextFactory` built
   inside `SagaServer.tlsConnector` whose reference is not retained, and gRPC never consumes the
   parsed material at all (`applyGrpcTransportSettings` hands file *paths* to
   `useTransportSecurity`, whose SslContext is built once). A future implementer following the
   javadoc would ship a no-op reload. Minimum fix now: reword the javadoc to say reload = re-run
   `load` **plus** rewire both consumers (retain the factory for its `reload(...)` hook; move gRPC
   off the file-path API — `AdvancedTlsX509KeyManager` or a server rebuild). Optionally retain the
   `SslContextFactory` as a field today, which is cheap.

### P3

2. **gRPC dual-read TOCTOU** (security + architecture; /code-review verdict PLAUSIBLE). Netty
   re-reads the two files at builder-build time, after `TlsMaterial` validated them; a rotation in
   that window means gRPC serves material the validator never saw, and the two transports can
   present different certificates for the whole process lifetime — cert and key are even two
   separate reads, so old-cert+new-key is possible across a rotation. Millisecond window at boot,
   hence P3, but it widens structurally when reload lands. Fix that also halves finding 1: feed
   Netty the already-validated bytes via the stable
   `useTransportSecurity(InputStream certChain, InputStream privateKey)` overload
   (`ByteArrayInputStream` from bytes TlsMaterial retains).

3. **Trust-requires-TLS guard duplicated in both client builders** (4 votes: simplicity,
   architecture, patterns, /code-review CONFIRMED). Byte-identical `IllegalStateException` blocks in
   `GrpcSagaOrchestratorClient.build()` and `GrpcSagaAdminClient.build()`; the shared seam
   `GrpcClientSupport.openChannel` already receives both `useTls` and `trustCaCertPath` (the ALPN
   guard already lives there). Centralize; then fold `GrpcClientTlsBuilderTest` into the two
   per-subject test classes (its `build_adminWith...` names smuggle the subject into the condition,
   against CLAUDE.md naming), dropping the now-redundant admin duplicates.

4. **`grpc-health-probe` download is version-pinned but not checksum-pinned** (security +
   architecture). GitHub release assets are mutable; the binary executes on the runner. Add the
   asset sha256 beside the URL and `sha256sum -c` before `chmod +x`, with a comment noting version
   and checksum rotate together (Dependabot cannot bump this pair).

5. **Root-logger-quiet assertions are flake bait** (architecture). `SagaServerTlsIntegrationTest`
   asserts zero WARN+ events on the ROOT logger while a live server, sqlite store, and background
   recovery/retention run — any unrelated WARN fails a TLS test with a misleading message, and the
   repo already tracks one unreproduced daemon flake. Filter captured events to the loggers that
   carry handshake noise (`org.eclipse.jetty`, `io.grpc`, `io.netty` prefixes) instead.

6. **Log-capture harness now exists in ~5 near-identical copies** (patterns; /code-review
   CONFIRMED). `TlsMaterialTest`, `SagaServerTest`, `SagaServerTlsIntegrationTest`, plus the
   pre-existing `SagaServerJulBridgeTest` — ListAppender field + detach + capture + logger cast,
   three different helper names. The branch already created the server `testFixtures` source set;
   extract one shared capture fixture (AutoCloseable holder or JUnit extension) there.

7. **Corrupt-but-PKCS#8 key yields a misleading message** (/code-review CONFIRMED). Valid base64
   that is not valid DER fails both KeyFactory attempts and lands in the "neither RSA nor EC —
   other algorithms are not supported" message, telling an operator with a corrupt file to change
   algorithms. Distinguish "does not parse as a key at all" from "parses but unsupported
   algorithm", e.g. by checking the PKCS#8 AlgorithmIdentifier OID before the KeyFactory cascade or
   by wording the message to cover both ("corrupt, or an unsupported algorithm").

8. **Encrypted legacy keys miss the encrypted-specific guidance** (/code-review, verifier
   unresolved at synthesis time — verify then fix). `KEY_BLOCK`'s body charset `[A-Za-z0-9+/=\s]*`
   excludes `:` and `,`, so a PKCS#1-encrypted PEM (`BEGIN RSA PRIVATE KEY` with
   `Proc-Type: 4,ENCRYPTED` headers inside the block) fails to match and falls to the generic "no
   PRIVATE KEY block" message instead of the encrypted-key or PKCS#1 guidance. Either let the block
   regex tolerate RFC 1421 headers and branch on them, or note the limitation in the message
   ("...or a legacy key with encryption headers").

9. **`TlsTestCerts.runKeytool`'s timeout branch is dead code** (/code-review, verifier unresolved —
   verify then fix). `readAllBytes()` on the process stream blocks until the process exits, so
   `waitFor(60, SECONDS)` can never time out. Either read output after `waitFor` or drop the
   timeout branch.

10. **`TlsMaterial.privateKeyPath()` accessor is production-dead** (simplicity). Read only by one
    test assert; the reload story holds paths on `SagaServerConfig` anyway. Drop, or leave until
    the reload design decides. Interacts with finding 2 (bytes-based wiring changes what needs
    retaining).

11. **Test overlap trims** (simplicity). (a)
    `SagaServerTlsIntegrationTest.constructor_mismatchedKeyAndCert_...` re-proves three
    already-pinned facts through the one-arg constructor and pays a keytool + sqlite boot; shrink
    or drop. (b) The bare-TCP test's full saga round trip could be a single `getStateSnapshot`
    liveness call — though it is incidentally the only trust-without-overrideAuthority exercise,
    so keep the client, drop the `start()`.

12. **PKCS#8 issuer guidance is maintained in four prose copies** (simplicity). TlsMaterial error
    text (doctrine — keep), `SagaServerConfig` javadoc, `server.properties` template, docker
    README. Suggest the template keeps only "unencrypted PKCS#8 — see server/docker/README.md" and
    drops the per-issuer flags; judgment call given the house's verbose-template style.

13. **Admin builder `overrideAuthority` has no test** (/code-review CONFIRMED). Folding the TLS
    builder tests per finding 3 is the natural moment to add the one-line case.

### P4 (mechanical polish, bundle into any touch of these files)

14. **Stale javadoc**: `SagaServer.applyGrpcTransportSettings` still opens "Applies the two inbound
    caps" though it now also enables TLS; `createHttpServer`'s javadoc describes only the thread
    pool. **Comment-prose slash** at `SagaServer.java:441` ("isSecure()/the https scheme") — the
    CLAUDE.md reflow rule says prefer "or" over "/" in comment prose (/code-review CONFIRMED).
15. **SagaServerTest organization** (patterns; /code-review CONFIRMED on the keytool point): log
    scaffolding declared mid-class (precedent: top of class), the insertion strands
    `applyGrpcTransportSettings_withCapsUnset_...` ~200 lines from its group, and each TLS start
    test generates a fresh keytool pair — hoist scaffolding, reunite groups, share one static pair.
16. **Assert log levels via `Level.WARN` constants**, not `getLevel().toString()` — the integration
    test already does it right; align `TlsMaterialTest`/`SagaServerTest`.
17. **`ThrowingCase` duplicates AssertJ's `ThrowableAssert.ThrowingCallable`** (already used
    elsewhere in the module) — delete the private interface.
18. **POSIX permissions test lacks an environment guard**: `assumeTrue` on the POSIX
    file-attribute view (and non-root) in `TlsMaterialTest.load_keyFileUnreadable_...`.
19. **`warnIfCallbackBaseUrlIsPlaintextUnderTls` placed after `start()`**; its guard siblings sit
    together above it — move up. **Build-file comment under-inclusive**: "The bridge test
    captures..." now also covers the TLS tests' logback need.

### Noted, not this branch's debt

20. **`redacted()` exists in both `SagaServerConfig` and `JwtConfig`** (/code-review CONFIRMED) —
    duplication introduced by PR #95's sweep, pre-existing relative to this branch. Worth one
    shared home when either file is next touched; flagged to avoid a third copy appearing.

## Proposed Solutions

1. **One follow-up pass on this branch before the PR opens** (recommended): findings 1-6 and 13
   (the P2, the three multi-vote items, the flake risk, the missing test) plus the P4 bundle —
   roughly an hour, all mechanical, and the PR then ships review-clean. Findings 7-9 need a short
   verification first (8 and 9 carry unresolved verifier status); 10-12 are judgment calls that can
   ride any later touch. Effort: Small-Medium. Risk: Low — every item has tests around it.
2. **Ship the branch as-is and land this todo as the follow-up backlog**: nothing here blocks
   merge. Effort now: none. Risk: the P2 javadoc misleads the hot-reload implementer if that work
   starts before the follow-up; the flake risk (5) ages badly in CI.
3. **Split**: fix 1 (javadoc reword only), 4, and 5 on this branch (the three that age worst),
   defer the rest. Effort: ~20 minutes. Risk: Low.

## Recommended Action

(leave blank for triage)

## Technical Details

- Branch: `feat/server-tls` (stacked on `fix/redact-config-parse-error-values`, PR #95),
  uncommitted working tree as of 2026-08-13.
- Affected files: `server/src/main/java/com/scalar/db/saga/server/{TlsMaterial,SagaServer}.java`,
  `client/src/main/java/com/scalar/db/saga/grpc/{GrpcClientSupport,GrpcSagaOrchestratorClient,GrpcSagaAdminClient}.java`,
  the four TLS test classes, `server/src/testFixtures/.../TlsTestCerts.java`,
  `.github/workflows/ci.yml`, docs/template.
- Already fixed on the branch (not in this todo): P1 resolved-path redaction leak; P2 CI wait-loop
  `set -e` abort; the `badFile()` message helper (landed as part of the P1 fix).

## Acceptance Criteria

- [ ] Each finding either fixed on a branch or explicitly triaged to rejected/deferred with a note
- [ ] Findings 8 and 9 verified (or refuted) before fixing
- [ ] `./gradlew spotlessApply` → `check` → `clean compileTestJava --no-build-cache` clean after
      any fixes

## Work Log

- 2026-08-13: Created from the five-way review of feat/server-tls (four ce-review agents +
  /code-review high). P1/P2 fixed on the branch the same day; everything else recorded here.
- 2026-08-13 (later): Follow-up pass fixed 1–6, 13, 14–19 (see Status section); file renamed
  p2 → p3 accordingly.
- 2026-08-13 (later still): Second pass fixed 7–9 (8 and 9 verified real first).
- 2026-08-13 (final): 10 resolved by removing both path accessors and fields (usage check showed
  certChainPath() had gone production-dead too; reload reads paths from SagaServerConfig).
  Remaining: 11–12, 20 — pure judgment calls or other branches' debt.

## Resources

- Plan: `docs/plans/2026-08-13-001-feat-server-tls-http-grpc-plan.md` (error-matrix section
  amended with the post-review key-only redaction rule)
- Brainstorm: `docs/brainstorms/2026-08-13-server-tls-brainstorm.md`
- Redaction doctrine: `todos/050-pending-p2-config-error-messages-echo-resolved-secret-values.md` / PR #95
