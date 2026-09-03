package com.scalar.db.saga.server;

import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.engine.RetentionConfig;
import com.scalar.db.saga.engine.ShutdownMode;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for a {@link SagaServer}, built from a single {@link Properties} object (typically
 * a ScalarDB properties file with extra {@code scalar.db.saga.server.*} keys).
 *
 * <p>Daemon mode ships as a container, so this file is the operator's <b>only</b> channel: every
 * engine setting an embedded application would pass to {@link DefaultSagaOrchestrator.Builder} has
 * a key here, except the ones with no properties analogue (a {@code Clock}, a {@code StepResolver}
 * or injected resources, and a custom {@code HttpClient} — all Java objects, and code steps are
 * rejected in daemon mode anyway).
 *
 * <h2>Server</h2>
 *
 * <ul>
 *   <li>{@code host} — interface to bind, shared by both transports (default {@value
 *       #DEFAULT_HOST}, all interfaces); set to {@code 127.0.0.1} or a specific interface to
 *       restrict reach
 *   <li>{@code owner_id} — identity this instance stamps on the sagas it claims during recovery
 *       (default: a random UUID per process). Set it to something an operator can trace back to a
 *       process — e.g. {@code ${env:HOSTNAME}} for the pod name — so a stuck saga names the replica
 *       holding it. Two live instances must never share a value: the claim is what stops two
 *       replicas from driving the same saga. The value also seeds the instance's recovery and
 *       retention sweep scatter (bucket order and schedule offset), so a stable value gives stable
 *       sweep phases across restarts. Must match {@code [a-zA-Z0-9._-]{1,128}} — it is stamped on
 *       claimed rows and echoed in log lines
 *   <li>{@code definitions_path} — path to a JSON/YAML saga definition file or directory
 *   <li>{@code default_saga_timeout_millis} — a default saga timeout enforced at execution for
 *       definitions that set none ({@code 0} = unbounded); {@code 0} (default) disables it. A
 *       definition's own timeout always wins. Applied at every execution entry (start, recovery
 *       resume, parked resume) rather than baked into the stored definition, so changing it takes
 *       effect for in-flight sagas at their next drive and never conflicts with stored content
 *   <li>{@code max_start_requests_per_minute} — per-principal rate limit on {@code POST}/{@code PUT
 *       /sagas}; {@code 0} (default) disables rate limiting. The limit is keyed on the
 *       authenticated principal, so it is only per-caller once a real provider (jwt or apikey) is
 *       configured; under {@code noop} every request is the same {@code "anonymous"} principal and
 *       the limit acts as one global bucket shared by all callers
 * </ul>
 *
 * <h2>Transports ({@code http.*} / {@code grpc.*})</h2>
 *
 * <p>Both are served by default, each on its own listener bound to {@code host}. At least one must
 * be enabled — the server refuses to start with both disabled — and when both are enabled they must
 * not name the same fixed port.
 *
 * <ul>
 *   <li>{@code http.enabled} / {@code grpc.enabled} — whether to serve that transport (default
 *       {@code true} each); set one to {@code false} to run single-transport
 *   <li>{@code http.port} — HTTP listen port (default {@value #DEFAULT_HTTP_PORT}; {@code 0} binds
 *       an ephemeral port, useful in tests)
 *   <li>{@code grpc.port} — gRPC listen port (default {@value #DEFAULT_GRPC_PORT}; {@code 0} binds
 *       an ephemeral port)
 *   <li>{@code http.max_threads} / {@code http.min_threads} — Jetty request thread-pool bounds
 *       (defaults {@value #DEFAULT_MAX_THREADS} / {@value #DEFAULT_MIN_THREADS}). Handlers run on
 *       virtual threads, so the max caps how many requests are <em>dispatched</em> at once, not how
 *       many are in flight: a request waiting on its saga costs no pool thread. It therefore does
 *       not bound concurrent saga execution — nothing does yet
 *   <li>{@code http.max_queued_requests} — cap on the pool's job queue, so the dispatch backlog is
 *       memory-bounded and the pool rejects rather than growing without limit. Defaults to {@value
 *       #DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD} × {@code http.max_threads}. Because a blocking
 *       handler is dispatched to a virtual thread rather than queued, this does not shed a burst of
 *       slow sagas
 *   <li>{@code grpc.max_inbound_metadata_bytes} — cap on a call's total request metadata (default
 *       {@value #DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES}). Raise it only if legitimate credentials
 *       do not fit — a JWT access token with many claims is the usual reason
 * </ul>
 *
 * <p>The gRPC <i>message</i> cap is deliberately not a key of its own: it is derived from {@code
 * scalar.db.saga.store.max_event_payload_bytes} (see {@link #grpcMaxInboundMessageBytes()}) so no
 * transport can accept an input the store would then reject.
 *
 * <h2>TLS ({@code tls.*})</h2>
 *
 * <p>Native TLS for <b>both</b> transports, all-or-nothing: one certificate covers the HTTP and
 * gRPC listeners, which share {@code host}. Off by default — terminating TLS at a mesh, ingress, or
 * load balancer remains the recommended deployment; enable this where no such layer exists.
 *
 * <ul>
 *   <li>{@code tls.enabled} — serve TLS on both enabled transports (default {@value
 *       #DEFAULT_TLS_ENABLED}). Requires both paths below. Setting either path without this key is
 *       rejected, so mounted certificates cannot sit unused behind a forgotten switch while the
 *       server silently serves plaintext; {@code tls.enabled=false} with paths set is legal and
 *       ignores them, which is how an operator toggles TLS off without unmounting the material.
 *       Ignored values must still be well-formed: shape checks run regardless of the toggle, as for
 *       every key here, so the file stays valid for the day the toggle flips back
 *   <li>{@code tls.cert_chain_path} — path to the PEM certificate chain, leaf first
 *   <li>{@code tls.private_key_path} — path to the matching PEM private key: unencrypted PKCS#8
 *       ({@code BEGIN PRIVATE KEY}), RSA or EC. PKCS#1/SEC1 ({@code BEGIN RSA/EC PRIVATE KEY}) and
 *       encrypted keys are rejected with conversion guidance; note that cert-manager and Vault emit
 *       those legacy encodings unless asked for PKCS#8
 * </ul>
 *
 * <p>The keys take paths, never certificate or key contents, so the material stays re-readable for
 * a future reload. The files are read and validated at startup, before either port binds, and every
 * failure names the offending key — never the configured value, which like any value here could be
 * a mis-pasted secret (see {@code TlsMaterial}). Protocols default to TLS 1.3 and 1.2 with no keys
 * to tune, and ciphers to each stack's hardened defaults (Jetty's standard exclusions over the JDK
 * list; gRPC's HTTP-2-approved suites); the rare compliance need is served by JVM flags (e.g.
 * {@code jdk.tls.server.protocols}).
 *
 * <h2>Synchronous starts ({@code sync.*})</h2>
 *
 * <ul>
 *   <li>{@code sync.max_wait_millis} — absolute ceiling (ms) on a synchronous start's server-side
 *       wait, so it can never block indefinitely (default {@value #DEFAULT_SYNC_MAX_WAIT_MILLIS}).
 *       Applies to both transports.
 *   <li>{@code sync.timeout_millis} — optional tightening of that ceiling; {@code 0} (default)
 *       means no tightening, <em>not</em> an unbounded wait. A gRPC client's call deadline tightens
 *       it further.
 * </ul>
 *
 * <p>Read the pair through {@link #syncWaitBoundMillis(long)} rather than separately; a transport
 * should be handed the resolved bound, not the two keys behind it.
 *
 * <h2>Shutdown ({@code shutdown.*})</h2>
 *
 * <ul>
 *   <li>{@code shutdown.mode} — {@code WAIT_CURRENT_STEP} (default) finishes the running step and
 *       stops between steps, leaving the saga for recovery; {@code WAIT_ALL_SAGAS} waits for
 *       in-flight sagas to reach a terminal state
 *   <li>{@code shutdown.timeout_millis} — ceiling (ms) on that drain (default {@value
 *       #DEFAULT_SHUTDOWN_TIMEOUT_MILLIS}). It is the second of the two shutdown windows the daemon
 *       spends in sequence; budget a container's termination grace period for their sum. {@code 0}
 *       drains nothing: in-flight work is cancelled at once and left for the recovery scan, which
 *       trades shutdown latency for reclaim latency on the next boot
 * </ul>
 *
 * <h2>Saga detail reads ({@code detail.*})</h2>
 *
 * <ul>
 *   <li>{@code detail.max_timeline_events} — maximum timeline events one {@code getSagaDetail} read
 *       returns (default {@value #DEFAULT_DETAIL_MAX_TIMELINE_EVENTS}). A longer history is cut to
 *       the newest events and the response is flagged {@code truncated}; the full history remains
 *       in the store. This bound, together with the reader's cap of 1024 chars on each entry's
 *       detail text, keeps a pathological saga's detail under a gRPC client's default 4 MB inbound
 *       message cap, so the saga stays diagnosable. A window widened far beyond the default can
 *       exceed that cap again; the oversized detail still arrives over REST, which has no
 *       equivalent limit, but not through the Java client SDK
 * </ul>
 *
 * <h2>Crash recovery ({@code recovery.*})</h2>
 *
 * <p>Every replica scans for sagas abandoned by a crashed instance and resumes them. Defaults come
 * from {@link RecoveryConfig#defaults()}.
 *
 * <ul>
 *   <li>{@code recovery.staleness_threshold_millis} — a saga that has made no progress for longer
 *       is considered abandoned and eligible for recovery. Progress is judged from the saga's
 *       newest event, so a saga executing a long step is not mistaken for a dead one. Set it above
 *       the longest a <b>single step</b> can take, which is simply its step timeout: that deadline
 *       is an absolute instant computed once before the retry loop, so it bounds the entire attempt
 *       sequence — every retry and all backoff — rather than each attempt. A healthy saga emits an
 *       event only at step boundaries, so that is the longest silence to expect. <b>With neither a
 *       step timeout nor a saga timeout configured there is no such bound</b> — the step runs until
 *       it returns — and no value here is safe; set one of those timeouts if you rely on recovery
 *       leaving long steps alone. Raising it delays recovery of genuinely crashed sagas by the same
 *       amount: this value is your crash-recovery MTTR.
 *       <p>Recovery is at-least-once across replicas. A step whose attempt sequence outlives this
 *       threshold produces no events while it runs, so another replica can begin recovering the
 *       saga while the first is still executing it; the loser is fenced on its next state write,
 *       but the participant call itself has already been made. Size the threshold above the step
 *       envelope, and make participants tolerate a duplicate invocation
 *   <li>{@code recovery.interval_seconds} — how often the scan runs
 *   <li>{@code recovery.compensation_grace_period_seconds} — how long a saga may stay stuck with
 *       failing compensation before it is escalated for manual intervention
 *   <li>{@code recovery.max_recoveries_per_sweep} — a pass makes two sweeps, one for abandoned
 *       sagas and one for parked sagas past their deadline, and this bounds each separately, so one
 *       pass can do up to twice this number (the pass summary reports them as {@code stale[...]}
 *       and {@code parked[...]}). Claims lost to another replica do not count against it (failed
 *       attempts do), and a sweep stopped by the cap resumes where it left off next pass, so a
 *       small value never skips sagas — it only spreads recovery over more passes. Keep it well
 *       above 200: a bucket is read as one page per recoverable status and a sweep stops submitting
 *       once its budget runs out, so a budget below 200 truncates the page, and the truncation
 *       always falls on the trailing status. At or below 100, compensating sagas are never
 *       recovered; between 100 and 200 they are served but throttled behind running ones. 200 is a
 *       floor, not a target — the budget is spent across buckets, so a sweep budgeted at 200 covers
 *       a single bucket
 *   <li>{@code recovery.max_concurrent_recoveries} — how many sagas are <b>recovered</b> at once:
 *       claimed and driven, participant calls included. It bounds the expensive half of a pass. The
 *       cheap screening that decides whether a saga needs recovering at all runs outside this
 *       limit, under its own fixed bound, so a pass is never held up waiting for a running saga to
 *       finish before it can answer a one-read question
 * </ul>
 *
 * <h2>Retention ({@code retention.*})</h2>
 *
 * <p>Purges terminal sagas so the tables do not grow without bound. Defaults come from {@link
 * RetentionConfig#defaults()}. {@code ESCALATED} sagas are never purged — they await an operator.
 *
 * <ul>
 *   <li>{@code retention.period_seconds} — how long a terminal saga is kept before it is purgeable
 *       (default 7 days). This is the window in which a saga's history can still be inspected
 *   <li>{@code retention.interval_seconds} — how often the purge runs
 *   <li>{@code retention.max_purges_per_pass} — cap on sagas actually purged per pass (deletes that
 *       turn out to be no-ops because another replica already purged the saga do not count); it
 *       must keep up with the terminal-saga rate over one interval or the backlog grows
 *   <li>{@code retention.max_concurrent_purges} — how many of those are purged at once
 * </ul>
 *
 * <h2>Declarative services ({@code services_path})</h2>
 *
 * <p>Downstream services live in a directory of per-service files, one {@code
 * <service-name>.properties} per service a declarative step's {@code "service":"<name>"} resolves
 * to. The service name is the file name minus the {@code .properties} extension. Inside a file the
 * keys are prefix-free:
 *
 * <ul>
 *   <li>{@code base_url} — the service's base URL (required)
 *   <li>{@code allowed_hosts} — comma-separated SSRF allowlist for this service; unset = any host.
 *       Matching is on the host name only, so it is defense in depth for a trusted endpoint, not a
 *       sandbox
 *   <li>{@code max_body_bytes} — request/response body cap for this service; unset uses the engine
 *       default
 *   <li>{@code header.<HeaderName>} — a header sent on every request to this service, repeated per
 *       header. This is the channel for calling an <b>authenticated</b> service (e.g. {@code
 *       header.Authorization}), and the value takes a secret reference — confined to {@code
 *       secrets_root}, see below. The engine stamps its own headers ({@code X-Saga-Id}, {@code
 *       X-Saga-Step}, {@code X-Saga-Callback-Url}) on every request and they always win, so
 *       configuring one of those names is rejected. Header names are case-insensitive per the HTTP
 *       spec, so setting one name in two spellings is rejected too. A header value is trimmed,
 *       which is what lets a {@code ${file:...}} secret ending in a newline be sent at all — an
 *       untrimmed newline is a control character no HTTP header value may carry. The names the
 *       JDK's HTTP client refuses to send ({@code Connection}, {@code Content-Length}, {@code
 *       Expect}, {@code Host}, {@code Upgrade}) are rejected as well
 * </ul>
 *
 * <p>The directory-level keys:
 *
 * <ul>
 *   <li>{@code services_path} — the directory of service files; unset = no services
 *   <li>{@code reload.interval_seconds} — seconds between configuration reload passes (default
 *       {@value #DEFAULT_RELOAD_INTERVAL_SECONDS}): service files and definitions are re-read and
 *       validated as a complete set, so changes land without a restart. Validation is all or
 *       nothing; applying is not — services swap before definitions register, one definition at a
 *       time, so a failure part way through leaves what committed live and retries the rest next
 *       pass. {@code 0} disables reload (startup-only loading)
 *   <li>{@code secrets_root} — the directory {@code ${file:...}} references in service files must
 *       resolve inside, after symlink resolution (default {@value #DEFAULT_SECRETS_ROOT}). Service
 *       files are a live trust boundary under reload, so their file references are confined to the
 *       secrets mounted for that purpose; {@code server.properties} itself is bootstrap
 *       configuration and stays unconfined
 *   <li>{@code egress.allowed_hosts_ceiling} — optional comma-separated ceiling; when set, every
 *       service's {@code allowed_hosts} must be a non-empty subset of it, so no service file can
 *       authorize egress beyond what the operator allowed
 * </ul>
 *
 * <h2>Async callbacks ({@code callback.*})</h2>
 *
 * <p>All three keys configure one feature: the callback URL an async step hands a participant, and
 * the HMAC that authenticates the participant's callback. {@code base_url} and {@code secret} are
 * set together or not at all — one without the other is a configuration that cannot complete an
 * async step, so it fails at startup rather than at the first async saga.
 *
 * <ul>
 *   <li>{@code callback.base_url} — the daemon's externally reachable base URL, used to build the
 *       callback URL. Any trailing {@code /} is stripped
 *   <li>{@code callback.secret} — the HMAC secret authenticating callbacks. Supply it as a secret
 *       reference. Unlike a service header value, this one is <b>not</b> trimmed: it is opaque key
 *       material, and silently dropping surrounding whitespace would change the key and break
 *       verification against a participant that signs with the untrimmed value
 *   <li>{@code callback.max_age_seconds} — TTL on a callback token's {@code iat}, so a leaked
 *       callback URL is not a non-expiring credential. {@code 0} (default) disables the check; when
 *       set it must exceed the longest a step can legitimately stay parked (its callback timeout),
 *       or a genuine late callback is rejected
 * </ul>
 *
 * <h2>Security ({@code security.*})</h2>
 *
 * <ul>
 *   <li>{@code security.provider} — the authentication provider (default {@value
 *       #DEFAULT_SECURITY_PROVIDER} — no authentication; suitable only for a trusted/isolated
 *       network). Set to a real provider ({@code jwt} or {@code apikey}) to enforce access control
 *   <li>{@code security.insecure_mode.enabled} — acknowledges running {@code noop} on a
 *       network-reachable interface, which {@link SagaServer} otherwise refuses (default {@value
 *       #DEFAULT_INSECURE_MODE_ENABLED})
 *   <li>{@code security.jwt.*} / {@code security.apikey.*} — the selected provider's own settings,
 *       documented on {@code JwtConfig} and {@code ApiKeyConfig}, which parse them. Their names are
 *       still checked here: a provider is built only when it is the selected one, and even then it
 *       notices only a missing <em>required</em> key
 * </ul>
 *
 * <h2>Secret references and unknown keys</h2>
 *
 * <p>Any {@code scalar.db.saga.*} value may use a secret reference — {@code ${file:UTF-8:/path}}
 * (preferred; e.g. a Kubernetes mounted Secret) or {@code ${env:NAME}} — resolved at load time. See
 * {@link SecretResolver}. The {@code ${file:...}} form is resolved <b>only</b> in this {@code
 * scalar.db.saga.*} namespace. {@code scalar.db.*} store keys are instead left for ScalarDB to
 * resolve, and ScalarDB supports only {@code ${env:...}} and {@code ${sys:...}} — not {@code
 * ${file:...}}. Use {@code ${env:...}} for {@code scalar.db.*} store secrets; a {@code ${file:...}}
 * reference there passes through verbatim and fails later at DB-connect time.
 *
 * <p>An unrecognized {@code scalar.db.saga.server.*} key fails startup. Every key here has a
 * default or is optional, so a typo would otherwise be indistinguishable from leaving the setting
 * unset — silently serving traffic under a policy the operator believes they changed.
 *
 * <p>A blank value means unset: the key takes its default, as an absent key does, so a templated
 * configuration whose variable resolves empty starts on documented defaults rather than crashing.
 * Keys where that default would leave a protection <b>off</b> reject a blank value instead. Blank
 * is never a deliberate way to disable a control, since omitting the key already says that, so an
 * empty value there is far more likely a template that failed to resolve than an intent to run
 * without the protection. That covers {@code callback.max_age_seconds}, {@code
 * max_start_requests_per_minute}, and {@code tls.enabled}, whose defaults disable the check
 * outright, plus, inside a service file, the settings whose blank fallback would be open ({@code
 * allowed_hosts} would admit any host; a {@code header.<HeaderName>} would send an empty header,
 * and an empty {@code Authorization} is an unauthenticated call) or meaningless ({@code base_url}
 * has no default to fall back to). A service file's {@code max_body_bytes} sits on the other side
 * of that line deliberately: unset leaves the engine's own 1 MiB cap in place, so the body stays
 * bounded either way.
 *
 * <p>All other properties configure the saga engine's persistence (e.g. ScalarDB connection
 * settings and the {@code scalar.db.saga.store.*} keys documented on {@code
 * ScalarDbSagaStoreFactory}) and are forwarded as-is. In daemon mode, {@code
 * scalar.db.saga.store.max_event_payload_bytes} defaults to 1 MiB when unset, bounding how large an
 * event payload a caller can persist; set it explicitly to override.
 */
public final class SagaServerConfig {

  // The daemon's config namespace: the base for every key below, and the boundary within which
  // secret references are resolved (scalar.db.* store keys are left for ScalarDB — see
  // resolveSecrets).
  static final String PREFIX = "scalar.db.saga.";
  static final String SERVER_PREFIX = PREFIX + "server.";

  // Mirrors the store's saga-ID discipline: the owner id lands in state rows and log lines, so it
  // gets the same character set and length bound.
  private static final java.util.regex.Pattern OWNER_ID_PATTERN =
      java.util.regex.Pattern.compile("[a-zA-Z0-9._-]{1,128}");

  static final String HOST_KEY = SERVER_PREFIX + "host";
  static final String OWNER_ID_KEY = SERVER_PREFIX + "owner_id";
  static final String DEFINITIONS_PATH_KEY = SERVER_PREFIX + "definitions_path";
  static final String DEFAULT_SAGA_TIMEOUT_MILLIS_KEY =
      SERVER_PREFIX + "default_saga_timeout_millis";
  static final String MAX_START_REQUESTS_PER_MINUTE_KEY =
      SERVER_PREFIX + "max_start_requests_per_minute";

  static final String HTTP_PREFIX = SERVER_PREFIX + "http.";
  static final String HTTP_ENABLED_KEY = HTTP_PREFIX + "enabled";
  static final String HTTP_PORT_KEY = HTTP_PREFIX + "port";
  static final String HTTP_MAX_THREADS_KEY = HTTP_PREFIX + "max_threads";
  static final String HTTP_MIN_THREADS_KEY = HTTP_PREFIX + "min_threads";
  static final String HTTP_MAX_QUEUED_REQUESTS_KEY = HTTP_PREFIX + "max_queued_requests";

  static final String GRPC_PREFIX = SERVER_PREFIX + "grpc.";
  static final String GRPC_ENABLED_KEY = GRPC_PREFIX + "enabled";
  static final String GRPC_PORT_KEY = GRPC_PREFIX + "port";
  static final String GRPC_MAX_INBOUND_METADATA_BYTES_KEY =
      GRPC_PREFIX + "max_inbound_metadata_bytes";

  // The TLS keys configure one feature (native TLS on both transports), so they share one group.
  // They take paths, never contents: paths keep the material re-readable for a future reload and
  // the operator's Secret mount the single source of the bytes. The path values themselves still
  // follow the redaction rule — errors name the key only, since a mis-pasted secret reference
  // arrives here as the resolved secret.
  static final String TLS_PREFIX = SERVER_PREFIX + "tls.";
  static final String TLS_ENABLED_KEY = TLS_PREFIX + "enabled";
  static final String TLS_CERT_CHAIN_PATH_KEY = TLS_PREFIX + "cert_chain_path";
  static final String TLS_PRIVATE_KEY_PATH_KEY = TLS_PREFIX + "private_key_path";

  static final String SYNC_PREFIX = SERVER_PREFIX + "sync.";
  static final String SYNC_TIMEOUT_MILLIS_KEY = SYNC_PREFIX + "timeout_millis";
  static final String SYNC_MAX_WAIT_MILLIS_KEY = SYNC_PREFIX + "max_wait_millis";

  static final String SHUTDOWN_PREFIX = SERVER_PREFIX + "shutdown.";
  static final String SHUTDOWN_MODE_KEY = SHUTDOWN_PREFIX + "mode";
  static final String SHUTDOWN_TIMEOUT_MILLIS_KEY = SHUTDOWN_PREFIX + "timeout_millis";

  static final String DETAIL_PREFIX = SERVER_PREFIX + "detail.";
  static final String DETAIL_MAX_TIMELINE_EVENTS_KEY = DETAIL_PREFIX + "max_timeline_events";
  static final String MAX_CONCURRENT_SAGA_EXECUTIONS_KEY =
      SERVER_PREFIX + "max_concurrent_saga_executions";

  static final String RECOVERY_PREFIX = SERVER_PREFIX + "recovery.";
  static final String RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY =
      RECOVERY_PREFIX + "staleness_threshold_millis";
  static final String RECOVERY_INTERVAL_SECONDS_KEY = RECOVERY_PREFIX + "interval_seconds";
  static final String RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY =
      RECOVERY_PREFIX + "compensation_grace_period_seconds";
  static final String RECOVERY_MAX_RECOVERIES_PER_SWEEP_KEY =
      RECOVERY_PREFIX + "max_recoveries_per_sweep";
  static final String RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY =
      RECOVERY_PREFIX + "max_concurrent_recoveries";

  static final String RETENTION_PREFIX = SERVER_PREFIX + "retention.";
  static final String RETENTION_PERIOD_SECONDS_KEY = RETENTION_PREFIX + "period_seconds";
  static final String RETENTION_INTERVAL_SECONDS_KEY = RETENTION_PREFIX + "interval_seconds";
  static final String RETENTION_MAX_PURGES_PER_PASS_KEY = RETENTION_PREFIX + "max_purges_per_pass";
  static final String RETENTION_MAX_CONCURRENT_PURGES_KEY =
      RETENTION_PREFIX + "max_concurrent_purges";

  // The callback keys configure one feature (async step completion), so they share one group rather
  // than splitting the secret away from the URL it authenticates.
  static final String CALLBACK_PREFIX = SERVER_PREFIX + "callback.";
  static final String CALLBACK_BASE_URL_KEY = CALLBACK_PREFIX + "base_url";
  static final String CALLBACK_SECRET_KEY = CALLBACK_PREFIX + "secret";
  static final String CALLBACK_MAX_AGE_SECONDS_KEY = CALLBACK_PREFIX + "max_age_seconds";

  // Security keys — daemon-only (embedded mode delegates auth to the host framework). The
  // per-provider namespaces are parsed by the security package; their names are still checked
  // here, since a provider notices only a missing required key (see SECURITY_PROVIDER_KEYS).
  static final String SECURITY_PREFIX = SERVER_PREFIX + "security.";
  static final String SECURITY_PROVIDER_KEY = SECURITY_PREFIX + "provider";
  static final String INSECURE_MODE_ENABLED_KEY = SECURITY_PREFIX + "insecure_mode.enabled";
  static final String SECURITY_JWT_PREFIX = SECURITY_PREFIX + "jwt.";
  static final String SECURITY_APIKEY_PREFIX = SECURITY_PREFIX + "apikey.";
  static final String SECURITY_APIKEY_HEADER_KEY = SECURITY_APIKEY_PREFIX + "header";
  static final String SECURITY_APIKEY_KEY_PREFIX = SECURITY_APIKEY_PREFIX + "key.";
  static final String SECURITY_APIKEY_SECRET_SUFFIX = ".secret";
  static final String SECURITY_APIKEY_ROLES_SUFFIX = ".roles";
  static final String SECURITY_APIKEY_PRINCIPAL_SUFFIX = ".principal";

  static final String SERVICE_KEY_PREFIX = SERVER_PREFIX + "service.";
  static final String SERVICES_PATH_KEY = SERVER_PREFIX + "services_path";
  static final String RELOAD_PREFIX = SERVER_PREFIX + "reload.";
  static final String RELOAD_INTERVAL_SECONDS_KEY = RELOAD_PREFIX + "interval_seconds";
  static final String SECRETS_ROOT_KEY = SERVER_PREFIX + "secrets_root";
  static final String EGRESS_ALLOWED_HOSTS_CEILING_KEY =
      SERVER_PREFIX + "egress.allowed_hosts_ceiling";

  static final long DEFAULT_RELOAD_INTERVAL_SECONDS = 30L;
  static final String DEFAULT_SECRETS_ROOT = "/run/secrets";

  static final String STORE_MAX_EVENT_PAYLOAD_BYTES_KEY = PREFIX + "store.max_event_payload_bytes";

  static final String DEFAULT_HOST = "0.0.0.0";
  static final int DEFAULT_HTTP_PORT = 12080;
  static final int DEFAULT_GRPC_PORT = 12051;
  static final boolean DEFAULT_HTTP_ENABLED = true;
  static final boolean DEFAULT_GRPC_ENABLED = true;
  static final boolean DEFAULT_TLS_ENABLED = false; // plaintext; a mesh/ingress terminates TLS
  static final int DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES = 8 * 1024;
  static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 1_048_576; // 1 MiB
  // 0 = no extra tightening, NOT an unbounded wait: sync.max_wait_millis is the ceiling and always
  // applies, on both transports.
  static final long DEFAULT_SYNC_TIMEOUT_MILLIS = 0L;
  static final long DEFAULT_SYNC_MAX_WAIT_MILLIS =
      60_000L; // ceiling on a synchronous server-side wait
  static final ShutdownMode DEFAULT_SHUTDOWN_MODE = DefaultSagaOrchestrator.DEFAULT_SHUTDOWN_MODE;
  static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS =
      DefaultSagaOrchestrator.DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;
  // Deliberately bounded here even though the embedded engine defaults to unbounded: the daemon
  // serves the detail over a network, where a pathological timeline exceeds the gRPC client's
  // default 4 MB inbound cap. 1000 events covers every realistic saga (a 100-step saga that fully
  // compensated and was recovered a few times stays under ~500). The byte estimate needs the
  // reader's per-entry cap as well; each entry's detail text is cut at 1024 chars on read (step
  // failure messages are stored verbatim, bounded only by store.max_event_payload_bytes), so 1000
  // capped entries stay around 1.5 MB on the wire.
  static final int DEFAULT_DETAIL_MAX_TIMELINE_EVENTS = 1000;
  static final String DEFAULT_SECURITY_PROVIDER =
      "noop"; // no authentication (see NoopSecurityProvider)
  static final boolean DEFAULT_INSECURE_MODE_ENABLED = false; // must be enabled to run noop exposed
  static final long DEFAULT_CALLBACK_MAX_AGE_SECONDS = 0L; // 0 = disabled (no iat TTL)
  static final int DEFAULT_MAX_THREADS = 200; // Jetty's own default
  static final int DEFAULT_MIN_THREADS = 8; // Jetty's own default
  // Default queue cap = this multiple of maxThreads, bounding worst-case queueing delay to about
  // this many request service-times before the server sheds load.
  static final int DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD = 2;
  static final long DEFAULT_SAGA_TIMEOUT_MILLIS =
      DefaultSagaOrchestrator.DEFAULT_SAGA_TIMEOUT_MILLIS;
  static final int DEFAULT_MAX_START_REQUESTS_PER_MINUTE = 0; // 0 = disabled (no rate limiting)

  /**
   * Returns the maximum number of sagas that may execute concurrently, or {@code 0} for no cap (the
   * default). A start arriving at the cap is refused with {@code DB-SAGA-20006} and HTTP 503 / gRPC
   * {@code UNAVAILABLE}; nothing is persisted, so the saga does not exist and its ID stays free.
   *
   * <p>A permit is held per drive, so a parked saga holds none, and resumes, recovery and admin
   * drives are never refused.
   *
   * <p><b>Sizing.</b> In-flight population is arrival rate times saga duration, so start from the
   * rate you intend to serve and the duration you measure, add burst headroom, and bound the result
   * so worst-case saga latency stays inside {@code recovery.staleness_threshold_millis} — past it,
   * recovery starts claiming sagas that are still running, which is the collapse this prevents.
   *
   * <p><b>Size it together with {@code max_start_requests_per_minute}</b>, which bounds a different
   * quantity: that limits how often one principal may ask; this limits how much work the engine is
   * doing for everyone. Neither substitutes for the other — a rate limit cannot see duration, so a
   * downstream slowdown multiplies the in-flight population at an unchanged arrival rate, while a
   * cap alone leaves one caller free to spend everyone's capacity on cheap refusals. Both are off
   * by default. The check that connects them: aggregate admitted arrival (principals times
   * per-principal limit) times typical duration should sit comfortably below this cap, so the rate
   * limiter does the everyday shaping and the cap is the backstop for a duration blowout. If it
   * sits above, callers inside their limits are refused routinely and "server full" stops being an
   * exceptional signal.
   */
  public int maxConcurrentSagaExecutions() {
    return maxConcurrentSagaExecutions;
  }

  /**
   * Every key parsed here, for the unknown-key check. Keys the security package parses are listed
   * in {@link #SECURITY_PROVIDER_KEYS} instead, and a key under one of {@link #DELEGATED_PREFIXES}
   * carries an operator-chosen segment, so neither kind is in this set.
   */
  private static final Set<String> KNOWN_KEYS =
      Set.of(
          HOST_KEY,
          OWNER_ID_KEY,
          DEFINITIONS_PATH_KEY,
          SERVICES_PATH_KEY,
          RELOAD_INTERVAL_SECONDS_KEY,
          SECRETS_ROOT_KEY,
          EGRESS_ALLOWED_HOSTS_CEILING_KEY,
          DEFAULT_SAGA_TIMEOUT_MILLIS_KEY,
          MAX_START_REQUESTS_PER_MINUTE_KEY,
          HTTP_ENABLED_KEY,
          HTTP_PORT_KEY,
          HTTP_MAX_THREADS_KEY,
          HTTP_MIN_THREADS_KEY,
          HTTP_MAX_QUEUED_REQUESTS_KEY,
          GRPC_ENABLED_KEY,
          GRPC_PORT_KEY,
          GRPC_MAX_INBOUND_METADATA_BYTES_KEY,
          TLS_ENABLED_KEY,
          TLS_CERT_CHAIN_PATH_KEY,
          TLS_PRIVATE_KEY_PATH_KEY,
          SYNC_TIMEOUT_MILLIS_KEY,
          SYNC_MAX_WAIT_MILLIS_KEY,
          SHUTDOWN_MODE_KEY,
          SHUTDOWN_TIMEOUT_MILLIS_KEY,
          DETAIL_MAX_TIMELINE_EVENTS_KEY,
          MAX_CONCURRENT_SAGA_EXECUTIONS_KEY,
          RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY,
          RECOVERY_INTERVAL_SECONDS_KEY,
          RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY,
          RECOVERY_MAX_RECOVERIES_PER_SWEEP_KEY,
          RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY,
          RETENTION_PERIOD_SECONDS_KEY,
          RETENTION_INTERVAL_SECONDS_KEY,
          RETENTION_MAX_PURGES_PER_PASS_KEY,
          RETENTION_MAX_CONCURRENT_PURGES_KEY,
          CALLBACK_BASE_URL_KEY,
          CALLBACK_SECRET_KEY,
          CALLBACK_MAX_AGE_SECONDS_KEY,
          SECURITY_PROVIDER_KEY,
          INSECURE_MODE_ENABLED_KEY);

  /**
   * Keys the security package parses, listed here only so the unknown-key check accepts them. They
   * are fixed names, and the provider that owns them notices only a missing <em>required</em> key.
   * An optional one has a default, so a typo there reads as unset: a mistyped {@code
   * principal_claim} authenticates on the default claim, and a mistyped {@code token_type} drops a
   * check the operator asked for. A provider is built only when it is the selected one, so for the
   * others nothing parses these keys at all. {@code JwtConfig} and {@code ApiKeyConfig} own the
   * names, which are not visible from this package; their tests assert this list stays in step.
   */
  private static final Set<String> SECURITY_PROVIDER_KEYS = securityProviderKeys();

  private static Set<String> securityProviderKeys() {
    Set<String> keys = new TreeSet<>();
    for (String name :
        List.of(
            "jwks_url",
            "issuer",
            "audience",
            "token_type",
            "principal_claim",
            "roles_claim",
            "connect_timeout_millis",
            "read_timeout_millis")) {
      keys.add(SECURITY_JWT_PREFIX + name);
    }
    keys.add(SECURITY_APIKEY_HEADER_KEY);
    return Collections.unmodifiableSet(keys);
  }

  /**
   * Namespaces whose keys carry an operator-chosen segment, so they cannot be enumerated. Only that
   * one segment is the operator's; the sub-settings around it are fixed names, still checked key by
   * key (see {@link #rejectUnknownApiKeySetting}). The rest of {@code security.} has no such
   * segment and is enumerated in {@link #SECURITY_PROVIDER_KEYS}.
   */
  private static final List<String> DELEGATED_PREFIXES = List.of(SECURITY_APIKEY_KEY_PREFIX);

  private final String host;
  private final String ownerId;
  private final boolean httpEnabled;
  private final int httpPort;
  private final int httpMaxThreads;
  private final int httpMinThreads;
  private final int httpMaxQueuedRequests;
  private final boolean grpcEnabled;
  private final int grpcPort;
  private final int grpcMaxInboundMetadataBytes;
  private final int grpcMaxInboundMessageBytes;
  private final boolean tlsEnabled;
  // Whether tls.enabled appeared in the properties at all. Consumed only by validateCombinations,
  // which treats paths-with-absent-switch as a forgotten 'true' but paths-with-explicit-false as a
  // deliberate toggle-off.
  private final boolean tlsEnabledKeySet;
  private final @Nullable Path tlsCertChainPath;
  private final @Nullable Path tlsPrivateKeyPath;
  private final long syncTimeoutMillis;
  private final long syncMaxWaitMillis;
  private final ShutdownMode shutdownMode;
  private final long shutdownTimeoutMillis;
  private final int detailMaxTimelineEvents;
  private final int maxConcurrentSagaExecutions;
  private final RecoveryConfig recoveryConfig;
  private final RetentionConfig retentionConfig;
  private final String securityProvider;
  private final boolean insecureModeEnabled;
  private final @Nullable String callbackSecret;
  private final @Nullable String callbackBaseUrl;
  private final long callbackMaxAgeSeconds;
  private final long defaultSagaTimeoutMillis;
  private final int maxStartRequestsPerMinute;
  private final Properties properties;
  private final Properties rawProperties;
  private final @Nullable Path definitionsPath;
  private final ReloadConfig reloadConfig;

  /**
   * Parses every setting from the already secret-resolved {@code properties}, then cross-checks the
   * combinations that are individually valid but jointly unusable.
   *
   * @param resolved the secret-resolved properties
   * @param raw the pre-resolution properties (see {@link #rawProperties()})
   */
  private SagaServerConfig(Properties resolved, Properties raw) {
    rejectUnknownKeys(resolved);
    this.host = parseHost(resolved.getProperty(HOST_KEY));
    this.ownerId = parseOwnerId(resolved.getProperty(OWNER_ID_KEY));
    this.httpEnabled =
        parseBoolean(
            resolved.getProperty(HTTP_ENABLED_KEY), HTTP_ENABLED_KEY, DEFAULT_HTTP_ENABLED);
    this.httpPort =
        parsePort(resolved.getProperty(HTTP_PORT_KEY), HTTP_PORT_KEY, DEFAULT_HTTP_PORT);
    this.httpMaxThreads =
        parseBoundedInt(
            resolved.getProperty(HTTP_MAX_THREADS_KEY),
            HTTP_MAX_THREADS_KEY,
            DEFAULT_MAX_THREADS,
            1);
    this.httpMinThreads =
        parseBoundedInt(
            resolved.getProperty(HTTP_MIN_THREADS_KEY),
            HTTP_MIN_THREADS_KEY,
            DEFAULT_MIN_THREADS,
            1);
    // Cap the handler-thread backlog. Unset defaults to a multiple of maxThreads so the worst-case
    // queueing delay stays proportional to the pool (about that many request service-times) rather
    // than being an absolute number that means wildly different latency at different pool sizes.
    // Computed and clamped in long so a large maxThreads cannot overflow the default.
    this.httpMaxQueuedRequests =
        parseBoundedInt(
            resolved.getProperty(HTTP_MAX_QUEUED_REQUESTS_KEY),
            HTTP_MAX_QUEUED_REQUESTS_KEY,
            (int)
                Math.min(
                    (long) DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD * httpMaxThreads,
                    Integer.MAX_VALUE),
            1);
    this.grpcEnabled =
        parseBoolean(
            resolved.getProperty(GRPC_ENABLED_KEY), GRPC_ENABLED_KEY, DEFAULT_GRPC_ENABLED);
    this.grpcPort =
        parsePort(resolved.getProperty(GRPC_PORT_KEY), GRPC_PORT_KEY, DEFAULT_GRPC_PORT);
    this.grpcMaxInboundMetadataBytes =
        parseBoundedInt(
            resolved.getProperty(GRPC_MAX_INBOUND_METADATA_BYTES_KEY),
            GRPC_MAX_INBOUND_METADATA_BYTES_KEY,
            DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES,
            1);
    // tls.enabled rejects blank (its default leaves the protection off); the paths follow the
    // ordinary blank-is-unset rule, so the pairing checks in validateCombinations see a blank path
    // exactly as an absent one.
    String tlsEnabledValue =
        requireNonBlankIfSet(TLS_ENABLED_KEY, resolved.getProperty(TLS_ENABLED_KEY));
    this.tlsEnabledKeySet = tlsEnabledValue != null;
    this.tlsEnabled = parseBoolean(tlsEnabledValue, TLS_ENABLED_KEY, DEFAULT_TLS_ENABLED);
    this.tlsCertChainPath =
        parseOptionalPath(resolved.getProperty(TLS_CERT_CHAIN_PATH_KEY), TLS_CERT_CHAIN_PATH_KEY);
    this.tlsPrivateKeyPath =
        parseOptionalPath(resolved.getProperty(TLS_PRIVATE_KEY_PATH_KEY), TLS_PRIVATE_KEY_PATH_KEY);
    this.syncTimeoutMillis =
        parseBoundedLong(
            resolved.getProperty(SYNC_TIMEOUT_MILLIS_KEY),
            SYNC_TIMEOUT_MILLIS_KEY,
            DEFAULT_SYNC_TIMEOUT_MILLIS,
            0L);
    this.syncMaxWaitMillis =
        parseBoundedLong(
            resolved.getProperty(SYNC_MAX_WAIT_MILLIS_KEY),
            SYNC_MAX_WAIT_MILLIS_KEY,
            DEFAULT_SYNC_MAX_WAIT_MILLIS,
            1L);
    this.shutdownMode = parseShutdownMode(resolved.getProperty(SHUTDOWN_MODE_KEY));
    // 0 is a real setting, not a disabled one: it drains nothing and cancels in flight work at
    // once. The engine accepts it, so the daemon must too, or daemon mode cannot express a
    // configuration embedded mode can.
    this.shutdownTimeoutMillis =
        parseBoundedLong(
            resolved.getProperty(SHUTDOWN_TIMEOUT_MILLIS_KEY),
            SHUTDOWN_TIMEOUT_MILLIS_KEY,
            DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
            0L);
    this.detailMaxTimelineEvents =
        parseBoundedInt(
            resolved.getProperty(DETAIL_MAX_TIMELINE_EVENTS_KEY),
            DETAIL_MAX_TIMELINE_EVENTS_KEY,
            DEFAULT_DETAIL_MAX_TIMELINE_EVENTS,
            1);
    this.maxConcurrentSagaExecutions =
        parseBoundedInt(
            resolved.getProperty(MAX_CONCURRENT_SAGA_EXECUTIONS_KEY),
            MAX_CONCURRENT_SAGA_EXECUTIONS_KEY,
            DefaultSagaOrchestrator.DEFAULT_MAX_CONCURRENT_SAGA_EXECUTIONS,
            0);
    this.recoveryConfig = parseRecoveryConfig(resolved);
    this.retentionConfig = parseRetentionConfig(resolved);
    this.securityProvider = parseSecurityProvider(resolved.getProperty(SECURITY_PROVIDER_KEY));
    this.insecureModeEnabled =
        parseBoolean(
            resolved.getProperty(INSECURE_MODE_ENABLED_KEY),
            INSECURE_MODE_ENABLED_KEY,
            DEFAULT_INSECURE_MODE_ENABLED);
    // Treat blank as unset (no callback auth configured → no callback route). Do not trim: an HMAC
    // secret is opaque and could legitimately contain leading/trailing characters.
    String callbackSecretValue = resolved.getProperty(CALLBACK_SECRET_KEY);
    this.callbackSecret =
        (callbackSecretValue == null || callbackSecretValue.isBlank()) ? null : callbackSecretValue;
    this.callbackBaseUrl = parseCallbackBaseUrl(resolved.getProperty(CALLBACK_BASE_URL_KEY));
    this.callbackMaxAgeSeconds =
        parseBoundedLong(
            requireNonBlankIfSet(
                CALLBACK_MAX_AGE_SECONDS_KEY, resolved.getProperty(CALLBACK_MAX_AGE_SECONDS_KEY)),
            CALLBACK_MAX_AGE_SECONDS_KEY,
            DEFAULT_CALLBACK_MAX_AGE_SECONDS,
            0L);
    this.defaultSagaTimeoutMillis =
        parseBoundedLong(
            resolved.getProperty(DEFAULT_SAGA_TIMEOUT_MILLIS_KEY),
            DEFAULT_SAGA_TIMEOUT_MILLIS_KEY,
            DEFAULT_SAGA_TIMEOUT_MILLIS,
            0L);
    this.maxStartRequestsPerMinute =
        parseBoundedInt(
            requireNonBlankIfSet(
                MAX_START_REQUESTS_PER_MINUTE_KEY,
                resolved.getProperty(MAX_START_REQUESTS_PER_MINUTE_KEY)),
            MAX_START_REQUESTS_PER_MINUTE_KEY,
            DEFAULT_MAX_START_REQUESTS_PER_MINUTE,
            0);
    this.definitionsPath =
        parseOptionalPath(resolved.getProperty(DEFINITIONS_PATH_KEY), DEFINITIONS_PATH_KEY);
    this.reloadConfig = parseReloadConfig(resolved);
    this.properties = applyStoreDefaults(copyOf(resolved));
    this.grpcMaxInboundMessageBytes = parseGrpcMaxInboundMessageBytes(this.properties);
    this.rawProperties = copyOf(raw);
    validateCombinations();
  }

  /**
   * Checks the settings that are each valid alone but contradict one another. Every check here
   * would otherwise surface as a late, confusing failure — a bind error, a definition that cannot
   * be registered, or a feature that silently never activates.
   */
  private void validateCombinations() {
    if (!httpEnabled && !grpcEnabled) {
      throw new IllegalArgumentException(
          "At least one transport must be enabled, but '"
              + HTTP_ENABLED_KEY
              + "' and '"
              + GRPC_ENABLED_KEY
              + "' are both false. A server that exposes no transport can serve no requests.");
    }
    // Port 0 is exempt: each transport then binds its own ephemeral port, so they cannot collide.
    if (httpEnabled && grpcEnabled && httpPort != 0 && httpPort == grpcPort) {
      // The colliding number stays out of the message: echoing it would confirm that a numeric
      // secret resolved onto one port key equals the other key's port.
      throw new IllegalArgumentException(
          "'"
              + HTTP_PORT_KEY
              + "' and '"
              + GRPC_PORT_KEY
              + "' are set to the same port, but each transport binds its own listener. Give them"
              + " different ports, or disable one transport.");
    }
    if (httpMinThreads > httpMaxThreads) {
      throw new IllegalArgumentException(
          "'" + HTTP_MIN_THREADS_KEY + "' must not exceed '" + HTTP_MAX_THREADS_KEY + "'.");
    }
    // Provisioning a callback needs the URL to hand out; authenticating one needs the secret.
    // Either alone is a half-configured feature that fails only once an async saga runs.
    if ((callbackBaseUrl == null) != (callbackSecret == null)) {
      String missing = callbackBaseUrl == null ? CALLBACK_BASE_URL_KEY : CALLBACK_SECRET_KEY;
      String present = callbackBaseUrl == null ? CALLBACK_SECRET_KEY : CALLBACK_BASE_URL_KEY;
      throw new IllegalArgumentException(
          "'"
              + present
              + "' is set but '"
              + missing
              + "' is not. Async step completion needs both — the base URL to build the callback"
              + " URL handed to a participant, and the secret to authenticate the callback it sends"
              + " back. Set both, or neither to leave async completion disabled.");
    }
    // TLS needs both halves of the key pair; either alone is a half-configured feature that would
    // otherwise surface only as handshake failures after the ports are already serving.
    if (tlsEnabled && (tlsCertChainPath == null || tlsPrivateKeyPath == null)) {
      boolean bothMissing = tlsCertChainPath == null && tlsPrivateKeyPath == null;
      String missing =
          bothMissing
              ? "'" + TLS_CERT_CHAIN_PATH_KEY + "' and '" + TLS_PRIVATE_KEY_PATH_KEY + "' are"
              : "'"
                  + (tlsCertChainPath == null ? TLS_CERT_CHAIN_PATH_KEY : TLS_PRIVATE_KEY_PATH_KEY)
                  + "' is";
      throw new IllegalArgumentException(
          "'"
              + TLS_ENABLED_KEY
              + "' is true but "
              + missing
              + " not set. Serving TLS needs both the certificate chain and its private key. Set"
              + " both paths, or set '"
              + TLS_ENABLED_KEY
              + "=false' to serve plaintext.");
    }
    // Material without the switch: an operator who mounts certificates but forgets tls.enabled=true
    // would silently serve plaintext. Explicit tls.enabled=false stays legal — that is the
    // deliberate "toggle TLS off, leave the material mounted" move.
    if (!tlsEnabledKeySet && (tlsCertChainPath != null || tlsPrivateKeyPath != null)) {
      String present =
          tlsCertChainPath != null ? TLS_CERT_CHAIN_PATH_KEY : TLS_PRIVATE_KEY_PATH_KEY;
      throw new IllegalArgumentException(
          "'"
              + present
              + "' is set but '"
              + TLS_ENABLED_KEY
              + "' is not. Set '"
              + TLS_ENABLED_KEY
              + "=true' to serve TLS with this material, or '"
              + TLS_ENABLED_KEY
              + "=false' to keep it unused and serve plaintext. The explicit switch is required so"
              + " a forgotten 'true' cannot leave the server silently serving plaintext with"
              + " certificates mounted.");
    }
  }

  /**
   * Applies daemon-mode defaults to the forwarded store properties — currently a bounded
   * event-payload size, so a client cannot persist arbitrarily large payloads. The operator
   * overrides it by setting the key explicitly.
   */
  private static Properties applyStoreDefaults(Properties properties) {
    String maxPayload = properties.getProperty(STORE_MAX_EVENT_PAYLOAD_BYTES_KEY);
    // Treat blank as unset (like the port and definitions_path); otherwise a blank value would
    // propagate to the store factory and fail to parse as an integer, crashing startup.
    if (maxPayload == null || maxPayload.isBlank()) {
      properties.setProperty(
          STORE_MAX_EVENT_PAYLOAD_BYTES_KEY, Integer.toString(DEFAULT_MAX_EVENT_PAYLOAD_BYTES));
    }
    return properties;
  }

  /**
   * Parses a {@link SagaServerConfig} from properties. The same object is reused for both the
   * server settings and the ScalarDB store connection.
   *
   * @param properties server + ScalarDB properties
   * @return the parsed configuration
   * @throws IllegalArgumentException if a key is unrecognized, a value is invalid, or two settings
   *     contradict each other
   */
  public static SagaServerConfig load(Properties properties) {
    Objects.requireNonNull(properties, "properties must not be null");
    // Keep the pre-resolution properties so a provider can tell a secret reference from an inline
    // value (both look identical after resolution) — e.g. the API-key provider requires references.
    return new SagaServerConfig(resolveSecrets(properties), properties);
  }

  /**
   * Fails on any {@code scalar.db.saga.server.*} key this class does not recognize. Without this a
   * misspelled key is silently dropped and the setting stays at its default, which for a bound or a
   * security-adjacent setting means the daemon serves traffic under a policy the operator believes
   * they changed.
   */
  private static void rejectUnknownKeys(Properties properties) {
    for (String key : new TreeSet<>(properties.stringPropertyNames())) {
      if (!key.startsWith(SERVER_PREFIX)
          || KNOWN_KEYS.contains(key)
          || SECURITY_PROVIDER_KEYS.contains(key)) {
        continue;
      }
      if (DELEGATED_PREFIXES.stream().anyMatch(key::startsWith)) {
        // The name segment is the operator's, but the settings around it are fixed.
        if (key.startsWith(SECURITY_APIKEY_KEY_PREFIX)) {
          rejectUnknownApiKeySetting(key);
        }
        continue;
      }
      if (key.startsWith(SERVICE_KEY_PREFIX)) {
        // The one unknown-key family with a known history: these keys were the pre-services_path
        // format, so the error names the migration instead of reading as a typo.
        throw new IllegalArgumentException(
            "'"
                + key
                + "': service configuration moved to services_path. Put each service in its own"
                + " <name>.properties file (keys base_url, allowed_hosts, max_body_bytes,"
                + " header.<HeaderName>) in the directory '"
                + SERVICES_PATH_KEY
                + "' points at, and remove the service.* keys.");
      }
      throw new IllegalArgumentException(
          "Unknown configuration key '"
              + key
              + "'. Check it against the keys documented on SagaServerConfig; unknown keys under '"
              + SERVER_PREFIX
              + "' are rejected so a typo cannot silently leave a setting at its default.");
    }
  }

  /**
   * Fails on a {@code security.apikey.key.<name>.<setting>} key whose trailing setting is not one
   * the API-key provider reads. Only {@code <name>} is the operator's; the three settings are fixed
   * names, and {@code principal} is optional, so a typo of it would otherwise record the key's own
   * name for audit instead of the principal configured. Split at the last dot rather than the
   * first: the provider derives the name by stripping the prefix and the suffix, so a name may
   * itself contain dots and the first dot need not end it.
   */
  private static void rejectUnknownApiKeySetting(String key) {
    String remainder = key.substring(SECURITY_APIKEY_KEY_PREFIX.length());
    int dot = remainder.lastIndexOf('.');
    String name = dot <= 0 ? "" : remainder.substring(0, dot);
    // Keep the leading dot so the setting matches the suffix constants directly.
    String setting = dot < 0 ? "" : remainder.substring(dot);
    boolean known =
        setting.equals(SECURITY_APIKEY_SECRET_SUFFIX)
            || setting.equals(SECURITY_APIKEY_ROLES_SUFFIX)
            || setting.equals(SECURITY_APIKEY_PRINCIPAL_SUFFIX);
    if (name.isBlank() || !known) {
      throw new IllegalArgumentException(
          "'"
              + key
              + "' is not a valid API-key setting. Use '"
              + SECURITY_APIKEY_KEY_PREFIX
              + "<name>"
              + SECURITY_APIKEY_SECRET_SUFFIX
              + "' and the other per-key settings documented on ApiKeyConfig. Valid settings are"
              + " secret, roles, and principal.");
    }
  }

  /**
   * Returns a copy of {@code properties} with secret references resolved in the daemon's own
   * namespace. Only {@code scalar.db.saga.*} values pass through {@link SecretResolver} (so an
   * operator can write {@code ${env:NAME}} / {@code ${file:UTF-8:/path}} in any daemon key); {@code
   * scalar.db.*} store keys are left untouched, since ScalarDB resolves those itself with the same
   * syntax.
   */
  private static Properties resolveSecrets(Properties properties) {
    SecretResolver resolver = new SecretResolver();
    // Rebuild from stringPropertyNames() so every string property is flattened into one table,
    // including any inherited from a defaults chain (new Properties(defaults)). A plain putAll or
    // copyOf would silently drop those inherited entries. Resolve secret references only within the
    // daemon's own scalar.db.saga.* namespace; scalar.db.* store keys pass through to ScalarDB.
    Properties resolved = new Properties();
    for (String key : properties.stringPropertyNames()) {
      String value = properties.getProperty(key);
      if (value == null) {
        continue; // stringPropertyNames() only lists string-valued keys; guard for null-safety
      }
      resolved.setProperty(key, key.startsWith(PREFIX) ? resolver.resolve(value) : value);
    }
    // Non-string entries aren't listed by stringPropertyNames(); carry them through. forEach covers
    // the main table only; a non-string entry in a defaults chain is intentionally not flattened
    // (Properties are conventionally string-only, and ScalarDB reads config via getProperty).
    properties.forEach(
        (key, value) -> {
          if (!(key instanceof String) || !(value instanceof String)) {
            resolved.put(key, value);
          }
        });
    return resolved;
  }

  /**
   * Builds the recovery configuration, taking each unset value from {@link
   * RecoveryConfig#defaults()} rather than restating it, so the daemon tracks the engine's defaults
   * automatically.
   */
  private static RecoveryConfig parseRecoveryConfig(Properties properties) {
    RecoveryConfig defaults = RecoveryConfig.defaults();
    return new RecoveryConfig(
        parseBoundedLong(
            properties.getProperty(RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY),
            RECOVERY_STALENESS_THRESHOLD_MILLIS_KEY,
            defaults.stalenessThresholdMillis(),
            1L),
        parseBoundedLong(
            properties.getProperty(RECOVERY_INTERVAL_SECONDS_KEY),
            RECOVERY_INTERVAL_SECONDS_KEY,
            defaults.intervalSeconds(),
            1L),
        Duration.ofSeconds(
            parseBoundedLong(
                properties.getProperty(RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY),
                RECOVERY_COMPENSATION_GRACE_PERIOD_SECONDS_KEY,
                defaults.compensationGracePeriod().toSeconds(),
                1L)),
        parseBoundedInt(
            properties.getProperty(RECOVERY_MAX_RECOVERIES_PER_SWEEP_KEY),
            RECOVERY_MAX_RECOVERIES_PER_SWEEP_KEY,
            defaults.maxRecoveriesPerSweep(),
            1),
        parseBoundedInt(
            properties.getProperty(RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY),
            RECOVERY_MAX_CONCURRENT_RECOVERIES_KEY,
            defaults.maxConcurrentRecoveries(),
            1),
        defaults.clock());
  }

  /** Builds the retention configuration, defaulting from {@link RetentionConfig#defaults()}. */
  private static RetentionConfig parseRetentionConfig(Properties properties) {
    RetentionConfig defaults = RetentionConfig.defaults();
    return new RetentionConfig(
        Duration.ofSeconds(
            parseBoundedLong(
                properties.getProperty(RETENTION_PERIOD_SECONDS_KEY),
                RETENTION_PERIOD_SECONDS_KEY,
                defaults.retentionPeriod().toSeconds(),
                1L)),
        parseBoundedLong(
            properties.getProperty(RETENTION_INTERVAL_SECONDS_KEY),
            RETENTION_INTERVAL_SECONDS_KEY,
            defaults.intervalSeconds(),
            1L),
        parseBoundedInt(
            properties.getProperty(RETENTION_MAX_PURGES_PER_PASS_KEY),
            RETENTION_MAX_PURGES_PER_PASS_KEY,
            defaults.maxPurgesPerPass(),
            1),
        parseBoundedInt(
            properties.getProperty(RETENTION_MAX_CONCURRENT_PURGES_KEY),
            RETENTION_MAX_CONCURRENT_PURGES_KEY,
            defaults.maxConcurrentPurges(),
            1),
        defaults.clock());
  }

  /**
   * Parses {@code egress.allowed_hosts_ceiling}, holding every entry to the same host shape a
   * service file's {@code allowed_hosts} must have.
   *
   * <p>An unshaped ceiling entry is the worst of the config typos this module can take: the ceiling
   * is compared against entries that ARE shaped like hosts, so one that never can be matches
   * nothing, and every service in the fleet is rejected as exceeding it — pointing the operator at
   * the service files rather than at the property they mistyped.
   */
  private static List<String> parseCeiling(String ceiling) {
    List<String> entries = parseCommaSeparated(EGRESS_ALLOWED_HOSTS_CEILING_KEY, ceiling);
    entries.forEach(
        host -> ServiceFileParser.requireHostShape(EGRESS_ALLOWED_HOSTS_CEILING_KEY, host));
    return entries;
  }

  /**
   * Parses the services-directory settings into a {@link ReloadConfig}: where the service files
   * live, how often they are re-read, where their secret references may resolve, and the optional
   * egress ceiling. The files themselves are read by the reconciler, not here.
   */
  private static ReloadConfig parseReloadConfig(Properties resolved) {
    String secretsRoot = resolved.getProperty(SECRETS_ROOT_KEY);
    String ceiling =
        requireNonBlankIfSet(
            EGRESS_ALLOWED_HOSTS_CEILING_KEY,
            resolved.getProperty(EGRESS_ALLOWED_HOSTS_CEILING_KEY));
    return new ReloadConfig(
        parseOptionalPath(resolved.getProperty(SERVICES_PATH_KEY), SERVICES_PATH_KEY),
        parseBoundedLong(
            resolved.getProperty(RELOAD_INTERVAL_SECONDS_KEY),
            RELOAD_INTERVAL_SECONDS_KEY,
            DEFAULT_RELOAD_INTERVAL_SECONDS,
            0L),
        Path.of(
            secretsRoot == null || secretsRoot.isBlank()
                ? DEFAULT_SECRETS_ROOT
                : secretsRoot.trim()),
        ceiling == null ? List.of() : parseCeiling(ceiling),
        Clock.systemUTC());
  }

  /**
   * One downstream service a declarative step can call: the base URL plus the outbound policy the
   * engine applies to every request to it. Produced by {@link ServiceFileParser} from a service
   * file; this class only points at the directory they live in.
   *
   * @param baseUrl the service base URL
   * @param allowedHosts the SSRF allowlist; empty allows any host
   * @param maxBodyBytes the request/response body cap in bytes, or {@code 0} for the engine default
   * @param headers headers sent on every request to this service — the channel for downstream
   *     authentication
   */
  public record ServiceConfig(
      String baseUrl, List<String> allowedHosts, long maxBodyBytes, Map<String, String> headers) {

    /** Copies the collections so the record is deeply immutable. */
    public ServiceConfig {
      allowedHosts = List.copyOf(allowedHosts);
      headers = Map.copyOf(headers);
    }
  }

  /**
   * Returns the host/interface both transports bind to (default {@value #DEFAULT_HOST} — all
   * interfaces, the norm for a container behind network controls). Under the {@code noop} security
   * provider the endpoints are unauthenticated, so {@link SagaServer} refuses to start on a
   * non-loopback host unless insecure mode is acknowledged.
   */
  public String host() {
    return host;
  }

  /**
   * Returns the identity this instance stamps on the sagas it claims during recovery, defaulting to
   * a random UUID generated per process. Configure it (e.g. from the pod name) to make a claim
   * traceable to a process; distinct live instances must never share a value.
   */
  public String ownerId() {
    return ownerId;
  }

  /**
   * Returns whether the HTTP (REST) transport is served (default {@code true}). When {@code false},
   * the server runs gRPC-only and binds no HTTP port. At least one of {@link #httpEnabled()} /
   * {@link #grpcEnabled()} is always {@code true}.
   */
  public boolean httpEnabled() {
    return httpEnabled;
  }

  /** Returns the configured HTTP port ({@code 0} binds an ephemeral port). */
  public int httpPort() {
    return httpPort;
  }

  /**
   * Returns the maximum size of the HTTP (Jetty) request-handling thread pool (default {@value
   * #DEFAULT_MAX_THREADS}). Caps concurrent request threads so a burst of slow requests cannot
   * exhaust threads.
   */
  public int httpMaxThreads() {
    return httpMaxThreads;
  }

  /**
   * Returns the minimum (core) size of the HTTP thread pool (default {@value
   * #DEFAULT_MIN_THREADS}).
   */
  public int httpMinThreads() {
    return httpMinThreads;
  }

  /**
   * Returns the cap on requests waiting for a handler thread once all {@link #httpMaxThreads()} are
   * busy. Beyond it the server sheds load (fast failure) instead of queueing unboundedly. Defaults
   * to {@value #DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD} × {@link #httpMaxThreads()}, keeping the
   * worst-case queueing delay proportional to the pool.
   */
  public int httpMaxQueuedRequests() {
    return httpMaxQueuedRequests;
  }

  /**
   * Returns whether the gRPC transport is served (default {@code true}). When {@code false}, the
   * server runs HTTP-only and binds no gRPC port. At least one of {@link #httpEnabled()} / {@link
   * #grpcEnabled()} is always {@code true}.
   */
  public boolean grpcEnabled() {
    return grpcEnabled;
  }

  /**
   * Returns the configured gRPC port ({@code 0} binds an ephemeral port). The gRPC server binds the
   * same {@link #host()} as HTTP, on its own listener; when both transports are enabled this
   * differs from {@link #httpPort()}.
   */
  public int grpcPort() {
    return grpcPort;
  }

  /**
   * Returns the cap on a gRPC call's total request metadata in bytes (default {@value
   * #DEFAULT_GRPC_MAX_INBOUND_METADATA_BYTES}), bounding how much header data an unauthenticated
   * caller can push. Raise it when legitimate credentials do not fit — a JWT access token with many
   * claims is the usual reason.
   */
  public int grpcMaxInboundMetadataBytes() {
    return grpcMaxInboundMetadataBytes;
  }

  /**
   * Returns the gRPC server's maximum inbound message size in bytes, aligned with the store's
   * max-event-payload cap so neither transport accepts an input the store would reject. The store's
   * {@code 0} ("no limit") is mapped to {@link Integer#MAX_VALUE} here, since gRPC reads {@code 0}
   * as "reject all non-empty messages". Defaults to {@value #DEFAULT_MAX_EVENT_PAYLOAD_BYTES}.
   */
  public int grpcMaxInboundMessageBytes() {
    return grpcMaxInboundMessageBytes;
  }

  /**
   * Whether the daemon serves TLS on both enabled transports (the {@code tls.enabled} key; default
   * {@value #DEFAULT_TLS_ENABLED}). When {@code true}, {@link #tlsCertChainPath()} and {@link
   * #tlsPrivateKeyPath()} are both present — the combination is validated at load.
   */
  public boolean tlsEnabled() {
    return tlsEnabled;
  }

  /**
   * Returns the path to the PEM certificate chain (leaf first) served on both transports, or empty
   * when TLS is disabled. A path configured alongside an explicit {@code tls.enabled=false} is
   * deliberately not returned: the material is ignored, which is what lets an operator toggle TLS
   * off without unmounting it.
   */
  public Optional<Path> tlsCertChainPath() {
    return tlsEnabled ? Optional.ofNullable(tlsCertChainPath) : Optional.empty();
  }

  /**
   * Returns the path to the PEM private key matching {@link #tlsCertChainPath()} — unencrypted
   * PKCS#8, RSA or EC — or empty when TLS is disabled, under the same ignore-when-off rule.
   */
  public Optional<Path> tlsPrivateKeyPath() {
    return tlsEnabled ? Optional.ofNullable(tlsPrivateKeyPath) : Optional.empty();
  }

  /**
   * Returns the synchronous-start timeout in milliseconds, or {@code 0} when unset (the default).
   * When positive it tightens {@link #syncMaxWaitMillis()}; when {@code 0} the ceiling alone
   * applies. A synchronous start that has not reached a terminal state within the resulting bound
   * returns {@code 202} and the saga keeps running on the engine's executor (the client polls
   * {@code GET /sagas/{id}}).
   */
  public long syncTimeoutMillis() {
    return syncTimeoutMillis;
  }

  /**
   * Returns the absolute ceiling (ms) on a synchronous start's server-side wait, for both
   * transports. {@link #syncTimeoutMillis()} and a gRPC client's call deadline can only tighten
   * this bound, never exceed it, so a synchronous start can never block indefinitely. Defaults to
   * {@value #DEFAULT_SYNC_MAX_WAIT_MILLIS} ms.
   */
  public long syncMaxWaitMillis() {
    return syncMaxWaitMillis;
  }

  /**
   * Returns the bound (ms) a synchronous start may wait: the ceiling {@link #syncMaxWaitMillis()},
   * tightened by {@code requestedCapMillis} and by {@link #syncTimeoutMillis()} when that is set.
   * Pass {@link Long#MAX_VALUE} when the caller has no cap of its own.
   *
   * <p>Derived state of two configuration keys, so it lives here rather than in either transport. A
   * transport that needs to tighten it further does so on top of this result, as the gRPC call
   * deadline does.
   *
   * <p>Deliberately {@code public} though only {@link SagaServer} calls it. Restricting it would
   * protect nothing while {@link #syncTimeoutMillis()} and {@link #syncMaxWaitMillis()} stay
   * public: combining that pair by hand is the mistake this method exists to prevent, so hiding the
   * safe method while leaving the raw one reachable is backwards. It also keeps the gRPC tests able
   * to resolve a bound through a real config instead of restating the policy.
   *
   * @param requestedCapMillis a caller-supplied cap, or {@code Long.MAX_VALUE} for none
   * @return the wait bound in milliseconds
   */
  public long syncWaitBoundMillis(long requestedCapMillis) {
    long bound = Math.min(syncMaxWaitMillis, requestedCapMillis);
    return syncTimeoutMillis > 0L ? Math.min(bound, syncTimeoutMillis) : bound;
  }

  /**
   * Returns how in-flight sagas are treated on shutdown (default {@link
   * ShutdownMode#WAIT_CURRENT_STEP}): finish the running step and leave the saga for recovery, or
   * wait for in-flight sagas to reach a terminal state.
   */
  public ShutdownMode shutdownMode() {
    return shutdownMode;
  }

  /**
   * Returns the ceiling (ms) on the saga-engine drain at shutdown (default {@value
   * #DEFAULT_SHUTDOWN_TIMEOUT_MILLIS}). Past it, whatever has not drained is abandoned and
   * reclaimed by the recovery scan after the next start. Raise it together with a container's
   * termination grace period when {@link #shutdownMode()} is {@link ShutdownMode#WAIT_ALL_SAGAS},
   * which waits for whole sagas rather than a single step.
   */
  public long shutdownTimeoutMillis() {
    return shutdownTimeoutMillis;
  }

  /**
   * Returns the maximum number of timeline events a single {@code getSagaDetail} read returns
   * (default {@value #DEFAULT_DETAIL_MAX_TIMELINE_EVENTS}). When a saga's history is longer, the
   * newest events are returned and the detail is flagged truncated; the full history remains in the
   * store.
   */
  public int detailMaxTimelineEvents() {
    return detailMaxTimelineEvents;
  }

  /**
   * Returns the crash-recovery configuration: how stale a saga must be to be reclaimed, how often
   * the scan runs, and how much work one pass may do.
   */
  public RecoveryConfig recoveryConfig() {
    return recoveryConfig;
  }

  /**
   * Returns the retention configuration: how long a terminal saga is kept, and the shape of the
   * purge that removes it afterwards.
   */
  public RetentionConfig retentionConfig() {
    return retentionConfig;
  }

  /**
   * Returns the configured security-provider name (normalized to lower case), defaulting to {@value
   * #DEFAULT_SECURITY_PROVIDER} — no authentication. Selects which {@link
   * com.scalar.db.saga.server.security.SagaSecurityProvider} the server authenticates requests
   * with; the value is validated against the known providers when the provider is built.
   */
  public String securityProvider() {
    return securityProvider;
  }

  /**
   * Whether the operator has acknowledged running without authentication on a network-reachable
   * interface (the {@code insecure_mode.enabled} key). Consulted by {@link SagaServer} at startup
   * to gate the {@code noop} provider on a non-loopback host. Defaults to {@value
   * #DEFAULT_INSECURE_MODE_ENABLED}.
   */
  public boolean insecureModeEnabled() {
    return insecureModeEnabled;
  }

  /**
   * Returns the HMAC secret used to authenticate async-callback requests, or empty when unset. When
   * empty, the daemon registers no callback route (async completion is not enabled). The value may
   * be supplied as a {@code ${file:}}/{@code ${env:}} secret reference. Present exactly when {@link
   * #callbackBaseUrl()} is.
   */
  public Optional<String> callbackSecret() {
    return Optional.ofNullable(callbackSecret);
  }

  /**
   * Returns the daemon's externally-reachable base URL used to build async-step callback URLs, or
   * empty when unset. Any trailing {@code /} is stripped so a callback path can be appended
   * directly. Present exactly when {@link #callbackSecret()} is.
   */
  public Optional<String> callbackBaseUrl() {
    return Optional.ofNullable(callbackBaseUrl);
  }

  /**
   * Returns the TTL (seconds) applied to an async callback token's {@code iat}: a callback whose
   * token is older than this is rejected as expired. {@code 0} (the default) disables the check.
   * When enabled it must exceed the longest a step can stay parked (its callback timeout), or a
   * genuine late callback is rejected.
   */
  public long callbackMaxAgeSeconds() {
    return callbackMaxAgeSeconds;
  }

  /**
   * Returns the server-wide default saga timeout (ms) enforced at execution for definitions that
   * specified none ({@code 0} = unbounded); {@code 0} (the default) disables it. A definition's own
   * timeout always takes precedence — this only fills in for definitions that left it unset, so a
   * daemon-hosted saga cannot run without a deadline. Forwarded to the engine (which applies it at
   * deadline computation on every execution entry) instead of being baked into the stored
   * definition, so changing it never conflicts with stored content.
   */
  public long defaultSagaTimeoutMillis() {
    return defaultSagaTimeoutMillis;
  }

  /**
   * Returns the per-principal rate limit on saga-start requests ({@code POST}/{@code PUT /sagas}),
   * in requests per minute; {@code 0} (the default) disables rate limiting.
   */
  public int maxStartRequestsPerMinute() {
    return maxStartRequestsPerMinute;
  }

  /**
   * Returns a defensive copy of the underlying configuration properties forwarded to construct the
   * saga engine's persistence.
   */
  public Properties properties() {
    return copyOf(properties);
  }

  /**
   * Returns a defensive copy of the <b>pre-resolution</b> properties (secret references not yet
   * expanded). A provider uses this to distinguish a {@code ${...}} secret reference from an inline
   * value — indistinguishable after resolution — e.g. the API-key provider rejects inline keys.
   */
  Properties rawProperties() {
    return copyOf(rawProperties);
  }

  /** Returns the optional path to declarative saga definitions loaded at startup. */
  public Optional<Path> definitionsPath() {
    return Optional.ofNullable(definitionsPath);
  }

  /**
   * Returns the services-directory settings: the directory itself, the reload interval (parsed now,
   * consumed once the reload pass ships), the secrets root confining {@code ${file:...}} references
   * in service files, and the optional egress ceiling.
   */
  public ReloadConfig reloadConfig() {
    return reloadConfig;
  }

  private static String parseHost(@Nullable String value) {
    return (value == null || value.isBlank()) ? DEFAULT_HOST : value.trim();
  }

  /**
   * Returns the configured owner id, or a fresh random UUID when unset — the same default the
   * engine builder applies, restated here so the value is fixed once at load and every consumer
   * sees one identity for the process. A configured value is validated like a saga ID: the owner id
   * is stamped on claimed rows and echoed in log lines, so control characters or unbounded length
   * must not pass (the error deliberately does not echo the value).
   */
  private static String parseOwnerId(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return UUID.randomUUID().toString();
    }
    String ownerId = value.trim();
    if (!OWNER_ID_PATTERN.matcher(ownerId).matches()) {
      throw new IllegalArgumentException(
          "'" + OWNER_ID_KEY + "' must match " + OWNER_ID_PATTERN.pattern());
    }
    return ownerId;
  }

  /**
   * Normalizes the security-provider name (trimmed, lower-cased), defaulting to {@value
   * #DEFAULT_SECURITY_PROVIDER} when unset/blank. The value is validated against the known
   * providers by the provider factory, not here.
   */
  private static String parseSecurityProvider(@Nullable String value) {
    return (value == null || value.isBlank())
        ? DEFAULT_SECURITY_PROVIDER
        : value.trim().toLowerCase(Locale.ROOT);
  }

  /** Trims the callback base URL and strips trailing {@code /}s; blank (or slashes only) ⇒ null. */
  private static @Nullable String parseCallbackBaseUrl(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String trimmed = value.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed.isEmpty() ? null : trimmed;
  }

  /**
   * Parses the shutdown mode by enum name (case-insensitive), listing the valid names on a mismatch
   * so a typo does not silently leave the drain policy at its default.
   */
  private static ShutdownMode parseShutdownMode(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return DEFAULT_SHUTDOWN_MODE;
    }
    try {
      return ShutdownMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for '"
              + SHUTDOWN_MODE_KEY
              + "' "
              + Redaction.redacted(value)
              + ". Valid modes: WAIT_CURRENT_STEP, WAIT_ALL_SAGAS.");
    }
  }

  /**
   * Parses an optional path value: blank-is-unset, trimmed, and rejected without echoing when the
   * platform refuses it as a path — {@link InvalidPathException}'s own message embeds the raw
   * input, which for a mis-pasted secret reference would be the secret's plaintext.
   */
  private static @Nullable Path parseOptionalPath(@Nullable String value, String key) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Path.of(value.trim());
    } catch (InvalidPathException e) {
      throw new IllegalArgumentException(
          "Invalid value for '"
              + key
              + "': not a valid file system path "
              + Redaction.redacted(value));
    }
  }

  private static int parsePort(@Nullable String value, String key, int defaultPort) {
    if (value == null || value.isBlank()) {
      return defaultPort;
    }
    int port;
    try {
      port = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for '" + key + "': not a number " + Redaction.redacted(value));
    }
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException(
          "'" + key + "' must be between 0 and 65535 " + Redaction.redacted(value));
    }
    return port;
  }

  /**
   * Parses a boolean config value: applies {@code defaultValue} when unset/blank, accepts only
   * {@code true}/{@code false} (case-insensitive), and rejects anything else so a typo (e.g. {@code
   * yes}, {@code 1}) fails fast rather than being silently read as {@code false}.
   */
  private static boolean parseBoolean(@Nullable String value, String key, boolean defaultValue) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    String trimmed = value.trim();
    if (trimmed.equalsIgnoreCase("true")) {
      return true;
    }
    if (trimmed.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException(
        "'" + key + "' must be 'true' or 'false' " + Redaction.redacted(value));
  }

  /**
   * Parses a long config value: applies {@code defaultValue} when unset/blank, rejects non-numeric
   * values, and enforces {@code value >= minInclusive} (e.g. {@code 0} for an optional bound that
   * may be disabled, {@code 1} for a strictly-positive ceiling).
   */
  static long parseBoundedLong(
      @Nullable String value, String key, long defaultValue, long minInclusive) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    long parsed;
    try {
      parsed = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for '" + key + "': not a number " + Redaction.redacted(value));
    }
    if (parsed < minInclusive) {
      throw new IllegalArgumentException(
          "'" + key + "' must be >= " + minInclusive + " " + Redaction.redacted(value));
    }
    return parsed;
  }

  /**
   * Parses an int config value with the same rules as {@link #parseBoundedLong}, and additionally
   * rejects anything above {@link Integer#MAX_VALUE}. Parsing wide and then narrowing is what makes
   * that check possible: a plain {@code Integer.parseInt} rejects the overflow too, but a {@code
   * (int)} cast of a parsed long would silently wrap — turning, say, a thread-pool bound of
   * 4294967296 into 0.
   */
  private static int parseBoundedInt(
      @Nullable String value, String key, int defaultValue, int minInclusive) {
    long parsed = parseBoundedLong(value, key, defaultValue, minInclusive);
    if (parsed > Integer.MAX_VALUE) {
      // parsed can only exceed an int when an explicit value was parsed; the null-value path
      // returns defaultValue, an int. requireNonNull records what NullAway cannot derive.
      throw new IllegalArgumentException(
          "'"
              + key
              + "' must be <= "
              + Integer.MAX_VALUE
              + " "
              + Redaction.redacted(Objects.requireNonNull(value)));
    }
    return (int) parsed;
  }

  /** Returns the trimmed value, rejecting a missing or blank one as a misconfigured key. */
  static String requireNonBlank(String key, @Nullable String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("'" + key + "' must not be blank");
    }
    return value.trim();
  }

  /**
   * Returns {@code value} unchanged, rejecting one that is present but blank. For the keys whose
   * default leaves a protection off, where {@link #parseBoundedLong}'s blank-is-unset rule would
   * turn a templated value that resolved empty into a silently disabled control. Absent stays
   * legal: omitting the key is how an operator asks for the default, so only the empty spelling is
   * refused.
   *
   * @param key the property key, for the error message
   * @param value the raw property value, or null when unset
   * @return {@code value}, unchanged
   */
  private static @Nullable String requireNonBlankIfSet(String key, @Nullable String value) {
    if (value != null && value.isBlank()) {
      throw new IllegalArgumentException(
          "'"
              + key
              + "' is set to a blank value. Blank is not a way to disable this setting — it would"
              + " take the default, which leaves the protection off. Remove the key to accept that"
              + " default, or give it a value.");
    }
    return value;
  }

  /**
   * Splits a comma-separated list, trimming each element and rejecting an empty one so a stray
   * comma cannot introduce a blank entry the consumer would have to interpret.
   */
  static List<String> parseCommaSeparated(String key, String value) {
    List<String> elements = new ArrayList<>();
    for (String element : value.split(",", -1)) {
      String trimmed = element.trim();
      if (trimmed.isEmpty()) {
        throw new IllegalArgumentException(
            "'"
                + key
                + "' has an empty element "
                + Redaction.redacted(value)
                + ". Remove the stray comma.");
      }
      elements.add(trimmed);
    }
    return elements;
  }

  /**
   * Parses the gRPC inbound cap from the store's max-event-payload key once at construction (like
   * every other setting), so an invalid value fails fast at load rather than when the server is
   * built.
   */
  private static int parseGrpcMaxInboundMessageBytes(Properties properties) {
    String value = properties.getProperty(STORE_MAX_EVENT_PAYLOAD_BYTES_KEY);
    if (value == null) {
      return DEFAULT_MAX_EVENT_PAYLOAD_BYTES;
    }
    int bytes;
    try {
      bytes = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for '"
              + STORE_MAX_EVENT_PAYLOAD_BYTES_KEY
              + "': not a number "
              + Redaction.redacted(value));
    }
    if (bytes < 0) {
      throw new IllegalArgumentException(
          "'"
              + STORE_MAX_EVENT_PAYLOAD_BYTES_KEY
              + "' must not be negative "
              + Redaction.redacted(value));
    }
    return bytes == 0 ? Integer.MAX_VALUE : bytes;
  }

  /**
   * Returns a flattened defensive copy of {@code source}. Rebuilds from {@code
   * stringPropertyNames()} so every string property is copied into the single table, including any
   * inherited from a defaults chain ({@code new Properties(defaults)}); a plain {@code putAll}
   * would silently drop those inherited entries, leaving {@code getProperty} on the copy
   * inconsistent with the resolved properties (which {@link #resolveSecrets} already flattens the
   * same way).
   */
  private static Properties copyOf(Properties source) {
    Properties copy = new Properties();
    for (String key : source.stringPropertyNames()) {
      String value = source.getProperty(key);
      if (value == null) {
        continue; // stringPropertyNames() only lists string-valued keys; guard for null-safety
      }
      copy.setProperty(key, value);
    }
    // Non-string entries aren't listed by stringPropertyNames(); carry the main table's through.
    source.forEach(
        (key, value) -> {
          if (!(key instanceof String) || !(value instanceof String)) {
            copy.put(key, value);
          }
        });
    return copy;
  }
}
