package com.scalar.db.saga.daemon;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for a {@link SagaServer}: the HTTP port, the ScalarDB connection/store properties,
 * and an optional path to declarative saga definitions loaded at startup.
 *
 * <p>Built from a single {@link Properties} object (typically a ScalarDB properties file with extra
 * {@code scalar.db.saga.server.*} keys). Server-specific keys:
 *
 * <ul>
 *   <li>{@code scalar.db.saga.server.host} — interface to bind (default {@code 0.0.0.0}, all
 *       interfaces); set to {@code 127.0.0.1} or a specific interface to restrict reach, as the
 *       endpoints are unauthenticated
 *   <li>{@code scalar.db.saga.server.port} — HTTP listen port (default {@code 8080}; {@code 0}
 *       binds an ephemeral port, useful in tests)
 *   <li>{@code scalar.db.saga.server.grpc_port} — gRPC listen port (default {@code 50051}; {@code
 *       0} binds an ephemeral port). Bound to the same {@code host} as HTTP, on its own listener
 *   <li>{@code scalar.db.saga.server.http_enabled} — whether to serve the REST transport (default
 *       {@code true}); set {@code false} to run a gRPC-only server
 *   <li>{@code scalar.db.saga.server.grpc_enabled} — whether to serve the gRPC transport (default
 *       {@code true}); set {@code false} to run an HTTP-only server. At least one transport must be
 *       enabled — the server refuses to start with both disabled
 *   <li>{@code scalar.db.saga.server.definitions_path} — path to a JSON/YAML saga definition file
 *       or directory (optional)
 *   <li>{@code scalar.db.saga.server.service.<name>.base_url} — base URL of the HTTP service a
 *       declarative step's {@code "service":"<name>"} resolves to; repeat the key per service
 *       (optional)
 *   <li>{@code scalar.db.saga.server.sync_timeout_millis} — bound (ms) on how long a synchronous
 *       start blocks before returning {@code 202} while the saga continues; {@code 0} (default)
 *       disables this operator bound
 *   <li>{@code scalar.db.saga.server.sync_max_wait_millis} — absolute ceiling (ms) on a synchronous
 *       gRPC start's server-side wait, so it can never block indefinitely (default {@code 60000});
 *       {@code sync_timeout_millis} and the client's deadline only tighten it
 *   <li>{@code scalar.db.saga.server.security.provider} — the authentication provider (default
 *       {@code noop} — no authentication; suitable only for a trusted/isolated network). Set to a
 *       real provider to enforce access control
 *   <li>{@code scalar.db.saga.server.max_threads} / {@code min_threads} — HTTP request thread-pool
 *       bounds (defaults {@code 200} / {@code 8}); the max caps concurrent request threads so a
 *       burst of slow requests cannot exhaust threads
 *   <li>{@code scalar.db.saga.server.max_queued_requests} — cap on requests waiting for a handler
 *       thread once all {@code max_threads} are busy; further requests are shed (fast failure)
 *       rather than queued unboundedly. Defaults to {@code 2 × max_threads}, bounding worst-case
 *       queueing delay to roughly twice a request's service time
 *   <li>{@code scalar.db.saga.server.default_saga_timeout_millis} — a default saga timeout applied
 *       to a loaded definition that set none ({@code 0} = unbounded); {@code 0} (default) disables
 *       it. A definition's own timeout always wins
 *   <li>{@code scalar.db.saga.server.max_start_requests_per_minute} — per-principal rate limit on
 *       {@code POST}/{@code PUT /sagas}; {@code 0} (default) disables rate limiting. The limit is
 *       keyed on the authenticated principal, so it is only per-caller once a real provider (jwt or
 *       apikey) is configured; under {@code noop} every request is the same {@code "anonymous"}
 *       principal and the limit acts as one global bucket shared by all callers
 * </ul>
 *
 * <p>Any {@code scalar.db.saga.*} value may use a secret reference — {@code ${file:UTF-8:/path}}
 * (preferred; e.g. a Kubernetes mounted Secret) or {@code ${env:NAME}} — resolved at load time. See
 * {@link SecretResolver}. The {@code ${file:...}} form is resolved <b>only</b> in this {@code
 * scalar.db.saga.*} namespace. {@code scalar.db.*} store keys are instead left for ScalarDB to
 * resolve, and ScalarDB supports only {@code ${env:...}} and {@code ${sys:...}} — not {@code
 * ${file:...}}. Use {@code ${env:...}} for {@code scalar.db.*} store secrets; a {@code ${file:...}}
 * reference there passes through verbatim and fails later at DB-connect time.
 *
 * <p>All other properties configure the saga engine's persistence (e.g. ScalarDB connection
 * settings) and are forwarded as-is. In daemon mode, {@code
 * scalar.db.saga.store.max_event_payload_bytes} defaults to 1 MiB when unset, bounding how large an
 * event payload an unauthenticated caller can persist; set it explicitly to override.
 */
public final class SagaServerConfig {

  // The daemon's config namespace: the base for every key below, and the boundary within which
  // secret references are resolved (scalar.db.* store keys are left for ScalarDB — see
  // resolveSecrets).
  static final String PREFIX = "scalar.db.saga.";
  static final String SERVER_PREFIX = PREFIX + "server.";
  static final String HOST_KEY = SERVER_PREFIX + "host";
  static final String PORT_KEY = SERVER_PREFIX + "port";
  static final String GRPC_PORT_KEY = SERVER_PREFIX + "grpc_port";
  static final String HTTP_ENABLED_KEY = SERVER_PREFIX + "http_enabled";
  static final String GRPC_ENABLED_KEY = SERVER_PREFIX + "grpc_enabled";
  static final String DEFINITIONS_PATH_KEY = SERVER_PREFIX + "definitions_path";
  static final String SERVICE_KEY_PREFIX = SERVER_PREFIX + "service.";
  static final String SERVICE_BASE_URL_SUFFIX = ".base_url";
  static final String SYNC_TIMEOUT_MILLIS_KEY = SERVER_PREFIX + "sync_timeout_millis";
  static final String SYNC_MAX_WAIT_MILLIS_KEY = SERVER_PREFIX + "sync_max_wait_millis";
  // Security keys — daemon-only (embedded mode delegates auth to the host framework).
  static final String SECURITY_PREFIX = SERVER_PREFIX + "security.";
  static final String SECURITY_PROVIDER_KEY = SECURITY_PREFIX + "provider";
  static final String INSECURE_MODE_ENABLED_KEY = SECURITY_PREFIX + "insecure_mode.enabled";
  // The HMAC callback secret enables async-callback authentication; its value may be a
  // ${file:}/${env:} secret reference (resolved by resolveSecrets, like every other
  // scalar.db.saga.* key).
  static final String CALLBACK_SECRET_KEY = SECURITY_PREFIX + "callback_secret";
  // The daemon's externally-reachable base URL, used to build the callback URL handed to a
  // participant for an async step. Not a secret (a plain server address), so it lives directly
  // under server.* rather than server.security.*.
  static final String CALLBACK_BASE_URL_KEY = SERVER_PREFIX + "callback_base_url";
  // Optional TTL (seconds) on an async callback token's iat: a token older than this is rejected,
  // so a leaked callback URL is not a non-expiring credential. 0 (default) disables the check; when
  // set it must exceed the longest a step can legitimately stay parked (its callback timeout), or a
  // genuine late callback is rejected.
  static final String CALLBACK_MAX_AGE_SECONDS_KEY = SECURITY_PREFIX + "callback_max_age_seconds";
  static final String MAX_THREADS_KEY = SERVER_PREFIX + "max_threads";
  static final String MIN_THREADS_KEY = SERVER_PREFIX + "min_threads";
  static final String MAX_QUEUED_REQUESTS_KEY = SERVER_PREFIX + "max_queued_requests";
  static final String DEFAULT_SAGA_TIMEOUT_MILLIS_KEY =
      SERVER_PREFIX + "default_saga_timeout_millis";
  static final String MAX_START_REQUESTS_PER_MINUTE_KEY =
      SERVER_PREFIX + "max_start_requests_per_minute";
  static final String STORE_MAX_EVENT_PAYLOAD_BYTES_KEY = PREFIX + "store.max_event_payload_bytes";
  static final String DEFAULT_HOST = "0.0.0.0";
  static final int DEFAULT_PORT = 8080;
  static final int DEFAULT_GRPC_PORT = 50051;
  static final boolean DEFAULT_HTTP_ENABLED = true;
  static final boolean DEFAULT_GRPC_ENABLED = true;
  static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 1_048_576; // 1 MiB
  static final long DEFAULT_SYNC_TIMEOUT_MILLIS = 0L; // 0 = disabled (sync blocks to terminal)
  static final long DEFAULT_SYNC_MAX_WAIT_MILLIS =
      60_000L; // ceiling on a synchronous server-side wait
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
      0L; // 0 = disabled (definition's own timeout wins)
  static final int DEFAULT_MAX_START_REQUESTS_PER_MINUTE = 0; // 0 = disabled (no rate limiting)

  private final String host;
  private final int port;
  private final int grpcPort;
  private final boolean httpEnabled;
  private final boolean grpcEnabled;
  private final long syncTimeoutMillis;
  private final long syncMaxWaitMillis;
  private final String securityProvider;
  private final boolean insecureModeEnabled;
  private final @Nullable String callbackSecret;
  private final @Nullable String callbackBaseUrl;
  private final long callbackMaxAgeSeconds;
  private final int maxThreads;
  private final int minThreads;
  private final int maxQueuedRequests;
  private final long defaultSagaTimeoutMillis;
  private final int maxStartRequestsPerMinute;
  private final int grpcMaxInboundMessageBytes;
  private final Properties properties;
  private final Properties rawProperties;
  private final @Nullable Path definitionsPath;
  private final Map<String, String> serviceBaseUrls;

  private SagaServerConfig(
      String host,
      int port,
      int grpcPort,
      boolean httpEnabled,
      boolean grpcEnabled,
      long syncTimeoutMillis,
      long syncMaxWaitMillis,
      String securityProvider,
      boolean insecureModeEnabled,
      @Nullable String callbackSecret,
      @Nullable String callbackBaseUrl,
      long callbackMaxAgeSeconds,
      int maxThreads,
      int minThreads,
      int maxQueuedRequests,
      long defaultSagaTimeoutMillis,
      int maxStartRequestsPerMinute,
      Properties properties,
      Properties rawProperties,
      @Nullable Path definitionsPath,
      Map<String, String> serviceBaseUrls) {
    this.host = host;
    this.port = port;
    this.grpcPort = grpcPort;
    this.httpEnabled = httpEnabled;
    this.grpcEnabled = grpcEnabled;
    this.syncTimeoutMillis = syncTimeoutMillis;
    this.syncMaxWaitMillis = syncMaxWaitMillis;
    this.securityProvider = securityProvider;
    this.insecureModeEnabled = insecureModeEnabled;
    this.callbackSecret = callbackSecret;
    this.callbackBaseUrl = callbackBaseUrl;
    this.callbackMaxAgeSeconds = callbackMaxAgeSeconds;
    this.maxThreads = maxThreads;
    this.minThreads = minThreads;
    this.maxQueuedRequests = maxQueuedRequests;
    this.defaultSagaTimeoutMillis = defaultSagaTimeoutMillis;
    this.maxStartRequestsPerMinute = maxStartRequestsPerMinute;
    this.properties = applyStoreDefaults(copyOf(properties));
    this.grpcMaxInboundMessageBytes = parseGrpcMaxInboundMessageBytes(this.properties);
    this.rawProperties = copyOf(rawProperties);
    this.definitionsPath = definitionsPath;
    this.serviceBaseUrls = Map.copyOf(serviceBaseUrls);
  }

  /**
   * Applies daemon-mode defaults to the forwarded store properties — currently a bounded
   * event-payload size, so an unauthenticated client cannot persist arbitrarily large payloads. The
   * operator overrides it by setting the key explicitly.
   */
  private static Properties applyStoreDefaults(Properties properties) {
    String maxPayload = properties.getProperty(STORE_MAX_EVENT_PAYLOAD_BYTES_KEY);
    // Treat blank as unset (like port/definitions_path); otherwise a blank value would propagate to
    // the store factory and fail to parse as an integer, crashing startup.
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
   * @throws IllegalArgumentException if {@code scalar.db.saga.server.port} is not a valid port
   */
  public static SagaServerConfig load(Properties properties) {
    Objects.requireNonNull(properties, "properties must not be null");
    // Keep the pre-resolution properties so a provider can tell a secret reference from an inline
    // value (both look identical after resolution) — e.g. the API-key provider requires references.
    Properties rawProperties = copyOf(properties);
    properties = resolveSecrets(properties);
    String host = parseHost(properties.getProperty(HOST_KEY));
    int port = parsePort(properties.getProperty(PORT_KEY), PORT_KEY, DEFAULT_PORT);
    int grpcPort =
        parsePort(properties.getProperty(GRPC_PORT_KEY), GRPC_PORT_KEY, DEFAULT_GRPC_PORT);
    boolean httpEnabled =
        parseBoolean(
            properties.getProperty(HTTP_ENABLED_KEY), HTTP_ENABLED_KEY, DEFAULT_HTTP_ENABLED);
    boolean grpcEnabled =
        parseBoolean(
            properties.getProperty(GRPC_ENABLED_KEY), GRPC_ENABLED_KEY, DEFAULT_GRPC_ENABLED);
    if (!httpEnabled && !grpcEnabled) {
      throw new IllegalArgumentException(
          "At least one transport must be enabled, but '"
              + HTTP_ENABLED_KEY
              + "' and '"
              + GRPC_ENABLED_KEY
              + "' are both false. A server that exposes no transport can serve no requests.");
    }
    long syncTimeoutMillis =
        parseBoundedLong(
            properties.getProperty(SYNC_TIMEOUT_MILLIS_KEY),
            SYNC_TIMEOUT_MILLIS_KEY,
            DEFAULT_SYNC_TIMEOUT_MILLIS,
            0L);
    long syncMaxWaitMillis =
        parseBoundedLong(
            properties.getProperty(SYNC_MAX_WAIT_MILLIS_KEY),
            SYNC_MAX_WAIT_MILLIS_KEY,
            DEFAULT_SYNC_MAX_WAIT_MILLIS,
            1L);
    String securityProvider = parseSecurityProvider(properties.getProperty(SECURITY_PROVIDER_KEY));
    boolean insecureModeEnabled =
        parseBoolean(
            properties.getProperty(INSECURE_MODE_ENABLED_KEY),
            INSECURE_MODE_ENABLED_KEY,
            DEFAULT_INSECURE_MODE_ENABLED);
    // Treat blank as unset (no callback auth configured → no callback route). Do not trim: an HMAC
    // secret is opaque and could legitimately contain leading/trailing characters.
    String callbackSecretRaw = properties.getProperty(CALLBACK_SECRET_KEY);
    String callbackSecret =
        (callbackSecretRaw == null || callbackSecretRaw.isBlank()) ? null : callbackSecretRaw;
    String callbackBaseUrl = parseCallbackBaseUrl(properties.getProperty(CALLBACK_BASE_URL_KEY));
    long callbackMaxAgeSeconds =
        parseBoundedLong(
            properties.getProperty(CALLBACK_MAX_AGE_SECONDS_KEY),
            CALLBACK_MAX_AGE_SECONDS_KEY,
            DEFAULT_CALLBACK_MAX_AGE_SECONDS,
            0L);
    int maxThreads =
        (int)
            parseBoundedLong(
                properties.getProperty(MAX_THREADS_KEY), MAX_THREADS_KEY, DEFAULT_MAX_THREADS, 1L);
    int minThreads =
        (int)
            parseBoundedLong(
                properties.getProperty(MIN_THREADS_KEY), MIN_THREADS_KEY, DEFAULT_MIN_THREADS, 1L);
    if (minThreads > maxThreads) {
      throw new IllegalArgumentException(
          "'"
              + MIN_THREADS_KEY
              + "' ("
              + minThreads
              + ") must not exceed '"
              + MAX_THREADS_KEY
              + "' ("
              + maxThreads
              + ").");
    }
    // Cap the handler-thread backlog. Unset defaults to a multiple of maxThreads so the worst-case
    // queueing delay stays proportional to the pool (about that many request service-times) rather
    // than being an absolute number that means wildly different latency at different pool sizes.
    // Computed in long to avoid int overflow before the (int) narrowing.
    int maxQueuedRequests =
        (int)
            parseBoundedLong(
                properties.getProperty(MAX_QUEUED_REQUESTS_KEY),
                MAX_QUEUED_REQUESTS_KEY,
                (long) DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD * maxThreads,
                1L);
    long defaultSagaTimeoutMillis =
        parseBoundedLong(
            properties.getProperty(DEFAULT_SAGA_TIMEOUT_MILLIS_KEY),
            DEFAULT_SAGA_TIMEOUT_MILLIS_KEY,
            DEFAULT_SAGA_TIMEOUT_MILLIS,
            0L);
    int maxStartRequestsPerMinute =
        (int)
            parseBoundedLong(
                properties.getProperty(MAX_START_REQUESTS_PER_MINUTE_KEY),
                MAX_START_REQUESTS_PER_MINUTE_KEY,
                DEFAULT_MAX_START_REQUESTS_PER_MINUTE,
                0L);
    String definitions = properties.getProperty(DEFINITIONS_PATH_KEY);
    Path definitionsPath =
        (definitions == null || definitions.isBlank()) ? null : Path.of(definitions.trim());
    return new SagaServerConfig(
        host,
        port,
        grpcPort,
        httpEnabled,
        grpcEnabled,
        syncTimeoutMillis,
        syncMaxWaitMillis,
        securityProvider,
        insecureModeEnabled,
        callbackSecret,
        callbackBaseUrl,
        callbackMaxAgeSeconds,
        maxThreads,
        minThreads,
        maxQueuedRequests,
        defaultSagaTimeoutMillis,
        maxStartRequestsPerMinute,
        properties,
        rawProperties,
        definitionsPath,
        parseServiceBaseUrls(properties));
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
   * Collects {@code scalar.db.saga.server.service.<name>.base_url} entries into a {@code name ->
   * baseUrl} map, one per declarative service the server can call. The URL itself is validated
   * later by the saga engine's {@code httpEndpoint(...)} builder.
   */
  private static Map<String, String> parseServiceBaseUrls(Properties properties) {
    Map<String, String> serviceBaseUrls = new LinkedHashMap<>();
    for (String key : properties.stringPropertyNames()) {
      if (!key.startsWith(SERVICE_KEY_PREFIX) || !key.endsWith(SERVICE_BASE_URL_SUFFIX)) {
        continue;
      }
      String name =
          key.substring(
              SERVICE_KEY_PREFIX.length(), key.length() - SERVICE_BASE_URL_SUFFIX.length());
      if (name.isBlank()) {
        throw new IllegalArgumentException("service name must not be blank in '" + key + "'");
      }
      String baseUrl = properties.getProperty(key).trim();
      if (baseUrl.isBlank()) {
        throw new IllegalArgumentException("'" + key + "' must not be blank");
      }
      serviceBaseUrls.put(name, baseUrl);
    }
    return serviceBaseUrls;
  }

  /**
   * Returns the host/interface the HTTP server binds to (default {@value #DEFAULT_HOST} — all
   * interfaces, the norm for a container behind network controls). The endpoints are
   * unauthenticated, so set this to a specific interface (e.g. {@code 127.0.0.1}) when the daemon
   * is not on a trusted/isolated network.
   */
  public String host() {
    return host;
  }

  /** Returns the configured HTTP port ({@code 0} binds an ephemeral port). */
  public int port() {
    return port;
  }

  /**
   * Returns the configured gRPC port ({@code 0} binds an ephemeral port). The gRPC server binds the
   * same {@link #host()} as HTTP, on its own listener.
   */
  public int grpcPort() {
    return grpcPort;
  }

  /**
   * Returns whether the HTTP (REST) transport is served (default {@code true}). When {@code false},
   * the server runs gRPC-only and binds no HTTP port. At least one of {@link #httpEnabled()} /
   * {@link #grpcEnabled()} is always {@code true}.
   */
  public boolean httpEnabled() {
    return httpEnabled;
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
   * Returns the synchronous-start timeout in milliseconds, or {@code 0} when disabled (the
   * default). When positive, a synchronous {@code POST}/{@code PUT} that has not reached a terminal
   * state within this bound returns {@code 202} and the saga keeps running on the engine's executor
   * (the client polls {@code GET /sagas/{id}}) — so a slow saga cannot pin a request thread
   * indefinitely.
   */
  public long syncTimeoutMillis() {
    return syncTimeoutMillis;
  }

  /**
   * Returns the absolute ceiling (ms) on a synchronous gRPC {@code StartSaga}'s server-side wait.
   * {@link #syncTimeoutMillis()} and the client's call deadline can only tighten this bound, never
   * exceed it, so a synchronous start can never pin a server thread indefinitely. Defaults to
   * {@value #DEFAULT_SYNC_MAX_WAIT_MILLIS} ms.
   */
  public long syncMaxWaitMillis() {
    return syncMaxWaitMillis;
  }

  /**
   * Returns the configured security-provider name (normalized to lower case), defaulting to {@value
   * #DEFAULT_SECURITY_PROVIDER} — no authentication. Selects which {@link
   * com.scalar.db.saga.daemon.security.SagaSecurityProvider} the server authenticates requests
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
   * be supplied as a {@code ${file:}}/{@code ${env:}} secret reference.
   */
  public Optional<String> callbackSecret() {
    return Optional.ofNullable(callbackSecret);
  }

  /**
   * Returns the daemon's externally-reachable base URL used to build async-step callback URLs, or
   * empty when unset. Any trailing {@code /} is stripped so a callback path can be appended
   * directly.
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
   * Returns the maximum size of the HTTP (Jetty) request-handling thread pool (default {@value
   * #DEFAULT_MAX_THREADS}). Caps concurrent request threads so a burst of slow requests cannot
   * exhaust threads.
   */
  public int maxThreads() {
    return maxThreads;
  }

  /**
   * Returns the minimum (core) size of the HTTP thread pool (default {@value
   * #DEFAULT_MIN_THREADS}).
   */
  public int minThreads() {
    return minThreads;
  }

  /**
   * Returns the cap on requests waiting for a handler thread once all {@link #maxThreads()} are
   * busy. Beyond it the server sheds load (fast failure) instead of queueing unboundedly. Defaults
   * to {@value #DEFAULT_MAX_QUEUED_REQUESTS_PER_THREAD} × {@link #maxThreads()}, keeping the
   * worst-case queueing delay proportional to the pool.
   */
  public int maxQueuedRequests() {
    return maxQueuedRequests;
  }

  /**
   * Returns the server-wide default saga timeout (ms) applied to a loaded definition that specified
   * none ({@code 0} = unbounded); {@code 0} (the default) disables it. A definition's own timeout
   * always takes precedence — this only fills in for definitions that left it unset, so a
   * daemon-hosted saga cannot run without a deadline.
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
   * Returns the gRPC server's maximum inbound message size in bytes, aligned with the store's
   * max-event-payload cap so neither transport accepts an input the store would reject (and so an
   * unauthenticated caller cannot push an oversized message). The store's {@code 0} ("no limit") is
   * mapped to {@link Integer#MAX_VALUE} here, since gRPC reads {@code 0} as "reject all non-empty
   * messages". Defaults to {@value #DEFAULT_MAX_EVENT_PAYLOAD_BYTES} bytes.
   */
  public int grpcMaxInboundMessageBytes() {
    return grpcMaxInboundMessageBytes;
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
   * Returns the configured {@code service name -> base URL} map, each registered as an HTTP
   * endpoint a declarative step can call. Empty when no {@code service.<name>.base_url} keys are
   * set.
   */
  public Map<String, String> serviceBaseUrls() {
    return serviceBaseUrls;
  }

  private static String parseHost(@Nullable String value) {
    return (value == null || value.isBlank()) ? DEFAULT_HOST : value.trim();
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

  private static int parsePort(@Nullable String value, String key, int defaultPort) {
    if (value == null || value.isBlank()) {
      return defaultPort;
    }
    int port;
    try {
      port = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for '" + key + "': " + value, e);
    }
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("'" + key + "' must be between 0 and 65535, got " + port);
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
    throw new IllegalArgumentException("'" + key + "' must be 'true' or 'false', got " + value);
  }

  /**
   * Parses a long config value: applies {@code defaultValue} when unset/blank, rejects non-numeric
   * values, and enforces {@code value >= minInclusive} (e.g. {@code 0} for an optional bound that
   * may be disabled, {@code 1} for a strictly-positive ceiling).
   */
  private static long parseBoundedLong(
      @Nullable String value, String key, long defaultValue, long minInclusive) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    long millis;
    try {
      millis = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for '" + key + "': " + value, e);
    }
    if (millis < minInclusive) {
      throw new IllegalArgumentException(
          "'" + key + "' must be >= " + minInclusive + ", got " + millis);
    }
    return millis;
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
          "Invalid value for '" + STORE_MAX_EVENT_PAYLOAD_BYTES_KEY + "': " + value, e);
    }
    if (bytes < 0) {
      throw new IllegalArgumentException(
          "'" + STORE_MAX_EVENT_PAYLOAD_BYTES_KEY + "' must not be negative, got " + bytes);
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
