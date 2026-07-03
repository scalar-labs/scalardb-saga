package com.scalar.db.saga.daemon;

import java.nio.file.Path;
import java.util.LinkedHashMap;
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
 * </ul>
 *
 * <p>All other properties configure the saga engine's persistence (e.g. ScalarDB connection
 * settings) and are forwarded as-is. In daemon mode, {@code
 * scalar.db.saga.store.max_event_payload_bytes} defaults to 1 MiB when unset, bounding how large an
 * event payload an unauthenticated caller can persist; set it explicitly to override.
 */
public final class SagaServerConfig {

  static final String HOST_KEY = "scalar.db.saga.server.host";
  static final String PORT_KEY = "scalar.db.saga.server.port";
  static final String GRPC_PORT_KEY = "scalar.db.saga.server.grpc_port";
  static final String HTTP_ENABLED_KEY = "scalar.db.saga.server.http_enabled";
  static final String GRPC_ENABLED_KEY = "scalar.db.saga.server.grpc_enabled";
  static final String DEFINITIONS_PATH_KEY = "scalar.db.saga.server.definitions_path";
  static final String SERVICE_KEY_PREFIX = "scalar.db.saga.server.service.";
  static final String SERVICE_BASE_URL_SUFFIX = ".base_url";
  static final String STORE_MAX_EVENT_PAYLOAD_BYTES_KEY =
      "scalar.db.saga.store.max_event_payload_bytes";
  static final String SYNC_TIMEOUT_MILLIS_KEY = "scalar.db.saga.server.sync_timeout_millis";
  static final String SYNC_MAX_WAIT_MILLIS_KEY = "scalar.db.saga.server.sync_max_wait_millis";
  static final String DEFAULT_HOST = "0.0.0.0";
  static final int DEFAULT_PORT = 8080;
  static final int DEFAULT_GRPC_PORT = 50051;
  static final boolean DEFAULT_HTTP_ENABLED = true;
  static final boolean DEFAULT_GRPC_ENABLED = true;
  static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 1_048_576; // 1 MiB
  static final long DEFAULT_SYNC_TIMEOUT_MILLIS = 0L; // 0 = disabled (sync blocks to terminal)
  static final long DEFAULT_SYNC_MAX_WAIT_MILLIS =
      60_000L; // ceiling on a synchronous server-side wait

  private final String host;
  private final int port;
  private final int grpcPort;
  private final boolean httpEnabled;
  private final boolean grpcEnabled;
  private final long syncTimeoutMillis;
  private final long syncMaxWaitMillis;
  private final Properties properties;
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
      Properties properties,
      @Nullable Path definitionsPath,
      Map<String, String> serviceBaseUrls) {
    this.host = host;
    this.port = port;
    this.grpcPort = grpcPort;
    this.httpEnabled = httpEnabled;
    this.grpcEnabled = grpcEnabled;
    this.syncTimeoutMillis = syncTimeoutMillis;
    this.syncMaxWaitMillis = syncMaxWaitMillis;
    this.properties = applyStoreDefaults(copyOf(properties));
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
        properties,
        definitionsPath,
        parseServiceBaseUrls(properties));
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
   * Returns the gRPC server's maximum inbound message size in bytes, aligned with the store's
   * max-event-payload cap so neither transport accepts an input the store would reject (and so an
   * unauthenticated caller cannot push an oversized message). The store's {@code 0} ("no limit") is
   * mapped to {@link Integer#MAX_VALUE} here, since gRPC reads {@code 0} as "reject all non-empty
   * messages". Defaults to {@value #DEFAULT_MAX_EVENT_PAYLOAD_BYTES} bytes.
   */
  public int grpcMaxInboundMessageBytes() {
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
   * Returns a defensive copy of the underlying configuration properties forwarded to construct the
   * saga engine's persistence.
   */
  public Properties properties() {
    return copyOf(properties);
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

  private static Properties copyOf(Properties source) {
    Properties copy = new Properties();
    copy.putAll(source);
    return copy;
  }
}
