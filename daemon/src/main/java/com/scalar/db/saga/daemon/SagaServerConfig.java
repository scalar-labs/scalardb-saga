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
 *   <li>{@code scalar.db.saga.server.port} — HTTP listen port (default {@code 8080}; {@code 0}
 *       binds an ephemeral port, useful in tests)
 *   <li>{@code scalar.db.saga.server.definitions_path} — path to a JSON/YAML saga definition file
 *       or directory (optional)
 *   <li>{@code scalar.db.saga.server.service.<name>.base_url} — base URL of the HTTP service a
 *       declarative step's {@code "service":"<name>"} resolves to; repeat the key per service
 *       (optional)
 *   <li>{@code scalar.db.saga.server.sync_timeout_millis} — bound (ms) on how long a synchronous
 *       start blocks before returning {@code 202} while the saga continues; {@code 0} (default)
 *       disables it (sync blocks to terminal)
 * </ul>
 *
 * <p>All other properties configure the saga engine's persistence (e.g. ScalarDB connection
 * settings) and are forwarded as-is. In daemon mode, {@code
 * scalar.db.saga.store.max_event_payload_bytes} defaults to 1 MiB when unset, bounding how large an
 * event payload an unauthenticated caller can persist; set it explicitly to override.
 */
public final class SagaServerConfig {

  static final String PORT_KEY = "scalar.db.saga.server.port";
  static final String DEFINITIONS_PATH_KEY = "scalar.db.saga.server.definitions_path";
  static final String SERVICE_KEY_PREFIX = "scalar.db.saga.server.service.";
  static final String SERVICE_BASE_URL_SUFFIX = ".base_url";
  static final String STORE_MAX_EVENT_PAYLOAD_BYTES_KEY =
      "scalar.db.saga.store.max_event_payload_bytes";
  static final String SYNC_TIMEOUT_MILLIS_KEY = "scalar.db.saga.server.sync_timeout_millis";
  static final int DEFAULT_PORT = 8080;
  static final int DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 1_048_576; // 1 MiB
  static final long DEFAULT_SYNC_TIMEOUT_MILLIS = 0L; // 0 = disabled (sync blocks to terminal)

  private final int port;
  private final long syncTimeoutMillis;
  private final Properties properties;
  private final @Nullable Path definitionsPath;
  private final Map<String, String> serviceBaseUrls;

  private SagaServerConfig(
      int port,
      long syncTimeoutMillis,
      Properties properties,
      @Nullable Path definitionsPath,
      Map<String, String> serviceBaseUrls) {
    this.port = port;
    this.syncTimeoutMillis = syncTimeoutMillis;
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
    int port = parsePort(properties.getProperty(PORT_KEY));
    long syncTimeoutMillis =
        parseSyncTimeoutMillis(properties.getProperty(SYNC_TIMEOUT_MILLIS_KEY));
    String definitions = properties.getProperty(DEFINITIONS_PATH_KEY);
    Path definitionsPath =
        (definitions == null || definitions.isBlank()) ? null : Path.of(definitions.trim());
    return new SagaServerConfig(
        port, syncTimeoutMillis, properties, definitionsPath, parseServiceBaseUrls(properties));
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

  /** Returns the configured HTTP port ({@code 0} binds an ephemeral port). */
  public int port() {
    return port;
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

  private static int parsePort(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return DEFAULT_PORT;
    }
    int port;
    try {
      port = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for '" + PORT_KEY + "': " + value, e);
    }
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException(
          "'" + PORT_KEY + "' must be between 0 and 65535, got " + port);
    }
    return port;
  }

  private static long parseSyncTimeoutMillis(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return DEFAULT_SYNC_TIMEOUT_MILLIS;
    }
    long millis;
    try {
      millis = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for '" + SYNC_TIMEOUT_MILLIS_KEY + "': " + value, e);
    }
    if (millis < 0) {
      throw new IllegalArgumentException(
          "'" + SYNC_TIMEOUT_MILLIS_KEY + "' must not be negative, got " + millis);
    }
    return millis;
  }

  private static Properties copyOf(Properties source) {
    Properties copy = new Properties();
    copy.putAll(source);
    return copy;
  }
}
