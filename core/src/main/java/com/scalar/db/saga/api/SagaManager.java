package com.scalar.db.saga.api;

import com.scalar.db.saga.engine.SagaManagerBuilder;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;

/**
 * Top-level API for saga orchestration. Manages saga definitions, execution, and recovery.
 *
 * <p>Implementations must be thread-safe — multiple sagas execute concurrently.
 *
 * <p>Create instances via the builder:
 *
 * <pre>{@code
 * SagaManager manager = SagaManager.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .build();
 * }</pre>
 */
public interface SagaManager extends AutoCloseable {

  /**
   * Creates a new builder for constructing a {@link SagaManager}.
   *
   * @return a new builder instance
   */
  static Builder newBuilder() {
    return SagaManagerBuilder.newBuilder();
  }

  /**
   * Registers a saga definition. The definition is validated and persisted.
   *
   * @param definition the saga definition
   * @throws SagaDefinitionException if the definition fails validation
   */
  void register(SagaDefinition definition);

  /**
   * Parses a saga definition from a file and registers it. Detects JSON or YAML by file extension
   * ({@code .json}, {@code .yaml}, {@code .yml}).
   *
   * @param definitionFile the path to the definition file
   * @throws SagaDefinitionException if the file cannot be parsed or fails validation
   */
  void register(Path definitionFile);

  /**
   * Starts a new saga instance with a server-generated ID (synchronous — blocks until the saga
   * completes or fails).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #start(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionException if step resolution fails
   */
  String start(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID (synchronous). Enables idempotent retries:
   * if the caller crashes after the saga is persisted, it can retry with the same ID.
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #start(String, SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   * @throws SagaDefinitionException if step resolution fails
   */
  void start(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (synchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionException if step resolution fails
   */
  String start(SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (synchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   * @throws SagaDefinitionException if step resolution fails
   */
  void start(String sagaId, SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous — returns immediately).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map)} to avoid the store
   * round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   */
  String startAsync(String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID (asynchronous with completion callback).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(SagaDefinitionId, Map, SagaCallback)} to
   * avoid the store round-trip.
   *
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   */
  String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a client-supplied ID (asynchronous).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(String, SagaDefinitionId, Map)} to avoid the
   * store round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID (asynchronous with completion callback).
   *
   * <p>This method queries the store on every call to resolve the latest definition version. If you
   * know the exact version, prefer {@link #startAsync(String, SagaDefinitionId, Map, SagaCallback)}
   * to avoid the store round-trip.
   *
   * @param sagaId the client-supplied saga ID
   * @param sagaName the registered saga definition name
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @throws SagaDefinitionNotFoundException if no definition matches the given name
   */
  void startAsync(String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (asynchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   */
  String startAsync(SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a server-generated ID, using a specific definition version
   * (asynchronous with completion callback). The definition is resolved from the in-memory cache
   * first, falling back to the store only on a cache miss.
   *
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @return the generated saga ID
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   */
  String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (asynchronous). The definition is resolved from the in-memory cache first, falling back to the
   * store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   */
  void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input);

  /**
   * Starts a new saga instance with a client-supplied ID, using a specific definition version
   * (asynchronous with completion callback). The definition is resolved from the in-memory cache
   * first, falling back to the store only on a cache miss.
   *
   * @param sagaId the client-supplied saga ID
   * @param id the saga definition name and version
   * @param input initial data for the saga context
   * @param callback callback for completion/compensation/escalation
   * @throws SagaDefinitionNotFoundException if no definition matches the given name and version
   */
  void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback);

  /**
   * Resumes a failed or crashed saga (crash recovery).
   *
   * @param sagaId the saga instance ID
   * @return the saga state after resumption
   * @throws SagaDefinitionNotFoundException if the saga's definition is no longer registered
   * @throws SagaDefinitionException if step resolution fails
   */
  SagaStateSnapshot resume(String sagaId);

  /**
   * Manually triggers compensation for a saga.
   *
   * @param sagaId the saga instance ID
   * @return the saga state after compensation
   * @throws SagaDefinitionNotFoundException if the saga's definition is no longer registered
   * @throws SagaDefinitionException if step resolution fails
   */
  SagaStateSnapshot compensate(String sagaId);

  /**
   * Queries the current state of a saga instance.
   *
   * @param sagaId the saga instance ID
   * @return the current saga state snapshot
   */
  SagaStateSnapshot getStateSnapshot(String sagaId);

  /**
   * Completes an asynchronous step via external callback (daemon mode only). Resumes a parked saga
   * with the step's output.
   *
   * @param sagaId the saga instance ID
   * @param stepName the name of the step to complete
   * @param output the step's output data
   * @return the saga state after step completion
   */
  SagaStateSnapshot completeStep(String sagaId, String stepName, Map<String, Object> output);

  /**
   * Runs a single recovery pass: scans for stale sagas, claims them, and resumes or compensates as
   * appropriate. This is the same logic that runs periodically when {@link #startBackgroundTasks()}
   * is called.
   *
   * <p>Useful for on-demand recovery (e.g., admin tooling) or testing crash recovery without
   * relying on the periodic background scanner.
   */
  void recover();

  /**
   * Starts periodic background tasks: crash recovery scanning and retention cleanup of terminal
   * sagas. Call after registering all saga definitions.
   */
  void startBackgroundTasks();

  /**
   * Shuts down the saga manager, stopping background tasks and waiting for in-flight sagas to
   * complete.
   */
  @Override
  void close();

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  /**
   * Builder for constructing a {@link SagaManager}. Three step resolution modes are supported:
   *
   * <ol>
   *   <li><b>No dependencies (default):</b> Steps must have a single public no-arg constructor.
   *   <li><b>Resource injection:</b> Register shared resources via {@link #resource(Class, Object)}
   *       or {@link #resource(Class, Object, String)}.
   *   <li><b>Custom resolver:</b> Supply a {@link StepResolver} via {@link
   *       #stepResolver(StepResolver)} for full control over step instantiation.
   * </ol>
   *
   * <p>{@code resource()} and {@code stepResolver()} are mutually exclusive.
   *
   * <p>Remote calls are wired with a single mode-free method, {@link #httpEndpoint(String,
   * String)}. The registered endpoint both injects a {@code @Named} {@code SagaHttpClient} into
   * code steps and backs declaratively-defined service steps whose {@code service} resolves to it;
   * {@code httpEndpoint} is orthogonal to step resolution (allowed alongside either {@code
   * resource()} or {@code stepResolver()}).
   */
  interface Builder {

    /**
     * Sets the store factory. The factory's {@link SagaStoreFactory#createStore()} method is called
     * during {@link #build()}, and the resulting store is closed during {@link
     * SagaManager#close()}.
     *
     * <p>For testing, a lambda returning a mock store can be used:
     *
     * <pre>{@code
     * SagaManager.newBuilder().storeFactory(() -> mockStore).build();
     * }</pre>
     *
     * @param factory the store factory
     * @return this builder
     */
    Builder storeFactory(SagaStoreFactory factory);

    /**
     * Sets the shutdown mode. Defaults to {@link ShutdownMode#WAIT_CURRENT_STEP}.
     *
     * @param mode the shutdown mode
     * @return this builder
     */
    Builder shutdownMode(ShutdownMode mode);

    /**
     * Sets the shutdown timeout in milliseconds. Defaults to 30,000 (30 seconds).
     *
     * @param millis the shutdown timeout
     * @return this builder
     */
    Builder shutdownTimeoutMillis(long millis);

    /**
     * Sets the owner ID for this engine instance. Defaults to a random UUID.
     *
     * @param ownerId the owner ID (e.g., pod name, hostname)
     * @return this builder
     */
    Builder ownerId(String ownerId);

    /**
     * Sets a custom step resolver for full control over step instantiation. Mutually exclusive with
     * {@link #resource}.
     *
     * @param resolver the step resolver
     * @return this builder
     */
    Builder stepResolver(StepResolver resolver);

    /**
     * Begins registering an HTTP endpoint under {@code name} pointing at {@code baseUrl}. One
     * mode-free remote method: the registered endpoint both (a) injects an {@code @Named(name)
     * SagaHttpClient} into code steps and (b) backs declaratively-defined service steps whose
     * {@code service} resolves to it. The framework propagates the saga correlation headers,
     * enforces the outbound HTTP policy (SSRF allowlist + body limits), and disables redirect
     * following.
     *
     * <p>{@code httpEndpoint} is <b>orthogonal</b> to step resolution: it may be combined with
     * either {@link #resource} or {@link #stepResolver(StepResolver)} (those two remain mutually
     * exclusive; {@code httpEndpoint} does not trip that rule).
     *
     * <p>Configure the optional outbound policy / client on the returned sub-builder, then call
     * {@link HttpEndpointBuilder#add()} to commit it and return to this builder:
     *
     * <pre>{@code
     * SagaManager.newBuilder()
     *     .storeFactory(...)
     *     .httpEndpoint("account-svc", "https://account-svc:8443")
     *         .allowedHosts("account-svc")
     *         .maxBodyBytes(2_000_000)
     *         .add()
     *     .build();
     * }</pre>
     *
     * @param name the endpoint name (the {@code @Named} qualifier for injection and the {@code
     *     service} referenced by declarative steps)
     * @param baseUrl the endpoint base URL (e.g. {@code http://account-svc:8080})
     * @return a sub-builder for this endpoint's optional outbound configuration
     */
    HttpEndpointBuilder httpEndpoint(String name, String baseUrl);

    /**
     * Configures one HTTP endpoint. Obtained from {@link Builder#httpEndpoint(String, String)}; set
     * the optional outbound policy and client, then call {@link #add()} to register the endpoint
     * and return to the parent {@link Builder}. All settings are optional — calling {@code add()}
     * immediately registers the endpoint with the defaults (allow-all hosts, 1 MB body limit, a
     * framework-created client with redirects disabled).
     */
    interface HttpEndpointBuilder {

      /**
       * Restricts outbound calls for this endpoint to the given hosts (SSRF allowlist). Empty (the
       * default) = allow all. Matching is by exact, case-insensitive host name only — it does not
       * resolve the host or inspect the connect-time IP, so it does not defend against DNS
       * rebinding or a hostname that points at a private/link-local/metadata address. It is
       * defense-in-depth for trusted, operator-configured endpoints, not a sandbox.
       *
       * @param hosts the allowed host names
       * @return this sub-builder
       */
      HttpEndpointBuilder allowedHosts(String... hosts);

      /**
       * Sets the maximum request/response body size in bytes for this endpoint. Defaults to 1 MB.
       *
       * @param maxBodyBytes the maximum body size; must be {@code > 0}
       * @return this sub-builder
       */
      HttpEndpointBuilder maxBodyBytes(long maxBodyBytes);

      /**
       * Uses a custom {@link HttpClient} (e.g. with a proxy or custom TLS) for this endpoint. The
       * caller owns the supplied client's lifecycle — unlike the framework-created default client,
       * it is <em>not</em> closed on {@link SagaManager#close()}. The caller is responsible for its
       * redirect policy; the framework-created default disables redirects to protect the SSRF
       * allowlist.
       *
       * @param client the client to use for this endpoint
       * @return this sub-builder
       */
      HttpEndpointBuilder httpClient(HttpClient client);

      /**
       * Adds a default header sent on <em>every</em> request to this endpoint — by both declarative
       * steps and the injected {@link com.scalar.db.saga.api.SagaHttpClient SagaHttpClient} for
       * code steps. This is the channel for authentication and other secrets (e.g. {@code
       * Authorization}), since default headers are <b>not</b> persisted in the saga definition.
       *
       * <p>Per-name precedence: a default header here is overridden by a per-call header of the
       * same name (only code steps set per-call headers, via {@link
       * com.scalar.db.saga.api.SagaHttpClient.Request#header(String, String)}); the framework
       * correlation headers ({@code X-Saga-Id}/{@code X-Saga-Step}) are always set by the
       * framework. Repeatable; a later call for the same name replaces the earlier value.
       *
       * <p><b>Cluster-wide config caveat:</b> default headers (like the endpoint URL and allowlist)
       * are non-persisted per-node configuration. A node recovering a saga must be configured with
       * the same endpoint default headers; divergent per-node config is the operator's
       * responsibility (uniform deployment, as with a database connection string).
       *
       * @param name the header name
       * @param value the header value
       * @return this sub-builder
       */
      HttpEndpointBuilder defaultHeader(String name, String value);

      /**
       * Adds all of {@code headers} as endpoint default headers (see {@link #defaultHeader(String,
       * String)}). Merged with any previously set default headers; a key present in {@code headers}
       * replaces an earlier value for the same name.
       *
       * @param headers the default headers to add
       * @return this sub-builder
       */
      HttpEndpointBuilder defaultHeaders(Map<String, String> headers);

      /**
       * Registers this endpoint on the parent builder and returns it for chaining.
       *
       * @return the parent builder
       */
      Builder add();
    }

    /**
     * Overrides the default recovery configuration.
     *
     * @param config the recovery configuration
     * @return this builder
     */
    Builder recoveryConfig(RecoveryConfig config);

    /**
     * Overrides the default retention configuration.
     *
     * @param config the retention configuration
     * @return this builder
     */
    Builder retentionConfig(RetentionConfig config);

    /**
     * Sets the clock (for testing). Defaults to {@link Clock#systemUTC()}.
     *
     * @param clock the clock
     * @return this builder
     */
    Builder clock(Clock clock);

    /**
     * Registers an unnamed resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @return this builder
     */
    <T> Builder resource(Class<T> type, T instance);

    /**
     * Registers a named resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @param name the qualifier name (must match {@code @Named} on constructor parameters)
     * @return this builder
     */
    <T> Builder resource(Class<T> type, T instance, String name);

    /**
     * Builds and returns a configured {@link SagaManager}.
     *
     * @return the saga manager
     * @throws IllegalStateException if no store factory is configured, or if both {@code
     *     resource()} and {@code stepResolver()} were called
     */
    SagaManager build();
  }
}
