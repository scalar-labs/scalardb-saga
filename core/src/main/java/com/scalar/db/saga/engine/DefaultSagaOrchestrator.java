package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStoreFactory;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default (in-process) implementation of {@link SagaOrchestrator}. Delegates saga lifecycle logic
 * to {@link SagaEngine}, handles definition registration via {@link SagaDefinitionRegistry}, async
 * execution via virtual threads, and callback dispatch. Beyond the lean {@link SagaOrchestrator}
 * surface it also exposes registration, recovery, and admin operations, and is constructed via
 * {@link #newBuilder()}.
 */
@ThreadSafe
public class DefaultSagaOrchestrator implements SagaOrchestrator {

  private static final Logger logger = LoggerFactory.getLogger(DefaultSagaOrchestrator.class);

  private final SagaEngine engine;
  private final SagaStore store;
  private final SagaDefinitionRegistry definitionRegistry;
  private final SagaRecoveryManager recoveryManager;
  private final SagaRetentionManager retentionManager;
  private final long shutdownTimeoutMillis;
  private final ExecutorService asyncExecutor;
  private volatile boolean closed;

  DefaultSagaOrchestrator(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry definitionRegistry,
      SagaRecoveryManager recoveryManager,
      SagaRetentionManager retentionManager,
      long shutdownTimeoutMillis) {
    this(
        engine,
        store,
        definitionRegistry,
        recoveryManager,
        retentionManager,
        shutdownTimeoutMillis,
        Executors.newVirtualThreadPerTaskExecutor());
  }

  // Visible for testing
  DefaultSagaOrchestrator(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry definitionRegistry,
      SagaRecoveryManager recoveryManager,
      SagaRetentionManager retentionManager,
      long shutdownTimeoutMillis,
      ExecutorService asyncExecutor) {
    this.engine = engine;
    this.store = store;
    this.definitionRegistry = definitionRegistry;
    this.recoveryManager = recoveryManager;
    this.retentionManager = retentionManager;
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    this.asyncExecutor = asyncExecutor;
  }

  /** Creates a new builder for constructing a {@link DefaultSagaOrchestrator}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  // ---------------------------------------------------------------------------
  // Registration
  // ---------------------------------------------------------------------------

  public void register(SagaDefinition definition) {
    Objects.requireNonNull(definition, "definition must not be null");
    // Eagerly resolve all steps — fail fast on missing resources or unresolvable constructors.
    // This must happen before persisting to the store, so invalid definitions are never stored.
    engine.getOrBuildPlan(definition);
    definitionRegistry.register(definition);
  }

  public void register(Path definitionFile) {
    Objects.requireNonNull(definitionFile, "definitionFile must not be null");
    register(SagaDefinitionParser.parseFile(definitionFile));
  }

  // ---------------------------------------------------------------------------
  // Synchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return engine.execute(def, null, input);
  }

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    engine.execute(def, sagaId, input);
  }

  @Override
  public String start(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return engine.execute(def, null, input);
  }

  @Override
  public void start(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    engine.execute(def, sagaId, input);
  }

  // ---------------------------------------------------------------------------
  // Asynchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return startAsyncInternal(def, null, input, null).getSagaId();
  }

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return startAsyncInternal(def, null, input, callback).getSagaId();
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    startAsyncInternal(def, sagaId, input, null);
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    startAsyncInternal(def, sagaId, input, callback);
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return startAsyncInternal(def, null, input, null).getSagaId();
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return startAsyncInternal(def, null, input, callback).getSagaId();
  }

  @Override
  public void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    startAsyncInternal(def, sagaId, input, null);
  }

  @Override
  public void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    startAsyncInternal(def, sagaId, input, callback);
  }

  /**
   * Shared async-launch path. Persists the saga synchronously (so it is recoverable even if the
   * process crashes before the virtual thread starts), then submits execution to a virtual thread.
   */
  private SagaStateSnapshot startAsyncInternal(
      SagaDefinition def,
      @Nullable String sagaId,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    // Persist synchronously — saga is recoverable from this point
    SagaStateSnapshot saga = engine.createSaga(def, sagaId, input);

    // Submit execution to a virtual thread. The returned Future is intentionally unused:
    // saga state is persisted, so recovery handles failures. Storing the future would require
    // managing its lifecycle (fire-and-forget pattern).
    submitAsync(def, saga, input, callback);

    return saga;
  }

  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget; recovery handles failures
  private void submitAsync(
      SagaDefinition def,
      SagaStateSnapshot saga,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    try {
      asyncExecutor.submit(
          () -> {
            try {
              engine.executeSaga(def, saga, input);
            } catch (Exception e) {
              // Saga state is persisted — recovery will pick it up
              logger.error("Async saga {} failed unexpectedly", saga.getSagaId(), e);
            } finally {
              try {
                dispatchCallback(saga.getSagaId(), callback);
              } catch (Exception e) {
                logger.error("Failed to dispatch callback for saga {}", saga.getSagaId(), e);
              }
            }
          });
    } catch (RejectedExecutionException e) {
      // Race between close() and submit — saga is already persisted, recovery will handle it
      logger.warn(
          "Async executor rejected saga {} (shutting down); recovery will handle it",
          saga.getSagaId(),
          e);
    }
  }

  private void dispatchCallback(String sagaId, @Nullable SagaCallback callback) {
    if (callback == null) {
      return;
    }
    SagaStateSnapshot result =
        store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
    switch (result.getStatus()) {
      case COMPLETED -> callback.onCompleted(result);
      case COMPENSATED -> callback.onCompensated(result);
      case ESCALATED -> callback.onEscalated(result);
      default ->
          logger.warn("Saga {} ended in non-terminal status: {}", sagaId, result.getStatus());
    }
  }

  // ---------------------------------------------------------------------------
  // Resume / Compensate
  // ---------------------------------------------------------------------------

  public SagaStateSnapshot resume(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    SagaStateSnapshot saga = getStateSnapshot(sagaId);
    if (saga.getStatus() != SagaStatus.RUNNING) {
      throw new IllegalStateException(
          "Cannot resume saga " + sagaId + " in status " + saga.getStatus());
    }
    SagaDefinition def = resolveDefinition(saga);
    List<SagaEvent> events = store.getEvents(sagaId);
    ExecutionContext context = engine.replayEvents(saga, events);

    int lastCompleted = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);

    return engine.resumeFrom(def, context, lastCompleted + 1);
  }

  public SagaStateSnapshot compensate(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    SagaStateSnapshot saga = getStateSnapshot(sagaId);
    if (saga.getStatus() != SagaStatus.COMPENSATING) {
      throw new IllegalStateException(
          "Cannot compensate saga " + sagaId + " in status " + saga.getStatus());
    }
    SagaDefinition def = resolveDefinition(saga);
    List<SagaEvent> events = store.getEvents(sagaId);
    ExecutionContext context = engine.replayEvents(saga, events);

    int lastCompensated =
        stepIndices(events, EventType.STEP_COMPENSATED).min().orElse(Integer.MAX_VALUE);

    // Compensate from the step before the last compensated one (or from last completed if none)
    int fromStep;
    if (lastCompensated < Integer.MAX_VALUE) {
      fromStep = lastCompensated - 1;
    } else {
      // No compensation started yet — find the last completed step
      fromStep = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);
    }

    engine.compensateFrom(def, context, fromStep);
    return context.getCurrentState();
  }

  // ---------------------------------------------------------------------------
  // Query
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
  }

  // ---------------------------------------------------------------------------
  // Daemon mode only
  // ---------------------------------------------------------------------------

  public SagaStateSnapshot completeStep(
      String sagaId, String stepName, Map<String, Object> output) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(stepName, "stepName must not be null");
    Objects.requireNonNull(output, "output must not be null");
    // completeStep resumes a parked (WAITING) saga when an external callback arrives.
    // It is only available in daemon mode, which uses a separate orchestrator implementation.
    throw new UnsupportedOperationException("completeStep is only available in daemon mode");
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  public void recover() {
    ensureOpen();
    recoveryManager.recover();
  }

  // ---------------------------------------------------------------------------
  // Background Tasks
  // ---------------------------------------------------------------------------

  public void startBackgroundTasks() {
    ensureOpen();
    recoveryManager.start();
    retentionManager.start();
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() {
    closed = true;
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMillis);

    retentionManager.stop(deadline);
    recoveryManager.stop(deadline);

    asyncExecutor.shutdown();
    engine.shutdown();

    long remainingNanos = deadline - System.nanoTime();
    try {
      if (remainingNanos <= 0
          || !asyncExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        asyncExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      asyncExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      // Sagas are drained. The engine.shutdown() above already released the HTTP clients held by
      // the step instantiator's registries (it owns them); here the orchestrator closes the store,
      // the one external resource it owns directly.
      try {
        store.close();
      } catch (Exception e) {
        logger.error("Failed to close store", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("orchestrator is closed");
    }
  }

  private SagaDefinition requireLatestDefinition(String sagaName) {
    SagaDefinition def = definitionRegistry.resolve(sagaName);
    if (def == null) {
      throw new SagaDefinitionNotFoundException(sagaName);
    }
    return def;
  }

  private SagaDefinition requireVersionedDefinition(SagaDefinitionId id) {
    SagaDefinition def = definitionRegistry.resolve(id.name(), id.version());
    if (def == null) {
      throw new SagaDefinitionNotFoundException(id);
    }
    return def;
  }

  private SagaDefinition resolveDefinition(SagaStateSnapshot saga) {
    SagaDefinition def =
        definitionRegistry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      throw new SagaDefinitionNotFoundException(saga.getSagaName(), saga.getDefinitionVersion());
    }
    return def;
  }

  private static IntStream stepIndices(List<SagaEvent> events, EventType eventType) {
    return events.stream()
        .filter(e -> e instanceof StepEvent)
        .map(e -> (StepEvent) e)
        .filter(e -> e.getEventType() == eventType)
        .mapToInt(StepEvent::getStepIndex);
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  /**
   * DI-free builder for constructing a {@link DefaultSagaOrchestrator}. Wires together the engine,
   * step resolver, definition registry, and store. Obtain one via {@link
   * DefaultSagaOrchestrator#newBuilder()}.
   *
   * <p><b>Three step resolution modes:</b>
   *
   * <ol>
   *   <li><b>No dependencies (default):</b> Steps must have a single public no-arg constructor.
   *   <li><b>Resource injection:</b> Register shared resources via {@link #resource(Class, Object)}
   *       or {@link #resource(Class, Object, String)}. Steps must have exactly one public
   *       constructor whose parameter types match registered resources.
   *   <li><b>Custom resolver:</b> Supply a {@link StepResolver} via {@link
   *       #stepResolver(StepResolver)} for full control over step instantiation (e.g., manual
   *       lookup, DI framework integration).
   * </ol>
   *
   * <p>{@code resource()} and {@code stepResolver()} are mutually exclusive — calling both causes
   * {@link #build()} to throw.
   *
   * <pre>{@code
   * DefaultSagaOrchestrator orchestrator = DefaultSagaOrchestrator.newBuilder()
   *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
   *     .resource(EmailClient.class, emailClient)
   *     .build();
   * }</pre>
   */
  public static final class Builder {

    private @Nullable SagaStoreFactory storeFactory;
    private String ownerId = java.util.UUID.randomUUID().toString();
    private ShutdownMode shutdownMode = ShutdownMode.WAIT_CURRENT_STEP;
    private long shutdownTimeoutMillis = 30_000;
    private Clock clock = Clock.systemUTC();
    private ResourceRegistry.@Nullable Builder resourceRegistryBuilder;
    private @Nullable StepResolver customStepResolver;
    private final Map<String, HttpServiceConfig> httpEndpoints = new HashMap<>();
    private @Nullable RecoveryConfig recoveryConfig;
    private @Nullable RetentionConfig retentionConfig;

    private Builder() {}

    /**
     * Sets the store factory. The factory's {@link SagaStoreFactory#createStore()} method is called
     * during {@link #build()}, and the resulting store is closed during {@link
     * DefaultSagaOrchestrator#close()}.
     *
     * <p>For testing, a lambda returning a mock store can be used:
     *
     * <pre>{@code
     * DefaultSagaOrchestrator.newBuilder().storeFactory(() -> mockStore).build();
     * }</pre>
     *
     * @param factory the store factory
     * @return this builder
     */
    public Builder storeFactory(SagaStoreFactory factory) {
      this.storeFactory = Objects.requireNonNull(factory, "factory must not be null");
      return this;
    }

    /**
     * Sets the owner ID for this engine instance. Defaults to a random UUID.
     *
     * @param ownerId the owner ID (e.g., pod name, hostname)
     * @return this builder
     */
    public Builder ownerId(String ownerId) {
      this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
      return this;
    }

    /**
     * Sets the shutdown mode. Defaults to {@link ShutdownMode#WAIT_CURRENT_STEP}.
     *
     * @param shutdownMode the shutdown mode
     * @return this builder
     */
    public Builder shutdownMode(ShutdownMode shutdownMode) {
      this.shutdownMode = Objects.requireNonNull(shutdownMode, "shutdownMode must not be null");
      return this;
    }

    /**
     * Sets the shutdown timeout in milliseconds. Defaults to 30,000 (30 seconds).
     *
     * @param shutdownTimeoutMillis the shutdown timeout
     * @return this builder
     */
    public Builder shutdownTimeoutMillis(long shutdownTimeoutMillis) {
      this.shutdownTimeoutMillis = shutdownTimeoutMillis;
      return this;
    }

    /**
     * Sets the clock (for testing). Defaults to {@link Clock#systemUTC()}.
     *
     * @param clock the clock
     * @return this builder
     */
    public Builder clock(Clock clock) {
      this.clock = Objects.requireNonNull(clock, "clock must not be null");
      return this;
    }

    /**
     * Registers a named resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @param name the qualifier name (must match {@code @Named} on constructor parameters)
     * @return this builder
     */
    public <T> Builder resource(Class<T> type, T instance, String name) {
      Objects.requireNonNull(type, "type must not be null");
      Objects.requireNonNull(instance, "instance must not be null");
      Objects.requireNonNull(name, "name must not be null");
      getOrCreateResourceRegistryBuilder().add(type, instance, name);
      return this;
    }

    /**
     * Registers an unnamed resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @return this builder
     */
    public <T> Builder resource(Class<T> type, T instance) {
      Objects.requireNonNull(type, "type must not be null");
      Objects.requireNonNull(instance, "instance must not be null");
      getOrCreateResourceRegistryBuilder().add(type, instance);
      return this;
    }

    /**
     * Sets a custom step resolver for full control over step instantiation. Mutually exclusive with
     * {@link #resource}.
     *
     * @param stepResolver the step resolver
     * @return this builder
     */
    public Builder stepResolver(StepResolver stepResolver) {
      Objects.requireNonNull(stepResolver, "stepResolver must not be null");
      this.customStepResolver = stepResolver;
      return this;
    }

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
     * DefaultSagaOrchestrator.newBuilder()
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
    public HttpEndpointBuilder httpEndpoint(String name, String baseUrl) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(baseUrl, "baseUrl must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("name must not be blank");
      }
      if (baseUrl.isBlank()) {
        throw new IllegalArgumentException("baseUrl must not be blank");
      }
      validateBaseUrl(baseUrl);
      return new HttpEndpointBuilder(name, baseUrl);
    }

    /**
     * Fails fast on a malformed or misleading {@code baseUrl} at build time rather than at the
     * first saga run: it must be a valid absolute {@code http}/{@code https} URL with a host and no
     * user-info component (a {@code user@host} authority silently retargets the host — e.g. {@code
     * http://svc@evil.com} resolves to {@code evil.com}).
     */
    private static void validateBaseUrl(String baseUrl) {
      URI uri;
      try {
        uri = URI.create(baseUrl);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("baseUrl is not a valid URI: " + baseUrl, e);
      }
      String scheme = uri.getScheme();
      if (scheme == null
          || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
        throw new IllegalArgumentException("baseUrl must use the http or https scheme: " + baseUrl);
      }
      if (uri.getHost() == null) {
        throw new IllegalArgumentException("baseUrl must have a host: " + baseUrl);
      }
      if (uri.getUserInfo() != null) {
        throw new IllegalArgumentException(
            "baseUrl must not contain a user-info component (it silently retargets the host): "
                + baseUrl);
      }
    }

    /**
     * Overrides the default recovery configuration.
     *
     * @param recoveryConfig the recovery configuration
     * @return this builder
     */
    public Builder recoveryConfig(RecoveryConfig recoveryConfig) {
      this.recoveryConfig =
          Objects.requireNonNull(recoveryConfig, "recoveryConfig must not be null");
      return this;
    }

    /**
     * Overrides the default retention configuration.
     *
     * @param retentionConfig the retention configuration
     * @return this builder
     */
    public Builder retentionConfig(RetentionConfig retentionConfig) {
      this.retentionConfig =
          Objects.requireNonNull(retentionConfig, "retentionConfig must not be null");
      return this;
    }

    /**
     * Builds and returns a configured {@link DefaultSagaOrchestrator}.
     *
     * @return the orchestrator
     * @throws IllegalStateException if no store factory is configured, or if both {@code
     *     resource()} and {@code stepResolver()} were called
     */
    public DefaultSagaOrchestrator build() {
      if (storeFactory == null) {
        throw new IllegalStateException(
            "SagaStoreFactory is required — call storeFactory() before build()");
      }
      if (resourceRegistryBuilder != null && customStepResolver != null) {
        throw new IllegalStateException(
            "resource() and stepResolver() are mutually exclusive — use one or the other");
      }

      SagaStore store = null;
      HttpEndpointRegistry httpEndpointRegistry = null;
      try {
        store = storeFactory.createStore();
        // The orchestrator owns the HTTP endpoints created from httpEndpoint(...): they are closed
        // on close (or here if build fails) — mirroring the store's lifecycle. A code step's
        // SagaHttpClient and a declarative step against the same endpoint share one HttpExchange
        // (one client, one policy).
        httpEndpointRegistry = HttpEndpointRegistry.create(httpEndpoints);
        StepResolver resolver = buildStepResolver();

        RecoveryConfig resolvedRecoveryConfig =
            recoveryConfig != null ? recoveryConfig : RecoveryConfig.defaults(clock);
        RetentionConfig resolvedRetentionConfig =
            retentionConfig != null ? retentionConfig : RetentionConfig.defaults(clock);

        SagaEngine.ShutdownConfig shutdownConfig =
            new SagaEngine.ShutdownConfig(shutdownMode, shutdownTimeoutMillis);
        StepInstantiator stepInstantiator = new StepInstantiator(resolver, httpEndpointRegistry);
        SagaEngine engine = new SagaEngine(store, stepInstantiator, ownerId, shutdownConfig, clock);
        SagaDefinitionRegistry definitionRegistry = new SagaDefinitionRegistry(store);

        SagaRecoveryManager recoveryManager =
            new SagaRecoveryManager(
                store, engine, definitionRegistry, ownerId, resolvedRecoveryConfig);
        SagaRetentionManager retentionManager =
            new SagaRetentionManager(store, resolvedRetentionConfig);

        return new DefaultSagaOrchestrator(
            engine,
            store,
            definitionRegistry,
            recoveryManager,
            retentionManager,
            shutdownTimeoutMillis);
      } catch (Exception e) {
        // Roll back the resources that hold real external connections: the store (DB sessions) and
        // the HTTP endpoint registry (holds HTTP clients). Each is null if its own creation threw,
        // so each close is null-guarded. The engine and the recovery/retention managers constructed
        // inside the try only hold executors that stay inert until started — their threads spin up
        // on start()/first task, never during build — so a failed build leaves them with no live
        // threads to stop, and GC reclaims them. Hence no engine.shutdown() here.
        if (httpEndpointRegistry != null) {
          try {
            httpEndpointRegistry.close();
          } catch (Exception closeException) {
            e.addSuppressed(closeException);
          }
        }
        if (store != null) {
          try {
            store.close();
          } catch (Exception closeException) {
            e.addSuppressed(closeException);
          }
        }
        throw e;
      }
    }

    private StepResolver buildStepResolver() {
      if (customStepResolver != null) {
        return customStepResolver;
      }
      ResourceRegistry resourceRegistry =
          resourceRegistryBuilder != null
              ? resourceRegistryBuilder.build()
              : ResourceRegistry.newBuilder().build();
      return new ReflectiveStepResolver(resourceRegistry);
    }

    private ResourceRegistry.Builder getOrCreateResourceRegistryBuilder() {
      if (resourceRegistryBuilder == null) {
        resourceRegistryBuilder = ResourceRegistry.newBuilder();
      }
      return resourceRegistryBuilder;
    }

    /** Accumulates one HTTP endpoint's optional outbound config until {@link #add()}. */
    public final class HttpEndpointBuilder {

      private final String name;
      private final String baseUrl;
      private final List<String> allowedHosts = new ArrayList<>();
      private final Map<String, String> defaultHeaders = new LinkedHashMap<>();
      private long maxBodyBytes = -1; // -1 = use the default
      private @Nullable HttpClient httpClient;

      private HttpEndpointBuilder(String name, String baseUrl) {
        this.name = name;
        this.baseUrl = baseUrl;
      }

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
      public HttpEndpointBuilder allowedHosts(String... hosts) {
        Objects.requireNonNull(hosts, "hosts must not be null");
        for (String host : hosts) {
          allowedHosts.add(Objects.requireNonNull(host, "host must not be null"));
        }
        return this;
      }

      /**
       * Sets the maximum request/response body size in bytes for this endpoint. Defaults to 1 MB.
       *
       * @param maxBodyBytes the maximum body size; must be {@code > 0}
       * @return this sub-builder
       */
      public HttpEndpointBuilder maxBodyBytes(long maxBodyBytes) {
        if (maxBodyBytes <= 0) {
          throw new IllegalArgumentException("maxBodyBytes must be > 0, got " + maxBodyBytes);
        }
        this.maxBodyBytes = maxBodyBytes;
        return this;
      }

      /**
       * Uses a custom {@link HttpClient} (e.g. with a proxy or custom TLS) for this endpoint. The
       * caller owns the supplied client's lifecycle — unlike the framework-created default client,
       * it is <em>not</em> closed on {@link DefaultSagaOrchestrator#close()}. The caller is
       * responsible for its redirect policy; the framework-created default disables redirects to
       * protect the SSRF allowlist.
       *
       * @param client the client to use for this endpoint
       * @return this sub-builder
       */
      public HttpEndpointBuilder httpClient(HttpClient client) {
        this.httpClient = Objects.requireNonNull(client, "client must not be null");
        return this;
      }

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
       * @param name the header name
       * @param value the header value
       * @return this sub-builder
       */
      public HttpEndpointBuilder defaultHeader(String name, String value) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(value, "value must not be null");
        defaultHeaders.put(name, value);
        return this;
      }

      /**
       * Adds all of {@code headers} as endpoint default headers (see {@link #defaultHeader(String,
       * String)}). Merged with any previously set default headers; a key present in {@code headers}
       * replaces an earlier value for the same name.
       *
       * @param headers the default headers to add
       * @return this sub-builder
       */
      public HttpEndpointBuilder defaultHeaders(Map<String, String> headers) {
        Objects.requireNonNull(headers, "headers must not be null");
        headers.forEach(
            (name, value) -> {
              Objects.requireNonNull(name, "header name must not be null");
              Objects.requireNonNull(value, "header value must not be null");
              defaultHeaders.put(name, value);
            });
        return this;
      }

      /**
       * Registers this endpoint on the parent builder and returns it for chaining.
       *
       * @return the parent builder
       */
      public Builder add() {
        if (httpEndpoints.containsKey(name)) {
          throw new IllegalArgumentException("HTTP endpoint already registered: " + name);
        }
        httpEndpoints.put(
            name,
            new HttpServiceConfig(baseUrl, allowedHosts, maxBodyBytes, httpClient, defaultHeaders));
        return Builder.this;
      }
    }
  }
}
