package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.RecoveryConfig;
import com.scalar.db.saga.api.RetentionConfig;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.SagaStoreFactory;
import com.scalar.db.saga.api.ShutdownMode;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.recovery.SagaRecoveryManager;
import com.scalar.db.saga.retention.SagaRetentionManager;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * DI-free builder for constructing a {@link SagaManager}. Wires together the engine, step resolver,
 * definition registry, and store.
 *
 * <p><b>Three step resolution modes:</b>
 *
 * <ol>
 *   <li><b>No dependencies (default):</b> Steps must have a single public no-arg constructor.
 *   <li><b>Resource injection:</b> Register shared resources via {@link #resource(Class, Object)}
 *       or {@link #resource(Class, Object, String)}. Steps must have exactly one public constructor
 *       whose parameter types match registered resources.
 *   <li><b>Custom resolver:</b> Supply a {@link StepResolver} via {@link
 *       #stepResolver(StepResolver)} for full control over step instantiation (e.g., manual lookup,
 *       DI framework integration).
 * </ol>
 *
 * <p>{@code resource()} and {@code stepResolver()} are mutually exclusive — calling both causes
 * {@link #build()} to throw.
 *
 * <p>Usage (resource injection):
 *
 * <pre>{@code
 * SagaManager manager = SagaManager.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .resource(ManagedChannel.class, accountChannel, "account")
 *     .resource(ManagedChannel.class, shippingChannel, "shipping")
 *     .resource(EmailClient.class, emailClient)
 *     .build();
 * }</pre>
 *
 * <p>Usage (custom step resolver — DI framework integration). The resolver must return singleton,
 * thread-safe instances per the lifecycle contract of {@link com.scalar.db.saga.api.Step}:
 *
 * <pre>{@code
 * SagaManager manager = SagaManager.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .stepResolver((name, className) -> applicationContext.getBean(Class.forName(className)))
 *     .build();
 * }</pre>
 *
 * <p>Usage (custom step resolver — pre-built step instances):
 *
 * <pre>{@code
 * Map<String, Object> steps = Map.of(
 *     "payment", paymentStep,
 *     "shipping", shippingStep);
 * SagaManager manager = SagaManager.newBuilder()
 *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
 *     .stepResolver((name, className) -> steps.get(name))
 *     .build();
 * }</pre>
 */
public class SagaManagerBuilder implements SagaManager.Builder {

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

  private SagaManagerBuilder() {}

  /** Creates a new builder instance. */
  public static SagaManagerBuilder newBuilder() {
    return new SagaManagerBuilder();
  }

  @Override
  public SagaManagerBuilder storeFactory(SagaStoreFactory factory) {
    this.storeFactory = Objects.requireNonNull(factory, "factory must not be null");
    return this;
  }

  @Override
  public SagaManagerBuilder ownerId(String ownerId) {
    this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
    return this;
  }

  @Override
  public SagaManagerBuilder shutdownMode(ShutdownMode shutdownMode) {
    this.shutdownMode = Objects.requireNonNull(shutdownMode, "shutdownMode must not be null");
    return this;
  }

  @Override
  public SagaManagerBuilder shutdownTimeoutMillis(long shutdownTimeoutMillis) {
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    return this;
  }

  @Override
  public SagaManagerBuilder clock(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    return this;
  }

  @Override
  public <T> SagaManagerBuilder resource(Class<T> type, T instance, String name) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(instance, "instance must not be null");
    Objects.requireNonNull(name, "name must not be null");
    getOrCreateResourceRegistryBuilder().add(type, instance, name);
    return this;
  }

  @Override
  public <T> SagaManagerBuilder resource(Class<T> type, T instance) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(instance, "instance must not be null");
    getOrCreateResourceRegistryBuilder().add(type, instance);
    return this;
  }

  @Override
  public SagaManagerBuilder stepResolver(StepResolver stepResolver) {
    Objects.requireNonNull(stepResolver, "stepResolver must not be null");
    this.customStepResolver = stepResolver;
    return this;
  }

  @Override
  public SagaManager.Builder.HttpEndpointBuilder httpEndpoint(String name, String baseUrl) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(baseUrl, "baseUrl must not be null");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (baseUrl.isBlank()) {
      throw new IllegalArgumentException("baseUrl must not be blank");
    }
    validateBaseUrl(baseUrl);
    return new HttpEndpointBuilderImpl(name, baseUrl);
  }

  /**
   * Fails fast on a malformed or misleading {@code baseUrl} at build time rather than at the first
   * saga run: it must be a valid absolute {@code http}/{@code https} URL with a host and no
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
    if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
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

  @Override
  public SagaManagerBuilder recoveryConfig(RecoveryConfig recoveryConfig) {
    this.recoveryConfig = Objects.requireNonNull(recoveryConfig, "recoveryConfig must not be null");
    return this;
  }

  @Override
  public SagaManagerBuilder retentionConfig(RetentionConfig retentionConfig) {
    this.retentionConfig =
        Objects.requireNonNull(retentionConfig, "retentionConfig must not be null");
    return this;
  }

  @Override
  public SagaManager build() {
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
      // The manager owns the HTTP endpoints created from httpEndpoint(...): they are closed on
      // manager close (or here if build fails) — mirroring the store's lifecycle. A code step's
      // SagaHttpClient and a declarative step against the same endpoint share one HttpExchange (one
      // client, one policy).
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

      return new EmbeddedSagaManager(
          engine,
          store,
          definitionRegistry,
          recoveryManager,
          retentionManager,
          shutdownTimeoutMillis);
    } catch (Exception e) {
      // Roll back the resources that hold real external connections: the store (DB sessions) and
      // the HTTP endpoint registry (holds HTTP clients). Each is null if its own creation threw, so
      // each close is null-guarded. The engine and the recovery/retention managers constructed
      // inside the try only hold executors that stay inert until started — their threads spin up on
      // start()/first task, never during build — so a failed build leaves them with no live threads
      // to stop, and GC reclaims them. Hence no engine.shutdown() here.
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
  private final class HttpEndpointBuilderImpl implements SagaManager.Builder.HttpEndpointBuilder {

    private final String name;
    private final String baseUrl;
    private final List<String> allowedHosts = new ArrayList<>();
    private final Map<String, String> defaultHeaders = new LinkedHashMap<>();
    private long maxBodyBytes = -1; // -1 = use the default
    private @Nullable HttpClient httpClient;

    private HttpEndpointBuilderImpl(String name, String baseUrl) {
      this.name = name;
      this.baseUrl = baseUrl;
    }

    @Override
    public SagaManager.Builder.HttpEndpointBuilder allowedHosts(String... hosts) {
      Objects.requireNonNull(hosts, "hosts must not be null");
      for (String host : hosts) {
        allowedHosts.add(Objects.requireNonNull(host, "host must not be null"));
      }
      return this;
    }

    @Override
    public SagaManager.Builder.HttpEndpointBuilder maxBodyBytes(long maxBodyBytes) {
      if (maxBodyBytes <= 0) {
        throw new IllegalArgumentException("maxBodyBytes must be > 0, got " + maxBodyBytes);
      }
      this.maxBodyBytes = maxBodyBytes;
      return this;
    }

    @Override
    public SagaManager.Builder.HttpEndpointBuilder httpClient(HttpClient client) {
      this.httpClient = Objects.requireNonNull(client, "client must not be null");
      return this;
    }

    @Override
    public SagaManager.Builder.HttpEndpointBuilder defaultHeader(String name, String value) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(value, "value must not be null");
      defaultHeaders.put(name, value);
      return this;
    }

    @Override
    public SagaManager.Builder.HttpEndpointBuilder defaultHeaders(Map<String, String> headers) {
      Objects.requireNonNull(headers, "headers must not be null");
      headers.forEach(
          (name, value) -> {
            Objects.requireNonNull(name, "header name must not be null");
            Objects.requireNonNull(value, "header value must not be null");
            defaultHeaders.put(name, value);
          });
      return this;
    }

    @Override
    public SagaManager.Builder add() {
      httpEndpoints.put(
          name,
          new HttpServiceConfig(baseUrl, allowedHosts, maxBodyBytes, httpClient, defaultHeaders));
      return SagaManagerBuilder.this;
    }
  }
}
