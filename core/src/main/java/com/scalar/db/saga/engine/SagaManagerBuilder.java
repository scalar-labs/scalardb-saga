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
import java.time.Clock;
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

    SagaStore store = storeFactory.createStore();
    try {
      StepResolver resolver = buildStepResolver();

      RecoveryConfig resolvedRecoveryConfig =
          recoveryConfig != null ? recoveryConfig : RecoveryConfig.defaults(clock);
      RetentionConfig resolvedRetentionConfig =
          retentionConfig != null ? retentionConfig : RetentionConfig.defaults(clock);

      SagaEngine.ShutdownConfig shutdownConfig =
          new SagaEngine.ShutdownConfig(shutdownMode, shutdownTimeoutMillis);
      SagaEngine engine = new SagaEngine(store, resolver, ownerId, shutdownConfig, clock);
      SagaDefinitionRegistry registry = new SagaDefinitionRegistry(store);

      SagaRecoveryManager recoveryManager =
          new SagaRecoveryManager(store, engine, registry, ownerId, resolvedRecoveryConfig);
      SagaRetentionManager retentionManager =
          new SagaRetentionManager(store, resolvedRetentionConfig);

      return new EmbeddedSagaManager(
          engine, store, registry, recoveryManager, retentionManager, shutdownTimeoutMillis);
    } catch (Exception e) {
      try {
        store.close();
      } catch (RuntimeException closeException) {
        e.addSuppressed(closeException);
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
}
