package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.store.SagaStore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
 * SagaManager manager = SagaManagerBuilder.newBuilder()
 *     .store(store)
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
 * SagaManager manager = SagaManagerBuilder.newBuilder()
 *     .store(store)
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
 * SagaManager manager = SagaManagerBuilder.newBuilder()
 *     .store(store)
 *     .stepResolver((name, className) -> steps.get(name))
 *     .build();
 * }</pre>
 */
public class SagaManagerBuilder {

  private @Nullable SagaStore store;
  private String ownerId = java.util.UUID.randomUUID().toString();
  private SagaEngine.ShutdownMode shutdownMode = SagaEngine.ShutdownMode.WAIT_CURRENT_STEP;
  private long shutdownTimeoutMillis = 30_000;
  private Clock clock = Clock.systemUTC();
  private ResourceRegistry.@Nullable Builder resourceRegistryBuilder;
  private @Nullable StepResolver customStepResolver;

  private SagaManagerBuilder() {}

  /** Creates a new builder instance. */
  public static SagaManagerBuilder newBuilder() {
    return new SagaManagerBuilder();
  }

  /** Sets the saga store (required). */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "SagaStore is an interface; storing a reference is intentional")
  public SagaManagerBuilder store(SagaStore store) {
    this.store = store;
    return this;
  }

  /**
   * Sets the owner ID for this engine instance. Defaults to a random UUID. Override with a pod name
   * or hostname for better observability.
   */
  public SagaManagerBuilder ownerId(String ownerId) {
    this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
    return this;
  }

  /** Sets the shutdown mode. Defaults to {@link SagaEngine.ShutdownMode#WAIT_CURRENT_STEP}. */
  public SagaManagerBuilder shutdownMode(SagaEngine.ShutdownMode shutdownMode) {
    this.shutdownMode = Objects.requireNonNull(shutdownMode, "shutdownMode must not be null");
    return this;
  }

  /** Sets the shutdown timeout in milliseconds. Defaults to 30,000 (30 seconds). */
  public SagaManagerBuilder shutdownTimeoutMillis(long shutdownTimeoutMillis) {
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    return this;
  }

  /** Sets the clock (for testing). Defaults to {@link Clock#systemUTC()}. */
  public SagaManagerBuilder clock(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    return this;
  }

  /**
   * Registers a named resource for constructor injection during step resolution.
   *
   * <p>Use named resources when multiple resources of the same type are registered. Step
   * constructors disambiguate via {@link com.scalar.db.saga.api.Named @Named}.
   *
   * @param type the resource type (exact type match during resolution)
   * @param instance the resource instance
   * @param name the qualifier name (must match {@code @Named} on constructor parameters)
   * @throws IllegalArgumentException if a resource with the same type and name is already
   *     registered
   */
  public <T> SagaManagerBuilder resource(Class<T> type, T instance, String name) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(instance, "instance must not be null");
    Objects.requireNonNull(name, "name must not be null");
    getOrCreateResourceRegistryBuilder().add(type, instance, name);
    return this;
  }

  /**
   * Registers an unnamed resource for constructor injection during step resolution.
   *
   * <p>Use this when only one resource of a given type is needed. If multiple resources of the same
   * type are required, use {@link #resource(Class, Object, String)} with a qualifier name.
   *
   * @param type the resource type (exact type match during resolution)
   * @param instance the resource instance
   * @throws IllegalArgumentException if an unnamed resource of the same type is already registered
   */
  public <T> SagaManagerBuilder resource(Class<T> type, T instance) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(instance, "instance must not be null");
    getOrCreateResourceRegistryBuilder().add(type, instance);
    return this;
  }

  /**
   * Sets a custom step resolver for full control over step instantiation (e.g., manual lookup, DI
   * framework integration).
   *
   * <p>Mutually exclusive with {@link #resource} — calling both causes {@link #build()} to throw.
   */
  public SagaManagerBuilder stepResolver(StepResolver stepResolver) {
    Objects.requireNonNull(stepResolver, "stepResolver must not be null");
    this.customStepResolver = stepResolver;
    return this;
  }

  /**
   * Builds and returns a configured {@link SagaManager}.
   *
   * @throws IllegalStateException if the store is not set, or if both {@code resource()} and {@code
   *     stepResolver()} were called
   */
  public SagaManager build() {
    if (store == null) {
      throw new IllegalStateException("SagaStore is required — call store() before build()");
    }

    if (resourceRegistryBuilder != null && customStepResolver != null) {
      throw new IllegalStateException(
          "resource() and stepResolver() are mutually exclusive — use one or the other");
    }

    StepResolver resolver = buildStepResolver();

    SagaEngine.ShutdownConfig shutdownConfig =
        new SagaEngine.ShutdownConfig(shutdownMode, shutdownTimeoutMillis);
    SagaEngine engine = new SagaEngine(store, resolver, ownerId, shutdownConfig, clock);
    SagaDefinitionRegistry registry = new SagaDefinitionRegistry(store);

    return new EmbeddedSagaManager(engine, store, registry, shutdownTimeoutMillis);
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
