package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Defines a saga's structure: its steps, execution mode, recovery strategy, and timeout
 * configuration.
 *
 * <p>Use {@link #newBuilder(String, SagaMode)} to create definitions programmatically. Definitions
 * are validated at build time.
 */
@Immutable
public final class SagaDefinition {

  /** Execution mode: Saga (compensate on failure) or TCC (Try-Confirm-Cancel). */
  public enum SagaMode {
    SAGA,
    TCC
  }

  /** Recovery strategy. Determines which steps are compensatable vs retriable. */
  public enum RecoveryStrategy {
    /**
     * On step failure, compensate all completed steps (default for Saga mode). Pivot = last step.
     */
    BACKWARD,
    /** On step failure, saga stays RUNNING for automatic recovery retry. Pivot = -1. */
    FORWARD,
    /** Steps before pivot are compensatable, after pivot are retriable. User-specified pivot. */
    MIXED,
    /** Recovery strategy is predefined by the mode (e.g., TCC uses Cancel phase). */
    PREDEFINED
  }

  private final String name;
  private final String version;
  private final SagaMode mode;
  private final List<StepDefinition> steps;
  private final RecoveryStrategy recoveryStrategy;
  private final long timeoutMillis;
  private final @Nullable RetryPolicy defaultRetryPolicy;
  private final int pivotIndex;

  private SagaDefinition(Builder builder) {
    this.name = builder.name;
    this.version = builder.version;
    this.mode = builder.mode;
    this.steps = List.copyOf(builder.steps);
    this.recoveryStrategy = builder.recoveryStrategy;
    this.timeoutMillis = builder.timeoutMillis;
    this.defaultRetryPolicy = builder.defaultRetryPolicy;
    this.pivotIndex = computePivotIndex();
  }

  public static Builder newBuilder(String name, SagaMode mode) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(mode, "mode must not be null");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    return new Builder(name, mode);
  }

  /**
   * Returns the pivot index for the execution plan.
   *
   * <ul>
   *   <li>BACKWARD: last index (all steps are compensatable)
   *   <li>FORWARD: -1 (all steps are retriable)
   *   <li>MIXED: the index of the step with {@code pivot=true}
   *   <li>PREDEFINED: last index (e.g., TCC last try step; confirm steps are added by the engine)
   * </ul>
   */
  public int getPivotIndex() {
    return pivotIndex;
  }

  private int computePivotIndex() {
    if (recoveryStrategy == RecoveryStrategy.PREDEFINED
        || recoveryStrategy == RecoveryStrategy.BACKWARD) {
      return steps.size() - 1;
    }
    if (recoveryStrategy == RecoveryStrategy.FORWARD) {
      return -1;
    }
    // MIXED: find the pivot step; return -1 if not found (validate() will catch it)
    for (int i = 0; i < steps.size(); i++) {
      if (steps.get(i).isPivot()) {
        return i;
      }
    }
    return -1;
  }

  private void validate() {
    if (name.contains(":")) {
      throw new SagaDefinitionException("Saga name must not contain ':': '" + name + "'");
    }
    if (version.contains(":")) {
      throw new SagaDefinitionException("Saga version must not contain ':': '" + version + "'");
    }

    if (steps.isEmpty()) {
      throw new SagaDefinitionException(
          "Saga definition '" + name + "' must have at least one step");
    }

    if (timeoutMillis < 0) {
      throw new SagaDefinitionException(
          "Saga definition '" + name + "' timeoutMillis must be >= 0, got " + timeoutMillis);
    }

    // Single-pass: check name uniqueness, step timeouts, and count pivots
    Set<String> stepNames = new HashSet<>();
    int pivotCount = 0;
    for (StepDefinition step : steps) {
      if (!stepNames.add(step.getName())) {
        throw new SagaDefinitionException(
            "Duplicate step name '" + step.getName() + "' in saga '" + name + "'");
      }
      if (step.getTimeoutMillis() < 0) {
        throw new SagaDefinitionException(
            "Step '"
                + step.getName()
                + "' timeoutMillis must be >= 0, got "
                + step.getTimeoutMillis());
      }
      if (step.isPivot()) {
        pivotCount++;
      }
    }

    if (mode == SagaMode.TCC) {
      if (recoveryStrategy != RecoveryStrategy.PREDEFINED) {
        throw new SagaDefinitionException(
            "TCC mode must not specify a recovery strategy"
                + " — recovery is predefined via the Cancel phase");
      }
      if (pivotCount != 0) {
        throw new SagaDefinitionException(
            "TCC definitions must not specify a pivot step"
                + " — the pivot is implicit (last try step)");
      }
      // A TCC step may be backed by a class (implementing TccStep) or a service operation
      // (reserve/confirm/cancel). Capability — the class implementing TccStep, or the invoker
      // supporting the TCC phases — is validated at registration (StepInstantiator), where the
      // resolver/registry is available.
      return;
    }

    switch (recoveryStrategy) {
      case BACKWARD, FORWARD -> {
        if (pivotCount != 0) {
          throw new SagaDefinitionException(
              recoveryStrategy + " strategy must not specify a pivot step");
        }
      }
      case MIXED -> {
        if (pivotCount != 1) {
          throw new SagaDefinitionException(
              "MIXED strategy requires exactly one pivot step, found " + pivotCount);
        }
        if (pivotIndex == 0 || pivotIndex == steps.size() - 1) {
          throw new SagaDefinitionException(
              "MIXED pivot must not be the first or last step; a pivot at the first step is"
                  + " FORWARD, and a pivot at the last step is BACKWARD. MIXED requires at least"
                  + " one step before the pivot and one after");
        }
      }
      case PREDEFINED ->
          throw new SagaDefinitionException(
              "PREDEFINED recovery strategy is reserved for TCC mode");
    }
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public SagaMode getMode() {
    return mode;
  }

  public List<StepDefinition> getSteps() {
    return steps;
  }

  public RecoveryStrategy getRecoveryStrategy() {
    return recoveryStrategy;
  }

  /**
   * Returns the saga-level timeout in milliseconds. {@code 0} means no timeout (the saga runs until
   * completion or escalation).
   */
  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public @Nullable RetryPolicy getDefaultRetryPolicy() {
    return defaultRetryPolicy;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaDefinition that)) return false;
    return timeoutMillis == that.timeoutMillis
        && name.equals(that.name)
        && version.equals(that.version)
        && mode == that.mode
        && steps.equals(that.steps)
        && recoveryStrategy == that.recoveryStrategy
        && Objects.equals(defaultRetryPolicy, that.defaultRetryPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, version, mode, steps, recoveryStrategy, timeoutMillis, defaultRetryPolicy);
  }

  @Override
  public String toString() {
    return "SagaDefinition{name='"
        + name
        + "', version='"
        + version
        + "', mode="
        + mode
        + ", recoveryStrategy="
        + recoveryStrategy
        + ", steps="
        + steps.size()
        + '}';
  }

  /**
   * Defines a single step within a saga definition.
   *
   * <p>Sealed: a step is exactly one of
   *
   * <ul>
   *   <li>{@link ClassStep} — backed by a user {@link Step}/{@link TccStep} class (Layer 1)
   *   <li>{@link ServiceStep} — dispatched to a registered service invoker (Layer 2)
   * </ul>
   *
   * <p>Consumers that need step-kind-specific data use a Java pattern-matching {@code switch} over
   * the permitted subtypes. The common fields ({@link #getName()}, {@link #getTimeoutMillis()},
   * {@link #getRetryPolicy()}, {@link #isPivot()}) are available on the base type.
   */
  @Immutable
  public abstract static sealed class StepDefinition permits ClassStep, ServiceStep {

    private final String name;
    private final long timeoutMillis;
    private final @Nullable RetryPolicy retryPolicy;
    private final boolean pivot;

    private StepDefinition(StepBuilder builder) {
      this.name = builder.name;
      this.timeoutMillis = builder.timeoutMillis;
      this.retryPolicy = builder.retryPolicy;
      this.pivot = builder.pivot;
    }

    public String getName() {
      return name;
    }

    /**
     * Returns the step-level timeout in milliseconds. {@code 0} means no step-level timeout
     * (inherits the saga-level timeout).
     */
    public long getTimeoutMillis() {
      return timeoutMillis;
    }

    public @Nullable RetryPolicy getRetryPolicy() {
      return retryPolicy;
    }

    public boolean isPivot() {
      return pivot;
    }

    /** Compares the common base fields. Used by subtype {@code equals} implementations. */
    boolean equalsCommon(StepDefinition that) {
      return timeoutMillis == that.timeoutMillis
          && pivot == that.pivot
          && name.equals(that.name)
          && Objects.equals(retryPolicy, that.retryPolicy);
    }
  }

  /** A step backed by a user {@link Step} or {@link TccStep} class (Layer 1). */
  @Immutable
  public static final class ClassStep extends StepDefinition {

    private final String stepClass;

    private ClassStep(StepBuilder builder) {
      super(builder);
      this.stepClass = Objects.requireNonNull(builder.stepClass);
    }

    /** Returns the fully-qualified class name implementing {@link Step} or {@link TccStep}. */
    public String getStepClass() {
      return stepClass;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof ClassStep that)) return false;
      return equalsCommon(that) && stepClass.equals(that.stepClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), stepClass, getTimeoutMillis(), getRetryPolicy(), isPivot());
    }

    @Override
    public String toString() {
      return "ClassStep{name='" + getName() + "', stepClass='" + stepClass + "'}";
    }
  }

  /**
   * A step dispatched to a {@code ServiceInvoker} registered under {@link #getService()}, invoking
   * its {@link #getOperation()} (Layer 2).
   */
  @Immutable
  public static final class ServiceStep extends StepDefinition {

    private final String service;
    private final String operation;

    private ServiceStep(StepBuilder builder) {
      super(builder);
      this.service = Objects.requireNonNull(builder.service);
      this.operation = Objects.requireNonNull(builder.operation);
    }

    /** Returns the logical service name the invoker is registered under. */
    public String getService() {
      return service;
    }

    /** Returns the operation name to invoke on the service. */
    public String getOperation() {
      return operation;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof ServiceStep that)) return false;
      return equalsCommon(that) && service.equals(that.service) && operation.equals(that.operation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          getName(), service, operation, getTimeoutMillis(), getRetryPolicy(), isPivot());
    }

    @Override
    public String toString() {
      return "ServiceStep{name='"
          + getName()
          + "', service='"
          + service
          + "', operation='"
          + operation
          + "'}";
    }
  }

  /** Builder for {@link SagaDefinition}. */
  public static final class Builder {

    private final String name;
    private final SagaMode mode;
    private String version = "1.0";
    private final List<StepDefinition> steps = new ArrayList<>();
    private RecoveryStrategy recoveryStrategy;
    private long timeoutMillis;
    private @Nullable RetryPolicy defaultRetryPolicy;

    private Builder(String name, SagaMode mode) {
      this.name = name;
      this.mode = mode;
      this.recoveryStrategy =
          mode == SagaMode.TCC ? RecoveryStrategy.PREDEFINED : RecoveryStrategy.BACKWARD;
    }

    public Builder version(String version) {
      this.version = Objects.requireNonNull(version, "version must not be null");
      return this;
    }

    public Builder recoveryStrategy(RecoveryStrategy recoveryStrategy) {
      this.recoveryStrategy =
          Objects.requireNonNull(recoveryStrategy, "recoveryStrategy must not be null");
      return this;
    }

    public Builder timeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public Builder defaultRetryPolicy(RetryPolicy defaultRetryPolicy) {
      this.defaultRetryPolicy =
          Objects.requireNonNull(defaultRetryPolicy, "defaultRetryPolicy must not be null");
      return this;
    }

    /** Starts building a new step with the given name and step class. */
    public StepBuilder step(String name, String stepClass) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(stepClass, "stepClass must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("step name must not be blank");
      }
      if (stepClass.isBlank()) {
        throw new IllegalArgumentException("stepClass must not be blank");
      }
      return new StepBuilder(this, name, stepClass);
    }

    /** Starts building a new step with the given name and step class. */
    public StepBuilder step(String name, Class<?> stepClass) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(stepClass, "stepClass must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("step name must not be blank");
      }
      if (!Step.class.isAssignableFrom(stepClass) && !TccStep.class.isAssignableFrom(stepClass)) {
        throw new IllegalArgumentException(
            "stepClass must implement Step or TccStep: " + stepClass.getName());
      }
      return new StepBuilder(this, name, stepClass.getName());
    }

    /**
     * Starts building a step dispatched to a {@code ServiceInvoker} registered under {@code
     * service} (Layer 2). Valid in both {@link SagaMode#SAGA} and {@link SagaMode#TCC} mode: the
     * required phases — execute/compensate for SAGA, reserve/confirm/cancel for TCC — are
     * determined by the saga's mode and validated against the registered invoker at registration.
     */
    public StepBuilder serviceStep(String name, String service, String operation) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(service, "service must not be null");
      Objects.requireNonNull(operation, "operation must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("step name must not be blank");
      }
      if (service.isBlank()) {
        throw new IllegalArgumentException("service must not be blank");
      }
      if (operation.isBlank()) {
        throw new IllegalArgumentException("operation must not be blank");
      }
      return new StepBuilder(this, name, service, operation);
    }

    public SagaDefinition build() {
      SagaDefinition definition = new SagaDefinition(this);
      definition.validate();
      return definition;
    }
  }

  /**
   * Builder for {@link StepDefinition}. The step kind is fixed by the factory method that created
   * this builder.
   */
  public static final class StepBuilder {

    private enum Kind {
      CLASS,
      SERVICE
    }

    private final Builder parent;
    private final Kind kind;
    private final String name;
    private final @Nullable String stepClass;
    private final @Nullable String service;
    private final @Nullable String operation;
    private long timeoutMillis;
    private @Nullable RetryPolicy retryPolicy;
    private boolean pivot;

    private StepBuilder(Builder parent, String name, String stepClass) {
      this.parent = parent;
      this.kind = Kind.CLASS;
      this.name = name;
      this.stepClass = stepClass;
      this.service = null;
      this.operation = null;
    }

    private StepBuilder(Builder parent, String name, String service, String operation) {
      this.parent = parent;
      this.kind = Kind.SERVICE;
      this.name = name;
      this.stepClass = null;
      this.service = service;
      this.operation = operation;
    }

    public StepBuilder timeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public StepBuilder retryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
      return this;
    }

    public StepBuilder pivot(boolean pivot) {
      this.pivot = pivot;
      return this;
    }

    /** Adds this step to the parent builder and returns it for chaining. */
    public Builder add() {
      StepDefinition step =
          switch (kind) {
            case CLASS -> new ClassStep(this);
            case SERVICE -> new ServiceStep(this);
          };
      parent.steps.add(step);
      return parent;
    }
  }
}
