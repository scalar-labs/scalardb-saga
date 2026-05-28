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

  /** Defines a single step within a saga definition. */
  @Immutable
  public static final class StepDefinition {

    private final String name;
    private final String stepClass;
    private final long timeoutMillis;
    private final @Nullable RetryPolicy retryPolicy;
    private final boolean pivot;

    private StepDefinition(StepBuilder builder) {
      this.name = builder.name;
      this.stepClass = builder.stepClass;
      this.timeoutMillis = builder.timeoutMillis;
      this.retryPolicy = builder.retryPolicy;
      this.pivot = builder.pivot;
    }

    public String getName() {
      return name;
    }

    public String getStepClass() {
      return stepClass;
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

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof StepDefinition that)) return false;
      return timeoutMillis == that.timeoutMillis
          && pivot == that.pivot
          && name.equals(that.name)
          && stepClass.equals(that.stepClass)
          && Objects.equals(retryPolicy, that.retryPolicy);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, stepClass, timeoutMillis, retryPolicy, pivot);
    }

    @Override
    public String toString() {
      return "StepDefinition{name='" + name + "', pivot=" + pivot + '}';
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

    public SagaDefinition build() {
      SagaDefinition definition = new SagaDefinition(this);
      definition.validate();
      return definition;
    }
  }

  /** Builder for {@link StepDefinition}. */
  public static final class StepBuilder {

    private final Builder parent;
    private final String name;
    private final String stepClass;
    private long timeoutMillis;
    private @Nullable RetryPolicy retryPolicy;
    private boolean pivot;

    private StepBuilder(Builder parent, String name, String stepClass) {
      this.parent = parent;
      this.name = name;
      this.stepClass = stepClass;
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
      parent.steps.add(new StepDefinition(this));
      return parent;
    }
  }
}
