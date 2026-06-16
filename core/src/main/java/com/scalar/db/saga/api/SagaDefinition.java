package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
      // Every service step is declarative and inlines its phases, so its SAGA/TCC nature must match
      // the saga's mode. (Class steps resolve their mode capability at registration, where the
      // resolver is available.) This runs before any getTransport() so an empty-phase map can't
      // NPE.
      if (step instanceof ServiceStep service) {
        validateDeclarativePhases(service);
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

  private void validateDeclarativePhases(ServiceStep service) {
    Set<ServiceStep.Phase> expected =
        mode == SagaMode.TCC ? ServiceStep.TCC_PHASES : ServiceStep.SAGA_PHASES;
    if (!service.getPhases().keySet().equals(expected)) {
      throw new SagaDefinitionException(
          "Declarative service step '"
              + service.getName()
              + "' in "
              + mode
              + " mode must define exactly the phases "
              + expected
              + ", but has "
              + service.getPhases().keySet());
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
   *   <li>{@link ServiceStep} — a declaratively-defined step against a registered service,
   *       supplying an inline {@link CallSpec} per {@link ServiceStep.Phase phase} (Layer 2b)
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

    private StepDefinition(AbstractStepBuilder<?> builder) {
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
   * A declaratively-defined step against a registered {@code service} (Layer 2b): a {@link
   * CallSpec} per {@link Phase}, run by a registered transport adapter. A SAGA step defines {@link
   * Phase#EXECUTION} + {@link Phase#COMPENSATION}; a TCC step defines {@link Phase#RESERVATION} +
   * {@link Phase#CONFIRMATION} + {@link Phase#CANCELLATION}. The transport is derived from the call
   * specs; all phases share it.
   */
  @Immutable
  public static final class ServiceStep extends StepDefinition {

    /**
     * A phase of a service step's lifecycle, each backed by a {@link CallSpec}. A SAGA step defines
     * {@link #EXECUTION} + {@link #COMPENSATION}; a TCC step defines {@link #RESERVATION} + {@link
     * #CONFIRMATION} + {@link #CANCELLATION}. The {@code Step}/{@code TccStep} method names stay
     * verbs per the Java method convention.
     */
    public enum Phase {
      /** SAGA forward action. */
      EXECUTION,
      /** SAGA compensating action. */
      COMPENSATION,
      /** TCC try/reserve action. */
      RESERVATION,
      /** TCC confirm action. */
      CONFIRMATION,
      /** TCC cancel action. */
      CANCELLATION
    }

    static final Set<Phase> SAGA_PHASES = EnumSet.of(Phase.EXECUTION, Phase.COMPENSATION);
    static final Set<Phase> TCC_PHASES =
        EnumSet.of(Phase.RESERVATION, Phase.CONFIRMATION, Phase.CANCELLATION);

    private final String service;
    private final Map<Phase, CallSpec> phases;

    private ServiceStep(
        AbstractStepBuilder<?> builder, String service, Map<Phase, CallSpec> phases) {
      super(builder);
      this.service = service;
      this.phases = Map.copyOf(phases);
    }

    /** Returns the logical service name the transport adapter is registered under. */
    public String getService() {
      return service;
    }

    /** Returns the call specs by phase. Unmodifiable. */
    public Map<Phase, CallSpec> getPhases() {
      return phases;
    }

    /** Returns the call spec for {@code phase}, if defined. */
    public Optional<CallSpec> getPhase(Phase phase) {
      return Optional.ofNullable(phases.get(phase));
    }

    /** Returns the wire transport, derived from the call specs (all phases share one transport). */
    public CallSpec.Transport getTransport() {
      return phases.values().iterator().next().transport();
    }

    /** Whether this is a TCC step (reservation/confirmation/cancellation) rather than SAGA. */
    public boolean isTcc() {
      return phases.containsKey(Phase.RESERVATION);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof ServiceStep that)) return false;
      return equalsCommon(that) && service.equals(that.service) && phases.equals(that.phases);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          getName(), service, phases, getTimeoutMillis(), getRetryPolicy(), isPivot());
    }

    @Override
    public String toString() {
      return "ServiceStep{name='"
          + getName()
          + "', service='"
          + service
          + "', transport="
          + getTransport()
          + ", phases="
          + phases.keySet()
          + "}";
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
     * Starts building a declaratively-defined step against a registered {@code service} (Layer 2b).
     * On the returned selector choose SAGA ({@link ServiceStepBuilder#operation()}) or TCC ({@link
     * ServiceStepBuilder#tccOperation()}), then set the call spec per phase. Valid in both {@link
     * SagaMode#SAGA} and {@link SagaMode#TCC} mode; the step's phases must match the saga's mode
     * (checked at build).
     */
    public ServiceStepBuilder serviceStep(String name, String service) {
      checkStepNameAndService(name, service);
      return new ServiceStepBuilder(this, name, service);
    }

    private static void checkStepNameAndService(String name, String service) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(service, "service must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("step name must not be blank");
      }
      if (service.isBlank()) {
        throw new IllegalArgumentException("service must not be blank");
      }
    }

    public SagaDefinition build() {
      SagaDefinition definition = new SagaDefinition(this);
      definition.validate();
      return definition;
    }
  }

  /**
   * Common base for the step builders. Holds the fields shared by every step kind and the optional
   * setters ({@link #timeoutMillis}, {@link #retryPolicy}, {@link #pivot}). The {@code SELF} type
   * parameter lets those setters return the concrete builder type for fluent chaining.
   */
  public abstract static class AbstractStepBuilder<SELF extends AbstractStepBuilder<SELF>> {

    final Builder parent;
    final String name;
    long timeoutMillis;
    @Nullable RetryPolicy retryPolicy;
    boolean pivot;

    private AbstractStepBuilder(Builder parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    @SuppressWarnings("unchecked")
    final SELF self() {
      return (SELF) this;
    }

    public SELF timeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return self();
    }

    public SELF retryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
      return self();
    }

    public SELF pivot(boolean pivot) {
      this.pivot = pivot;
      return self();
    }

    /** Adds this step to the parent builder and returns it for chaining. */
    public abstract Builder add();
  }

  /**
   * Builder for a class step ({@link ClassStep}). Created by {@link Builder#step}; carries the
   * fully-qualified step class name.
   */
  public static final class StepBuilder extends AbstractStepBuilder<StepBuilder> {

    private final String stepClass;

    private StepBuilder(Builder parent, String name, String stepClass) {
      super(parent, name);
      this.stepClass = stepClass;
    }

    @Override
    public Builder add() {
      parent.steps.add(new ClassStep(this));
      return parent;
    }
  }

  /**
   * Selector for a {@link ServiceStep}. Created by {@link Builder#serviceStep}. Choose SAGA via
   * {@link #operation()} or TCC via {@link #tccOperation()}; the SAGA and TCC phase setters live on
   * distinct returned builders, so they cannot be mixed.
   */
  public static final class ServiceStepBuilder {

    private final Builder parent;
    private final String name;
    private final String service;

    private ServiceStepBuilder(Builder parent, String name, String service) {
      this.parent = parent;
      this.name = name;
      this.service = service;
    }

    /**
     * Builds a SAGA service step (Layer 2b). Set its {@link DeclarativeStepBuilder#execution} and
     * {@link DeclarativeStepBuilder#compensation} call specs, then {@link
     * DeclarativeStepBuilder#add()}.
     */
    public DeclarativeStepBuilder operation() {
      return new DeclarativeStepBuilder(parent, name, service);
    }

    /**
     * Builds a TCC service step (Layer 2b). Set its {@link TccDeclarativeStepBuilder#reservation},
     * {@link TccDeclarativeStepBuilder#confirmation}, and {@link
     * TccDeclarativeStepBuilder#cancellation} call specs, then {@link
     * TccDeclarativeStepBuilder#add()}.
     */
    public TccDeclarativeStepBuilder tccOperation() {
      return new TccDeclarativeStepBuilder(parent, name, service);
    }
  }

  /**
   * Common base for the declarative service-step builders. Accumulates the call specs by phase,
   * with duplicate-phase rejection and the completeness/transport validation run at {@link #add()}.
   * Subclasses expose only the phase setters valid for their mode.
   */
  abstract static sealed class AbstractDeclarativeStepBuilder<
          SELF extends AbstractDeclarativeStepBuilder<SELF>>
      extends AbstractStepBuilder<SELF> permits DeclarativeStepBuilder, TccDeclarativeStepBuilder {

    final String service;
    final Map<ServiceStep.Phase, CallSpec> phases = new EnumMap<>(ServiceStep.Phase.class);

    private AbstractDeclarativeStepBuilder(Builder parent, String name, String service) {
      super(parent, name);
      this.service = service;
    }

    /** Records {@code call} for {@code phase}, rejecting a second set of the same phase. */
    final SELF putPhase(ServiceStep.Phase phase, CallSpec call) {
      Objects.requireNonNull(call, "call spec must not be null");
      if (phases.containsKey(phase)) {
        throw new IllegalStateException(
            "phase " + phase + " is already set for declarative service step '" + name + "'");
      }
      phases.put(phase, call);
      return self();
    }

    /** The full phase set this mode requires; {@link #add()} rejects a step missing any of them. */
    abstract Set<ServiceStep.Phase> requiredPhases();

    @Override
    public Builder add() {
      Set<ServiceStep.Phase> required = requiredPhases();
      if (!phases.keySet().equals(required)) {
        Set<ServiceStep.Phase> missing = EnumSet.copyOf(required);
        missing.removeAll(phases.keySet());
        throw new IllegalStateException(
            "declarative service step '" + name + "' is missing required phase(s) " + missing);
      }
      validateSingleTransport();
      parent.steps.add(new ServiceStep(this, service, phases));
      return parent;
    }

    private void validateSingleTransport() {
      CallSpec.Transport transport = null;
      for (CallSpec spec : phases.values()) {
        if (transport == null) {
          transport = spec.transport();
        } else if (transport != spec.transport()) {
          throw new IllegalArgumentException(
              "all call specs of declarative service step '"
                  + name
                  + "' must use the same transport, but found "
                  + transport
                  + " and "
                  + spec.transport());
        }
      }
    }
  }

  /** Builder for a declarative SAGA service step. Exposes only the SAGA phase setters. */
  public static final class DeclarativeStepBuilder
      extends AbstractDeclarativeStepBuilder<DeclarativeStepBuilder> {

    private DeclarativeStepBuilder(Builder parent, String name, String service) {
      super(parent, name, service);
    }

    /** Sets the SAGA forward call spec. */
    public DeclarativeStepBuilder execution(CallSpec call) {
      return putPhase(ServiceStep.Phase.EXECUTION, call);
    }

    /** Sets the SAGA compensating call spec. */
    public DeclarativeStepBuilder compensation(CallSpec call) {
      return putPhase(ServiceStep.Phase.COMPENSATION, call);
    }

    @Override
    Set<ServiceStep.Phase> requiredPhases() {
      return ServiceStep.SAGA_PHASES;
    }
  }

  /** Builder for a declarative TCC service step. Exposes only the TCC phase setters. */
  public static final class TccDeclarativeStepBuilder
      extends AbstractDeclarativeStepBuilder<TccDeclarativeStepBuilder> {

    private TccDeclarativeStepBuilder(Builder parent, String name, String service) {
      super(parent, name, service);
    }

    /** Sets the TCC reserve call spec. */
    public TccDeclarativeStepBuilder reservation(CallSpec call) {
      return putPhase(ServiceStep.Phase.RESERVATION, call);
    }

    /** Sets the TCC confirm call spec. */
    public TccDeclarativeStepBuilder confirmation(CallSpec call) {
      return putPhase(ServiceStep.Phase.CONFIRMATION, call);
    }

    /** Sets the TCC cancel call spec. */
    public TccDeclarativeStepBuilder cancellation(CallSpec call) {
      return putPhase(ServiceStep.Phase.CANCELLATION, call);
    }

    @Override
    Set<ServiceStep.Phase> requiredPhases() {
      return ServiceStep.TCC_PHASES;
    }
  }
}
