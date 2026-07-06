package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.SagaMode;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.exception.StepTimeoutException;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.transport.SagaCorrelationContext;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core saga execution engine. Implements the pivot-based execution loop for both Saga and TCC
 * modes.
 *
 * <p>Manages saga lifecycle from creation through execution, compensation, and graceful shutdown.
 */
@ThreadSafe
public class SagaEngine implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SagaEngine.class);

  /** Shutdown configuration for the engine. */
  record ShutdownConfig(ShutdownMode mode, long timeoutMillis) {

    ShutdownConfig {
      Objects.requireNonNull(mode, "mode must not be null");
      if (timeoutMillis < 0) {
        throw new IllegalArgumentException("timeoutMillis must be >= 0, got " + timeoutMillis);
      }
    }

    private static final ShutdownConfig DEFAULT =
        new ShutdownConfig(ShutdownMode.WAIT_CURRENT_STEP, 30_000);

    public static ShutdownConfig defaultConfig() {
      return DEFAULT;
    }
  }

  private final SagaStore store;
  private final StepInstantiator stepInstantiator;
  private final String ownerId;
  private final ShutdownConfig shutdownConfig;
  private final Clock clock;
  private volatile boolean shuttingDown = false;
  private final Object shutdownLock = new Object();
  private final Set<String> activeSagas = ConcurrentHashMap.newKeySet();
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final ConcurrentHashMap<String, List<StepWithPolicy>> planCache =
      new ConcurrentHashMap<>();

  SagaEngine(
      SagaStore store,
      StepInstantiator stepInstantiator,
      String ownerId,
      ShutdownConfig shutdownConfig,
      Clock clock) {
    this.store = store;
    this.stepInstantiator = stepInstantiator;
    this.ownerId = ownerId;
    this.shutdownConfig = shutdownConfig;
    this.clock = clock;
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Creates a new saga instance without executing it.
   *
   * @param def the saga definition
   * @param sagaId client-supplied saga ID, or {@code null} to auto-generate
   * @param input the saga input data
   * @return the initial state snapshot
   */
  SagaStateSnapshot createSaga(
      SagaDefinition def, @Nullable String sagaId, Map<String, Object> input) {
    if (shuttingDown) {
      throw new IllegalStateException("Engine is shutting down; cannot create new sagas");
    }
    return store.createSaga(sagaId, def.getName(), ownerId, input, def.getVersion());
  }

  /**
   * Executes a saga from an existing snapshot.
   *
   * @param def the saga definition
   * @param saga the initial state snapshot (from {@link #createSaga})
   * @param input the saga input data
   */
  void executeSaga(SagaDefinition def, SagaStateSnapshot saga, Map<String, Object> input) {
    String sagaId = saga.getSagaId();
    if (!registerActive(sagaId)) {
      store.markForRecovery(sagaId);
      return;
    }
    try {
      ExecutionContext context = new ExecutionContext(sagaId, input, saga);
      context.setNextEventSequence(1); // SAGA_STARTED was seq 0
      executeSteps(def, context, 0);
    } finally {
      unregisterActive(sagaId);
    }
  }

  /**
   * Convenience method: creates and executes a saga in one call.
   *
   * @return the saga ID
   */
  String execute(SagaDefinition def, @Nullable String sagaId, Map<String, Object> input) {
    SagaStateSnapshot saga = createSaga(def, sagaId, input);
    executeSaga(def, saga, input);
    return saga.getSagaId();
  }

  /**
   * Resumes a saga from a specific step (used by recovery).
   *
   * @return the final state snapshot
   */
  public SagaStateSnapshot resumeFrom(SagaDefinition def, ExecutionContext context, int fromStep) {
    String sagaId = context.getSagaId();
    if (!registerActive(sagaId)) {
      store.markForRecovery(sagaId);
      return context.getCurrentState();
    }
    try {
      executeSteps(def, context, fromStep);
    } finally {
      unregisterActive(sagaId);
    }
    return context.getCurrentState();
  }

  /**
   * Triggers compensation from a specific step (used by recovery for sagas stuck in COMPENSATING).
   */
  public void compensateFrom(SagaDefinition def, ExecutionContext context, int fromStep) {
    String sagaId = context.getSagaId();
    if (!registerActive(sagaId)) {
      store.markForRecovery(sagaId);
      return;
    }
    try {
      List<StepWithPolicy> plan = getOrBuildPlan(def);
      compensate(plan, context, fromStep);
    } finally {
      unregisterActive(sagaId);
    }
  }

  /** Replays events to reconstruct an ExecutionContext for crash recovery. */
  public ExecutionContext replayEvents(SagaStateSnapshot saga, List<SagaEvent> events) {
    ExecutionContext context = new ExecutionContext(saga.getSagaId(), Map.of(), saga);

    for (SagaEvent event : events) {
      switch (event.getEventType()) {
        case SAGA_STARTED -> {
          Map<String, Object> input = EventPayloadSerializer.deserializeMap(event.getPayload());
          input.forEach(context::put);
        }
        case STEP_PENDING -> {
          // Park marker (RUNNING → WAITING); superseded by STEP_COMPLETED on resume or STEP_FAILED
          // on timeout, so there is nothing to fold into context here.
        }
        case STEP_COMPLETED -> {
          StepEvent stepEvent = (StepEvent) event;
          Map<String, Object> output =
              EventPayloadSerializer.deserializeMap(stepEvent.getPayload());
          if (!output.isEmpty()) {
            context.merge(StepResult.of(output));
          }
        }
        case STEP_FAILED -> context.markStepFailed(((StepEvent) event).getStepIndex());
        case STEP_COMPENSATED -> context.markStepCompensated(((StepEvent) event).getStepIndex());
        case STEP_COMPENSATION_FAILED -> {
          // Tracked for future observability; saga stays COMPENSATING
        }
        default -> {
          // Saga-level events (SAGA_COMPENSATING, etc.) — status tracked via snapshot
        }
      }
    }

    context.setNextEventSequence(events.size());
    context.setCurrentState(saga);
    return context;
  }

  /** Initiates graceful shutdown. */
  void shutdown() {
    synchronized (shutdownLock) {
      shuttingDown = true;
    }

    long deadline = clock.millis() + shutdownConfig.timeoutMillis();

    while (!activeSagas.isEmpty()) {
      long remaining = deadline - clock.millis();
      if (remaining <= 0) {
        break;
      }
      try {
        Thread.sleep(Math.min(remaining, 100));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    // Mark remaining active sagas for immediate recovery pickup
    for (String sagaId : activeSagas) {
      try {
        store.markForRecovery(sagaId);
        logger.info("Marked saga {} for recovery during shutdown", sagaId);
      } catch (RuntimeException e) {
        logger.warn("Failed to mark saga {} for recovery during shutdown", sagaId, e);
      }
    }

    executor.shutdownNow();

    // In-flight sagas have drained (or been marked for recovery), so no step is still calling out:
    // release the registries' HTTP clients. The step instantiator owns them.
    stepInstantiator.close();
  }

  @Override
  public void close() {
    shutdown();
  }

  // ---------------------------------------------------------------------------
  // Private — execution loop
  // ---------------------------------------------------------------------------

  private void executeSteps(SagaDefinition def, ExecutionContext context, int startIndex) {
    List<StepWithPolicy> plan = getOrBuildPlan(def);
    int pivotIndex = def.getPivotIndex();
    executeSagaSteps(plan, pivotIndex, context, startIndex, def.getTimeoutMillis());
  }

  private void executeSagaSteps(
      List<StepWithPolicy> plan,
      int pivotIndex,
      ExecutionContext context,
      int startIndex,
      long sagaTimeoutMillis) {

    long sagaDeadline = TimeoutPolicy.calculateSagaDeadline(sagaTimeoutMillis, clock.millis());

    for (int i = startIndex; i < plan.size(); i++) {
      // Check graceful shutdown between steps
      if (shouldStopBetweenSteps()) {
        logger.info("Stopping saga {} between steps due to shutdown", context.getSagaId());
        return;
      }

      // Check saga timeout
      if (TimeoutPolicy.isSagaTimedOut(sagaDeadline, clock.millis())) {
        logger.info("Saga {} timed out before step {}", context.getSagaId(), i);
        if (i <= pivotIndex) {
          compensate(plan, context, i - 1);
        }
        return;
      }

      StepWithPolicy stepWithPolicy = plan.get(i);
      long stepDeadline =
          TimeoutPolicy.calculateStepDeadline(
              stepWithPolicy.stepTimeoutMillis(), sagaDeadline, clock.millis());

      // Execute and record are SEPARATE try blocks for two reasons: they have different
      // compensation scopes, and a single broad catch would mistake an unexpected
      // RuntimeException from executeWithRetry (a bug — real step failures arrive as
      // StepExecutionException) for a record failure. Both paths durably record STEP_FAILED(i)
      // before compensating, so a crash leaves recovery a marker to include step i:
      //   - executeWithRetry fails: step i may have committed, so compensate from i (or
      //     from i - 1 if the failure proved non-delivery, knownNotCommitted).
      //   - recordStepCompleted fails: step i DID commit, so record STEP_FAILED with
      //     knownNotCommitted=false and compensate from i.

      // 1. Execute the step
      StepResult result;
      try {
        result =
            executeWithRetry(
                stepWithPolicy.step(),
                context,
                stepWithPolicy.executionRetryPolicy(),
                stepDeadline);
      } catch (StepExecutionException e) {
        recordStepFailed(context, i, stepWithPolicy.step().getName(), e);
        if (i <= pivotIndex) {
          // A forward failure may have committed step i's side effect (a 2xx with a mismatched
          // body, an in-doubt timeout, any non-HTTP class step). The honest default is to include
          // step i; skip it (compensate from i - 1) only when the failure proved non-delivery.
          int from = e.knownNotCommitted() ? i - 1 : i;
          compensate(plan, context, from);
        }
        return;
      }

      if (result.isPending()) {
        // The participant accepted the step (202) and will complete it later via a callback. Park
        // the saga (RUNNING -> WAITING) and release the thread; the callback or a deadline timeout
        // resumes it.
        parkStep(context, i, stepWithPolicy, sagaDeadline);
        return;
      }

      // 2. Record the completion event
      try {
        recordStepCompleted(context, i, stepWithPolicy.step().getName(), result);
      } catch (RuntimeException e) {
        // Step i's side effect is committed but recording STEP_COMPLETED failed. Record
        // STEP_FAILED(i) (knownNotCommitted=false) before compensating so the durable record
        // matches the in-process scope: without it, a crash before the first STEP_COMPENSATED
        // leaves recovery to start from the highest STEP_COMPLETED (i - 1) and orphan step i.
        // If STEP_COMPLETED(i) was actually persisted (ack lost), the STEP_FAILED insert
        // conflicts at the same sequence, so it throws, the saga stays RUNNING, and recovery
        // replays the persisted completion forward.
        logger.error(
            "Failed to record completion for step {} of saga {}", i, context.getSagaId(), e);
        if (i <= pivotIndex) {
          String stepName = stepWithPolicy.step().getName();
          StepExecutionException recordFailure =
              new StepExecutionException(
                  "Failed to record completion for step '" + stepName + "'", e, false);
          recordStepFailed(context, i, stepName, recordFailure);
          compensate(plan, context, i);
        }
        return;
      }
    }

    // All steps completed successfully
    transition(context, StatusEvent.completed());
  }

  private void recordStepCompleted(
      ExecutionContext context, int stepIndex, String stepName, StepResult result) {
    String payload = EventPayloadSerializer.serialize(result.getOutput());
    store.recordStepEvent(
        context.getSagaId(),
        context.nextSequence(),
        StepEvent.completed(stepIndex, stepName, payload));
    context.advanceSequence();
    context.merge(result);
  }

  private void recordStepFailed(
      ExecutionContext context, int stepIndex, String stepName, StepExecutionException e) {
    if (!context.hasFailureEvent(stepIndex)) {
      String errorPayload = EventPayloadSerializer.serializeError(e, e.knownNotCommitted());
      store.recordStepEvent(
          context.getSagaId(),
          context.nextSequence(),
          StepEvent.failed(stepIndex, stepName, errorPayload));
      context.advanceSequence();
      context.markStepFailed(stepIndex);
    }
  }

  /**
   * Executes a step with retry using virtual threads for timeout enforcement.
   *
   * @throws StepExecutionException if the step fails after all retries or is non-retryable
   */
  private StepResult executeWithRetry(
      Step step, ExecutionContext context, RetryPolicy retryPolicy, long stepDeadline)
      throws StepExecutionException {

    int maxAttempts = retryPolicy.getMaxAttempts();
    long interval = retryPolicy.getInitialIntervalMillis();

    // Sticky AND of knownNotCommitted across retries: the step is "known not committed" only if
    // EVERY attempt proved non-delivery. Once any attempt is in-doubt (may have committed), the
    // step stays possibly-committed even if a later attempt is a proven non-delivery.
    boolean possiblyCommitted = false;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Future<StepResult> future =
          executor.submit(
              () -> {
                // Bind the saga correlation on the step-execution thread so an injected
                // SagaHttpClient (an app singleton with no per-call context arg) propagates the
                // right X-Saga-Id/X-Saga-Step and bounds its per-request timeout to the step's
                // remaining deadline; restore on return.
                SagaCorrelationContext.Correlation previous =
                    SagaCorrelationContext.bind(
                        context.getSagaId(), step.getName(), stepDeadline, clock);
                try {
                  return step.execute(context);
                } finally {
                  SagaCorrelationContext.restore(previous);
                }
              });

      try {
        if (stepDeadline <= 0) {
          // No timeout configured
          return future.get();
        }
        long remaining = stepDeadline - clock.millis();
        if (remaining <= 0) {
          future.cancel(true);
          throw new StepTimeoutException(
              "Step '" + step.getName() + "' timed out (deadline already passed)");
        }
        return future.get(remaining, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        future.cancel(true);
        throw new StepTimeoutException(
            "Step '" + step.getName() + "' timed out after " + stepDeadline + "ms deadline");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw new StepExecutionException("Step '" + step.getName() + "' interrupted", e, false);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof StepExecutionException see) {
          if (!see.knownNotCommitted()) {
            possiblyCommitted = true;
          }
          if (see.isRetryable() && attempt < maxAttempts) {
            logger.warn(
                "Retryable failure on step '{}', attempt {}/{}",
                step.getName(),
                attempt,
                maxAttempts,
                see);
            try {
              interval = retryPolicy.sleepWithBackoff(interval);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new StepExecutionException(
                  "Step '" + step.getName() + "' interrupted during backoff", ie, false);
            }
            continue;
          }
          // If an earlier attempt was in-doubt, the step may have committed even though this final
          // attempt proved non-delivery — strip knownNotCommitted so the engine compensates it.
          if (possiblyCommitted && see.knownNotCommitted()) {
            Throwable seeCause = see.getCause();
            throw new StepExecutionException(
                seeCause != null ? seeCause : see, see.isRetryable(), false);
          }
          throw see;
        }
        // Unexpected exception — wrap as non-retryable
        throw new StepExecutionException(
            "Unexpected error in step '" + step.getName() + "'", cause != null ? cause : e, false);
      }
    }

    // Should not reach here, but defensive
    throw new StepExecutionException(
        "Step '" + step.getName() + "' failed: retries exhausted", false);
  }

  // ---------------------------------------------------------------------------
  // Private — plan building
  // ---------------------------------------------------------------------------

  private List<StepWithPolicy> buildPlan(SagaDefinition def) {
    if (def.getMode() == SagaMode.TCC) {
      return expandTccPlan(def);
    }
    RetryPolicy compensationPolicy = RetryPolicy.compensationDefault();
    List<StepWithPolicy> plan = new ArrayList<>();
    for (StepDefinition stepDef : def.getSteps()) {
      Step step = resolveStep(stepDef, Step.class);
      RetryPolicy policy = resolveRetryPolicy(stepDef, def);
      plan.add(
          new StepWithPolicy(
              step,
              policy,
              compensationPolicy,
              stepDef.getTimeoutMillis(),
              callbackTimeoutMillisFor(stepDef, Phase.EXECUTION)));
    }
    return plan;
  }

  private List<StepWithPolicy> expandTccPlan(SagaDefinition def) {
    List<StepWithPolicy> reserveSteps = new ArrayList<>();
    List<StepWithPolicy> confirmSteps = new ArrayList<>();
    RetryPolicy confirmPolicy = RetryPolicy.confirmDefault();
    RetryPolicy compensationPolicy = RetryPolicy.compensationDefault();

    for (StepDefinition stepDef : def.getSteps()) {
      TccStep tccStep = resolveStep(stepDef, TccStep.class);
      RetryPolicy reservePolicy = resolveRetryPolicy(stepDef, def);
      reserveSteps.add(
          new StepWithPolicy(
              new TccReserveStep(tccStep),
              reservePolicy,
              compensationPolicy,
              stepDef.getTimeoutMillis(),
              callbackTimeoutMillisFor(stepDef, Phase.RESERVATION)));
      confirmSteps.add(
          new StepWithPolicy(
              new TccConfirmStep(tccStep),
              confirmPolicy,
              compensationPolicy,
              stepDef.getTimeoutMillis(),
              callbackTimeoutMillisFor(stepDef, Phase.CONFIRMATION)));
    }

    reserveSteps.addAll(confirmSteps);
    return reserveSteps;
  }

  private <T> T resolveStep(StepDefinition stepDef, Class<T> expectedType) {
    return stepInstantiator.instantiate(stepDef, expectedType);
  }

  /**
   * The {@code callbackTimeoutMillis} of a step's phase-call, or {@code 0} for a class step (no
   * {@code CallSpec}) or a phase without one — such a step never parks.
   */
  private static long callbackTimeoutMillisFor(StepDefinition stepDef, Phase phase) {
    if (stepDef instanceof ServiceStep serviceStep) {
      return serviceStep.getPhase(phase).map(CallSpec::callbackTimeoutMillis).orElse(0L);
    }
    return 0L;
  }

  /**
   * Returns the cached execution plan for the given definition, building and caching it on first
   * access. Called during registration to fail fast on missing resources or unresolvable
   * constructors, and reused on every execution to avoid repeated resolution and allocation.
   */
  List<StepWithPolicy> getOrBuildPlan(SagaDefinition def) {
    return planCache.computeIfAbsent(planCacheKey(def), k -> List.copyOf(buildPlan(def)));
  }

  private static String planCacheKey(SagaDefinition def) {
    return def.getName() + ":" + def.getVersion();
  }

  private RetryPolicy resolveRetryPolicy(StepDefinition stepDef, SagaDefinition def) {
    if (stepDef.getRetryPolicy() != null) {
      return stepDef.getRetryPolicy();
    }
    if (def.getDefaultRetryPolicy() != null) {
      return def.getDefaultRetryPolicy();
    }
    return RetryPolicy.defaultPolicy();
  }

  // ---------------------------------------------------------------------------
  // Package-private — compensation
  // ---------------------------------------------------------------------------

  /**
   * Compensates steps in reverse order (LIFO) from {@code fromStepIndex} down to 0.
   *
   * <p>Package-private for testing.
   *
   * @param plan the execution plan
   * @param context the execution context
   * @param fromStepIndex the highest step index to compensate (inclusive)
   * @throws StepCompensationException if compensation fails after retries exhausted
   */
  void compensateSteps(List<StepWithPolicy> plan, ExecutionContext context, int fromStepIndex) {
    for (int i = fromStepIndex; i >= 0; i--) {
      if (context.isStepCompensated(i)) {
        logger.debug("Skipping already-compensated step at index {}", i);
        continue;
      }

      StepWithPolicy stepWithPolicy = plan.get(i);
      Step step = stepWithPolicy.step();
      String stepName = step.getName();

      try {
        long stepDeadline =
            stepWithPolicy.stepTimeoutMillis() <= 0
                ? 0
                : clock.millis() + stepWithPolicy.stepTimeoutMillis();
        compensateWithRetry(step, context, stepWithPolicy.compensationRetryPolicy(), stepDeadline);
        recordStepCompensated(context, i, stepName);
      } catch (StepCompensationException e) {
        recordStepCompensationFailed(context, i, stepName);
        throw new StepCompensationException(stepName, i, e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Private — compensation helpers
  // ---------------------------------------------------------------------------

  /**
   * Transitions to COMPENSATING (if not already), runs compensation, and transitions to COMPENSATED
   * on success. On failure, the saga stays COMPENSATING for recovery to retry.
   */
  private void compensate(List<StepWithPolicy> plan, ExecutionContext context, int fromStepIndex) {
    if (context.getCurrentState().getStatus() != SagaStatus.COMPENSATING) {
      transition(context, StatusEvent.compensating());
    }
    try {
      compensateSteps(plan, context, fromStepIndex);
      transition(context, StatusEvent.compensated());
    } catch (StepCompensationException e) {
      // Saga stays COMPENSATING — recovery will retry
      logger.warn("Compensation incomplete for saga {}: {}", context.getSagaId(), e.getMessage());
    }
  }

  private void compensateWithRetry(
      Step step, ExecutionContext context, RetryPolicy retryPolicy, long stepDeadline)
      throws StepCompensationException {
    int maxAttempts = retryPolicy.getMaxAttempts();
    long interval = retryPolicy.getInitialIntervalMillis();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Future<?> future =
          executor.submit(
              () -> {
                SagaCorrelationContext.Correlation previous =
                    SagaCorrelationContext.bind(
                        context.getSagaId(), step.getName(), stepDeadline, clock);
                try {
                  step.compensate(context);
                } finally {
                  SagaCorrelationContext.restore(previous);
                }
                return null;
              });

      try {
        if (stepDeadline <= 0) {
          future.get();
        } else {
          long remaining = stepDeadline - clock.millis();
          if (remaining <= 0) {
            future.cancel(true);
            throw new StepCompensationException(
                "Compensation of step '"
                    + step.getName()
                    + "' timed out (deadline already passed)");
          }
          future.get(remaining, TimeUnit.MILLISECONDS);
        }
        return;
      } catch (TimeoutException e) {
        future.cancel(true);
        throw new StepCompensationException(
            "Compensation of step '"
                + step.getName()
                + "' timed out after "
                + stepDeadline
                + "ms deadline");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw new StepCompensationException(
            "Compensation of step '" + step.getName() + "' interrupted");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        StepCompensationException sce;
        if (cause instanceof StepCompensationException s) {
          sce = s;
        } else {
          sce = new StepCompensationException(cause != null ? cause : e);
        }

        logger.warn(
            "Compensation attempt {}/{} failed for step '{}'",
            attempt,
            maxAttempts,
            step.getName(),
            sce);
        if (attempt < maxAttempts) {
          try {
            interval = retryPolicy.sleepWithBackoff(interval);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw sce;
          }
        } else {
          throw sce;
        }
      }
    }
  }

  private void recordStepCompensated(ExecutionContext context, int stepIndex, String stepName) {
    store.recordStepEvent(
        context.getSagaId(), context.nextSequence(), StepEvent.compensated(stepIndex, stepName));
    context.advanceSequence();
    context.markStepCompensated(stepIndex);
  }

  private void recordStepCompensationFailed(
      ExecutionContext context, int stepIndex, String stepName) {
    store.recordStepEvent(
        context.getSagaId(),
        context.nextSequence(),
        StepEvent.compensationFailed(stepIndex, stepName, null));
    context.advanceSequence();
  }

  // ---------------------------------------------------------------------------
  // Private — state transitions
  // ---------------------------------------------------------------------------

  /**
   * Parks a forward step that returned pending (a {@code 202}): transitions the saga to WAITING and
   * records a STEP_PENDING marker (+ a deadline row) via {@link SagaStore#park}. The parked
   * deadline is {@code min(now + callbackTimeoutMillis, sagaDeadline)}; when both are unbounded it
   * parks with no deadline (wait indefinitely).
   */
  private void parkStep(
      ExecutionContext context, int stepIndex, StepWithPolicy stepWithPolicy, long sagaDeadline) {
    long deadlineMillis =
        TimeoutPolicy.calculateStepDeadline(
            stepWithPolicy.callbackTimeoutMillis(), sagaDeadline, clock.millis());
    Instant parkedDeadline = deadlineMillis > 0 ? Instant.ofEpochMilli(deadlineMillis) : null;
    SagaStateSnapshot updated =
        store.park(
            context.getCurrentState(),
            context.nextSequence(),
            StepEvent.pending(stepIndex, stepWithPolicy.step().getName()),
            parkedDeadline);
    context.setCurrentState(updated);
    context.advanceSequence();
  }

  private void transition(ExecutionContext context, StatusEvent event) {
    SagaStateSnapshot newState =
        store.recordStatusEvent(context.getCurrentState(), context.nextSequence(), event);
    context.setCurrentState(newState);
    context.advanceSequence();
  }

  // ---------------------------------------------------------------------------
  // Private — shutdown coordination
  // ---------------------------------------------------------------------------

  private boolean registerActive(String sagaId) {
    synchronized (shutdownLock) {
      if (shuttingDown) {
        return false;
      }
      return activeSagas.add(sagaId);
    }
  }

  private void unregisterActive(String sagaId) {
    activeSagas.remove(sagaId);
  }

  private boolean shouldStopBetweenSteps() {
    return shuttingDown && shutdownConfig.mode() == ShutdownMode.WAIT_CURRENT_STEP;
  }
}
