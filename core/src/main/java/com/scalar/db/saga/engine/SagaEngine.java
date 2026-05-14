package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.exception.StepTimeoutException;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Clock;
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
  private static final long DEFAULT_WAIT_CURRENT_STEP_TIMEOUT_MILLIS = 30_000;

  /** Shutdown strategy for in-flight sagas. */
  public enum ShutdownMode {
    /** Complete the current step, then stop between steps and mark for recovery. */
    WAIT_CURRENT_STEP,
    /** Wait for all active sagas to reach a terminal state. */
    WAIT_ALL_SAGAS
  }

  private final SagaStore store;
  private final CompensationManager compensationManager;
  private final StepRegistry stepRegistry;
  private final String ownerId;
  private final ShutdownMode shutdownMode;
  private final long shutdownTimeoutMillis;
  private final Clock clock;
  private volatile boolean shuttingDown = false;
  private final Object shutdownLock = new Object();
  private final Set<String> activeSagas = ConcurrentHashMap.newKeySet();
  private final ExecutorService stepExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public SagaEngine(
      SagaStore store,
      CompensationManager compensationManager,
      StepRegistry stepRegistry,
      String ownerId,
      ShutdownMode shutdownMode,
      long shutdownTimeoutMillis,
      Clock clock) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.compensationManager =
        Objects.requireNonNull(compensationManager, "compensationManager must not be null");
    this.stepRegistry = Objects.requireNonNull(stepRegistry, "stepRegistry must not be null");
    this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
    this.shutdownMode = Objects.requireNonNull(shutdownMode, "shutdownMode must not be null");
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  public SagaEngine(
      SagaStore store,
      CompensationManager compensationManager,
      StepRegistry stepRegistry,
      String ownerId,
      ShutdownMode shutdownMode,
      long shutdownTimeoutMillis) {
    this(
        store,
        compensationManager,
        stepRegistry,
        ownerId,
        shutdownMode,
        shutdownTimeoutMillis,
        Clock.systemUTC());
  }

  public SagaEngine(
      SagaStore store,
      CompensationManager compensationManager,
      StepRegistry stepRegistry,
      String ownerId) {
    this(
        store,
        compensationManager,
        stepRegistry,
        ownerId,
        ShutdownMode.WAIT_CURRENT_STEP,
        DEFAULT_WAIT_CURRENT_STEP_TIMEOUT_MILLIS);
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
  public SagaStateSnapshot createSaga(
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
  public void executeSaga(SagaDefinition def, SagaStateSnapshot saga, Map<String, Object> input) {
    ExecutionContext context = new ExecutionContext(saga.getSagaId(), input, saga);
    context.setNextEventSequence(1); // SAGA_STARTED was seq 0
    executeSteps(def, context, 0);
  }

  /**
   * Convenience method: creates and executes a saga in one call.
   *
   * @return the saga ID
   */
  public String execute(SagaDefinition def, @Nullable String sagaId, Map<String, Object> input) {
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
    executeSteps(def, context, fromStep);
    return context.getCurrentState();
  }

  /**
   * Triggers compensation from a specific step (used by recovery for sagas stuck in COMPENSATING).
   */
  public void compensateFrom(SagaDefinition def, ExecutionContext context, int fromStep) {
    List<StepWithPolicy> plan = buildPlan(def);
    compensate(plan, context, fromStep);
  }

  /**
   * Replays events to reconstruct an ExecutionContext for crash recovery.
   *
   * <p>Package-private: used by SagaRecoveryManager and DefaultSagaManager.
   */
  ExecutionContext replayEvents(SagaStateSnapshot saga, List<SagaEvent> events) {
    ExecutionContext context = new ExecutionContext(saga.getSagaId(), Map.of(), saga);

    for (SagaEvent event : events) {
      switch (event.getEventType()) {
        case SAGA_STARTED -> {
          Map<String, Object> input = EventPayloadSerializer.deserializeMap(event.getPayload());
          input.forEach(context::put);
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
          // Tracked for logging; saga stays COMPENSATING
        }
        default -> {
          // Saga-level events (SAGA_CONFIRMING, etc.) — status tracked via snapshot
        }
      }
    }

    context.setNextEventSequence(events.size());
    context.setCurrentState(saga);
    return context;
  }

  /** Initiates graceful shutdown. */
  public void shutdown() {
    synchronized (shutdownLock) {
      shuttingDown = true;
    }

    long deadline = clock.millis() + shutdownTimeoutMillis;

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

    stepExecutor.shutdownNow();
  }

  @Override
  public void close() {
    shutdown();
  }

  // ---------------------------------------------------------------------------
  // Private — execution loop
  // ---------------------------------------------------------------------------

  private void executeSteps(SagaDefinition def, ExecutionContext context, int startIndex) {
    String sagaId = context.getSagaId();
    if (!registerActive(sagaId)) {
      store.markForRecovery(sagaId);
      return;
    }

    try {
      List<StepWithPolicy> plan = buildPlan(def);
      PivotPolicy pivot = buildPivotPolicy(def);
      executeSagaSteps(plan, pivot, context, startIndex, def.getTimeoutMillis());
    } finally {
      unregisterActive(sagaId);
    }
  }

  private void executeSagaSteps(
      List<StepWithPolicy> plan,
      PivotPolicy pivot,
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
        if (i <= pivot.index()) {
          compensate(plan, context, i - 1);
        }
        return;
      }

      // Emit crossing event when transitioning past pivot
      if (i == pivot.index() + 1 && pivot.crossingEvent() != null) {
        transition(context, pivot.crossingEvent());
      }

      StepWithPolicy stepWithPolicy = plan.get(i);
      long stepDeadline =
          TimeoutPolicy.calculateStepDeadline(
              stepWithPolicy.stepTimeoutMillis(), sagaDeadline, clock.millis());

      try {
        StepResult result =
            executeWithRetry(
                stepWithPolicy.step(), context, stepWithPolicy.retryPolicy(), stepDeadline);
        recordStepCompleted(context, i, stepWithPolicy.step().getName(), result);
      } catch (StepExecutionException e) {
        recordStepFailed(context, i, stepWithPolicy.step().getName(), e);
        if (i <= pivot.index()) {
          compensate(plan, context, i - 1);
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
      String errorPayload = EventPayloadSerializer.serializeError(e);
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
      Step step, ExecutionContext context, RetryPolicy policy, long stepDeadline)
      throws StepExecutionException {

    int maxAttempts = policy.getMaxAttempts();
    long interval = policy.getInitialIntervalMillis();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Future<StepResult> future = stepExecutor.submit(() -> step.execute(context));

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
          if (see.isRetryable() && attempt < maxAttempts) {
            logger.debug(
                "Retryable failure on step '{}', attempt {}/{}",
                step.getName(),
                attempt,
                maxAttempts);
            try {
              interval = policy.sleepWithBackoff(interval);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new StepExecutionException(
                  "Step '" + step.getName() + "' interrupted during backoff", ie, false);
            }
            continue;
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
    List<StepWithPolicy> plan = new ArrayList<>();
    for (StepDefinition stepDef : def.getSteps()) {
      Step step = stepRegistry.getStep(stepDef.getName());
      RetryPolicy policy = resolveRetryPolicy(stepDef, def);
      plan.add(new StepWithPolicy(step, policy, stepDef.getTimeoutMillis()));
    }
    return plan;
  }

  private List<StepWithPolicy> expandTccPlan(SagaDefinition def) {
    List<StepWithPolicy> plan = new ArrayList<>();
    RetryPolicy confirmPolicy = RetryPolicy.confirmDefault();

    for (StepDefinition stepDef : def.getSteps()) {
      TccStep tccStep = stepRegistry.getTccStep(stepDef.getName());
      RetryPolicy reservePolicy = resolveRetryPolicy(stepDef, def);
      plan.add(
          new StepWithPolicy(
              new TccReserveStep(tccStep), reservePolicy, stepDef.getTimeoutMillis()));
    }

    for (StepDefinition stepDef : def.getSteps()) {
      TccStep tccStep = stepRegistry.getTccStep(stepDef.getName());
      plan.add(
          new StepWithPolicy(
              new TccConfirmStep(tccStep), confirmPolicy, stepDef.getTimeoutMillis()));
    }

    return plan;
  }

  private PivotPolicy buildPivotPolicy(SagaDefinition def) {
    if (def.getMode() == SagaMode.TCC) {
      // Pivot is at the last reserve step (index = N-1 for N TCC steps)
      int pivotIndex = def.getSteps().size() - 1;
      return new PivotPolicy(pivotIndex, StatusEvent.confirming());
    }
    return new PivotPolicy(def.getPivotIndex(), null);
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
  // Private — compensation and state transitions
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
      compensationManager.compensate(plan, context, fromStepIndex);
      transition(context, StatusEvent.compensated());
    } catch (StepCompensationException e) {
      // Saga stays COMPENSATING — recovery will retry
      logger.warn("Compensation incomplete for saga {}: {}", context.getSagaId(), e.getMessage());
    }
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
      activeSagas.add(sagaId);
      return true;
    }
  }

  private void unregisterActive(String sagaId) {
    activeSagas.remove(sagaId);
  }

  private boolean shouldStopBetweenSteps() {
    return shuttingDown && shutdownMode == ShutdownMode.WAIT_CURRENT_STEP;
  }
}
