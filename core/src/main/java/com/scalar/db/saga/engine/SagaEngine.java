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
import com.scalar.db.saga.exception.SagaDefinitionException;
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

  /** Shutdown strategy for in-flight sagas. */
  enum ShutdownMode {
    /** Complete the current step, then stop between steps and mark for recovery. */
    WAIT_CURRENT_STEP,
    /** Wait for all active sagas to reach a terminal state. */
    WAIT_ALL_SAGAS
  }

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
  private final StepResolver stepResolver;
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
      StepResolver stepResolver,
      String ownerId,
      ShutdownConfig shutdownConfig,
      Clock clock) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.stepResolver = Objects.requireNonNull(stepResolver, "stepResolver must not be null");
    this.ownerId = Objects.requireNonNull(ownerId, "ownerId must not be null");
    this.shutdownConfig = Objects.requireNonNull(shutdownConfig, "shutdownConfig must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
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

      // Execute and record are in SEPARATE try blocks because they have different
      // compensation scopes:
      //   - executeWithRetry fails  → step i did NOT run  → compensate from i - 1
      //   - recordStepCompleted fails → step i DID run    → compensate from i
      // A single try block with catch(StepExecutionException) would let the
      // RuntimeException from recordStepCompleted escape entirely, orphaning
      // step i's side effect with no compensation.

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
          compensate(plan, context, i - 1);
        }
        return;
      }

      if (result.isPending()) {
        // Park the saga — release the thread without appending an event.
        // The saga stays RUNNING; recovery or a callback will resume it.
        return;
      }

      // 2. Record the completion event
      try {
        recordStepCompleted(context, i, stepWithPolicy.step().getName(), result);
      } catch (RuntimeException e) {
        // Step i's side effect is committed but recording failed. Compensation
        // must include step i. If the event was actually persisted (e.g., network
        // timeout after commit), the compensate() call below will fail with a
        // write-write conflict at the same sequence number, causing the saga to
        // stay RUNNING for recovery — which will replay the persisted completion
        // event and correctly resume forward.
        logger.error(
            "Failed to record completion for step {} of saga {}", i, context.getSagaId(), e);
        if (i <= pivotIndex) {
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
      Step step, ExecutionContext context, RetryPolicy retryPolicy, long stepDeadline)
      throws StepExecutionException {

    int maxAttempts = retryPolicy.getMaxAttempts();
    long interval = retryPolicy.getInitialIntervalMillis();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Future<StepResult> future = executor.submit(() -> step.execute(context));

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
      plan.add(new StepWithPolicy(step, policy, compensationPolicy, stepDef.getTimeoutMillis()));
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
              stepDef.getTimeoutMillis()));
      confirmSteps.add(
          new StepWithPolicy(
              new TccConfirmStep(tccStep),
              confirmPolicy,
              compensationPolicy,
              stepDef.getTimeoutMillis()));
    }

    reserveSteps.addAll(confirmSteps);
    return reserveSteps;
  }

  private <T> T resolveStep(StepDefinition stepDef, Class<T> expectedType) {
    Object resolved = stepResolver.resolve(stepDef.getName(), stepDef.getStepClass());
    if (expectedType.isInstance(resolved)) {
      return expectedType.cast(resolved);
    }
    throw new SagaDefinitionException(
        "Step '"
            + stepDef.getName()
            + "' (class "
            + stepDef.getStepClass()
            + ") does not implement "
            + expectedType.getName()
            + ". Found: "
            + resolved.getClass().getName());
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
                step.compensate(context);
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
