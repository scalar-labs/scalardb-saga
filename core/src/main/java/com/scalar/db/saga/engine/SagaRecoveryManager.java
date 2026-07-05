package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import com.scalar.db.saga.store.SagaStore.ScanCursor;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic recovery manager that scans for stale sagas and resumes them.
 *
 * <p>On each recovery pass, scans all {@code saga_state} buckets for sagas in {@link
 * SagaStatus#RUNNING} or {@link SagaStatus#COMPENSATING} whose {@code updated_at} is older than
 * {@link RecoveryConfig#recoveryTimeoutMillis()}. For each recoverable saga:
 *
 * <ol>
 *   <li>Claims via {@link SagaStore#claimForRecovery} (optimistic concurrency).
 *   <li>Resolves the exact definition version via {@link SagaDefinitionRegistry#resolve}.
 *   <li>Replays events via {@link SagaEngine#replayEvents} to reconstruct state.
 *   <li>Resumes forward ({@code RUNNING}) or compensation ({@code COMPENSATING}).
 *   <li>Escalates to {@link SagaStatus#ESCALATED} if stuck longer than the grace period.
 * </ol>
 */
class SagaRecoveryManager {

  private static final Logger logger = LoggerFactory.getLogger(SagaRecoveryManager.class);

  private final SagaStore store;
  private final SagaEngine engine;
  private final SagaDefinitionRegistry registry;
  private final String ownerId;
  private final RecoveryConfig config;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService recoveryExecutor;
  private final Semaphore recoverySemaphore;

  SagaRecoveryManager(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry registry,
      String ownerId,
      RecoveryConfig config) {
    this.store = store;
    this.engine = engine;
    this.registry = registry;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "saga-recovery");
              t.setDaemon(true);
              return t;
            });
    this.recoveryExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.recoverySemaphore = new Semaphore(config.maxConcurrentRecoveries());
  }

  // Visible for testing
  SagaRecoveryManager(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry registry,
      String ownerId,
      RecoveryConfig config,
      ScheduledExecutorService scheduler) {
    this.store = store;
    this.engine = engine;
    this.registry = registry;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler = scheduler;
    this.recoveryExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.recoverySemaphore = new Semaphore(config.maxConcurrentRecoveries());
  }

  /**
   * Starts periodic recovery scanning. Runs once immediately (startup recovery), then periodically
   * at the configured interval.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled task
  public void start() {
    scheduler.scheduleWithFixedDelay(
        this::recoverSafely, 0, config.recoveryIntervalSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Stops the recovery scheduler and waits for any in-flight recovery pass to complete, respecting
   * the given deadline. Both executors are signaled to shut down immediately; remaining time is
   * used for graceful termination before force-stopping.
   *
   * @param deadlineNanos absolute {@link System#nanoTime()} deadline
   */
  public void stop(long deadlineNanos) {
    scheduler.shutdown();
    recoveryExecutor.shutdown();

    try {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        scheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
      remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        recoveryExecutor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      scheduler.shutdownNow();
      recoveryExecutor.shutdownNow();
    }
  }

  /** Wraps {@link #recover()} with exception handling so the scheduler never stops on failure. */
  private void recoverSafely() {
    try {
      recover();
    } catch (Exception e) {
      logger.error("Recovery pass failed unexpectedly", e);
    }
  }

  /**
   * Single recovery pass: scan for stale sagas using cursor-based pagination, claim each one, and
   * resume or compensate. Individual recoveries are dispatched to virtual threads so that one slow
   * saga does not block recovery of others. Stops when the batch limit is reached — remaining sagas
   * are picked up on the next pass.
   */
  public void recover() {
    List<Future<?>> futures = new ArrayList<>();
    @Nullable ScanCursor cursor = null;
    int submitted = 0;
    try {
      do {
        Recoverables page = store.findRecoverable(config.recoveryTimeoutMillis(), cursor);
        cursor = page.nextCursor();

        for (SagaStateSnapshot saga : page.sagas()) {
          futures.add(recoveryExecutor.submit(() -> recoverOneSafely(saga)));
          if (++submitted >= config.batchSize()) {
            break;
          }
        }
      } while (cursor != null && submitted < config.batchSize());
    } catch (RejectedExecutionException e) {
      logger.warn("Recovery executor shut down; skipping remaining sagas", e);
    }
    awaitAll(futures);
  }

  private void recoverOneSafely(SagaStateSnapshot saga) {
    try {
      recoverySemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    try {
      Optional<SagaStateSnapshot> claimed = store.claimForRecovery(saga, ownerId);
      if (claimed.isEmpty()) {
        return;
      }
      recoverOne(claimed.get());
    } catch (Exception e) {
      // Log and continue — don't let one stuck saga block others
      logger.error("Failed to recover saga {}", saga.getSagaId(), e);
    } finally {
      recoverySemaphore.release();
    }
  }

  private void awaitAll(List<Future<?>> futures) {
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        // Already logged inside recoverOneSafely
      }
    }
  }

  private void recoverOne(SagaStateSnapshot saga) {
    // Replay events to reconstruct state (needed for both recovery and escalation)
    List<SagaEvent> events = store.getEvents(saga.getSagaId());
    ExecutionContext context = engine.replayEvents(saga, events);

    // Look up the EXACT definition version the saga was started with.
    // Using a different version during recovery would corrupt business data
    // if steps were added, removed, or reordered.
    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      escalate(
          context,
          "definition version "
              + saga.getDefinitionVersion()
              + " not found for "
              + saga.getSagaName());
      return;
    }

    SagaStatus status = context.getCurrentState().getStatus();

    if (status == SagaStatus.COMPENSATING) {
      recoverCompensating(def, context, events);
    } else {
      // RUNNING (or any other non-terminal, non-COMPENSATING status) — resume forward
      recoverRunning(def, context, events);
    }
  }

  private void recoverRunning(
      SagaDefinition def, ExecutionContext context, List<SagaEvent> events) {
    // Check if step failures have been stuck longer than the grace period.
    // Sagas with no STEP_FAILED events (crash recovery) skip escalation and proceed to resume.
    if (isStuckLongerThanGracePeriod(events, EventType.STEP_FAILED, EventType.STEP_COMPLETED)) {
      escalate(context, "step retry stuck for over " + config.compensationGracePeriod());
      return;
    }

    // A durable STEP_FAILED at or before the pivot means the engine had already exhausted retries
    // and decided to compensate: the forward path records STEP_FAILED, then transitions to
    // COMPENSATING. A crash between those two writes leaves the saga RUNNING with the failure
    // recorded but compensation never started. Resuming forward here would re-run a step whose side
    // effect may have committed (an in-doubt failure) and, if the re-run then proved non-delivery,
    // skip it — orphaning the original side effect. Drive compensation instead, mirroring
    // recoverCompensating's fromStep computation: an in-doubt/possibly-committed failure (in
    // failedIndicesToCompensate) is included; a knownNotCommitted=true failure is skipped (start
    // from the highest completed). A POST-pivot failure is intentionally left to resume forward —
    // FORWARD/MIXED recovery re-attempts the failed step rather than compensating past the pivot.
    int pivotIndex = def.getPivotIndex();
    if (hasUnresolvedPrePivotFailure(events, pivotIndex)) {
      int fromStep =
          Math.max(
              stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1),
              failedIndicesToCompensate(events)
                  .filter(index -> index <= pivotIndex)
                  .max()
                  .orElse(-1));
      engine.compensateFrom(def, context, fromStep);
      return;
    }

    int lastCompleted = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);

    engine.resumeFrom(def, context, lastCompleted + 1);
  }

  private void recoverCompensating(
      SagaDefinition def, ExecutionContext context, List<SagaEvent> events) {
    // Check if compensation has been stuck longer than the grace period.
    if (isStuckLongerThanGracePeriod(
        events, EventType.STEP_COMPENSATION_FAILED, EventType.STEP_COMPENSATED)) {
      escalate(context, "compensation stuck for over " + config.compensationGracePeriod());
      return;
    }

    int lastCompensated =
        stepIndices(events, EventType.STEP_COMPENSATED).min().orElse(Integer.MAX_VALUE);

    int fromStep;
    if (lastCompensated < Integer.MAX_VALUE) {
      fromStep = lastCompensated - 1;
    } else {
      // No compensation started yet (crash after the COMPENSATING transition, before any step was
      // compensated). Start from the highest completed step OR the highest forward failure whose
      // non-delivery was NOT proven — that failed step may have committed and must be compensated
      // too. A knownNotCommitted failure is safely skipped.
      fromStep =
          Math.max(
              stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1),
              failedIndicesToCompensate(events).max().orElse(-1));
    }

    // Pivot-scope invariant (documented, not enforced): unlike the forward path's
    // `if (i <= pivotIndex)` guard, this does not clamp fromStep to the pivot — it does not need
    // to. A saga reaches COMPENSATING only via a PRE-pivot forward failure, which stops execution
    // at that step, so every completed/failed index here is necessarily <= pivotIndex and fromStep
    // cannot point past the pivot. If a future path ever drives a past-the-pivot saga into
    // COMPENSATING (most likely a manual cancel/abort API), this breaks: such a saga cannot be
    // cleanly auto-rolled-back, so it must fail loud (escalate), NOT silently clamp here — that
    // would leave a torn saga (pre-pivot undone, post-pivot still committed). Handle the pivot
    // scope when that path is introduced.
    engine.compensateFrom(def, context, fromStep);
  }

  /**
   * STEP_FAILED indices whose persisted payload does not prove non-delivery — those failed steps
   * may have committed, so compensation must include them. A {@code knownNotCommitted} failure
   * (proven non-delivery) is excluded.
   */
  private static IntStream failedIndicesToCompensate(List<SagaEvent> events) {
    return events.stream()
        .filter(e -> e instanceof StepEvent)
        .map(e -> (StepEvent) e)
        .filter(e -> e.getEventType() == EventType.STEP_FAILED)
        .filter(e -> !EventPayloadSerializer.isKnownNotCommitted(e.getPayload()))
        .mapToInt(StepEvent::getStepIndex);
  }

  /**
   * Returns whether a RUNNING saga has a forward failure at or before the pivot that no later
   * {@code STEP_COMPLETED} at the same index resolved. Such a failure means the engine had decided
   * to compensate (retries exhausted) but crashed before the {@code COMPENSATING} transition, so
   * recovery must compensate rather than resume forward. Post-pivot failures are excluded — those
   * are resumed forward (FORWARD/MIXED recovery).
   */
  private static boolean hasUnresolvedPrePivotFailure(List<SagaEvent> events, int pivotIndex) {
    Set<Integer> resolvedIndices =
        stepIndices(events, EventType.STEP_COMPLETED).boxed().collect(Collectors.toSet());
    return stepIndices(events, EventType.STEP_FAILED)
        .filter(index -> index <= pivotIndex)
        .anyMatch(index -> !resolvedIndices.contains(index));
  }

  /**
   * Checks if a saga has been stuck longer than the grace period by examining step-level failure
   * events. Returns {@code false} if no matching failure events exist (e.g., crash recovery where
   * the saga was interrupted, not failed).
   */
  private boolean isStuckLongerThanGracePeriod(
      List<SagaEvent> events, EventType failureEventType, EventType successEventType) {
    // Step indices where the failure was later resolved by a success event.
    // Since events are append-only and ordered, a success at the same index
    // always follows the failure — the step cannot succeed before it fails.
    Set<Integer> resolvedIndices =
        stepIndices(events, successEventType).boxed().collect(Collectors.toSet());

    Optional<Instant> firstUnresolvedFailure =
        events.stream()
            .filter(
                e ->
                    e instanceof StepEvent step
                        && step.getEventType() == failureEventType
                        && !resolvedIndices.contains(step.getStepIndex()))
            .map(SagaEvent::getTimestamp)
            .filter(Objects::nonNull)
            .findFirst();
    // no unresolved failure events — crash recovery or all failures resolved
    return firstUnresolvedFailure
        .filter(
            instant ->
                Duration.between(instant, config.clock().instant())
                        .compareTo(config.compensationGracePeriod())
                    > 0)
        .isPresent();
  }

  private void escalate(ExecutionContext context, String reason) {
    logger.warn("Escalating saga {}: {}", context.getSagaId(), reason);
    store.recordStatusEvent(
        context.getCurrentState(), context.nextSequence(), StatusEvent.escalated(reason));
  }

  private static IntStream stepIndices(List<SagaEvent> events, EventType eventType) {
    return events.stream()
        .filter(e -> e instanceof StepEvent)
        .map(e -> (StepEvent) e)
        .filter(e -> e.getEventType() == eventType)
        .mapToInt(StepEvent::getStepIndex);
  }
}
