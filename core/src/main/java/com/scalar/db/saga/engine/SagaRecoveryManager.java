package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.OverdueParked;
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
    try {
      // Pass 1: stale RUNNING / COMPENSATING sagas (updated_at staleness scan). Compute the cutoff
      // once from the injected clock so every bucket in this cycle uses a consistent threshold.
      Instant staleThreshold = config.clock().instant().minusMillis(config.recoveryTimeoutMillis());
      @Nullable ScanCursor cursor = null;
      int recoverySubmitted = 0;
      do {
        Recoverables page = store.findRecoverable(staleThreshold, cursor);
        cursor = page.nextCursor();

        for (SagaStateSnapshot saga : page.sagas()) {
          futures.add(recoveryExecutor.submit(() -> recoverOneSafely(saga)));
          if (++recoverySubmitted >= config.batchSize()) {
            break;
          }
        }
      } while (cursor != null && recoverySubmitted < config.batchSize());

      // Pass 2: overdue parked (WAITING) sagas whose async-callback deadline has passed. Its own
      // batch budget, so a large staleness backlog cannot starve the timeout sweep.
      cursor = null;
      int timeoutSubmitted = 0;
      do {
        OverdueParked page = store.findOverdueParkedSagas(config.clock().instant(), cursor);
        cursor = page.nextCursor();

        for (String sagaId : page.sagaIds()) {
          futures.add(recoveryExecutor.submit(() -> recoverParkedTimeoutOneSafely(sagaId)));
          if (++timeoutSubmitted >= config.batchSize()) {
            break;
          }
        }
      } while (cursor != null && timeoutSubmitted < config.batchSize());
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
    // Grace-period policy (recovery-specific): escalate if a step failure has been stuck past the
    // grace period. Sagas with no STEP_FAILED events (crash recovery) skip this and act directly.
    if (isStuckLongerThanGracePeriod(events, EventType.STEP_FAILED, EventType.STEP_COMPLETED)) {
      escalate(context, "step retry stuck for over " + config.compensationGracePeriod());
      return;
    }
    engine.recover(RecoveryActionResolver.resolve(events, def, SagaStatus.RUNNING), def, context);
  }

  private void recoverCompensating(
      SagaDefinition def, ExecutionContext context, List<SagaEvent> events) {
    // Grace-period policy (recovery-specific): compensation stuck past the grace period escalates.
    if (isStuckLongerThanGracePeriod(
        events, EventType.STEP_COMPENSATION_FAILED, EventType.STEP_COMPENSATED)) {
      escalate(context, "compensation stuck for over " + config.compensationGracePeriod());
      return;
    }
    engine.recover(
        RecoveryActionResolver.resolve(events, def, SagaStatus.COMPENSATING), def, context);
  }

  private void recoverParkedTimeoutOneSafely(String sagaId) {
    try {
      recoverySemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    try {
      recoverParkedTimeoutOne(sagaId);
    } catch (SagaConcurrentModificationException e) {
      // A concurrent callback (or another replica's sweep) won the WAITING CK — nothing to do.
      logger.debug("Parked timeout for saga {} lost the WAITING race; skipping", sagaId);
    } catch (Exception e) {
      logger.error("Failed to time out parked saga {}", sagaId, e);
    } finally {
      recoverySemaphore.release();
    }
  }

  /**
   * Recovers one overdue parked ({@code WAITING}) saga. While within the re-drive bounds (the park-
   * attempt count vs the step's retry max-attempts AND the elapsed-since-first-park vs the grace
   * period), it un-parks and re-issues the step — so a transient participant failure is retried
   * before the saga rolls back or escalates. Once a bound is spent it gives up: fails the parked
   * step and either compensates (pre-pivot) or escalates (post-pivot), clearing the {@code
   * saga_parked} row. No claim is taken — the optimistic WAITING-CK check in the store ops is the
   * cross-replica de-dup and the callback-vs-timeout-vs-redrive guard.
   */
  private void recoverParkedTimeoutOne(String sagaId) {
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot(sagaId);
    if (snapshot.isEmpty() || snapshot.get().getStatus() != SagaStatus.WAITING) {
      // Already resolved (a callback won, or it moved on) — nothing to do.
      return;
    }
    SagaStateSnapshot saga = snapshot.get();
    List<SagaEvent> events = store.getEvents(sagaId);
    StepEvent parked = lastParkedEvent(events);
    if (parked == null) {
      logger.error("WAITING saga {} has no STEP_PENDING marker; leaving for inspection", sagaId);
      return;
    }
    int parkedIndex = parked.getStepIndex();
    String stepName = parked.getStepName();

    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      // Missing definition — escalate, clearing the parked row so it is not re-swept forever.
      logger.warn(
          "Escalating parked saga {}: definition {} not found",
          sagaId,
          saga.getDefinitionVersion());
      store.failParkedStep(
          saga,
          events.size(),
          giveUpFailedEvent(
              parkedIndex, stepName, "definition " + saga.getDefinitionVersion() + " not found"),
          SagaStatus.ESCALATED);
      return;
    }

    // Re-drive (retry) the parked step while within the attempt-count and grace bounds: un-park it
    // (WAITING -> RUNNING) and re-issue it, which re-parks with a fresh deadline. Only once a bound
    // is spent do we give up below.
    RedriveDecision decision = redriveDecision(events, parkedIndex, def);
    if (decision == RedriveDecision.REDRIVE) {
      StepEvent reissueEvent = StepEvent.reissuing(parkedIndex, stepName);
      SagaStateSnapshot running = store.redriveParkedStep(saga, events.size(), reissueEvent);
      List<SagaEvent> updatedEvents = new ArrayList<>(events);
      updatedEvents.add(reissueEvent);
      ExecutionContext context = engine.replayEvents(running, updatedEvents);
      engine.resumeFrom(def, context, parkedIndex);
      return;
    }

    // Give up: the re-drive budget is spent — record which bound was hit for the event log.
    String reason =
        decision == RedriveDecision.ATTEMPTS_EXHAUSTED
            ? "async re-drive attempts exhausted"
            : "async re-drive grace period exceeded";
    StepEvent failedEvent = giveUpFailedEvent(parkedIndex, stepName, reason);
    if (parkedIndex <= def.getPivotIndex()) {
      // Pre-pivot: WAITING -> COMPENSATING + STEP_FAILED + clear parked, then compensate from the
      // failed step (knownNotCommitted=false — the async step may have committed server-side).
      SagaStateSnapshot compensating =
          store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.COMPENSATING);
      // failParkedStep appended failedEvent at events.size() and nothing else; its successful
      // WAITING-CK check proves the log was untouched since we read `events`, so append locally
      // rather than re-reading (replayEvents ignores the timestamp).
      List<SagaEvent> updatedEvents = new ArrayList<>(events);
      updatedEvents.add(failedEvent);
      ExecutionContext context = engine.replayEvents(compensating, updatedEvents);
      engine.compensateFrom(def, context, parkedIndex);
    } else {
      // Post-pivot: cannot roll back and the give-up floor does not re-drive forward — escalate.
      logger.warn(
          "Escalating parked saga {}: step {} gave up post-pivot ({})",
          sagaId,
          parkedIndex,
          reason);
      store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.ESCALATED);
    }
  }

  /** The most recent {@code STEP_PENDING} event (the currently parked step), or {@code null}. */
  private static @Nullable StepEvent lastParkedEvent(List<SagaEvent> events) {
    StepEvent parked = null;
    for (SagaEvent event : events) {
      if (event.getEventType() == EventType.STEP_PENDING) {
        parked = (StepEvent) event;
      }
    }
    return parked;
  }

  /** Whether to re-drive a timed-out parked step, or which bound made it give up. */
  private enum RedriveDecision {
    REDRIVE,
    ATTEMPTS_EXHAUSTED,
    GRACE_EXCEEDED
  }

  /**
   * Decides whether a timed-out parked step is re-driven rather than given up on. Re-drives only
   * within BOTH bounds: the park-attempt count ({@code STEP_PENDING} events at the step) within the
   * step's retry max-attempts — caps hammering a dead participant under a short callback timeout —
   * AND the time since the first park within the compensation grace period — caps total stuck time
   * under a long one. Otherwise returns which bound was hit (attempts checked first).
   */
  private RedriveDecision redriveDecision(
      List<SagaEvent> events, int parkedIndex, SagaDefinition def) {
    long attempts =
        RecoveryActionResolver.stepIndices(events, EventType.STEP_PENDING)
            .filter(index -> index == parkedIndex)
            .count();
    if (attempts >= maxRedriveAttempts(def, parkedIndex)) {
      return RedriveDecision.ATTEMPTS_EXHAUSTED;
    }
    boolean withinGrace =
        events.stream()
            .filter(
                e ->
                    e instanceof StepEvent step
                        && step.getEventType() == EventType.STEP_PENDING
                        && step.getStepIndex() == parkedIndex)
            .map(SagaEvent::getTimestamp)
            .filter(Objects::nonNull)
            .findFirst()
            .map(
                firstPark ->
                    Duration.between(firstPark, config.clock().instant())
                            .compareTo(config.compensationGracePeriod())
                        <= 0)
            .orElse(false);
    return withinGrace ? RedriveDecision.REDRIVE : RedriveDecision.GRACE_EXCEEDED;
  }

  /**
   * The parked step's effective retry max-attempts: the step's own policy, else the saga default,
   * else the engine default (mirrors {@code SagaEngine.resolveRetryPolicy}).
   */
  private static int maxRedriveAttempts(SagaDefinition def, int stepIndex) {
    StepDefinition stepDef = def.getSteps().get(stepIndex);
    RetryPolicy policy = stepDef.getRetryPolicy();
    if (policy == null) {
      policy = def.getDefaultRetryPolicy();
    }
    if (policy == null) {
      policy = RetryPolicy.defaultPolicy();
    }
    return policy.getMaxAttempts();
  }

  /**
   * A {@code STEP_FAILED} event for a given-up parked step, with {@code knownNotCommitted=false}:
   * an async step whose callback never arrived may have committed server-side, so compensation must
   * include it.
   */
  private static StepEvent giveUpFailedEvent(int stepIndex, String stepName, String reason) {
    return StepEvent.failed(
        stepIndex,
        stepName,
        EventPayloadSerializer.serializeError(new StepExecutionException(reason, false), false));
  }

  /**
   * Checks if a saga has been stuck longer than the grace period by examining step-level failure
   * events. Returns {@code false} if no matching failure events exist (e.g., crash recovery where
   * the saga was interrupted, not failed).
   *
   * <p>Un-escalating restarts the clock: the stuck period is measured from the later of the
   * unresolved failure and the most recent {@code SAGA_RESET}. An escalated saga is outside the
   * recovery cycle entirely, so re-admitting it has to grant a fresh grace period — the failure it
   * was stuck on only ages, so anchoring on that alone would escalate it straight back on the next
   * pass, undoing the reset without ever driving it.
   *
   * <p>{@code SAGA_RECOVERING} deliberately does not restart the clock. It only forces a drive the
   * scheduled sweep would do anyway, on a saga that never left the recovery cycle; extending the
   * deadline would make escalation timing depend on whether an operator happened to intervene.
   */
  private boolean isStuckLongerThanGracePeriod(
      List<SagaEvent> events, EventType failureEventType, EventType successEventType) {
    // Step indices where the failure was later resolved by a success event.
    // Since events are append-only and ordered, a success at the same index
    // always follows the failure — the step cannot succeed before it fails.
    Set<Integer> resolvedIndices =
        RecoveryActionResolver.stepIndices(events, successEventType)
            .boxed()
            .collect(Collectors.toSet());

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
    if (firstUnresolvedFailure.isEmpty()) {
      // no unresolved failure events — crash recovery or all failures resolved
      return false;
    }

    Instant anchor = firstUnresolvedFailure.get();
    Optional<Instant> reset = lastResetTimestamp(events);
    if (reset.isPresent() && reset.get().isAfter(anchor)) {
      anchor = reset.get();
    }
    return Duration.between(anchor, config.clock().instant())
            .compareTo(config.compensationGracePeriod())
        > 0;
  }

  /**
   * The most recent timestamp at which an operator un-escalated this saga, if any. Compared by
   * timestamp rather than taken as the last matching event, because events are stamped by whichever
   * replica writes them, so stream order and clock order can diverge; taking the latest also yields
   * the failure, correctly, when a fresh failure follows the reset.
   */
  private static Optional<Instant> lastResetTimestamp(List<SagaEvent> events) {
    return events.stream()
        .filter(e -> e.getEventType() == EventType.SAGA_RESET)
        .map(SagaEvent::getTimestamp)
        .filter(Objects::nonNull)
        .max(Instant::compareTo);
  }

  private void escalate(ExecutionContext context, String reason) {
    logger.warn("Escalating saga {}: {}", context.getSagaId(), reason);
    store.recordStatusEvent(
        context.getCurrentState(), context.nextSequence(), StatusEvent.escalated(reason), ownerId);
  }
}
