package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Settles a parked ({@code WAITING}) step that will not complete: re-issues it while its retry
 * attempts and the compensation grace period allow, and otherwise gives it up, compensating the
 * saga at or before the pivot and escalating it after. Shared by the recovery sweep, for a parked
 * step past its deadline, and the failure callback, for a step its participant reports as failed,
 * so the two cannot drift apart.
 *
 * <p>{@link #resolve} commits the transition and returns the drive that continues the saga from it
 * without running it, so each caller runs the drive where it needs to: recovery inline, the
 * callback on the asynchronous executor after it has answered the participant.
 */
final class ParkedStepResolver {

  private static final Logger logger = LoggerFactory.getLogger(ParkedStepResolver.class);

  private final SagaStore store;
  private final SagaEngine engine;
  private final RecoveryConfig config;

  ParkedStepResolver(SagaStore store, SagaEngine engine, RecoveryConfig config) {
    this.store = store;
    this.engine = engine;
    this.config = config;
  }

  /** Where {@link #resolve} left the saga. */
  enum Kind {
    /** Un-parked and about to be re-issued. */
    REDRIVEN,
    /** The step was given up at or before the pivot, and the saga is compensating. */
    COMPENSATING,
    /** The step was given up after the pivot, and the saga is escalated. */
    ESCALATED
  }

  /**
   * A committed transition and what continues the saga from it.
   *
   * @param snapshot the saga's state as committed
   * @param kind which transition was committed
   * @param drive runs the saga on from the transition and returns where it settled, or {@code null}
   *     when nothing runs, as for an escalated saga
   */
  record Resolution(
      SagaStateSnapshot snapshot, Kind kind, @Nullable Supplier<SagaStateSnapshot> drive) {}

  /**
   * Re-issues or gives up the parked step and commits that transition.
   *
   * @param saga the {@code WAITING} snapshot
   * @param events the saga's full event log, as read alongside {@code saga}
   * @param parkedIndex the parked step's index
   * @param stepName the parked step's name
   * @param def the saga's definition
   * @param mayRedrive whether the step may be re-issued at all, within its bounds
   * @param cause what went wrong, recorded on the re-issue event or, when the step is given up, on
   *     the failure event; {@code null} to record only which bound was spent
   * @return the committed transition and the drive that continues from it
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if the saga left
   *     {@code WAITING} since it was read, such as when its callback arrived first
   */
  Resolution resolve(
      SagaStateSnapshot saga,
      List<SagaEvent> events,
      int parkedIndex,
      String stepName,
      SagaDefinition def,
      boolean mayRedrive,
      @Nullable String cause) {
    String spentBound = null;
    if (mayRedrive) {
      RedriveDecision decision = redriveDecision(events, parkedIndex, def);
      if (decision == RedriveDecision.REDRIVE) {
        // Un-park (WAITING -> RUNNING) and re-issue the step, which re-parks with a fresh deadline.
        // A reported cause is kept on the re-issue, since no failure event will record it.
        StepEvent reissueEvent =
            StepEvent.reissuing(
                parkedIndex,
                stepName,
                cause == null
                    ? null
                    : EventPayloadSerializer.serializeError(
                        new StepExecutionException(cause, false), false));
        SagaStateSnapshot running = store.redriveParkedStep(saga, events.size(), reissueEvent);
        ExecutionContext context = engine.replayEvents(running, appended(events, reissueEvent));
        return new Resolution(
            running,
            Kind.REDRIVEN,
            () -> {
              engine.resumeFrom(def, context, parkedIndex);
              return context.getCurrentState();
            });
      }
      spentBound =
          decision == RedriveDecision.ATTEMPTS_EXHAUSTED
              ? "async re-drive attempts exhausted"
              : "async re-drive grace period exceeded";
    }
    String reason =
        cause == null
            ? Objects.requireNonNull(spentBound)
            : spentBound == null ? cause : cause + "; " + spentBound;
    StepEvent failedEvent = giveUpFailedEvent(parkedIndex, stepName, reason);
    if (parkedIndex <= def.getPivotIndex()) {
      // WAITING -> COMPENSATING + STEP_FAILED + clear the parked row, then compensate from the
      // failed step, which is included because the participant may have committed it.
      SagaStateSnapshot compensating =
          store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.COMPENSATING);
      ExecutionContext context = engine.replayEvents(compensating, appended(events, failedEvent));
      return new Resolution(
          compensating,
          Kind.COMPENSATING,
          () -> {
            engine.compensateFrom(def, context, parkedIndex);
            return context.getCurrentState();
          });
    }
    // After the pivot the saga cannot roll back, and a step given up is not re-driven forward.
    logger.warn(
        "Escalating parked saga {}: step {} gave up post-pivot ({})",
        saga.getSagaId(),
        parkedIndex,
        reason);
    SagaStateSnapshot escalated =
        store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.ESCALATED);
    return new Resolution(escalated, Kind.ESCALATED, null);
  }

  /**
   * The log after a transition that appended {@code event} and nothing else. The transition's
   * optimistic {@code WAITING} check proves the log was untouched since it was read, so appending
   * locally stands in for re-reading it ({@code replayEvents} ignores the event timestamp).
   */
  private static List<SagaEvent> appended(List<SagaEvent> events, SagaEvent event) {
    List<SagaEvent> updated = new ArrayList<>(events);
    updated.add(event);
    return updated;
  }

  /** Whether to re-drive a parked step, or which bound made it give up. */
  private enum RedriveDecision {
    REDRIVE,
    ATTEMPTS_EXHAUSTED,
    GRACE_EXCEEDED
  }

  /**
   * Decides whether a parked step is re-driven rather than given up on. Re-drives only within BOTH
   * bounds: the park-attempt count ({@code STEP_PENDING} events at the step) within the step's
   * retry max-attempts — caps hammering a dead participant under a short callback timeout — AND the
   * time since the first park within the compensation grace period — caps total stuck time under a
   * long one. Otherwise returns which bound was hit (attempts checked first).
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
   * else the engine default (mirrors {@code SagaEngine.resolveRetryPolicy}). A TCC confirmation has
   * no definition step of its own: the plan puts the confirmations after all the reservations, at
   * {@code steps.size() + i}, under the fixed confirmation policy (mirrors {@code
   * SagaEngine.expandTccPlan}).
   */
  private static int maxRedriveAttempts(SagaDefinition def, int stepIndex) {
    if (stepIndex >= def.getSteps().size()) {
      return RetryPolicy.confirmDefault().getMaxAttempts();
    }
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
   * an async step whose callback never arrived, or whose participant reported a failure, may have
   * committed server-side, so compensation must include it.
   */
  static StepEvent giveUpFailedEvent(int stepIndex, String stepName, String reason) {
    return StepEvent.failed(
        stepIndex,
        stepName,
        EventPayloadSerializer.serializeError(new StepExecutionException(reason, false), false));
  }
}
