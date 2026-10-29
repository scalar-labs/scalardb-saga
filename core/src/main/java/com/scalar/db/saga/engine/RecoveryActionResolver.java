package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.StepEvent;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Shared recovery-action logic used by both {@link SagaRecoveryManager} (automatic recovery, after
 * a grace period) and the Admin API (operator-triggered, immediately). {@link #resolve} centralizes
 * the pivot-aware, {@code knownNotCommitted}-aware "compensate-from-N vs resume-from-M" decision
 * that was historically inline in the recovery manager, so the two callers can never drift.
 *
 * <p>This holds only the pure decision (and its event-stream helpers). The grace-period escalation
 * <em>policy</em> stays with {@link SagaRecoveryManager}; admin deliberately skips it.
 */
final class RecoveryActionResolver {

  private RecoveryActionResolver() {}

  /**
   * Decides the recovery action for a saga from its event stream, mirroring the engine's
   * forward/backward recovery rules exactly. This is the accumulation of the in-doubt / {@code
   * knownNotCommitted} fixes, so callers must use it rather than re-deriving the decision:
   *
   * <ul>
   *   <li>{@code COMPENSATING} &rarr; {@link RecoveryAction.Compensate} from the highest
   *       un-compensated step (or, if none compensated yet, the highest completed step or the
   *       highest possibly-committed forward failure).
   *   <li>{@code RUNNING} with an unresolved pre-pivot failure (the engine had decided to
   *       compensate but crashed before the {@code COMPENSATING} transition) &rarr; {@link
   *       RecoveryAction.Compensate}, including a possibly-committed failure and skipping a {@code
   *       knownNotCommitted} one.
   *   <li>{@code RUNNING} otherwise &rarr; {@link RecoveryAction.Resume} from the step after the
   *       highest completed one. A post-pivot failure is resumed forward, not compensated.
   *   <li>{@code ESCALATED} (an escalated saga being un-escalated by the Admin API) &rarr; the
   *       direction it was heading before it escalated is reconstructed from the event stream (the
   *       snapshot status no longer carries it), then resolved as {@code COMPENSATING} or {@code
   *       RUNNING} above.
   * </ul>
   */
  static RecoveryAction resolve(List<SagaEvent> events, SagaDefinition def, SagaStatus status) {
    SagaStatus effective = status == SagaStatus.ESCALATED ? reconstructDirection(events) : status;
    if (effective == SagaStatus.COMPENSATING) {
      return new RecoveryAction.Compensate(stepIndexToCompensateFrom(events));
    }

    // RUNNING (or any other non-terminal, non-COMPENSATING status): resume forward unless a durable
    // pre-pivot failure means the engine had already decided to compensate. See
    // hasUnresolvedPrePivotFailure for why resuming there would orphan a committed side effect.
    int pivotIndex = def.getPivotIndex();
    if (hasUnresolvedPrePivotFailure(events, pivotIndex)) {
      int fromStepIndex =
          Math.max(
              stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1),
              failedIndicesToCompensate(events)
                  .filter(index -> index <= pivotIndex)
                  .max()
                  .orElse(-1));
      return new RecoveryAction.Compensate(fromStepIndex);
    }

    int lastCompleted = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);
    return new RecoveryAction.Resume(lastCompleted + 1);
  }

  /**
   * The step to start compensating from for a saga already in {@code COMPENSATING}.
   *
   * <p>If some step has already been compensated, continue one below the lowest such step.
   * Otherwise the saga crashed right after the {@code COMPENSATING} transition, before undoing
   * anything, so start from the highest step that may still hold a live side effect: the highest
   * completed step, or the highest forward failure not proven undelivered (a {@code
   * knownNotCommitted} failure is skipped).
   *
   * <p>Unlike the forward path in {@link #resolve}, this does not clamp {@code fromStepIndex} to
   * the pivot, and does not need to: a saga reaches {@code COMPENSATING} only through a pre-pivot
   * forward failure, so every completed or failed index here is already at or before the pivot. If
   * a future path (most likely a manual cancel or abort API) ever drove a past-the-pivot saga into
   * {@code COMPENSATING}, that assumption would break. Such a saga cannot be cleanly rolled back,
   * so it must escalate rather than silently clamp here; clamping would leave a torn saga, with the
   * pre-pivot steps undone but the post-pivot steps still committed. Handle the pivot scope when
   * that path is introduced.
   */
  private static int stepIndexToCompensateFrom(List<SagaEvent> events) {
    OptionalInt lowestCompensated = stepIndices(events, EventType.STEP_COMPENSATED).min();
    if (lowestCompensated.isPresent()) {
      return lowestCompensated.getAsInt() - 1;
    }
    return Math.max(
        stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1),
        failedIndicesToCompensate(events).max().orElse(-1));
  }

  /**
   * Reconstructs the direction an escalated saga was heading before it escalated: {@code
   * COMPENSATING} if the event stream shows it had entered compensation, otherwise {@code RUNNING}.
   * The snapshot status is {@code ESCALATED}, so the direction must come from the stream.
   */
  private static SagaStatus reconstructDirection(List<SagaEvent> events) {
    for (SagaEvent event : events) {
      EventType type = event.getEventType();
      if (type == EventType.SAGA_COMPENSATING
          || type == EventType.STEP_COMPENSATED
          || type == EventType.STEP_COMPENSATION_FAILED) {
        return SagaStatus.COMPENSATING;
      }
    }
    return SagaStatus.RUNNING;
  }

  /**
   * STEP_FAILED indices whose persisted payload does not prove non-delivery — those failed steps
   * may have committed, so compensation must include them. A {@code knownNotCommitted} failure
   * (proven non-delivery) is excluded.
   */
  static IntStream failedIndicesToCompensate(List<SagaEvent> events) {
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
  static boolean hasUnresolvedPrePivotFailure(List<SagaEvent> events, int pivotIndex) {
    Set<Integer> resolvedIndices =
        stepIndices(events, EventType.STEP_COMPLETED).boxed().collect(Collectors.toSet());
    return stepIndices(events, EventType.STEP_FAILED)
        .filter(index -> index <= pivotIndex)
        .anyMatch(index -> !resolvedIndices.contains(index));
  }

  static IntStream stepIndices(List<SagaEvent> events, EventType eventType) {
    return events.stream()
        .filter(e -> e instanceof StepEvent)
        .map(e -> (StepEvent) e)
        .filter(e -> e.getEventType() == eventType)
        .mapToInt(StepEvent::getStepIndex);
  }
}
