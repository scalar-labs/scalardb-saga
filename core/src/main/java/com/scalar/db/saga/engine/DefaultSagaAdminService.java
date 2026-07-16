package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.store.AdminAuditPayload;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStateAndEvents;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Embedded implementation of the {@link SagaAdminService} control plane. Reads delegate to the
 * {@link SagaStore}; mutations record an operator-intervention event through {@link
 * SagaStore#recordStatusEvent} — an atomic CAS-guard + co-committed audit + status transition — and
 * then drive the saga inline in the direction {@link RecoveryActionResolver#resolve} chooses,
 * exactly as automatic recovery would (minus the grace-period wait).
 *
 * <p>The single-saga mutations drive inline and return the driven snapshot. The bulk {@link
 * #resetEscalated(SagaQuery, String)} sweep instead only un-escalates each row and hands the drive
 * to the recovery loop (via {@link SagaStore#markForRecovery}), so one call never blocks on a whole
 * page of participant round-trips.
 *
 * <p>The operator identity is read from the injected {@link OperatorContext}, never from the
 * caller; every mutation requires a non-blank {@code reason}, sanitized before it is persisted.
 */
@ThreadSafe
public class DefaultSagaAdminService implements SagaAdminService {

  private static final int MAX_REASON_LENGTH = 1024;

  private final SagaStore store;
  private final SagaEngine engine;
  private final SagaDefinitionRegistry definitionRegistry;
  private final OperatorContext operatorContext;

  DefaultSagaAdminService(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry definitionRegistry,
      OperatorContext operatorContext) {
    this.store = store;
    this.engine = engine;
    this.definitionRegistry = definitionRegistry;
    this.operatorContext = operatorContext;
  }

  // ---------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------

  @Override
  public SagaPage<SagaStateSnapshot> listSagas(SagaQuery query) {
    Objects.requireNonNull(query, "query must not be null");
    return store.listStateSnapshots(query);
  }

  @Override
  public SagaDetail getSagaDetail(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    // One atomic read pairs the snapshot with its event stream, so the status is always coherent
    // with the timeline (a concurrent transition can't wedge a newer event past a stale snapshot).
    SagaStateAndEvents data =
        store.getStateWithEvents(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
    return new SagaDetail(data.snapshot(), toTimeline(data.events()));
  }

  private static List<TimelineEvent> toTimeline(List<SagaEvent> events) {
    List<TimelineEvent> timeline = new ArrayList<>(events.size());
    for (SagaEvent event : events) {
      timeline.add(toTimelineEvent(event));
    }
    return timeline;
  }

  /**
   * Projects one persisted event to a timeline entry. Exposes metadata plus the failure error /
   * intervention reason only — never a raw step input/output payload (business data / PII).
   */
  private static TimelineEvent toTimelineEvent(SagaEvent event) {
    Instant timestamp =
        Objects.requireNonNull(event.getTimestamp(), "a loaded event must have a timestamp");
    String type = event.getEventType().name();
    return switch (event) {
      case StepEvent step ->
          new TimelineEvent(
              timestamp,
              type,
              step.getStepIndex(),
              step.getStepName(),
              null,
              stepDetail(step),
              null);
      case StatusEvent status ->
          new TimelineEvent(
              timestamp,
              type,
              null,
              null,
              status.getTargetStatus(),
              statusDetail(status),
              statusOperator(status));
    };
  }

  private static @Nullable String stepDetail(StepEvent step) {
    return switch (step.getEventType()) {
      case STEP_FAILED, STEP_COMPENSATION_FAILED ->
          EventPayloadSerializer.errorMessage(step.getPayload());
      default -> null; // STEP_COMPLETED etc. carry the step's raw output — never exposed here
    };
  }

  private static @Nullable String statusDetail(StatusEvent status) {
    return switch (status.getEventType()) {
      case SAGA_ESCALATED -> status.getPayload(); // the escalation reason (plain text)
      case SAGA_FORCE_COMPLETED, SAGA_RECOVERING, SAGA_RESET ->
          AdminAuditPayload.reason(status.getPayload());
      default -> null; // SAGA_STARTED carries the saga input — never exposed here
    };
  }

  private static @Nullable String statusOperator(StatusEvent status) {
    return switch (status.getEventType()) {
      case SAGA_FORCE_COMPLETED, SAGA_RECOVERING, SAGA_RESET ->
          AdminAuditPayload.operator(status.getPayload());
      default -> null;
    };
  }

  // ---------------------------------------------------------------------------
  // Mutations
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot recoverSaga(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    SagaStatus status = snapshot.getStatus();
    if (status == SagaStatus.WAITING) {
      throw parked(sagaId);
    }
    if (!status.isRecoverable()) {
      throw notRecoverable(sagaId, status);
    }

    SagaDefinition def = resolveDefinitionOrThrow(snapshot);
    List<SagaEvent> events = store.getEvents(sagaId);
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, status);
    StatusEvent recoveringEvent =
        StatusEvent.recovering(action.targetStatus(), operator, sanitizedReason);
    return recordAndRecover(snapshot, def, events, action, recoveringEvent);
  }

  @Override
  public SagaStateSnapshot forceComplete(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    if (snapshot.getStatus() != SagaStatus.ESCALATED) {
      throw notEscalated(sagaId, snapshot.getStatus(), "force-complete");
    }
    StatusEvent forceCompletedEvent = StatusEvent.forceCompleted(operator, sanitizedReason);
    // ESCALATED -> COMPLETED, atomic with the audit; no drive (terminal). A lost CAS (a concurrent
    // admin/recovery) surfaces as SagaConcurrentModificationException (409).
    return store.recordStatusEvent(
        snapshot, store.getEventCount(sagaId), forceCompletedEvent, engine.ownerId());
  }

  @Override
  public SagaStateSnapshot resetEscalated(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    if (snapshot.getStatus() != SagaStatus.ESCALATED) {
      throw notEscalated(sagaId, snapshot.getStatus(), "reset");
    }
    SagaDefinition def = resolveDefinitionOrThrow(snapshot);
    return driveReset(snapshot, def, operator, sanitizedReason);
  }

  @Override
  public ResetResult resetEscalated(SagaQuery query, String reason) {
    Objects.requireNonNull(query, "query must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    if (query.getStatus() != null && query.getStatus() != SagaStatus.ESCALATED) {
      throw new IllegalArgumentException(
          "resetEscalated only sweeps ESCALATED sagas; conflicting status filter: "
              + query.getStatus());
    }
    // Pin the scan to ESCALATED. The status filter selects the scan position; the per-row CAS at
    // write time is the authorization boundary (a row that changed status is skipped, not forced).
    SagaQuery escalatedQuery =
        SagaQuery.newBuilder()
            .status(SagaStatus.ESCALATED)
            .updatedAfter(query.getUpdatedAfter())
            .updatedBefore(query.getUpdatedBefore())
            .pageSize(query.getPageSize())
            .pageToken(query.getPageToken())
            .build();

    SagaPage<SagaStateSnapshot> page = store.listStateSnapshots(escalatedQuery);
    int resetCount = 0;
    List<ResetResult.SkippedSaga> skipped = new ArrayList<>();
    for (SagaStateSnapshot snapshot : page.getItems()) {
      SagaDefinition def =
          definitionRegistry.resolve(snapshot.getSagaName(), snapshot.getDefinitionVersion());
      if (def == null) {
        // Unresolvable definition — cannot compute a plan; never force-driven.
        skipped.add(
            new ResetResult.SkippedSaga(
                snapshot.getSagaId(), ResetResult.SkipReason.DEFINITION_NOT_FOUND));
        continue;
      }
      try {
        markReset(snapshot, def, operator, sanitizedReason);
        resetCount++;
      } catch (SagaConcurrentModificationException e) {
        // Lost the CAS race to a concurrent writer — leave it for the next sweep.
        skipped.add(
            new ResetResult.SkippedSaga(
                snapshot.getSagaId(), ResetResult.SkipReason.CONCURRENT_MODIFICATION));
      }
    }
    return new ResetResult(resetCount, skipped, page.getNextPageToken());
  }

  // ---------------------------------------------------------------------------
  // Shared intervention drive
  // ---------------------------------------------------------------------------

  /**
   * Un-escalates {@code snapshot} and drives it in the direction {@link
   * RecoveryActionResolver#resolve} chooses for its {@code ESCALATED} status — reconstructing from
   * the event stream whether it was compensating or running before it escalated — so a
   * compensation-stuck escalation resumes compensation and a post-pivot one resumes forward.
   */
  private SagaStateSnapshot driveReset(
      SagaStateSnapshot snapshot, SagaDefinition def, String operator, String reason) {
    List<SagaEvent> events = store.getEvents(snapshot.getSagaId());
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, snapshot.getStatus());
    StatusEvent resetEvent = StatusEvent.reset(action.targetStatus(), operator, reason);
    return recordAndRecover(snapshot, def, events, action, resetEvent);
  }

  /**
   * Un-escalates {@code snapshot} durably — the same audit-carrying CAS transition as {@link
   * #driveReset} — but hands the drive to the recovery loop via {@link SagaStore#markForRecovery}
   * instead of running it inline. The bulk sweep uses this so a single call never blocks on a whole
   * page of participant round-trips: the sweeper then drives each un-escalated saga through the
   * same {@link RecoveryActionResolver}/{@link SagaEngine#recover} path the inline reset would
   * have. The un-escalation is the durable work; the drive is only an optimization, so deferring it
   * is safe.
   */
  private void markReset(
      SagaStateSnapshot snapshot, SagaDefinition def, String operator, String reason) {
    List<SagaEvent> events = store.getEvents(snapshot.getSagaId());
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, snapshot.getStatus());
    StatusEvent resetEvent = StatusEvent.reset(action.targetStatus(), operator, reason);
    store.recordStatusEvent(snapshot, events.size(), resetEvent, engine.ownerId());
    store.markForRecovery(snapshot.getSagaId());
  }

  /**
   * Records the audit-carrying status transition atomically (the CAS guard), then drives the saga
   * inline in the resolved direction. The engine's compensate path skips its own transition when
   * the saga is already {@code COMPENSATING} (the state the audit event just set), so there is no
   * double transition.
   */
  private SagaStateSnapshot recordAndRecover(
      SagaStateSnapshot snapshot,
      SagaDefinition def,
      List<SagaEvent> events,
      RecoveryAction action,
      StatusEvent interventionEvent) {
    ExecutionContext context = engine.replayEvents(snapshot, events);
    SagaStateSnapshot recorded =
        store.recordStatusEvent(snapshot, events.size(), interventionEvent, engine.ownerId());
    context.setCurrentState(recorded);
    context.setNextEventSequence(events.size() + 1);
    engine.recover(action, def, context);
    return context.getCurrentState();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private SagaStateSnapshot requireSnapshot(String sagaId) {
    return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
  }

  private SagaDefinition resolveDefinitionOrThrow(SagaStateSnapshot snapshot) {
    SagaDefinition def =
        definitionRegistry.resolve(snapshot.getSagaName(), snapshot.getDefinitionVersion());
    if (def == null) {
      throw new SagaDefinitionNotFoundException(
          snapshot.getSagaName(), snapshot.getDefinitionVersion());
    }
    return def;
  }

  private static SagaStatePreconditionException parked(String sagaId) {
    return new SagaStatePreconditionException(
        sagaId,
        SagaStatePreconditionException.Code.SAGA_PARKED,
        "Saga " + sagaId + " is WAITING on an async callback; it resolves via callback or timeout");
  }

  private static SagaStatePreconditionException notEscalated(
      String sagaId, SagaStatus status, String action) {
    return wrongState(
        sagaId,
        "Cannot " + action + " saga " + sagaId + " in status " + status + " (expected ESCALATED)");
  }

  private static SagaStatePreconditionException notRecoverable(String sagaId, SagaStatus status) {
    return wrongState(
        sagaId,
        "Cannot recover saga "
            + sagaId
            + " in status "
            + status
            + " (recover accepts RUNNING or COMPENSATING; for ESCALATED use resetEscalated or"
            + " forceComplete)");
  }

  private static SagaStatePreconditionException wrongState(String sagaId, String message) {
    return new SagaStatePreconditionException(
        sagaId, SagaStatePreconditionException.Code.SAGA_WRONG_STATE, message);
  }

  private static String validateReason(String reason) {
    Objects.requireNonNull(reason, "reason must not be null");
    String sanitized = sanitizeControlChars(reason).trim();
    if (sanitized.isEmpty()) {
      throw new IllegalArgumentException("reason must not be blank");
    }
    if (sanitized.length() > MAX_REASON_LENGTH) {
      throw new IllegalArgumentException(
          "reason must be at most " + MAX_REASON_LENGTH + " characters, got " + sanitized.length());
    }
    return sanitized;
  }

  /**
   * Replaces ISO control characters (newlines included) with spaces; log-forging defense. Spaces
   * rather than removal so that a multi-line reason keeps its word boundaries once flattened.
   */
  private static String sanitizeControlChars(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      sb.append(Character.isISOControl(c) ? ' ' : c);
    }
    return sb.toString();
  }

  private String operator() {
    String operator = operatorContext.currentOperator();
    if (operator.isBlank()) {
      throw new IllegalStateException(
          "OperatorContext returned a blank operator; refusing to write an anonymous audit record");
    }
    return operator;
  }
}
