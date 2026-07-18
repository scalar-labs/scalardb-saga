package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaNotFoundException;
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
import org.jspecify.annotations.Nullable;

/**
 * Reads a saga's state together with a redacted timeline of its events. This is an application read
 * — the application that ran a saga uses it to diagnose why the saga failed — so it belongs to
 * neither the application orchestrator nor the admin control plane specifically; it is the shared
 * projection both would otherwise duplicate. The timeline exposes each event's metadata plus its
 * failure error or intervention reason only, <b>never</b> a raw step input/output payload (business
 * data / PII).
 */
final class SagaDetailReader {

  private SagaDetailReader() {}

  /**
   * Reads {@code sagaId}'s current state and timeline.
   *
   * @throws SagaNotFoundException if no saga has that id
   */
  static SagaDetail read(SagaStore store, String sagaId) {
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
}
