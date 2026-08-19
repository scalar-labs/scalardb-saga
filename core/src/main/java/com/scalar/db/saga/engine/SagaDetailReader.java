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
 * data / PII). Each detail string is capped at {@link #MAX_DETAIL_LENGTH}, so the timeline's byte
 * size is bounded by its entry count.
 */
final class SagaDetailReader {

  /**
   * The longest detail string a timeline entry carries. Admin reasons are already this short at
   * write time (see the reason cap in {@link DefaultSagaAdminService}), but a step failure's error
   * message is persisted verbatim and commonly embeds a downstream response body; without a read
   * cap, a few such events blow past a gRPC client's default 4 MB inbound message cap no matter how
   * few events the timeline keeps. The cut is a display bound only; the full message stays in the
   * store.
   */
  static final int MAX_DETAIL_LENGTH = 1024;

  /**
   * Suffixed to a detail string cut at {@link #MAX_DETAIL_LENGTH}, so a capped message is
   * distinguishable from a complete one.
   */
  static final String TRUNCATION_MARKER = "... (truncated)";

  private SagaDetailReader() {}

  /**
   * Reads {@code sagaId}'s current state and timeline. The timeline holds at most {@code
   * maxTimelineEvents} entries; when the saga's history is longer, the newest events are kept and
   * the detail is flagged truncated (see {@link SagaDetail#isTruncated()}).
   *
   * @throws SagaNotFoundException if no saga has that id
   */
  static SagaDetail read(SagaStore store, String sagaId, int maxTimelineEvents) {
    // One atomic read pairs the snapshot with its event stream, so the status is always coherent
    // with the timeline (a concurrent transition can't wedge a newer event past a stale snapshot).
    SagaStateAndEvents data =
        store
            .getStateWithEvents(sagaId, maxTimelineEvents)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));
    return new SagaDetail(data.snapshot(), toTimeline(data.events()), data.truncated());
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
   * intervention reason only — never a raw step input/output payload (business data / PII). The
   * detail string is capped at {@link #MAX_DETAIL_LENGTH} whatever its source, so the projection
   * enforces the bound even for legacy events persisted before any write-side cap.
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
              truncateDetail(stepDetail(step)),
              null);
      case StatusEvent status ->
          new TimelineEvent(
              timestamp,
              type,
              null,
              null,
              status.getTargetStatus(),
              truncateDetail(statusDetail(status)),
              statusOperator(status));
    };
  }

  /**
   * Caps {@code detail} at {@link #MAX_DETAIL_LENGTH} chars, suffixing a cut string with {@link
   * #TRUNCATION_MARKER}. The cut backs off one char rather than splitting a surrogate pair, which
   * would leave an unencodable lone surrogate at the boundary.
   */
  private static @Nullable String truncateDetail(@Nullable String detail) {
    if (detail == null || detail.length() <= MAX_DETAIL_LENGTH) {
      return detail;
    }
    int cut = MAX_DETAIL_LENGTH;
    if (Character.isHighSurrogate(detail.charAt(cut - 1))) {
      cut--;
    }
    return detail.substring(0, cut) + TRUNCATION_MARKER;
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
