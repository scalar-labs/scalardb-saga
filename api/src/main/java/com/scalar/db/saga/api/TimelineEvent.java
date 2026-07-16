package com.scalar.db.saga.api;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * One entry in a saga's flat event timeline, as returned by {@link SagaDetail#getTimeline()}.
 *
 * <p>Each entry is a direct, 1:1 projection of one persisted saga event — no derivation or roll-up.
 * To keep the read grant narrow, an entry exposes only <b>metadata</b> plus the failure error and
 * any intervention reason; it never exposes a step's raw input/output payload (business data that
 * may hold PII).
 */
@Immutable
public final class TimelineEvent {

  private final Instant timestamp;
  private final String type;
  private final @Nullable Integer stepIndex;
  private final @Nullable String stepName;
  private final @Nullable SagaStatus resultingStatus;
  private final @Nullable String detail;
  private final @Nullable String operator;

  public TimelineEvent(
      Instant timestamp,
      String type,
      @Nullable Integer stepIndex,
      @Nullable String stepName,
      @Nullable SagaStatus resultingStatus,
      @Nullable String detail,
      @Nullable String operator) {
    this.timestamp = Objects.requireNonNull(timestamp, "timestamp must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.stepIndex = stepIndex;
    this.stepName = stepName;
    this.resultingStatus = resultingStatus;
    this.detail = detail;
    this.operator = operator;
  }

  /** When the event was recorded. */
  public Instant getTimestamp() {
    return timestamp;
  }

  /** The event type name (e.g. {@code "STEP_FAILED"}, {@code "SAGA_RECOVERING"}). */
  public String getType() {
    return type;
  }

  /** The step index for a step-level event, or {@code null} for a saga-level event. */
  public @Nullable Integer getStepIndex() {
    return stepIndex;
  }

  /** The step name for a step-level event, or {@code null} for a saga-level event. */
  public @Nullable String getStepName() {
    return stepName;
  }

  /** The saga status this event transitioned to, or {@code null} for a step-level event. */
  public @Nullable SagaStatus getResultingStatus() {
    return resultingStatus;
  }

  /**
   * A human-readable detail: the failure error message (step failures), the escalation reason
   * ({@code SAGA_ESCALATED}), or the operator's reason (interventions); {@code null} if none. Never
   * a raw step input/output payload.
   */
  public @Nullable String getDetail() {
    return detail;
  }

  /** The operator who performed an intervention, or {@code null} for a non-intervention event. */
  public @Nullable String getOperator() {
    return operator;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof TimelineEvent)) return false;
    TimelineEvent that = (TimelineEvent) o;
    return timestamp.equals(that.timestamp)
        && type.equals(that.type)
        && Objects.equals(stepIndex, that.stepIndex)
        && Objects.equals(stepName, that.stepName)
        && resultingStatus == that.resultingStatus
        && Objects.equals(detail, that.detail)
        && Objects.equals(operator, that.operator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, type, stepIndex, stepName, resultingStatus, detail, operator);
  }

  @Override
  public String toString() {
    return "TimelineEvent{"
        + "timestamp="
        + timestamp
        + ", type='"
        + type
        + "', stepIndex="
        + stepIndex
        + ", stepName='"
        + stepName
        + "', resultingStatus="
        + resultingStatus
        + ", detail='"
        + detail
        + "', operator='"
        + operator
        + "'}";
  }
}
