package com.scalar.db.saga.store;

import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A step-level event that records a step outcome (completed, failed, compensated, etc.).
 *
 * <p>Each event carries a {@link #getStepIndex() stepIndex} and {@link #getStepName() stepName}
 * identifying the step. Use the static factory methods to create instances.
 */
@Immutable
public final class StepEvent implements SagaEvent {

  private final EventType eventType;
  private final int stepIndex;
  private final String stepName;
  private final @Nullable String payload;
  private final @Nullable Instant timestamp;

  private StepEvent(
      EventType eventType,
      int stepIndex,
      String stepName,
      @Nullable String payload,
      @Nullable Instant timestamp) {
    this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    if (stepIndex < 0) {
      throw new IllegalArgumentException("stepIndex must be >= 0, got " + stepIndex);
    }
    this.stepIndex = stepIndex;
    this.stepName = Objects.requireNonNull(stepName, "stepName must not be null");
    this.payload = payload;
    this.timestamp = timestamp;
  }

  // ---------------------------------------------------------------------------
  // Factory methods
  // ---------------------------------------------------------------------------

  /**
   * Creates a {@link EventType#STEP_PENDING} event, marking that a forward step has parked on an
   * async callback ({@code RUNNING → WAITING}). Carries no payload — the step's output arrives
   * later with the {@link EventType#STEP_COMPLETED} event when the callback resumes it.
   */
  public static StepEvent pending(int stepIndex, String stepName) {
    return new StepEvent(EventType.STEP_PENDING, stepIndex, stepName, null, null);
  }

  /** Creates a {@link EventType#STEP_COMPLETED} event. */
  public static StepEvent completed(int stepIndex, String stepName, @Nullable String payload) {
    return new StepEvent(EventType.STEP_COMPLETED, stepIndex, stepName, payload, null);
  }

  /** Creates a {@link EventType#STEP_FAILED} event. */
  public static StepEvent failed(int stepIndex, String stepName, @Nullable String payload) {
    return new StepEvent(EventType.STEP_FAILED, stepIndex, stepName, payload, null);
  }

  /** Creates a {@link EventType#STEP_COMPENSATED} event. */
  public static StepEvent compensated(int stepIndex, String stepName) {
    return new StepEvent(EventType.STEP_COMPENSATED, stepIndex, stepName, null, null);
  }

  /** Creates a {@link EventType#STEP_COMPENSATION_FAILED} event. */
  public static StepEvent compensationFailed(
      int stepIndex, String stepName, @Nullable String payload) {
    return new StepEvent(EventType.STEP_COMPENSATION_FAILED, stepIndex, stepName, payload, null);
  }

  // ---------------------------------------------------------------------------
  // Getters
  // ---------------------------------------------------------------------------

  @Override
  public EventType getEventType() {
    return eventType;
  }

  /** Returns the step index (always {@code >= 0}). */
  public int getStepIndex() {
    return stepIndex;
  }

  /** Returns the step name. */
  public String getStepName() {
    return stepName;
  }

  @Override
  public @Nullable String getPayload() {
    return payload;
  }

  @Override
  public @Nullable Instant getTimestamp() {
    return timestamp;
  }

  /** Returns a new {@code StepEvent} with the given timestamp set. */
  public StepEvent withTimestamp(Instant timestamp) {
    Objects.requireNonNull(timestamp, "timestamp must not be null");
    return new StepEvent(eventType, stepIndex, stepName, payload, timestamp);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof StepEvent that)) return false;
    return stepIndex == that.stepIndex
        && eventType.equals(that.eventType)
        && stepName.equals(that.stepName)
        && Objects.equals(payload, that.payload)
        && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventType, stepIndex, stepName, payload, timestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StepEvent{eventType='").append(eventType).append('\'');
    sb.append(", stepIndex=").append(stepIndex);
    sb.append(", stepName='").append(stepName).append('\'');
    if (payload != null) {
      sb.append(", payload='").append(payload).append('\'');
    }
    if (timestamp != null) {
      sb.append(", timestamp=").append(timestamp);
    }
    sb.append('}');
    return sb.toString();
  }
}
