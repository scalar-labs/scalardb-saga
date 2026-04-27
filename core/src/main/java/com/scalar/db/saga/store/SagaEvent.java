package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable value object representing a single event in a saga's event stream.
 *
 * <p>Events are append-only and fall into two categories:
 *
 * <ul>
 *   <li><b>Saga-level events</b> — carry a {@link #getTargetStatus() targetStatus} indicating the
 *       saga's next state (e.g., {@link #SAGA_COMPLETED} → {@link SagaStatus#COMPLETED}).
 *   <li><b>Step-level events</b> — carry {@link #getStepIndex() stepIndex} and {@link
 *       #getStepName() stepName} but no target status.
 * </ul>
 *
 * <p>Use the static factory methods to create instances.
 */
@Immutable
public final class SagaEvent {

  // --- Saga lifecycle (6) ---
  // In-progress states use present participle; terminal states use past participle.
  public static final String SAGA_STARTED = "SAGA_STARTED";
  public static final String SAGA_CONFIRMING = "SAGA_CONFIRMING";
  public static final String SAGA_COMPENSATING = "SAGA_COMPENSATING";
  public static final String SAGA_COMPLETED = "SAGA_COMPLETED";
  public static final String SAGA_COMPENSATED = "SAGA_COMPENSATED";
  public static final String SAGA_ESCALATED = "SAGA_ESCALATED";

  // --- Step outcomes (4) ---
  public static final String STEP_COMPLETED = "STEP_COMPLETED";
  public static final String STEP_FAILED = "STEP_FAILED";
  public static final String STEP_COMPENSATED = "STEP_COMPENSATED";
  public static final String STEP_COMPENSATION_FAILED = "STEP_COMPENSATION_FAILED";

  private final String eventType;
  private final int stepIndex;
  private final @Nullable String stepName;
  private final @Nullable String payload;
  private final @Nullable SagaStatus targetStatus;
  private final @Nullable Instant timestamp;

  private SagaEvent(
      String eventType,
      int stepIndex,
      @Nullable String stepName,
      @Nullable String payload,
      @Nullable SagaStatus targetStatus,
      @Nullable Instant timestamp) {
    this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    this.stepIndex = stepIndex;
    this.stepName = stepName;
    this.payload = payload;
    this.targetStatus = targetStatus;
    this.timestamp = timestamp;
  }

  // ---------------------------------------------------------------------------
  // Saga-level factory methods (each carries its target SagaStatus)
  // ---------------------------------------------------------------------------

  /** Creates a {@link #SAGA_STARTED} event with the given payload (serialized input JSON). */
  public static SagaEvent sagaStarted(@Nullable String payload) {
    return new SagaEvent(SAGA_STARTED, -1, null, payload, SagaStatus.RUNNING, null);
  }

  /** Creates a {@link #SAGA_CONFIRMING} event (TCC confirm phase). */
  public static SagaEvent sagaConfirming() {
    return new SagaEvent(SAGA_CONFIRMING, -1, null, null, SagaStatus.CONFIRMING, null);
  }

  /** Creates a {@link #SAGA_COMPENSATING} event. */
  public static SagaEvent sagaCompensating() {
    return new SagaEvent(SAGA_COMPENSATING, -1, null, null, SagaStatus.COMPENSATING, null);
  }

  /** Creates a {@link #SAGA_COMPLETED} event. */
  public static SagaEvent sagaCompleted() {
    return new SagaEvent(SAGA_COMPLETED, -1, null, null, SagaStatus.COMPLETED, null);
  }

  /** Creates a {@link #SAGA_COMPENSATED} event. */
  public static SagaEvent sagaCompensated() {
    return new SagaEvent(SAGA_COMPENSATED, -1, null, null, SagaStatus.COMPENSATED, null);
  }

  /** Creates a {@link #SAGA_ESCALATED} event with a reason message. */
  public static SagaEvent sagaEscalated(String reason) {
    Objects.requireNonNull(reason, "reason must not be null");
    return new SagaEvent(SAGA_ESCALATED, -1, null, reason, SagaStatus.ESCALATED, null);
  }

  // ---------------------------------------------------------------------------
  // Step-level factory methods (no status transition)
  // ---------------------------------------------------------------------------

  /** Creates a {@link #STEP_COMPLETED} event. */
  public static SagaEvent stepCompleted(int stepIndex, String stepName, @Nullable String payload) {
    validateStepIndex(stepIndex);
    Objects.requireNonNull(stepName, "stepName must not be null");
    return new SagaEvent(STEP_COMPLETED, stepIndex, stepName, payload, null, null);
  }

  /** Creates a {@link #STEP_FAILED} event. */
  public static SagaEvent stepFailed(int stepIndex, String stepName, @Nullable String payload) {
    validateStepIndex(stepIndex);
    Objects.requireNonNull(stepName, "stepName must not be null");
    return new SagaEvent(STEP_FAILED, stepIndex, stepName, payload, null, null);
  }

  /** Creates a {@link #STEP_COMPENSATED} event. */
  public static SagaEvent stepCompensated(int stepIndex, String stepName) {
    validateStepIndex(stepIndex);
    Objects.requireNonNull(stepName, "stepName must not be null");
    return new SagaEvent(STEP_COMPENSATED, stepIndex, stepName, null, null, null);
  }

  /** Creates a {@link #STEP_COMPENSATION_FAILED} event. */
  public static SagaEvent stepCompensationFailed(
      int stepIndex, String stepName, @Nullable String payload) {
    validateStepIndex(stepIndex);
    Objects.requireNonNull(stepName, "stepName must not be null");
    return new SagaEvent(STEP_COMPENSATION_FAILED, stepIndex, stepName, payload, null, null);
  }

  private static void validateStepIndex(int stepIndex) {
    if (stepIndex < 0) {
      throw new IllegalArgumentException("stepIndex must be >= 0, got " + stepIndex);
    }
  }

  // ---------------------------------------------------------------------------
  // Getters
  // ---------------------------------------------------------------------------

  public String getEventType() {
    return eventType;
  }

  /** Returns the step index, or {@code -1} for saga-level events. */
  public int getStepIndex() {
    return stepIndex;
  }

  /** Returns the step name, or {@code null} for saga-level events. */
  public @Nullable String getStepName() {
    return stepName;
  }

  /**
   * Returns the event-specific payload (e.g., serialized JSON for step results, plain text for
   * escalation reasons), or {@code null} if none.
   */
  public @Nullable String getPayload() {
    return payload;
  }

  /**
   * Returns the target saga status for saga-level events, or {@code null} for step-level events.
   */
  public @Nullable SagaStatus getTargetStatus() {
    return targetStatus;
  }

  /** Returns the timestamp set when loaded from the store, or {@code null} if not yet persisted. */
  public @Nullable Instant getTimestamp() {
    return timestamp;
  }

  /** Returns a new {@code SagaEvent} with the given timestamp set. */
  public SagaEvent withTimestamp(Instant timestamp) {
    Objects.requireNonNull(timestamp, "timestamp must not be null");
    return new SagaEvent(eventType, stepIndex, stepName, payload, targetStatus, timestamp);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof SagaEvent that)) return false;
    return stepIndex == that.stepIndex
        && eventType.equals(that.eventType)
        && Objects.equals(stepName, that.stepName)
        && Objects.equals(payload, that.payload)
        && targetStatus == that.targetStatus
        && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventType, stepIndex, stepName, payload, targetStatus, timestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SagaEvent{eventType='").append(eventType).append('\'');
    if (stepIndex >= 0) {
      sb.append(", stepIndex=").append(stepIndex);
      sb.append(", stepName='").append(stepName).append('\'');
    }
    if (targetStatus != null) {
      sb.append(", targetStatus=").append(targetStatus);
    }
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
