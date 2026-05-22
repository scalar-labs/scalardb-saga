package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A saga-level event that triggers a status transition.
 *
 * <p>Each event carries a {@link #getTargetStatus() targetStatus} indicating the saga's next state.
 * Use the static factory methods to create instances.
 */
@Immutable
public final class StatusEvent implements SagaEvent {

  private final EventType eventType;
  private final SagaStatus targetStatus;
  private final @Nullable String payload;
  private final @Nullable Instant timestamp;

  private StatusEvent(
      EventType eventType,
      SagaStatus targetStatus,
      @Nullable String payload,
      @Nullable Instant timestamp) {
    this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    this.targetStatus = Objects.requireNonNull(targetStatus, "targetStatus must not be null");
    this.payload = payload;
    this.timestamp = timestamp;
  }

  // ---------------------------------------------------------------------------
  // Factory methods
  // ---------------------------------------------------------------------------

  /**
   * Creates a {@link EventType#SAGA_STARTED} event with the given payload (serialized input JSON).
   */
  public static StatusEvent started(@Nullable String payload) {
    return new StatusEvent(EventType.SAGA_STARTED, SagaStatus.RUNNING, payload, null);
  }

  /** Creates a {@link EventType#SAGA_COMPENSATING} event. */
  public static StatusEvent compensating() {
    return new StatusEvent(EventType.SAGA_COMPENSATING, SagaStatus.COMPENSATING, null, null);
  }

  /** Creates a {@link EventType#SAGA_COMPLETED} event. */
  public static StatusEvent completed() {
    return new StatusEvent(EventType.SAGA_COMPLETED, SagaStatus.COMPLETED, null, null);
  }

  /** Creates a {@link EventType#SAGA_COMPENSATED} event. */
  public static StatusEvent compensated() {
    return new StatusEvent(EventType.SAGA_COMPENSATED, SagaStatus.COMPENSATED, null, null);
  }

  /** Creates a {@link EventType#SAGA_ESCALATED} event with a reason message. */
  public static StatusEvent escalated(String reason) {
    Objects.requireNonNull(reason, "reason must not be null");
    return new StatusEvent(EventType.SAGA_ESCALATED, SagaStatus.ESCALATED, reason, null);
  }

  // ---------------------------------------------------------------------------
  // Getters
  // ---------------------------------------------------------------------------

  @Override
  public EventType getEventType() {
    return eventType;
  }

  /** Returns the target saga status for this transition event. */
  public SagaStatus getTargetStatus() {
    return targetStatus;
  }

  @Override
  public @Nullable String getPayload() {
    return payload;
  }

  @Override
  public @Nullable Instant getTimestamp() {
    return timestamp;
  }

  /** Returns a new {@code StatusEvent} with the given timestamp set. */
  public StatusEvent withTimestamp(Instant timestamp) {
    Objects.requireNonNull(timestamp, "timestamp must not be null");
    return new StatusEvent(eventType, targetStatus, payload, timestamp);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof StatusEvent that)) return false;
    return eventType.equals(that.eventType)
        && targetStatus == that.targetStatus
        && Objects.equals(payload, that.payload)
        && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventType, targetStatus, payload, timestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StatusEvent{eventType='").append(eventType).append('\'');
    sb.append(", targetStatus=").append(targetStatus);
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
