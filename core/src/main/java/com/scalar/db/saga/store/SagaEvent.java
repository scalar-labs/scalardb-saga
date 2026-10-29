package com.scalar.db.saga.store;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Sealed base type for all events in a saga's event stream.
 *
 * <p>Events are append-only and fall into two categories:
 *
 * <ul>
 *   <li>{@link StatusEvent} — saga-level events that change the saga's status (e.g., {@code
 *       RUNNING} → {@code COMPENSATING}).
 *   <li>{@link StepEvent} — step-level events that record step outcomes (e.g., completed, failed).
 * </ul>
 *
 * <p>Use the factory methods on the concrete types to create instances.
 */
public sealed interface SagaEvent permits StatusEvent, StepEvent {

  /**
   * Returns the event type (e.g., {@link EventType#SAGA_STARTED}, {@link
   * EventType#STEP_COMPLETED}).
   *
   * @return the event type
   */
  EventType getEventType();

  /**
   * Returns the event-specific payload (e.g., serialized JSON for step results, plain text for
   * escalation reasons), or {@code null} if none.
   *
   * @return the payload, or {@code null} if the event carries none
   */
  @Nullable String getPayload();

  /**
   * Returns the timestamp set when loaded from the store, or {@code null} if not yet persisted.
   *
   * @return the store's timestamp, or {@code null} before the event is persisted
   */
  @Nullable Instant getTimestamp();
}
