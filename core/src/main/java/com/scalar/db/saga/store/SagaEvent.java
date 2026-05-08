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
   */
  EventType getEventType();

  /**
   * Returns the event-specific payload (e.g., serialized JSON for step results, plain text for
   * escalation reasons), or {@code null} if none.
   */
  @Nullable String getPayload();

  /** Returns the timestamp set when loaded from the store, or {@code null} if not yet persisted. */
  @Nullable Instant getTimestamp();
}
