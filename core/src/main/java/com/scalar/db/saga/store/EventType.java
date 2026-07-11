package com.scalar.db.saga.store;

/**
 * Enumerates all event types in a saga's event stream.
 *
 * <p>Saga-level types (used by {@link StatusEvent}) trigger status transitions. Step-level types
 * (used by {@link StepEvent}) record step outcomes. The {@link #name()} value is stored in the
 * database; renaming or removing a constant requires a data migration.
 */
public enum EventType {

  // --- Saga-level (StatusEvent) ---
  // In-progress states use present participle; terminal states use past participle.
  SAGA_STARTED,
  SAGA_COMPENSATING,
  SAGA_COMPLETED,
  SAGA_COMPENSATED,
  SAGA_ESCALATED,

  // --- Step-level (StepEvent) ---
  STEP_PENDING,
  STEP_REISSUING,
  STEP_COMPLETED,
  STEP_FAILED,
  STEP_COMPENSATED,
  STEP_COMPENSATION_FAILED
}
