package com.scalar.db.saga.store;

/**
 * Enumerates all event types in a saga's event stream.
 *
 * <p>Saga-level types (used by {@link StatusEvent}) trigger status transitions. Step-level types
 * (used by {@link StepEvent}) record step outcomes. The {@link #name()} value is stored in the
 * database; renaming or removing a constant requires a data migration.
 *
 * <p>Naming: a past participle records a completed fact — an action that was performed or a state
 * that was reached. A present participle marks entry into an ongoing phase that a later event
 * resolves ({@code SAGA_COMPENSATING} by {@code SAGA_COMPENSATED}; {@code STEP_PENDING} by {@code
 * STEP_COMPLETED} or {@code STEP_FAILED}). So a past-participle name says what happened, not that
 * the saga is finished: {@code SAGA_STARTED} and {@code SAGA_RESET} both leave it in progress.
 */
public enum EventType {

  // --- Saga-level (StatusEvent) ---
  SAGA_STARTED,
  SAGA_COMPENSATING,
  SAGA_COMPLETED,
  SAGA_COMPENSATED,
  SAGA_ESCALATED,

  // --- Operator interventions (Admin API, StatusEvent) ---
  // A discrete action an operator performed, recorded for audit. Unlike the status-mirroring events
  // above, the resulting status varies — SAGA_RECOVERING and SAGA_RESET drive to RUNNING or
  // COMPENSATING — so the direction the engine takes is carried on the event's target status rather
  // than implied by the name. SAGA_RECOVERING is written before the drive it requests, so it names
  // the phase the saga enters, not a finished recovery.
  SAGA_FORCE_COMPLETED,
  SAGA_RECOVERING,
  SAGA_RESET,

  // --- Step-level (StepEvent) ---
  STEP_PENDING,
  STEP_REISSUING,
  STEP_COMPLETED,
  STEP_FAILED,
  STEP_COMPENSATED,
  STEP_COMPENSATION_FAILED
}
