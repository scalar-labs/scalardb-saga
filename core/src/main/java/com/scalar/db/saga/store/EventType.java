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
  /** The saga was created and entered {@code RUNNING}; the payload is its serialized input. */
  SAGA_STARTED,
  /**
   * The saga entered {@code COMPENSATING}: forward execution stopped and the completed steps are
   * being compensated. Resolved by {@link #SAGA_COMPENSATED}.
   */
  SAGA_COMPENSATING,
  /** Every step completed (and, in TCC mode, confirmed); the saga reached {@code COMPLETED}. */
  SAGA_COMPLETED,
  /** Every compensation (or TCC cancel) ran; the saga reached {@code COMPENSATED}. */
  SAGA_COMPENSATED,
  /** The saga was handed to an operator as {@code ESCALATED}; the payload is the reason. */
  SAGA_ESCALATED,

  // --- Operator interventions (Admin API, StatusEvent) ---
  /**
   * An operator overrode an {@code ESCALATED} saga to {@code COMPLETED}. A discrete action an
   * operator performed, recorded for audit: the payload carries the operator and reason (see {@link
   * AdminAuditPayload}).
   */
  SAGA_FORCE_COMPLETED,
  /**
   * An operator asked for a stuck {@code RUNNING} or {@code COMPENSATING} saga to be driven now
   * rather than by the next recovery sweep. Unlike the status-mirroring events above, the resulting
   * status varies, {@code RUNNING} or {@code COMPENSATING}, so the direction the engine takes is
   * carried on the event's target status and in the audit payload rather than implied by the name.
   * Written before the drive it requests, so it names the phase the saga enters, not a finished
   * recovery.
   */
  SAGA_RECOVERING,
  /**
   * An operator un-escalated an {@code ESCALATED} saga, driving it in the direction recovery would
   * take it. As with {@link #SAGA_RECOVERING}, the resulting status, {@code RUNNING} or {@code
   * COMPENSATING}, is carried on the event's target status and in the audit payload.
   */
  SAGA_RESET,

  // --- Step-level (StepEvent) ---
  /**
   * A forward step parked on an asynchronous callback ({@code RUNNING → WAITING}); its output
   * arrives with the later {@link #STEP_COMPLETED}.
   */
  STEP_PENDING,
  /**
   * The recovery sweep un-parked a timed-out asynchronous step to re-issue its call ({@code WAITING
   * → RUNNING}); a fresh {@link #STEP_PENDING} follows when it parks again.
   */
  STEP_REISSUING,
  /** A step's forward action completed; the payload is its serialized output. */
  STEP_COMPLETED,
  /** A step's forward action failed; the payload describes the failure. */
  STEP_FAILED,
  /** A step's compensation (or TCC cancel) completed. */
  STEP_COMPENSATED,
  /**
   * A step's compensation threw; the payload describes the failure. The saga stays {@code
   * COMPENSATING} for recovery to retry.
   */
  STEP_COMPENSATION_FAILED
}
