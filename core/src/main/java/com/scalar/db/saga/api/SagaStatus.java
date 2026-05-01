package com.scalar.db.saga.api;

/** Lifecycle status of a saga instance. */
public enum SagaStatus {

  /** Executing forward steps (Saga) or Try phase (TCC). */
  RUNNING,

  /** TCC only: all Try steps succeeded, executing Confirm phase. */
  CONFIRMING,

  /** All steps succeeded (and confirmed, in TCC mode). */
  COMPLETED,

  /** Executing compensation steps (Saga) or Cancel phase (TCC). */
  COMPENSATING,

  /** All compensations/cancellations completed. */
  COMPENSATED,

  /** Stuck beyond grace period, needs manual intervention. */
  ESCALATED
}
