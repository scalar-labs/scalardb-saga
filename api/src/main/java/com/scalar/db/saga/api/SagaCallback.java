package com.scalar.db.saga.api;

/**
 * Callback interface for asynchronous saga completion notifications. Passed to {@link
 * SagaOrchestrator#startAsync} to receive notifications when the saga reaches a terminal state.
 */
public interface SagaCallback {

  /** Called when the saga completes successfully (all steps executed and confirmed). */
  void onCompleted(SagaStateSnapshot saga);

  /** Called when the saga is fully compensated (all compensations succeeded). */
  void onCompensated(SagaStateSnapshot saga);

  /**
   * Called when the saga is escalated (stuck beyond grace period, needs manual intervention).
   *
   * <p>Note: Currently, escalation only occurs during recovery, which runs asynchronously on a
   * separate thread without access to the original callback. This method is provided for future
   * use. To detect escalations, poll {@link SagaOrchestrator#getStateSnapshot}.
   */
  void onEscalated(SagaStateSnapshot saga);
}
