package com.scalar.db.saga.engine;

/**
 * The recovery action for a non-terminal saga, as decided by {@link
 * RecoveryActionResolver#resolve}: either drive compensation from a step, or resume forward
 * execution from a step. Shared by {@link SagaRecoveryManager} (automatic recovery) and the Admin
 * API (operator-triggered) so both apply the same pivot-aware, {@code knownNotCommitted}-aware
 * decision.
 */
sealed interface RecoveryAction permits RecoveryAction.Compensate, RecoveryAction.Resume {

  /** Drive compensation starting at {@code fromStep} (compensating downward from it). */
  record Compensate(int fromStep) implements RecoveryAction {}

  /** Resume forward execution starting at {@code fromStep}. */
  record Resume(int fromStep) implements RecoveryAction {}
}
