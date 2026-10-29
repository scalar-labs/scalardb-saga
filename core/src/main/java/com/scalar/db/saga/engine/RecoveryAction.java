package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStatus;

/**
 * The recovery action for a non-terminal saga, as decided by {@link
 * RecoveryActionResolver#resolve}: either drive compensation from a step, or resume forward
 * execution from a step. Shared by {@link SagaRecoveryManager} (automatic recovery) and the Admin
 * API (operator-triggered) so both apply the same pivot-aware, {@code knownNotCommitted}-aware
 * decision.
 */
sealed interface RecoveryAction permits RecoveryAction.Compensate, RecoveryAction.Resume {

  /**
   * The status the saga occupies while this action is driven — {@code COMPENSATING} for a
   * compensate, {@code RUNNING} for a resume. The Admin API uses it as the target status of the
   * intervention event it records before driving.
   */
  default SagaStatus targetStatus() {
    return switch (this) {
      case Compensate compensate -> SagaStatus.COMPENSATING;
      case Resume resume -> SagaStatus.RUNNING;
    };
  }

  /** Drive compensation starting at {@code fromStepIndex} (compensating downward from it). */
  record Compensate(int fromStepIndex) implements RecoveryAction {}

  /** Resume forward execution starting at {@code fromStepIndex}. */
  record Resume(int fromStepIndex) implements RecoveryAction {}
}
