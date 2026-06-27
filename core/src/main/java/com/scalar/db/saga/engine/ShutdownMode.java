package com.scalar.db.saga.engine;

/** Shutdown strategy for in-flight sagas. */
public enum ShutdownMode {
  /** Complete the current step, then stop between steps and mark for recovery. */
  WAIT_CURRENT_STEP,
  /** Wait for all active sagas to reach a terminal state. */
  WAIT_ALL_SAGAS
}
