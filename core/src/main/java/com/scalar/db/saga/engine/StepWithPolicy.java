package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.definition.RetryPolicy;

/**
 * Bundles a {@link Step} with its resolved retry policies, per-step (call-execution) timeout, and
 * async callback-wait timeout.
 *
 * <p>{@code callbackTimeoutMillis} is the phase-call's {@code callbackTimeoutMillis} (0 for a
 * non-async or class step); it bounds how long the engine parks waiting for the callback after a
 * {@code 202}.
 *
 * <p>Internal record — not part of the public API.
 */
record StepWithPolicy(
    Step step,
    RetryPolicy executionRetryPolicy,
    RetryPolicy compensationRetryPolicy,
    long stepTimeoutMillis,
    long callbackTimeoutMillis) {

  StepWithPolicy {
    if (stepTimeoutMillis < 0) {
      throw new IllegalArgumentException(
          "stepTimeoutMillis must be >= 0, got " + stepTimeoutMillis);
    }
    if (callbackTimeoutMillis < 0) {
      throw new IllegalArgumentException(
          "callbackTimeoutMillis must be >= 0, got " + callbackTimeoutMillis);
    }
  }
}
