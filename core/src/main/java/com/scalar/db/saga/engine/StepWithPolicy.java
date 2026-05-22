package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.Step;

/**
 * Bundles a {@link Step} with its resolved retry policies and per-step timeout.
 *
 * <p>Internal record — not part of the public API.
 */
record StepWithPolicy(
    Step step,
    RetryPolicy executionRetryPolicy,
    RetryPolicy compensationRetryPolicy,
    long stepTimeoutMillis) {

  StepWithPolicy {
    if (stepTimeoutMillis < 0) {
      throw new IllegalArgumentException(
          "stepTimeoutMillis must be >= 0, got " + stepTimeoutMillis);
    }
  }
}
