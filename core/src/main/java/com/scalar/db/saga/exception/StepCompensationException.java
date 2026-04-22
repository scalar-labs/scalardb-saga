package com.scalar.db.saga.exception;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when a step's compensation ({@code compensate} or {@code cancel}) fails.
 *
 * <p>Compensation failures are never retryable inline — the saga is escalated and periodic recovery
 * retries compensation later.
 */
public class StepCompensationException extends RuntimeException {

  private final @Nullable String stepName;
  private final int stepIndex;

  public StepCompensationException(String message) {
    super(message);
    this.stepName = null;
    this.stepIndex = -1;
  }

  public StepCompensationException(Throwable cause) {
    super(cause);
    this.stepName = null;
    this.stepIndex = -1;
  }

  public StepCompensationException(String stepName, int stepIndex, Throwable cause) {
    super("Compensation failed for step '" + stepName + "' at index " + stepIndex, cause);
    this.stepName = stepName;
    this.stepIndex = stepIndex;
  }

  public @Nullable String getStepName() {
    return stepName;
  }

  public int getStepIndex() {
    return stepIndex;
  }
}
