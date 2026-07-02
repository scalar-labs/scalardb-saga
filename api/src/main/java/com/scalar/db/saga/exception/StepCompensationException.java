package com.scalar.db.saga.exception;

import java.util.Objects;
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
    super(Objects.requireNonNull(message, "message must not be null"));
    this.stepName = null;
    this.stepIndex = -1;
  }

  public StepCompensationException(Throwable cause) {
    super(Objects.requireNonNull(cause, "cause must not be null"));
    this.stepName = null;
    this.stepIndex = -1;
  }

  public StepCompensationException(String stepName, int stepIndex, Throwable cause) {
    super(
        "Compensation failed for step '"
            + Objects.requireNonNull(stepName, "stepName must not be null")
            + "' at index "
            + validateStepIndex(stepIndex),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.stepName = stepName;
    this.stepIndex = stepIndex;
  }

  private static int validateStepIndex(int stepIndex) {
    if (stepIndex < 0) {
      throw new IllegalArgumentException("stepIndex must not be negative: " + stepIndex);
    }
    return stepIndex;
  }

  public @Nullable String getStepName() {
    return stepName;
  }

  public int getStepIndex() {
    return stepIndex;
  }
}
