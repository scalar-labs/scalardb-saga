package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a step's forward action times out.
 *
 * <p>Step timeouts are always non-retryable — the engine begins compensation immediately.
 */
public class StepTimeoutException extends StepExecutionException {

  /**
   * The step's forward action did not finish within its timeout. Never retryable.
   *
   * @param message what timed out, and after how long
   */
  public StepTimeoutException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"), false);
  }

  /**
   * As {@link #StepTimeoutException(String)}, with the failure that ended the wait as the cause.
   *
   * @param message what timed out, and after how long
   * @param cause the failure that ended the wait
   */
  public StepTimeoutException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"),
        false);
  }
}
