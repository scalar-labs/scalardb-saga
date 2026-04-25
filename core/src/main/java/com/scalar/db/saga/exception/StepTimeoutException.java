package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a step's forward action times out.
 *
 * <p>Step timeouts are always non-retryable — the engine begins compensation immediately.
 */
public class StepTimeoutException extends StepExecutionException {

  public StepTimeoutException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"), false);
  }

  public StepTimeoutException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"),
        false);
  }
}
