package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the overall saga deadline expires (checked between steps).
 *
 * <p>This is an unchecked exception in a separate hierarchy from {@link StepTimeoutException}
 * because saga-level and step-level timeouts are semantically different.
 */
public class SagaTimeoutException extends RuntimeException {

  public SagaTimeoutException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }

  public SagaTimeoutException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
