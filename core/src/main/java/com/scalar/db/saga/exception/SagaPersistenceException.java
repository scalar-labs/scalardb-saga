package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when the saga store layer encounters a failure (e.g., database write error). */
public class SagaPersistenceException extends RuntimeException {

  public SagaPersistenceException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
