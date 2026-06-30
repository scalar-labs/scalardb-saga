package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when a saga definition is invalid (e.g., duplicate step names, missing pivot step). */
public class SagaDefinitionException extends SagaRuntimeException {

  public SagaDefinitionException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }

  public SagaDefinitionException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
