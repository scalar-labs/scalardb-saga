package com.scalar.db.saga.exception;

/** Thrown when a saga definition is invalid (e.g., duplicate step names, missing pivot step). */
public class SagaDefinitionException extends RuntimeException {

  public SagaDefinitionException(String message) {
    super(message);
  }

  public SagaDefinitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
