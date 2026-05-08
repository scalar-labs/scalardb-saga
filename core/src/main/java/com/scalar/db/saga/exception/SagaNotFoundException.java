package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when looking up a saga instance that does not exist. */
public class SagaNotFoundException extends RuntimeException {

  private final String sagaId;

  public SagaNotFoundException(String sagaId) {
    super("Saga not found: " + Objects.requireNonNull(sagaId, "sagaId must not be null"));
    this.sagaId = sagaId;
  }

  public String getSagaId() {
    return sagaId;
  }
}
