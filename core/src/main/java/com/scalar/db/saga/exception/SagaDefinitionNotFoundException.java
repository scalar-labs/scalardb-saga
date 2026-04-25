package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when looking up a saga definition by name that has not been registered. */
public class SagaDefinitionNotFoundException extends RuntimeException {

  private final String sagaName;

  public SagaDefinitionNotFoundException(String sagaName) {
    super(
        "No saga definition registered for: "
            + Objects.requireNonNull(sagaName, "sagaName must not be null"));
    this.sagaName = sagaName;
  }

  public String getSagaName() {
    return sagaName;
  }
}
