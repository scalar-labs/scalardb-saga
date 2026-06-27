package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when another replica is already processing a saga (optimistic locking conflict). */
public class SagaConcurrentModificationException extends RuntimeException {

  private final String sagaId;

  public SagaConcurrentModificationException(String sagaId) {
    super(
        "Saga is being processed by another replica: "
            + Objects.requireNonNull(sagaId, "sagaId must not be null"));
    this.sagaId = sagaId;
  }

  public SagaConcurrentModificationException(String sagaId, Throwable cause) {
    super(
        "Saga is being processed by another replica: "
            + Objects.requireNonNull(sagaId, "sagaId must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.sagaId = sagaId;
  }

  public String getSagaId() {
    return sagaId;
  }
}
