package com.scalar.db.saga.exception;

import java.util.Objects;

/** Thrown when another writer modified the saga first (optimistic-concurrency conflict). */
public class SagaConcurrentModificationException extends SagaRuntimeException {

  private final String sagaId;

  public SagaConcurrentModificationException(String sagaId) {
    super(
        SagaErrorCode.SAGA_CONCURRENT_MODIFICATION,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
  }

  public SagaConcurrentModificationException(String sagaId, Throwable cause) {
    super(
        SagaErrorCode.SAGA_CONCURRENT_MODIFICATION,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.sagaId = sagaId;
  }

  public String getSagaId() {
    return sagaId;
  }
}
