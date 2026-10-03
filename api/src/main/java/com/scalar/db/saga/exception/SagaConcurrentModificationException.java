package com.scalar.db.saga.exception;

import java.util.Map;
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

  /** Reconstructs the exception from a wire-received metadata map. */
  static SagaConcurrentModificationException fromWire(Map<String, String> metadata) {
    return new SagaConcurrentModificationException(
        Objects.requireNonNull(metadata.get("saga_id"), "sagaId must not be null"));
  }

  public String getSagaId() {
    return sagaId;
  }
}
