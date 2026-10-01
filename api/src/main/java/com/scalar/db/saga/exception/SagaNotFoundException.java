package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/** Thrown when looking up a saga instance that does not exist. */
public class SagaNotFoundException extends SagaRuntimeException {

  private final String sagaId;

  public SagaNotFoundException(String sagaId) {
    super(
        SagaErrorCode.SAGA_NOT_FOUND,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
  }

  /** Reconstructs the exception from a wire-received metadata map. */
  static SagaNotFoundException fromWire(Map<String, String> metadata) {
    return new SagaNotFoundException(
        Objects.requireNonNull(metadata.get("saga_id"), "sagaId must not be null"));
  }

  public String getSagaId() {
    return sagaId;
  }
}
