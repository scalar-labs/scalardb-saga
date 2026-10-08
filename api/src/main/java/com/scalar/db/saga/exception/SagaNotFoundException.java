package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/** Thrown when looking up a saga instance that does not exist. */
public class SagaNotFoundException extends SagaRuntimeException {

  /** The ID that matched no saga. */
  private final String sagaId;

  /**
   * No saga instance exists with {@code sagaId}. Carries {@link SagaErrorCode#SAGA_NOT_FOUND} with
   * the ID in its metadata.
   *
   * @param sagaId the ID that matched no saga
   */
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

  /**
   * The ID that matched no saga.
   *
   * @return the saga ID, never {@code null}
   */
  public String getSagaId() {
    return sagaId;
  }
}
