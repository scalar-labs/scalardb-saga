package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/** Thrown when another writer modified the saga first (optimistic-concurrency conflict). */
public class SagaConcurrentModificationException extends SagaRuntimeException {

  /** The saga another writer modified first. */
  private final String sagaId;

  /**
   * Another writer modified saga {@code sagaId} before this one could. Carries {@link
   * SagaErrorCode#SAGA_CONCURRENT_MODIFICATION} with the ID in its metadata.
   *
   * @param sagaId the saga whose update was lost
   */
  public SagaConcurrentModificationException(String sagaId) {
    super(
        SagaErrorCode.SAGA_CONCURRENT_MODIFICATION,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
  }

  /**
   * As {@link #SagaConcurrentModificationException(String)}, with the store's conflict as the
   * cause.
   *
   * @param sagaId the saga whose update was lost
   * @param cause the store's report of the conflict
   */
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

  /**
   * The saga another writer modified first.
   *
   * @return the saga ID, never {@code null}
   */
  public String getSagaId() {
    return sagaId;
  }
}
