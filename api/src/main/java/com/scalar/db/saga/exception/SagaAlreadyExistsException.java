package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.Objects;

/** Thrown when a caller-supplied saga ID collides with an existing saga. */
public class SagaAlreadyExistsException extends SagaRuntimeException {

  /** The ID that collided. */
  private final String sagaId;

  /** The state of the saga that already holds the ID. */
  private final SagaStateSnapshot existing;

  /**
   * The caller-supplied {@code sagaId} is already taken. Carries {@link
   * SagaErrorCode#SAGA_ALREADY_EXISTS} with the ID in its metadata.
   *
   * @param sagaId the ID that collided
   * @param existing the current state of the saga that already holds it
   */
  public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing) {
    super(
        SagaErrorCode.SAGA_ALREADY_EXISTS,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
    this.existing = Objects.requireNonNull(existing, "existing must not be null");
  }

  /**
   * As {@link #SagaAlreadyExistsException(String, SagaStateSnapshot)}, with the store's rejection
   * as the cause.
   *
   * @param sagaId the ID that collided
   * @param existing the current state of the saga that already holds it
   * @param cause the store's rejection of the duplicate
   */
  public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing, Throwable cause) {
    super(
        SagaErrorCode.SAGA_ALREADY_EXISTS,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.sagaId = sagaId;
    this.existing = Objects.requireNonNull(existing, "existing must not be null");
  }

  /**
   * The ID that collided.
   *
   * @return the saga ID, never {@code null}
   */
  public String getSagaId() {
    return sagaId;
  }

  /**
   * The saga that already holds the ID, as it stood when the collision was detected.
   *
   * @return its state snapshot, never {@code null}
   */
  public SagaStateSnapshot getExisting() {
    return existing;
  }
}
