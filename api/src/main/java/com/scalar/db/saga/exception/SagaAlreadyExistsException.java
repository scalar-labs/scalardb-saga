package com.scalar.db.saga.exception;

import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.Objects;

/** Thrown when a caller-supplied saga ID collides with an existing saga. */
public class SagaAlreadyExistsException extends SagaRuntimeException {

  private final String sagaId;
  private final SagaStateSnapshot existing;

  public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing) {
    super(
        SagaErrorCode.SAGA_ALREADY_EXISTS,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
    this.existing = Objects.requireNonNull(existing, "existing must not be null");
  }

  public SagaAlreadyExistsException(String sagaId, SagaStateSnapshot existing, Throwable cause) {
    super(
        SagaErrorCode.SAGA_ALREADY_EXISTS,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.sagaId = sagaId;
    this.existing = Objects.requireNonNull(existing, "existing must not be null");
  }

  public String getSagaId() {
    return sagaId;
  }

  public SagaStateSnapshot getExisting() {
    return existing;
  }
}
