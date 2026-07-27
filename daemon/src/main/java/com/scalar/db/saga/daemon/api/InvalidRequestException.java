package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import java.util.Objects;

/**
 * Thrown when an incoming request is malformed or missing a required field. Extends {@link
 * SagaRuntimeException} carrying {@link SagaErrorCode#INVALID_REQUEST} with the daemon-authored
 * specifics in the {@code detail} metadata; the wire message includes the detail so the caller can
 * fix the offending field.
 */
public final class InvalidRequestException extends SagaRuntimeException {

  public InvalidRequestException(String detail) {
    super(
        SagaErrorCode.INVALID_REQUEST,
        ErrorMetadata.of("detail", Objects.requireNonNull(detail, "detail must not be null")));
  }
}
