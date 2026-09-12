package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when the request message itself fails validation at the daemon edge — a missing or
 * malformed field, an unparseable body, an unrecognized query parameter. Carries {@link
 * SagaErrorCode#INVALID_REQUEST} with the daemon-authored specifics in the {@code detail} metadata,
 * so the caller can fix the offending field.
 *
 * <p>Remote-only: the embedded engine has no request to validate, and a well-behaved client SDK
 * marshals its own requests, so in practice only a hand-rolled HTTP or gRPC caller reaches this. A
 * caller <i>value</i> the engine rejected is {@link SagaIllegalArgumentException} instead, which
 * both implementations throw.
 */
public class SagaInvalidRequestException extends SagaRuntimeException {

  public SagaInvalidRequestException(String detail) {
    super(
        SagaErrorCode.INVALID_REQUEST,
        ErrorMetadata.of("detail", Objects.requireNonNull(detail, "detail must not be null")));
  }

  /** Reconstructs the exception from a wire-received metadata map. */
  static SagaInvalidRequestException fromWire(Map<String, String> metadata) {
    return new SagaInvalidRequestException(
        Objects.requireNonNull(metadata.get("detail"), "detail must not be null"));
  }
}
