package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when a value the caller passed fails validation — a malformed page token, a blank reason,
 * an out-of-range timestamp. Carries {@link SagaErrorCode#INVALID_ARGUMENT} with the specifics in
 * the {@code detail} metadata.
 *
 * <p>Both implementations throw this: the embedded engine validates directly, and a remote client
 * reconstructs it from the wire code, so a caller handles a rejected argument identically in either
 * mode. Contrast {@link SagaInvalidRequestException}, which is the wire <i>message</i> failing
 * validation and therefore remote-only.
 *
 * <p>It deliberately extends {@link SagaRuntimeException} rather than {@link
 * IllegalArgumentException}, so that {@code catch (SagaRuntimeException)} covers every saga failure
 * including this one and {@link #getErrorCode()} is always reachable. The trade-off is that {@code
 * catch (IllegalArgumentException)} does not catch it; the name describes the condition, not a
 * superclass.
 */
public class SagaIllegalArgumentException extends SagaRuntimeException {

  public SagaIllegalArgumentException(String detail) {
    super(
        SagaErrorCode.INVALID_ARGUMENT,
        ErrorMetadata.of("detail", Objects.requireNonNull(detail, "detail must not be null")));
  }

  public SagaIllegalArgumentException(String detail, Throwable cause) {
    super(
        SagaErrorCode.INVALID_ARGUMENT,
        ErrorMetadata.of("detail", Objects.requireNonNull(detail, "detail must not be null")),
        Objects.requireNonNull(cause, "cause must not be null"));
  }

  /** Reconstructs the exception from a wire-received metadata map. */
  static SagaIllegalArgumentException fromWire(Map<String, String> metadata) {
    return new SagaIllegalArgumentException(
        Objects.requireNonNull(metadata.get("detail"), "detail must not be null"));
  }
}
