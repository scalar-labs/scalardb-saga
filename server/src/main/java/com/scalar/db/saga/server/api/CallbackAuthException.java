package com.scalar.db.saga.server.api;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import java.util.Objects;

/**
 * Thrown when an async-callback request carries a missing or invalid HMAC token. Extends {@link
 * SagaRuntimeException} carrying {@link SagaErrorCode#UNAUTHENTICATED}; the wire response is
 * deliberately generic — the specific reason (missing vs invalid vs expired) is server-side only,
 * so a probing caller cannot use it as an oracle.
 *
 * <p>The reason lives in {@link #getInternalDetail()} for server-side logging only.
 */
public final class CallbackAuthException extends SagaRuntimeException {

  private static final long serialVersionUID = 1L;

  /** The server-side-only reason, never sent on the wire. */
  private final String internalDetail;

  /**
   * Creates the exception; the callback route throws it when the request's token or {@code iat} is
   * missing, malformed, invalid or expired.
   *
   * @param internalDetail which check failed, for the server-side log only
   */
  public CallbackAuthException(String internalDetail) {
    super(SagaErrorCode.UNAUTHENTICATED, ErrorMetadata.of());
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
  }

  /**
   * The server-side-only reason (never sent on the wire); used by the daemon's log statement.
   *
   * @return which check failed, never {@code null}
   */
  public String getInternalDetail() {
    return internalDetail;
  }
}
