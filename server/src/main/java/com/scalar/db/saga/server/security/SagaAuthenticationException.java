package com.scalar.db.saga.server.security;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import java.util.Objects;

/**
 * Thrown by a {@link SagaSecurityProvider} when a request's credential is missing, malformed,
 * expired, or otherwise cannot be verified — the caller is <b>not authenticated</b>. Extends {@link
 * SagaRuntimeException} carrying {@link SagaErrorCode#UNAUTHENTICATED}; maps to HTTP {@code 401
 * Unauthorized} (and gRPC {@code UNAUTHENTICATED}).
 *
 * <p>Distinct from {@link SagaAuthorizationException}, which is a <em>known</em> caller lacking the
 * required role ({@code 403}). The specific reason for the auth failure is operator-facing (via
 * {@link #getInternalDetail()}, logged server-side); the client receives a generic {@code 401}
 * without echoing it, so a probing caller learns nothing about why a credential was rejected.
 */
public final class SagaAuthenticationException extends SagaRuntimeException {

  private static final long serialVersionUID = 1L;

  private final String internalDetail;

  public SagaAuthenticationException(String internalDetail) {
    super(SagaErrorCode.UNAUTHENTICATED, ErrorMetadata.of());
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
  }

  public SagaAuthenticationException(String internalDetail, Throwable cause) {
    super(
        SagaErrorCode.UNAUTHENTICATED,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
  }

  /** The server-side-only reason (never sent on the wire); used by the daemon's log statement. */
  public String getInternalDetail() {
    return internalDetail;
  }
}
