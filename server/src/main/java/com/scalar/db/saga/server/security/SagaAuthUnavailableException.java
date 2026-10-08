package com.scalar.db.saga.server.security;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import java.util.Objects;

/**
 * Thrown by a {@link SagaSecurityProvider} when a credential cannot be verified for a reason that
 * is <b>not</b> the caller's fault — most importantly when the identity provider is unreachable
 * (e.g. a JWT provider cannot fetch the JWKS). Extends {@link SagaRuntimeException} carrying {@link
 * SagaErrorCode#SERVICE_UNAVAILABLE}; a transient upstream outage, so it maps to HTTP {@code 503
 * Service Unavailable} (and gRPC {@code UNAVAILABLE}) and is safe for the caller to retry.
 *
 * <p>Distinct from {@link SagaAuthenticationException} ({@code 401}), which is a genuinely
 * missing/malformed/rejected credential the caller must fix. The specific reason (which provider,
 * what upstream error) is operator-facing (via {@link #getInternalDetail()}, logged server-side);
 * the client receives a generic {@code 503} without echoing it.
 */
public final class SagaAuthUnavailableException extends SagaRuntimeException {

  private static final long serialVersionUID = 1L;

  /** The operator-facing reason, logged server-side and never sent on the wire. */
  private final String internalDetail;

  /**
   * Creates the exception a provider throws when it could not reach what it needs to verify the
   * credential.
   *
   * @param internalDetail which provider failed and why, for the server log only
   * @param cause the upstream failure, such as the failed JWKS fetch
   */
  public SagaAuthUnavailableException(String internalDetail, Throwable cause) {
    super(
        SagaErrorCode.SERVICE_UNAVAILABLE,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
  }

  /**
   * The server-side-only reason (never sent on the wire); used by the daemon's log statement.
   *
   * @return the operator-facing reason
   */
  public String getInternalDetail() {
    return internalDetail;
  }
}
