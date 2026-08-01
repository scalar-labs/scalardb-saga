package com.scalar.db.saga.server.security;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by a {@link SagaSecurityProvider} when a credential cannot be verified for a reason that
 * is <b>not</b> the caller's fault — most importantly when the identity provider is unreachable
 * (e.g. a JWT provider cannot fetch the JWKS). This is a transient upstream outage, not a bad
 * credential, so it maps to HTTP {@code 503 Service Unavailable} (and gRPC {@code UNAVAILABLE}) and
 * is safe for the caller to retry.
 *
 * <p>Distinct from {@link SagaAuthenticationException} ({@code 401}), which is a genuinely
 * missing/malformed/rejected credential the caller must fix. The message is operator-facing
 * (logged); the client receives a generic {@code 503} without echoing it.
 */
public final class SagaAuthUnavailableException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public SagaAuthUnavailableException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
