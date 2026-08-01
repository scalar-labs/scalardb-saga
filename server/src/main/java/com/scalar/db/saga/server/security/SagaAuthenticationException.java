package com.scalar.db.saga.server.security;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by a {@link SagaSecurityProvider} when a request's credential is missing, malformed,
 * expired, or otherwise cannot be verified — the caller is <b>not authenticated</b>. Maps to HTTP
 * {@code 401 Unauthorized} (and gRPC {@code UNAUTHENTICATED}).
 *
 * <p>Distinct from {@link SagaAuthorizationException}, which is a <em>known</em> caller lacking the
 * required role ({@code 403}). The message is operator-facing (logged); the client receives a
 * generic {@code 401} without echoing it, so a probing caller learns nothing about why a credential
 * was rejected.
 */
public final class SagaAuthenticationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public SagaAuthenticationException(String message) {
    super(message);
  }

  public SagaAuthenticationException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
