package com.scalar.db.saga.server.security;

/**
 * Thrown when an authenticated caller lacks the {@link SagaRole} an endpoint requires — the caller
 * is known but <b>not authorized</b>. Maps to HTTP {@code 403 Forbidden} (and gRPC {@code
 * PERMISSION_DENIED}).
 *
 * <p>Distinct from {@link SagaAuthenticationException} ({@code 401}), which is a request that could
 * not be authenticated at all. Carries the required role and the caller's principal for the audit
 * log; the client receives a generic {@code 403} without those details.
 */
public final class SagaAuthorizationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String principal;
  private final SagaRole requiredRole;

  public SagaAuthorizationException(String principal, SagaRole requiredRole) {
    super("Caller '" + principal + "' lacks the required role " + requiredRole.wireName());
    this.principal = principal;
    this.requiredRole = requiredRole;
  }

  /** Returns the principal of the caller that was denied (for audit). */
  public String getPrincipal() {
    return principal;
  }

  /** Returns the role the endpoint required. */
  public SagaRole getRequiredRole() {
    return requiredRole;
  }
}
