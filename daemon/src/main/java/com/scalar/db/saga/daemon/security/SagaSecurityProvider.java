package com.scalar.db.saga.daemon.security;

/**
 * The daemon's pluggable authentication SPI: resolves a request's credential to an authenticated
 * {@link SagaIdentity}, or rejects it.
 *
 * <p>A provider does <b>authentication only</b> — it verifies the credential and maps it to a
 * principal plus a role set. <b>Authorization</b> (does that role satisfy the endpoint?) is
 * enforced uniformly by the RBAC layer against {@link SagaIdentity#hasRole}, so it is the same for
 * every provider and every transport.
 *
 * <p>Built-in providers: {@link NoopSecurityProvider} (default — authenticates every request as a
 * full-access identity; for trusted/isolated networks and local dev), {@link JwtSecurityProvider}
 * (a JWT validated against a JWKS), and {@link ApiKeySecurityProvider} (pre-shared keys). Custom
 * providers (mTLS, a secret manager) are SPI follow-ons. A single provider instance authenticates
 * both the REST and the gRPC transport.
 *
 * <p>Implementations must be <b>thread-safe</b> — one instance authenticates concurrent requests.
 */
public interface SagaSecurityProvider extends AutoCloseable {

  /**
   * Authenticates a request, returning the caller's identity.
   *
   * @param request the credential-bearing request to authenticate
   * @return the authenticated caller's identity (never {@code null})
   * @throws SagaAuthenticationException if the credential is missing, malformed, expired, or cannot
   *     be verified
   */
  SagaIdentity authenticate(SagaAuthRequest request);

  /**
   * Returns a short, stable name for this provider (e.g. {@code "noop"}, {@code "jwt"}, {@code
   * "apikey"}) for logging and diagnostics.
   */
  String name();

  /**
   * Releases any resources the provider holds (e.g. a JWKS-refresh HTTP client). The default is a
   * no-op for providers that hold none. Called once when the server shuts down.
   */
  @Override
  default void close() {}
}
