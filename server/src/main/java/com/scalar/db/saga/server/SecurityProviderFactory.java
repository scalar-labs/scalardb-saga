package com.scalar.db.saga.server;

import com.scalar.db.saga.server.security.ApiKeySecurityProvider;
import com.scalar.db.saga.server.security.JwtSecurityProvider;
import com.scalar.db.saga.server.security.NoopSecurityProvider;
import com.scalar.db.saga.server.security.SagaSecurityProvider;

/**
 * Builds the configured {@link SagaSecurityProvider} from a {@link SagaServerConfig}, selected by
 * {@link SagaServerConfig#securityProvider()}.
 *
 * <p>Supports {@code noop} (the default — no authentication), {@code jwt} (Bearer-JWT validation
 * against a remote JWKS), and {@code apikey} (pre-shared keys, for deployments without an IdP). An
 * unrecognized name fails startup with a clear message rather than silently falling back to no
 * authentication.
 */
final class SecurityProviderFactory {

  private SecurityProviderFactory() {}

  /**
   * Validates the configured provider's settings without building it, so {@code --validate-config}
   * covers authentication rather than leaving the one startup check an operator most wants covered
   * to the first boot.
   *
   * <p>Deliberately not {@code create(config).close()}: building the JWT provider starts a JWKS
   * refresh executor, and this command allocates nothing and calls nothing. The cases are the same
   * as {@link #create}'s and sit beside them so a new provider cannot be added to one and forgotten
   * in the other.
   *
   * @param config the server configuration
   * @throws IllegalArgumentException if the provider name or its settings are not valid
   */
  static void validate(SagaServerConfig config) {
    String name = config.securityProvider();
    switch (name) {
      case "noop" -> {}
      case "jwt" -> JwtSecurityProvider.validate(config.properties());
      case "apikey" -> ApiKeySecurityProvider.validate(config.properties(), config.rawProperties());
      default -> throw unknownProvider(name);
    }
  }

  /**
   * Creates the provider named by {@code config.securityProvider()}.
   *
   * @param config the server configuration
   * @return the security provider
   * @throws IllegalArgumentException if the configured provider name is not recognized
   */
  static SagaSecurityProvider create(SagaServerConfig config) {
    String name = config.securityProvider();
    return switch (name) {
      case "noop" -> new NoopSecurityProvider();
      case "jwt" -> JwtSecurityProvider.create(config.properties());
      case "apikey" -> ApiKeySecurityProvider.create(config.properties(), config.rawProperties());
      default -> throw unknownProvider(name);
    };
  }

  private static IllegalArgumentException unknownProvider(String name) {
    return new IllegalArgumentException(
        "Unknown security provider "
            + Redaction.redacted(name)
            + " for '"
            + SagaServerConfig.SECURITY_PROVIDER_KEY
            + "'. Supported: noop, jwt, apikey.");
  }
}
