package com.scalar.db.saga.daemon;

import com.scalar.db.saga.daemon.security.ApiKeySecurityProvider;
import com.scalar.db.saga.daemon.security.JwtSecurityProvider;
import com.scalar.db.saga.daemon.security.NoopSecurityProvider;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;

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
      default ->
          throw new IllegalArgumentException(
              "Unknown security provider '"
                  + name
                  + "' for '"
                  + SagaServerConfig.SECURITY_PROVIDER_KEY
                  + "'. Supported: noop, jwt, apikey.");
    };
  }
}
