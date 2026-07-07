package com.scalar.db.saga.daemon;

import com.scalar.db.saga.daemon.security.NoopSecurityProvider;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import java.util.Objects;

/**
 * Builds the configured {@link SagaSecurityProvider} from a {@link SagaServerConfig}, selected by
 * {@link SagaServerConfig#securityProvider()}.
 *
 * <p>Currently supports only {@code noop} (the default — no authentication). The JWT provider (PR
 * C2) and the pre-shared-API-key provider (PR C3) register their own {@code case} branches here as
 * they land; an unrecognized name fails startup with a clear message rather than silently falling
 * back to no authentication.
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
    Objects.requireNonNull(config, "config must not be null");
    String name = config.securityProvider();
    return switch (name) {
      case "noop" -> new NoopSecurityProvider();
      default ->
          throw new IllegalArgumentException(
              "Unknown security provider '"
                  + name
                  + "' for '"
                  + SagaServerConfig.SECURITY_PROVIDER_KEY
                  + "'. Supported: noop.");
    };
  }
}
