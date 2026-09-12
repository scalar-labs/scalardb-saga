package com.scalar.db.saga.server;

import com.scalar.db.saga.server.security.ApiKeySecurityProvider;
import com.scalar.db.saga.server.security.JwtSecurityProvider;
import com.scalar.db.saga.server.security.NoopSecurityProvider;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import java.util.Set;

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
   * <p>{@code unresolvedKeys} names the settings whose secret this machine could not read, so the
   * few rules that judge a resolved value can stand down for those alone. Every other rule — the
   * provider name, the JWKS scheme, a key given inline rather than as a reference — needs no
   * resolved value and runs regardless, which is what keeps an unreadable secret from taking the
   * whole check with it. Only the API-key provider is given the set: nothing under {@code
   * security.jwt.} is a secret (a JWKS URL, an issuer, an audience are all public identifiers), so
   * no JWT rule reads a value that could be standing in.
   *
   * @param config the server configuration
   * @param unresolvedKeys the settings whose secret this machine could not read
   * @throws IllegalArgumentException if the provider name or its settings are not valid
   */
  static void validate(SagaServerConfig config, Set<String> unresolvedKeys) {
    if (unresolvedKeys.contains(SagaServerConfig.SECURITY_PROVIDER_KEY)) {
      // The provider name is itself standing in for a value this machine could not read, so
      // neither it nor the settings belonging to whichever provider it names can be judged. The
      // caller already warns that the setting went unchecked; reporting the reference text as an
      // unknown provider would contradict that warning in the same report and refuse a
      // configuration that starts wherever the value can be read.
      return;
    }
    String name = config.securityProvider();
    switch (name) {
      case "noop" -> {}
      case "jwt" -> JwtSecurityProvider.validate(config.properties());
      case "apikey" ->
          ApiKeySecurityProvider.validate(
              config.properties(), config.rawProperties(), unresolvedKeys);
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
