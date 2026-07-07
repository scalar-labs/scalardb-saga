package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.daemon.security.NoopSecurityProvider;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class SecurityProviderFactoryTest {

  @Test
  void create_defaultConfig_returnsNoopProvider() {
    // Arrange — no provider configured: defaults to noop
    SagaServerConfig config = SagaServerConfig.load(new Properties());

    // Act
    SagaSecurityProvider provider = SecurityProviderFactory.create(config);

    // Assert
    assertThat(provider).isInstanceOf(NoopSecurityProvider.class);
    assertThat(provider.name()).isEqualTo("noop");
  }

  @Test
  void create_noopConfiguredExplicitly_returnsNoopProvider() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "NOOP");

    // Act
    SagaSecurityProvider provider =
        SecurityProviderFactory.create(SagaServerConfig.load(properties));

    // Assert — case-insensitive selection
    assertThat(provider).isInstanceOf(NoopSecurityProvider.class);
  }

  @Test
  void create_unknownProviderGiven_throwsException() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "mystery");

    // Act / Assert
    assertThatThrownBy(() -> SecurityProviderFactory.create(SagaServerConfig.load(properties)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void create_jwtWithValidConfig_returnsJwtProvider() {
    // Arrange — JWKSourceBuilder does not fetch until first use, so no network is touched here
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "jwt");
    properties.setProperty(
        "scalar.db.saga.server.security.jwt.jwks_url", "https://issuer.example/jwks.json");
    properties.setProperty("scalar.db.saga.server.security.jwt.issuer", "https://issuer.example");

    // Act
    SagaSecurityProvider provider =
        SecurityProviderFactory.create(SagaServerConfig.load(properties));

    // Assert
    assertThat(provider.name()).isEqualTo("jwt");
  }

  @Test
  void create_jwtWithMissingConfig_throwsException() {
    // Arrange — 'jwt' selected but no jwks_url/issuer configured
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "jwt");

    // Act / Assert
    assertThatThrownBy(() -> SecurityProviderFactory.create(SagaServerConfig.load(properties)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
