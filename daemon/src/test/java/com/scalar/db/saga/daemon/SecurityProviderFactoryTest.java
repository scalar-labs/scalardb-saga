package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.daemon.security.NoopSecurityProvider;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    properties.setProperty("scalar.db.saga.server.security.jwt.audience", "saga-daemon");

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

  @Test
  void create_apikeyWithReferenceKey_returnsApiKeyProvider(@TempDir Path dir) throws Exception {
    // Arrange — key supplied as a file secret reference that actually resolves
    Path keyFile = dir.resolve("svc.key");
    Files.writeString(keyFile, "s3cr3t", StandardCharsets.UTF_8);
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");
    properties.setProperty(
        "scalar.db.saga.server.security.apikey.key.svc.secret", "${file:UTF-8:" + keyFile + "}");
    properties.setProperty("scalar.db.saga.server.security.apikey.key.svc.roles", "saga:write");

    // Act
    SagaSecurityProvider provider =
        SecurityProviderFactory.create(SagaServerConfig.load(properties));

    // Assert
    assertThat(provider.name()).isEqualTo("apikey");
  }

  @Test
  void create_apikeyWithInlineKey_throwsException() {
    // Arrange — an inline (non-reference) key must be rejected end-to-end through config loading
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");
    properties.setProperty(
        "scalar.db.saga.server.security.apikey.key.svc.secret", "inline-plaintext-key");
    properties.setProperty("scalar.db.saga.server.security.apikey.key.svc.roles", "saga:read");

    // Act / Assert
    assertThatThrownBy(() -> SecurityProviderFactory.create(SagaServerConfig.load(properties)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void create_apikeyWithNoKeys_throwsException() {
    // Arrange — 'apikey' selected but no keys configured
    Properties properties = new Properties();
    properties.setProperty(SagaServerConfig.SECURITY_PROVIDER_KEY, "apikey");

    // Act / Assert
    assertThatThrownBy(() -> SecurityProviderFactory.create(SagaServerConfig.load(properties)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
