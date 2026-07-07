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
}
