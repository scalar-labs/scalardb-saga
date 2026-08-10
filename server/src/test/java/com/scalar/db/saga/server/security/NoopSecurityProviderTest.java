package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class NoopSecurityProviderTest {

  private final NoopSecurityProvider provider = new NoopSecurityProvider();

  @Test
  void name_returnsNoop() {
    // Assert
    assertThat(provider.name()).isEqualTo("noop");
  }

  @Test
  void authenticate_anyRequest_returnsFullAccessIdentity() {
    // Arrange — a request with no credential at all
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("GET /sagas/x", null, Map.of());

    // Act
    SagaIdentity identity = provider.authenticate(request);

    // Assert — every role is granted (noop authenticates everyone as full admin)
    assertThat(identity.hasRole(SagaRole.READ)).isTrue();
    assertThat(identity.hasRole(SagaRole.WRITE)).isTrue();
    assertThat(identity.hasRole(SagaRole.ADMIN)).isTrue();
  }

  @Test
  void authenticate_returnsStablePrincipal() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("POST /sagas", null, Map.of());

    // Act
    SagaIdentity identity = provider.authenticate(request);

    // Assert
    assertThat(identity.principal()).isEqualTo("anonymous");
  }
}
