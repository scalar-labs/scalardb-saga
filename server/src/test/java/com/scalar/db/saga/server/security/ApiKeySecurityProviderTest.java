package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ApiKeySecurityProviderTest {

  /** Registers one key: raw secret is a reference; {@code secretValue} is the resolved key. */
  private static void addKey(
      Properties resolved,
      Properties raw,
      String name,
      String secretValue,
      String roles,
      String principal) {
    String secretKey = ApiKeyConfig.KEY_PREFIX + name + ApiKeyConfig.SECRET_SUFFIX;
    resolved.setProperty(secretKey, secretValue);
    raw.setProperty(secretKey, "${env:" + name.toUpperCase(java.util.Locale.ROOT) + "}");
    resolved.setProperty(ApiKeyConfig.KEY_PREFIX + name + ApiKeyConfig.ROLES_SUFFIX, roles);
    resolved.setProperty(ApiKeyConfig.KEY_PREFIX + name + ApiKeyConfig.PRINCIPAL_SUFFIX, principal);
  }

  private static ApiKeySecurityProvider twoKeyProvider() {
    Properties resolved = new Properties();
    Properties raw = new Properties();
    addKey(resolved, raw, "alice", "alice-secret", "saga:read", "alice-svc");
    addKey(resolved, raw, "bob", "bob-secret", "saga:write", "bob-svc");
    return ApiKeySecurityProvider.create(resolved, raw);
  }

  private static SagaAuthRequest withHeader(String headerName, String value) {
    return SagaAuthRequest.fromHeaders("POST /sagas", null, Map.of(headerName, value));
  }

  @Test
  void name_returnsApikey() {
    assertThat(twoKeyProvider().name()).isEqualTo("apikey");
  }

  @Test
  void authenticate_validKey_returnsMappedPrincipalAndRoles() {
    // Act
    SagaIdentity identity = twoKeyProvider().authenticate(withHeader("X-API-Key", "alice-secret"));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice-svc");
    assertThat(identity.roles()).containsExactly(SagaRole.READ);
  }

  @Test
  void authenticate_differentKeys_resolveToDifferentIdentities() {
    // Arrange
    ApiKeySecurityProvider provider = twoKeyProvider();

    // Act / Assert — each key maps to its own principal + roles
    assertThat(provider.authenticate(withHeader("X-API-Key", "bob-secret")).principal())
        .isEqualTo("bob-svc");
    assertThat(provider.authenticate(withHeader("X-API-Key", "bob-secret")).roles())
        .containsExactly(SagaRole.WRITE);
  }

  @Test
  void authenticate_headerLookupIsCaseInsensitive() {
    // Act — the client sends a lower-cased header name
    SagaIdentity identity = twoKeyProvider().authenticate(withHeader("x-api-key", "alice-secret"));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice-svc");
  }

  @Test
  void authenticate_unknownKey_throwsAuthenticationException() {
    // Act / Assert
    assertThatThrownBy(() -> twoKeyProvider().authenticate(withHeader("X-API-Key", "nope")))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_missingHeader_throwsAuthenticationException() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("POST /sagas", null, Map.of());

    // Act / Assert
    assertThatThrownBy(() -> twoKeyProvider().authenticate(request))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_blankHeaderValue_throwsAuthenticationException() {
    // Act / Assert
    assertThatThrownBy(() -> twoKeyProvider().authenticate(withHeader("X-API-Key", "   ")))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_customHeader_isUsed() {
    // Arrange
    Properties resolved = new Properties();
    Properties raw = new Properties();
    addKey(resolved, raw, "svc", "the-key", "saga:admin", "svc");
    resolved.setProperty(ApiKeyConfig.HEADER_KEY, "X-Saga-Key");
    ApiKeySecurityProvider provider = ApiKeySecurityProvider.create(resolved, raw);

    // Act
    SagaIdentity identity = provider.authenticate(withHeader("X-Saga-Key", "the-key"));

    // Assert — and the default header no longer authenticates
    assertThat(identity.roles()).containsExactly(SagaRole.ADMIN);
    assertThatThrownBy(() -> provider.authenticate(withHeader("X-API-Key", "the-key")))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_multiRoleKey_grantsAllConfiguredRoles() {
    // Arrange
    Properties resolved = new Properties();
    Properties raw = new Properties();
    addKey(resolved, raw, "ops", "ops-key", "saga:read,saga:admin", "ops");
    ApiKeySecurityProvider provider = ApiKeySecurityProvider.create(resolved, raw);

    // Act
    SagaIdentity identity = provider.authenticate(withHeader("X-API-Key", "ops-key"));

    // Assert
    assertThat(identity.roles()).containsExactlyInAnyOrder(SagaRole.READ, SagaRole.ADMIN);
    assertThat(identity.hasRole(SagaRole.WRITE)).isTrue(); // ADMIN implies WRITE
  }
}
