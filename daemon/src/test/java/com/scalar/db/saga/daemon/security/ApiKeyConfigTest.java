package com.scalar.db.saga.daemon.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ApiKeyConfigTest {

  /** Builds a resolved/raw pair for a single key {@code name}. */
  private static Properties[] keyProps(
      String name,
      String rawSecret,
      String resolvedSecret,
      @org.jspecify.annotations.Nullable String roles) {
    Properties resolved = new Properties();
    Properties raw = new Properties();
    String secretKey = ApiKeyConfig.KEY_PREFIX + name + ApiKeyConfig.SECRET_SUFFIX;
    resolved.setProperty(secretKey, resolvedSecret);
    raw.setProperty(secretKey, rawSecret);
    if (roles != null) {
      resolved.setProperty(ApiKeyConfig.KEY_PREFIX + name + ApiKeyConfig.ROLES_SUFFIX, roles);
    }
    return new Properties[] {resolved, raw};
  }

  @Test
  void from_singleKey_parsesRolesAndDefaultsHeaderAndPrincipal() {
    // Arrange
    Properties[] p = keyProps("svc", "${env:SVC_KEY}", "s3cr3t", "saga:read,saga:write");

    // Act
    ApiKeyConfig config = ApiKeyConfig.from(p[0], p[1]);

    // Assert
    assertThat(config.header()).isEqualTo(ApiKeyConfig.DEFAULT_HEADER);
    assertThat(config.definitions()).hasSize(1);
    ApiKeyConfig.Definition def = config.definitions().get(0);
    assertThat(def.principal()).isEqualTo("svc"); // defaults to the key name
    assertThat(def.secret()).isEqualTo("s3cr3t");
    assertThat(def.roles()).containsExactlyInAnyOrder(SagaRole.READ, SagaRole.WRITE);
  }

  @Test
  void from_customHeaderAndPrincipal_areParsed() {
    // Arrange
    Properties[] p = keyProps("svc", "${env:SVC_KEY}", "s3cr3t", "saga:admin");
    p[0].setProperty(ApiKeyConfig.HEADER_KEY, "X-Saga-Key");
    p[0].setProperty(
        ApiKeyConfig.KEY_PREFIX + "svc" + ApiKeyConfig.PRINCIPAL_SUFFIX, "svc-account");

    // Act
    ApiKeyConfig config = ApiKeyConfig.from(p[0], p[1]);

    // Assert
    assertThat(config.header()).isEqualTo("X-Saga-Key");
    assertThat(config.definitions().get(0).principal()).isEqualTo("svc-account");
  }

  @Test
  void from_rolesWithWhitespace_areTrimmed() {
    // Arrange
    Properties[] p = keyProps("svc", "${env:K}", "s3cr3t", " saga:read , saga:admin ");

    // Act
    ApiKeyConfig config = ApiKeyConfig.from(p[0], p[1]);

    // Assert
    assertThat(config.definitions().get(0).roles())
        .containsExactlyInAnyOrder(SagaRole.READ, SagaRole.ADMIN);
  }

  @Test
  void from_multipleKeys_areAllParsed() {
    // Arrange
    Properties[] a = keyProps("alice", "${env:A}", "aaa", "saga:read");
    Properties[] b = keyProps("bob", "${env:B}", "bbb", "saga:write");
    a[0].putAll(b[0]);
    a[1].putAll(b[1]);

    // Act
    ApiKeyConfig config = ApiKeyConfig.from(a[0], a[1]);

    // Assert
    assertThat(config.definitions()).hasSize(2);
    assertThat(config.definitions())
        .extracting(ApiKeyConfig.Definition::principal)
        .containsExactlyInAnyOrder("alice", "bob");
  }

  @Test
  void from_inlineSecret_throwsException() {
    // Arrange — raw secret is an inline literal, not a ${...} reference
    Properties[] p = keyProps("svc", "plaintext-key", "plaintext-key", "saga:read");

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_partiallyInlineSecret_throwsException() {
    // Arrange — a reference embedded in a literal is still not a pure reference
    Properties[] p = keyProps("svc", "prefix-${env:K}", "prefix-resolved", "saga:read");

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_unresolvedReference_throwsException() {
    // Arrange — the reference passed through unchanged (e.g. an undefined ${env:...} the resolver
    // left verbatim): resolved == raw
    Properties[] p = keyProps("svc", "${env:MISSING}", "${env:MISSING}", "saga:read");

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_blankResolvedSecret_throwsException() {
    // Arrange — reference is well-formed but resolves to empty (e.g. an empty secret file)
    Properties[] p = keyProps("svc", "${file:UTF-8:/run/secrets/svc}", "", "saga:read");

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_unknownRole_throwsException() {
    // Arrange
    Properties[] p = keyProps("svc", "${env:K}", "s3cr3t", "saga:superuser");

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_missingRoles_throwsException() {
    // Arrange — no roles key at all
    Properties[] p = keyProps("svc", "${env:K}", "s3cr3t", null);

    // Act / Assert
    assertThatThrownBy(() -> ApiKeyConfig.from(p[0], p[1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_noKeys_throwsException() {
    // Act / Assert — provider selected but nothing configured
    assertThatThrownBy(() -> ApiKeyConfig.from(new Properties(), new Properties()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
