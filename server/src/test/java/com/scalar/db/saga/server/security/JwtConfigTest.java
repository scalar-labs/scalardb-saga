package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class JwtConfigTest {

  private static Properties minimalProps() {
    Properties props = new Properties();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "https://issuer.example/.well-known/jwks.json");
    props.setProperty(JwtConfig.ISSUER_KEY, "https://issuer.example");
    props.setProperty(JwtConfig.AUDIENCE_KEY, "saga-daemon");
    return props;
  }

  @Test
  void from_minimalProps_appliesDefaults() {
    // Act
    JwtConfig config = JwtConfig.from(minimalProps());

    // Assert
    assertThat(config.jwksUrl().toString())
        .isEqualTo("https://issuer.example/.well-known/jwks.json");
    assertThat(config.issuer()).isEqualTo("https://issuer.example");
    assertThat(config.audience()).isEqualTo("saga-daemon");
    assertThat(config.tokenType()).isNull();
    assertThat(config.principalClaim()).isEqualTo(JwtConfig.DEFAULT_PRINCIPAL_CLAIM);
    assertThat(config.rolesClaim()).isEqualTo(JwtConfig.DEFAULT_ROLES_CLAIM);
    assertThat(config.connectTimeoutMillis()).isEqualTo(JwtConfig.DEFAULT_TIMEOUT_MILLIS);
    assertThat(config.readTimeoutMillis()).isEqualTo(JwtConfig.DEFAULT_TIMEOUT_MILLIS);
  }

  @Test
  void from_allProps_areParsed() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.AUDIENCE_KEY, "saga-daemon");
    props.setProperty(JwtConfig.TOKEN_TYPE_KEY, "at+jwt");
    props.setProperty(JwtConfig.PRINCIPAL_CLAIM_KEY, "email");
    props.setProperty(JwtConfig.ROLES_CLAIM_KEY, "roles");
    props.setProperty(JwtConfig.CONNECT_TIMEOUT_MILLIS_KEY, "500");
    props.setProperty(JwtConfig.READ_TIMEOUT_MILLIS_KEY, "1500");

    // Act
    JwtConfig config = JwtConfig.from(props);

    // Assert
    assertThat(config.audience()).isEqualTo("saga-daemon");
    assertThat(config.tokenType()).isEqualTo("at+jwt");
    assertThat(config.principalClaim()).isEqualTo("email");
    assertThat(config.rolesClaim()).isEqualTo("roles");
    assertThat(config.connectTimeoutMillis()).isEqualTo(500);
    assertThat(config.readTimeoutMillis()).isEqualTo(1500);
  }

  @Test
  void from_missingJwksUrl_throwsException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(JwtConfig.ISSUER_KEY, "https://issuer.example");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_missingIssuer_throwsException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "https://issuer.example/jwks.json");
    props.setProperty(JwtConfig.AUDIENCE_KEY, "saga-daemon");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_missingAudience_throwsException() {
    // Arrange — audience is required so an ID token or a token for another service is rejected
    Properties props = new Properties();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "https://issuer.example/jwks.json");
    props.setProperty(JwtConfig.ISSUER_KEY, "https://issuer.example");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_blankAudience_throwsException() {
    // Arrange — a blank audience is treated as unset, and audience is required
    Properties props = minimalProps();
    props.setProperty(JwtConfig.AUDIENCE_KEY, "   ");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_blankTokenType_isTreatedAsUnset() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.TOKEN_TYPE_KEY, "   ");

    // Act
    JwtConfig config = JwtConfig.from(props);

    // Assert
    assertThat(config.tokenType()).isNull();
  }

  @Test
  void from_malformedJwksUrl_throwsException() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "not a url");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_plaintextHttpJwksUrl_throwsException() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "http://issuer.example/.well-known/jwks.json");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_nonHttpsJwksUrl_throwsException() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "file:///etc/jwks.json");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_plaintextHttpLoopbackJwksUrl_isAllowedForLocalDev() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "http://localhost:8080/jwks.json");

    // Act
    JwtConfig config = JwtConfig.from(props);

    // Assert
    assertThat(config.jwksUrl().toString()).isEqualTo("http://localhost:8080/jwks.json");
  }

  @Test
  void from_plaintextHttpLoopbackIpJwksUrl_isAllowedForLocalDev() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "http://127.0.0.1:8080/jwks.json");

    // Act
    JwtConfig config = JwtConfig.from(props);

    // Assert
    assertThat(config.jwksUrl().toString()).isEqualTo("http://127.0.0.1:8080/jwks.json");
  }

  @Test
  void from_plaintextHttpSpoofableLoopbackNameJwksUrl_throwsException() {
    // Arrange
    // "127.attacker.org" is a DNS hostname that merely starts with "127."; it is not loopback, so a
    // plaintext JWKS there must be rejected rather than trusted as the token signing anchor.
    Properties props = minimalProps();
    props.setProperty(JwtConfig.JWKS_URL_KEY, "http://127.attacker.org/jwks.json");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_nonNumericTimeout_throwsException() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.CONNECT_TIMEOUT_MILLIS_KEY, "soon");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void from_nonPositiveTimeout_throwsException() {
    // Arrange
    Properties props = minimalProps();
    props.setProperty(JwtConfig.READ_TIMEOUT_MILLIS_KEY, "0");

    // Act / Assert
    assertThatThrownBy(() -> JwtConfig.from(props)).isInstanceOf(IllegalArgumentException.class);
  }
}
