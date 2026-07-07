package com.scalar.db.saga.daemon.security;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for {@link JwtSecurityProvider}, parsed from {@code
 * scalar.db.saga.server.security.jwt.*} properties.
 *
 * <ul>
 *   <li>{@code jwks_url} (required) — the JWKS endpoint whose signing keys validate token
 *       signatures. Fetched and cached, with key-rotation refresh handled by the JWKS source.
 *   <li>{@code issuer} (required) — the expected {@code iss}; a token from any other issuer is
 *       rejected.
 *   <li>{@code audience} (optional) — the expected {@code aud}; when set, a token must carry it.
 *       Unset accepts any audience.
 *   <li>{@code principal_claim} (default {@code sub}) — the claim read as the caller's principal.
 *   <li>{@code roles_claim} (default {@code scope}) — the claim carrying the caller's roles (a
 *       space-delimited string like an OAuth2 {@code scope}, or a string array). Values matching a
 *       {@link SagaRole} wire name ({@code saga:read}/{@code saga:write}/{@code saga:admin}) grant
 *       that role; others are ignored.
 *   <li>{@code connect_timeout_millis} / {@code read_timeout_millis} (default {@code 2000} each) —
 *       HTTP timeouts for fetching the JWKS.
 * </ul>
 *
 * <p>Values are secret-reference-resolvable like any {@code scalar.db.saga.*} key (e.g. {@code
 * jwks_url} via {@code ${env:...}}), since the daemon resolves this namespace before parsing.
 */
final class JwtConfig {

  static final String PREFIX = "scalar.db.saga.server.security.jwt.";
  static final String JWKS_URL_KEY = PREFIX + "jwks_url";
  static final String ISSUER_KEY = PREFIX + "issuer";
  static final String AUDIENCE_KEY = PREFIX + "audience";
  static final String PRINCIPAL_CLAIM_KEY = PREFIX + "principal_claim";
  static final String ROLES_CLAIM_KEY = PREFIX + "roles_claim";
  static final String CONNECT_TIMEOUT_MILLIS_KEY = PREFIX + "connect_timeout_millis";
  static final String READ_TIMEOUT_MILLIS_KEY = PREFIX + "read_timeout_millis";
  static final String DEFAULT_PRINCIPAL_CLAIM = "sub";
  static final String DEFAULT_ROLES_CLAIM = "scope";
  static final int DEFAULT_TIMEOUT_MILLIS = 2000;

  private final URL jwksUrl;
  private final String issuer;
  private final @Nullable String audience;
  private final String principalClaim;
  private final String rolesClaim;
  private final int connectTimeoutMillis;
  private final int readTimeoutMillis;

  private JwtConfig(
      URL jwksUrl,
      String issuer,
      @Nullable String audience,
      String principalClaim,
      String rolesClaim,
      int connectTimeoutMillis,
      int readTimeoutMillis) {
    this.jwksUrl = jwksUrl;
    this.issuer = issuer;
    this.audience = audience;
    this.principalClaim = principalClaim;
    this.rolesClaim = rolesClaim;
    this.connectTimeoutMillis = connectTimeoutMillis;
    this.readTimeoutMillis = readTimeoutMillis;
  }

  /**
   * Parses the JWT configuration from the (already secret-resolved) server properties.
   *
   * @param properties the server properties
   * @return the parsed configuration
   * @throws IllegalArgumentException if a required key is missing/blank, the JWKS URL is malformed,
   *     or a timeout is not a positive integer
   */
  static JwtConfig from(Properties properties) {
    Objects.requireNonNull(properties, "properties must not be null");
    URL jwksUrl = parseUrl(required(properties, JWKS_URL_KEY));
    String issuer = required(properties, ISSUER_KEY);
    String audience = blankToNull(properties.getProperty(AUDIENCE_KEY));
    String principalClaim =
        valueOrDefault(properties, PRINCIPAL_CLAIM_KEY, DEFAULT_PRINCIPAL_CLAIM);
    String rolesClaim = valueOrDefault(properties, ROLES_CLAIM_KEY, DEFAULT_ROLES_CLAIM);
    int connectTimeout = parsePositiveInt(properties, CONNECT_TIMEOUT_MILLIS_KEY);
    int readTimeout = parsePositiveInt(properties, READ_TIMEOUT_MILLIS_KEY);
    return new JwtConfig(
        jwksUrl, issuer, audience, principalClaim, rolesClaim, connectTimeout, readTimeout);
  }

  URL jwksUrl() {
    return jwksUrl;
  }

  String issuer() {
    return issuer;
  }

  @Nullable String audience() {
    return audience;
  }

  String principalClaim() {
    return principalClaim;
  }

  String rolesClaim() {
    return rolesClaim;
  }

  int connectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  int readTimeoutMillis() {
    return readTimeoutMillis;
  }

  private static String required(Properties properties, String key) {
    String value = blankToNull(properties.getProperty(key));
    if (value == null) {
      throw new IllegalArgumentException(
          "'" + key + "' is required when the JWT security provider is selected");
    }
    return value;
  }

  private static String valueOrDefault(Properties properties, String key, String defaultValue) {
    String value = blankToNull(properties.getProperty(key));
    return value == null ? defaultValue : value;
  }

  private static int parsePositiveInt(Properties properties, String key) {
    String value = blankToNull(properties.getProperty(key));
    if (value == null) {
      return DEFAULT_TIMEOUT_MILLIS;
    }
    int parsed;
    try {
      parsed = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for '" + key + "': " + value, e);
    }
    if (parsed <= 0) {
      throw new IllegalArgumentException("'" + key + "' must be a positive integer, got " + parsed);
    }
    return parsed;
  }

  private static URL parseUrl(String value) {
    try {
      return new URI(value).toURL();
    } catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value for '" + JWKS_URL_KEY + "': " + value, e);
    }
  }

  private static @Nullable String blankToNull(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
