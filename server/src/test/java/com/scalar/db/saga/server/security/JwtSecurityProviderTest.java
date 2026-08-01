package com.scalar.db.saga.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.JWTProcessor;
import java.io.Closeable;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link JwtSecurityProvider} through its real {@link JWTProcessor} against an in-memory
 * JWKS (an RSA key pair generated per test), exercising signature, issuer, audience, expiry, and
 * token-type validation plus principal and role extraction without any network access.
 */
class JwtSecurityProviderTest {

  private static final String ISSUER = "https://issuer.example";
  private static final String AUDIENCE = "saga-daemon";
  private static final String KEY_ID = "test-key";

  private RSAKey signingKey;
  private RSAKey rogueKey;
  private JwtSecurityProvider provider;

  @BeforeEach
  void setUp() throws JOSEException {
    signingKey = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();
    rogueKey = new RSAKeyGenerator(2048).keyID(KEY_ID).generate();
    JWKSource<SecurityContext> jwkSource =
        new ImmutableJWKSet<>(new JWKSet(signingKey.toPublicJWK()));
    JWTProcessor<SecurityContext> processor =
        JwtSecurityProvider.buildProcessor(jwkSource, ISSUER, AUDIENCE, null, "sub");
    provider = new JwtSecurityProvider(processor, "sub", "scope");
  }

  @Test
  void name_returnsJwt() {
    assertThat(provider.name()).isEqualTo("jwt");
  }

  @Test
  void close_releasesJwksSource() throws Exception {
    // Arrange — a provider that adopts a recording closeable JWKS source
    AtomicBoolean closed = new AtomicBoolean(false);
    Closeable jwkSource = () -> closed.set(true);
    JWKSource<SecurityContext> keys = new ImmutableJWKSet<>(new JWKSet(signingKey.toPublicJWK()));
    JWTProcessor<SecurityContext> processor =
        JwtSecurityProvider.buildProcessor(keys, ISSUER, AUDIENCE, null, "sub");
    JwtSecurityProvider closeableProvider =
        new JwtSecurityProvider(processor, "sub", "scope", jwkSource);

    // Act
    closeableProvider.close();

    // Assert
    assertThat(closed).isTrue();
  }

  @Test
  void close_withNoOwnedResource_isNoop() {
    // The in-memory test seam holds no closeable resource; close() must not throw.
    provider.close();
  }

  @Test
  void authenticate_validTokenWithScopes_returnsPrincipalAndRoles() throws JOSEException {
    // Arrange — a valid token carrying two saga scopes
    String token =
        sign(baseClaims("alice").claim("scope", "saga:read saga:write").build(), signingKey);

    // Act
    SagaIdentity identity = provider.authenticate(bearer(token));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
    assertThat(identity.roles()).containsExactlyInAnyOrder(SagaRole.READ, SagaRole.WRITE);
  }

  @Test
  void authenticate_unrecognizedScopes_areIgnored() throws JOSEException {
    // Arrange — a non-saga scope alongside a saga one
    String token =
        sign(baseClaims("alice").claim("scope", "openid profile saga:read").build(), signingKey);

    // Act
    SagaIdentity identity = provider.authenticate(bearer(token));

    // Assert — only the recognized saga role is granted
    assertThat(identity.roles()).containsExactly(SagaRole.READ);
  }

  @Test
  void authenticate_noRolesClaim_returnsIdentityWithNoRoles() throws JOSEException {
    // Arrange — a valid token with no scope claim
    String token = sign(baseClaims("alice").build(), signingKey);

    // Act
    SagaIdentity identity = provider.authenticate(bearer(token));

    // Assert — authenticated, but holds no role (every gated endpoint would 403)
    assertThat(identity.principal()).isEqualTo("alice");
    assertThat(identity.roles()).isEmpty();
  }

  @Test
  void authenticate_rolesClaimAsArray_isSupported() throws JOSEException {
    // Arrange — a provider reading a 'roles' array claim
    JWKSource<SecurityContext> jwkSource =
        new ImmutableJWKSet<>(new JWKSet(signingKey.toPublicJWK()));
    JwtSecurityProvider arrayProvider =
        new JwtSecurityProvider(
            JwtSecurityProvider.buildProcessor(jwkSource, ISSUER, AUDIENCE, null, "sub"),
            "sub",
            "roles");
    String token =
        sign(
            baseClaims("root").claim("roles", List.of("saga:admin", "unrelated")).build(),
            signingKey);

    // Act
    SagaIdentity identity = arrayProvider.authenticate(bearer(token));

    // Assert
    assertThat(identity.roles()).containsExactly(SagaRole.ADMIN);
  }

  @Test
  void authenticate_missingAuthorizationHeader_throwsAuthenticationException() {
    // Arrange
    SagaAuthRequest request = SagaAuthRequest.fromHeaders("GET /sagas/x", null, Map.of());

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(request))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_nonBearerScheme_throwsAuthenticationException() {
    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(rawAuth("Basic dXNlcjpwYXNz")))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_expiredToken_throwsAuthenticationException() throws JOSEException {
    // Arrange — exp in the past
    String token =
        sign(
            baseClaims("alice").expirationTime(Date.from(Instant.now().minusSeconds(60))).build(),
            signingKey);

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_wrongIssuer_throwsAuthenticationException() throws JOSEException {
    // Arrange
    String token = sign(baseClaims("alice").issuer("https://evil.example").build(), signingKey);

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_wrongAudience_throwsAuthenticationException() throws JOSEException {
    // Arrange
    String token = sign(baseClaims("alice").audience("other-service").build(), signingKey);

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_signatureFromUnknownKey_throwsAuthenticationException() throws JOSEException {
    // Arrange — signed with a key whose public half is not in the JWKS (kid still points at the
    // trusted key, so the signature check fails)
    String token = sign(baseClaims("alice").build(), rogueKey);

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_hmacForgedWithPublicKeyBytes_throwsAuthenticationException()
      throws JOSEException {
    // Arrange — the RS256→HS256 algorithm-confusion attack: forge an HS256 token using the trusted
    // RSA public key's encoded bytes as the HMAC secret. A verifier that accepted HMAC would
    // recompute the same MAC from the published public key and wrongly trust the token; the
    // provider allows only asymmetric algorithms, so no verification key is selected.
    byte[] publicKeyBytes = signingKey.toRSAPublicKey().getEncoded();
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(KEY_ID).build(),
            baseClaims("alice").build());
    jwt.sign(new MACSigner(publicKeyBytes));

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(jwt.serialize())))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_unsignedNoneAlgToken_throwsAuthenticationException() {
    // Arrange — an unsecured (alg:none) token with otherwise-valid claims
    String token = new PlainJWT(baseClaims("alice").build()).serialize();

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_missingPrincipalClaim_throwsAuthenticationException() throws JOSEException {
    // Arrange — a token with no 'sub' (the required principal claim)
    String token =
        sign(
            new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .build(),
            signingKey);

    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_malformedToken_throwsAuthenticationException() {
    // Act / Assert
    assertThatThrownBy(() -> provider.authenticate(bearer("not.a.jwt")))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_requiredTokenTypeMatched_returnsIdentity() throws JOSEException {
    // Arrange — provider requires typ=at+jwt; token carries exactly that
    JwtSecurityProvider typedProvider = providerRequiringType("at+jwt");
    String token =
        signWithType(baseClaims("alice").build(), signingKey, new JOSEObjectType("at+jwt"));

    // Act
    SagaIdentity identity = typedProvider.authenticate(bearer(token));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
  }

  @Test
  void authenticate_requiredTokenTypeAsApplicationMediaType_returnsIdentity() throws JOSEException {
    // Arrange — RFC 7519 lets the "application/" prefix be present; at+jwt config must accept it
    JwtSecurityProvider typedProvider = providerRequiringType("at+jwt");
    String token =
        signWithType(
            baseClaims("alice").build(), signingKey, new JOSEObjectType("application/at+jwt"));

    // Act
    SagaIdentity identity = typedProvider.authenticate(bearer(token));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
  }

  @Test
  void authenticate_wrongTokenType_throwsAuthenticationException() throws JOSEException {
    // Arrange — provider requires at+jwt; token is a plain JWT (e.g. an ID token) typ
    JwtSecurityProvider typedProvider = providerRequiringType("at+jwt");
    String token = signWithType(baseClaims("alice").build(), signingKey, new JOSEObjectType("JWT"));

    // Act / Assert
    assertThatThrownBy(() -> typedProvider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_missingTokenType_throwsAuthenticationException() throws JOSEException {
    // Arrange — provider requires at+jwt; token has no typ header at all
    JwtSecurityProvider typedProvider = providerRequiringType("at+jwt");
    String token = signWithType(baseClaims("alice").build(), signingKey, null);

    // Act / Assert
    assertThatThrownBy(() -> typedProvider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthenticationException.class);
  }

  @Test
  void authenticate_noTokenTypeConfigured_ignoresTypHeader() throws JOSEException {
    // Arrange — default provider (no token_type); a token with an unrelated typ still passes
    String token = signWithType(baseClaims("alice").build(), signingKey, new JOSEObjectType("JWT"));

    // Act
    SagaIdentity identity = provider.authenticate(bearer(token));

    // Assert
    assertThat(identity.principal()).isEqualTo("alice");
  }

  @Test
  void authenticate_jwksUnreachable_throwsAuthUnavailableException() throws JOSEException {
    // Arrange — a JWKS source that fails to produce keys, standing in for an unreachable provider
    // (nimbus throws a KeySourceException, a JOSEException, from key selection)
    JWKSource<SecurityContext> failingSource =
        (selector, context) -> {
          throw new KeySourceException("JWKS endpoint unreachable");
        };
    JwtSecurityProvider unavailableProvider =
        new JwtSecurityProvider(
            JwtSecurityProvider.buildProcessor(failingSource, ISSUER, AUDIENCE, null, "sub"),
            "sub",
            "scope");
    String token = sign(baseClaims("alice").build(), signingKey);

    // Act / Assert — a provider outage is a retryable upstream failure, not a bad credential
    assertThatThrownBy(() -> unavailableProvider.authenticate(bearer(token)))
        .isInstanceOf(SagaAuthUnavailableException.class);
  }

  private static JWTClaimsSet.Builder baseClaims(String subject) {
    return new JWTClaimsSet.Builder()
        .subject(subject)
        .issuer(ISSUER)
        .audience(AUDIENCE)
        .expirationTime(Date.from(Instant.now().plusSeconds(300)));
  }

  private static String sign(JWTClaimsSet claims, RSAKey key) throws JOSEException {
    return signWithType(claims, key, null);
  }

  private static String signWithType(JWTClaimsSet claims, RSAKey key, @Nullable JOSEObjectType type)
      throws JOSEException {
    JWSHeader.Builder header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY_ID);
    if (type != null) {
      header.type(type);
    }
    SignedJWT jwt = new SignedJWT(header.build(), claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private JwtSecurityProvider providerRequiringType(String tokenType) {
    JWKSource<SecurityContext> jwkSource =
        new ImmutableJWKSet<>(new JWKSet(signingKey.toPublicJWK()));
    return new JwtSecurityProvider(
        JwtSecurityProvider.buildProcessor(jwkSource, ISSUER, AUDIENCE, tokenType, "sub"),
        "sub",
        "scope");
  }

  private static SagaAuthRequest bearer(String token) {
    return rawAuth("Bearer " + token);
  }

  private static SagaAuthRequest rawAuth(@Nullable String authorizationHeader) {
    Map<String, String> headers =
        authorizationHeader == null ? Map.of() : Map.of("Authorization", authorizationHeader);
    return SagaAuthRequest.fromHeaders("GET /sagas/x", null, headers);
  }
}
