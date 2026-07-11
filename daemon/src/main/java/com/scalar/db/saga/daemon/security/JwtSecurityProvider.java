package com.scalar.db.saga.daemon.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTProcessor;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * A {@link SagaSecurityProvider} that authenticates a {@code Bearer} JWT against a remote JWKS.
 *
 * <p>The daemon is an OAuth 2.0 resource server: it expects an OAuth 2.0 JWT <b>access token</b>
 * (RFC 9068), not an OIDC ID token. The token signature is verified with a signing key fetched from
 * the configured JWKS endpoint (cached, with key-rotation refresh), and the {@code iss}, {@code
 * exp}, and {@code aud} claims are validated (plus the {@code typ} header when a token type is
 * configured). Only <b>asymmetric</b> signature algorithms are accepted (RSA and EC families) —
 * never {@code none} or an HMAC algorithm — so a token cannot be forged by algorithm confusion
 * against a JWKS public key. The caller's principal and roles are read from configured claims (see
 * {@link JwtConfig}); a claim value matching a {@link SagaRole} wire name grants that role, and any
 * other value is ignored.
 *
 * <p>Thread-safe: the underlying {@link JWTProcessor} and JWKS source are safe for concurrent use.
 */
public final class JwtSecurityProvider implements SagaSecurityProvider {

  private static final String BEARER_PREFIX = "Bearer ";

  /**
   * The accepted JWS signature algorithms: the RSA ({@code RS*}/{@code PS*}) and EC ({@code ES*})
   * families. Symmetric ({@code HS*}) and {@code none} are deliberately excluded — a JWKS publishes
   * public keys, so accepting an HMAC algorithm would let an attacker sign a token with the public
   * key as the shared secret (algorithm-confusion attack).
   */
  private static final Set<JWSAlgorithm> ALLOWED_ALGORITHMS =
      Set.of(
          JWSAlgorithm.RS256,
          JWSAlgorithm.RS384,
          JWSAlgorithm.RS512,
          JWSAlgorithm.PS256,
          JWSAlgorithm.PS384,
          JWSAlgorithm.PS512,
          JWSAlgorithm.ES256,
          JWSAlgorithm.ES384,
          JWSAlgorithm.ES512);

  private final JWTProcessor<SecurityContext> processor;
  private final String principalClaim;
  private final String rolesClaim;

  /**
   * The JWKS source's closeable handle, released by {@link #close()}. The nimbus default source
   * caches keys and refreshes ahead of expiry on a dedicated executor thread; closing it cancels
   * that refresh and shuts the executor down. Null on the in-memory test seam, which owns no such
   * resource.
   */
  private final @Nullable Closeable jwkSource;

  /**
   * Visible for testing: builds a provider around an already-constructed {@link JWTProcessor}, so a
   * test can drive it with an in-memory JWKS (no network). The processor owns validation; this
   * class only extracts the principal and roles. Holds no closeable JWKS resource.
   */
  JwtSecurityProvider(
      JWTProcessor<SecurityContext> processor, String principalClaim, String rolesClaim) {
    this(processor, principalClaim, rolesClaim, null);
  }

  /**
   * Visible for testing: as above, but adopts a closeable JWKS source so a test can assert {@link
   * #close()} releases it.
   */
  JwtSecurityProvider(
      JWTProcessor<SecurityContext> processor,
      String principalClaim,
      String rolesClaim,
      @Nullable Closeable jwkSource) {
    this.processor = processor;
    this.principalClaim = principalClaim;
    this.rolesClaim = rolesClaim;
    this.jwkSource = jwkSource;
  }

  /**
   * Builds a provider from {@code scalar.db.saga.server.security.jwt.*} properties, validating
   * tokens against the configured remote JWKS.
   *
   * @param properties the (secret-resolved) server properties
   * @return the provider
   * @throws IllegalArgumentException if the JWT configuration is missing/invalid (see {@link
   *     JwtConfig})
   */
  public static JwtSecurityProvider create(Properties properties) {
    return create(JwtConfig.from(properties));
  }

  private static JwtSecurityProvider create(JwtConfig config) {
    // The default JWKS source caches keys and refreshes ahead of expiry on a dedicated executor
    // thread; that executor is released by close().
    JWKSource<SecurityContext> jwkSource =
        JWKSourceBuilder.create(
                config.jwksUrl(),
                new DefaultResourceRetriever(
                    config.connectTimeoutMillis(), config.readTimeoutMillis()))
            .build();
    JWTProcessor<SecurityContext> processor =
        buildProcessor(
            jwkSource,
            config.issuer(),
            config.audience(),
            config.tokenType(),
            config.principalClaim());
    return new JwtSecurityProvider(
        processor,
        config.principalClaim(),
        config.rolesClaim(),
        jwkSource instanceof Closeable closeable ? closeable : null);
  }

  /**
   * Builds the {@link JWTProcessor}: verifies the signature against {@code jwkSource} using only
   * the allowed asymmetric algorithms, and requires a matching {@code iss} and {@code aud}, a
   * present {@code exp}, and the principal claim. When {@code tokenType} is set, the JWS {@code
   * typ} header must match it too. Package-private so a test can supply an in-memory JWKS source
   * and exercise the same verification.
   */
  static JWTProcessor<SecurityContext> buildProcessor(
      JWKSource<SecurityContext> jwkSource,
      String issuer,
      String audience,
      @Nullable String tokenType,
      String principalClaim) {
    DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
    processor.setJWSKeySelector(new JWSVerificationKeySelector<>(ALLOWED_ALGORITHMS, jwkSource));
    if (tokenType != null) {
      processor.setJWSTypeVerifier(typeVerifier(tokenType));
    }
    JWTClaimsSet exactMatch = new JWTClaimsSet.Builder().issuer(issuer).build();
    // Require the principal claim and an expiry; iss presence + value are enforced by exactMatch.
    Set<String> requiredClaims = Set.of(principalClaim, "exp");
    processor.setJWTClaimsSetVerifier(
        new DefaultJWTClaimsVerifier<>(audience, exactMatch, requiredClaims));
    return processor;
  }

  /**
   * Builds a {@code typ}-header verifier for {@code tokenType}. RFC 7519 §5.1 lets the {@code
   * application/} media-type prefix be omitted, so both {@code <type>} and {@code
   * application/<type>} are accepted (e.g. {@code at+jwt} and {@code application/at+jwt} for an RFC
   * 9068 access token). Nimbus compares the value case-insensitively.
   */
  private static DefaultJOSEObjectTypeVerifier<SecurityContext> typeVerifier(String tokenType) {
    String prefix = "application/";
    String bare =
        tokenType.regionMatches(true, 0, prefix, 0, prefix.length())
            ? tokenType.substring(prefix.length())
            : tokenType;
    return new DefaultJOSEObjectTypeVerifier<>(
        new JOSEObjectType(bare), new JOSEObjectType(prefix + bare));
  }

  @Override
  public SagaIdentity authenticate(SagaAuthRequest request) {
    String token = bearerToken(request);
    JWTClaimsSet claims;
    try {
      claims = processor.process(token, null);
    } catch (ParseException | BadJOSEException e) {
      // The credential itself is bad: malformed, a bad signature, expired, a wrong issuer or
      // audience, an unknown key, or (when a token type is configured) a wrong typ. The caller must
      // fix it, so this is a 401.
      throw new SagaAuthenticationException("JWT validation failed", e);
    } catch (JOSEException e) {
      // Verification could not be completed for a reason that is not the caller's credential, most
      // importantly a JWKS fetch failure when the provider is unreachable (RemoteKeySourceException
      // extends JOSEException). That is a transient upstream outage, so it is a retryable 503.
      throw new SagaAuthUnavailableException("JWT provider unavailable", e);
    }
    return SagaIdentity.of(principal(claims), roles(claims));
  }

  @Override
  public String name() {
    return "jwt";
  }

  @Override
  public void close() {
    if (jwkSource == null) {
      return;
    }
    try {
      // Cascades to RefreshAheadCachingJWKSetSource.close(): cancels the scheduled refresh and
      // shuts down its executor. SagaServer logs (does not propagate) any failure from here.
      jwkSource.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close the JWKS source", e);
    }
  }

  private static String bearerToken(SagaAuthRequest request) {
    String header =
        request
            .header("Authorization")
            .orElseThrow(() -> new SagaAuthenticationException("missing Authorization header"));
    if (header.length() <= BEARER_PREFIX.length()
        || !header.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
      throw new SagaAuthenticationException("Authorization header is not a Bearer token");
    }
    String token = header.substring(BEARER_PREFIX.length()).trim();
    if (token.isEmpty()) {
      throw new SagaAuthenticationException("empty bearer token");
    }
    return token;
  }

  private String principal(JWTClaimsSet claims) {
    String value;
    try {
      value = claims.getStringClaim(principalClaim);
    } catch (ParseException e) {
      throw new SagaAuthenticationException(
          "principal claim '" + principalClaim + "' is not a string", e);
    }
    if (value == null || value.isBlank()) {
      throw new SagaAuthenticationException(
          "token has no '" + principalClaim + "' principal claim");
    }
    return value;
  }

  /**
   * Reads the roles claim and maps its values to {@link SagaRole}s. The claim may be a
   * whitespace-delimited string (an OAuth2 {@code scope}) or a string array/list; a value matching
   * a role wire name ({@code saga:read}/…) grants that role, others are ignored. An absent claim
   * yields no roles (the caller is authenticated but unauthorized for every gated endpoint → 403).
   */
  private Set<SagaRole> roles(JWTClaimsSet claims) {
    Object claim = claims.getClaim(rolesClaim);
    if (claim == null) {
      return Set.of();
    }
    List<String> tokens = new ArrayList<>();
    if (claim instanceof String s) {
      tokens.addAll(TextSplitter.split(s, Character::isWhitespace, false));
    } else if (claim instanceof Collection<?> collection) {
      for (Object element : collection) {
        if (element != null) {
          tokens.add(element.toString());
        }
      }
    }
    Set<SagaRole> roles = EnumSet.noneOf(SagaRole.class);
    for (String token : tokens) {
      SagaRole.fromWireName(token).ifPresent(roles::add);
    }
    return roles;
  }
}
