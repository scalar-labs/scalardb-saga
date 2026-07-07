package com.scalar.db.saga.daemon.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTProcessor;
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
 * <p>The token signature is verified with a signing key fetched from the configured JWKS endpoint
 * (cached, with key-rotation refresh), and the {@code iss} / {@code exp} (and optional {@code aud})
 * claims are validated. Only <b>asymmetric</b> signature algorithms are accepted (RSA and EC
 * families) — never {@code none} or an HMAC algorithm — so a token cannot be forged by algorithm
 * confusion against a JWKS public key. The caller's principal and roles are read from configured
 * claims (see {@link JwtConfig}); a claim value matching a {@link SagaRole} wire name grants that
 * role, and any other value is ignored.
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
   * Visible for testing: builds a provider around an already-constructed {@link JWTProcessor}, so a
   * test can drive it with an in-memory JWKS (no network). The processor owns validation; this
   * class only extracts the principal and roles.
   */
  JwtSecurityProvider(
      JWTProcessor<SecurityContext> processor, String principalClaim, String rolesClaim) {
    this.processor = processor;
    this.principalClaim = principalClaim;
    this.rolesClaim = rolesClaim;
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
    // The default JWKS source caches keys and refreshes on rotation without a background thread, so
    // the provider holds no resource to release on close.
    JWKSource<SecurityContext> jwkSource =
        JWKSourceBuilder.create(
                config.jwksUrl(),
                new DefaultResourceRetriever(
                    config.connectTimeoutMillis(), config.readTimeoutMillis()))
            .build();
    JWTProcessor<SecurityContext> processor =
        buildProcessor(jwkSource, config.issuer(), config.audience(), config.principalClaim());
    return new JwtSecurityProvider(processor, config.principalClaim(), config.rolesClaim());
  }

  /**
   * Builds the {@link JWTProcessor}: verifies the signature against {@code jwkSource} using only
   * the allowed asymmetric algorithms, and requires a matching {@code iss}, a present {@code exp},
   * the principal claim, and (when set) a matching {@code aud}. Package-private so a test can
   * supply an in-memory JWKS source and exercise the same verification.
   */
  static JWTProcessor<SecurityContext> buildProcessor(
      JWKSource<SecurityContext> jwkSource,
      String issuer,
      @Nullable String audience,
      String principalClaim) {
    DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
    processor.setJWSKeySelector(new JWSVerificationKeySelector<>(ALLOWED_ALGORITHMS, jwkSource));
    JWTClaimsSet exactMatch = new JWTClaimsSet.Builder().issuer(issuer).build();
    // Require the principal claim and an expiry; iss presence + value are enforced by exactMatch.
    Set<String> requiredClaims = Set.of(principalClaim, "exp");
    DefaultJWTClaimsVerifier<SecurityContext> verifier =
        audience == null
            ? new DefaultJWTClaimsVerifier<>(exactMatch, requiredClaims)
            : new DefaultJWTClaimsVerifier<>(audience, exactMatch, requiredClaims);
    processor.setJWTClaimsSetVerifier(verifier);
    return processor;
  }

  @Override
  public SagaIdentity authenticate(SagaAuthRequest request) {
    String token = bearerToken(request);
    JWTClaimsSet claims;
    try {
      claims = processor.process(token, null);
    } catch (ParseException | BadJOSEException | JOSEException e) {
      // Covers a malformed token, a bad signature, an unknown key, a wrong issuer/audience, and an
      // expired token — all "the credential could not be verified" → 401.
      throw new SagaAuthenticationException("JWT validation failed", e);
    }
    return SagaIdentity.of(principal(claims), roles(claims));
  }

  @Override
  public String name() {
    return "jwt";
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
      tokens.addAll(splitWhitespace(s));
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

  /** Splits on runs of whitespace, dropping empty tokens (without {@code String.split}). */
  private static List<String> splitWhitespace(String value) {
    List<String> tokens = new ArrayList<>();
    int start = -1;
    for (int i = 0; i < value.length(); i++) {
      if (Character.isWhitespace(value.charAt(i))) {
        if (start >= 0) {
          tokens.add(value.substring(start, i));
          start = -1;
        }
      } else if (start < 0) {
        start = i;
      }
    }
    if (start >= 0) {
      tokens.add(value.substring(start));
    }
    return tokens;
  }
}
