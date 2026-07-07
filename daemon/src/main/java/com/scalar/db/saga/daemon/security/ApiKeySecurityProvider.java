package com.scalar.db.saga.daemon.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * A {@link SagaSecurityProvider} that authenticates a pre-shared API key, for deployments without
 * an OIDC identity provider.
 *
 * <p>Each configured key maps to a principal (for audit) and a role set (for RBAC), so access
 * control works without an IdP. Keys are supplied only as secret references (never inline — see
 * {@link ApiKeyConfig}); each is stored as its SHA-256 digest and never kept in cleartext. A
 * request presents its key in the configured header (default {@code X-API-Key}); the presented key
 * is hashed and compared against the stored digests with a <b>constant-time</b> {@link
 * MessageDigest#isEqual}, so a mismatch leaks no timing information about the stored keys.
 *
 * <p>Immutable and thread-safe.
 */
public final class ApiKeySecurityProvider implements SagaSecurityProvider {

  private final String header;
  private final List<Entry> entries;

  private ApiKeySecurityProvider(String header, List<Entry> entries) {
    this.header = header;
    this.entries = entries;
  }

  /**
   * Builds a provider from {@code scalar.db.saga.server.security.apikey.*} properties.
   *
   * @param resolved the secret-resolved server properties
   * @param raw the pre-resolution server properties (used to reject inline keys)
   * @return the provider
   * @throws IllegalArgumentException if the API-key configuration is missing/invalid (see {@link
   *     ApiKeyConfig})
   */
  public static ApiKeySecurityProvider create(Properties resolved, Properties raw) {
    ApiKeyConfig config = ApiKeyConfig.from(resolved, raw);
    List<Entry> entries = new ArrayList<>();
    for (ApiKeyConfig.Definition definition : config.definitions()) {
      entries.add(
          new Entry(sha256(definition.secret()), definition.principal(), definition.roles()));
    }
    return new ApiKeySecurityProvider(config.header(), List.copyOf(entries));
  }

  @Override
  public SagaIdentity authenticate(SagaAuthRequest request) {
    String presented =
        request
            .header(header)
            .orElseThrow(() -> new SagaAuthenticationException("missing '" + header + "' header"));
    if (presented.isBlank()) {
      throw new SagaAuthenticationException("empty '" + header + "' header");
    }
    byte[] presentedHash = sha256(presented);
    // Compare against every configured key with a constant-time digest comparison, without breaking
    // on the first match, so neither the timing nor an early return reveals which key matched.
    Entry match = null;
    for (Entry entry : entries) {
      if (entry.matches(presentedHash)) {
        match = entry;
      }
    }
    if (match == null) {
      throw new SagaAuthenticationException("unrecognized API key");
    }
    return SagaIdentity.of(match.principal, match.roles);
  }

  @Override
  public String name() {
    return "apikey";
  }

  private static byte[] sha256(String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is a required algorithm on every JVM; its absence is not a recoverable condition.
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  /** One configured key, stored as its SHA-256 digest plus the principal and roles it grants. */
  private static final class Entry {
    private final byte[] keyHash;
    private final String principal;
    private final Set<SagaRole> roles;

    Entry(byte[] keyHash, String principal, Set<SagaRole> roles) {
      // Clone so the entry owns its digest (defensive; also keeps SpotBugs from flagging the stored
      // array as externally mutable).
      this.keyHash = keyHash.clone();
      this.principal = principal;
      this.roles = Set.copyOf(roles);
    }

    boolean matches(byte[] presentedHash) {
      return MessageDigest.isEqual(keyHash, presentedHash);
    }
  }
}
