package com.scalar.db.saga.server.security;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for {@link ApiKeySecurityProvider}, parsed from {@code
 * scalar.db.saga.server.security.apikey.*} properties.
 *
 * <ul>
 *   <li>{@code header} (default {@code X-API-Key}) — the request header the key is read from.
 *   <li>{@code key.<name>.secret} (required, one per key) — the pre-shared key, which <b>must</b>
 *       be a secret reference ({@code ${env:NAME}} / {@code ${file:UTF-8:/path}}), never an inline
 *       literal, so a key never sits in plaintext in the config file or image.
 *   <li>{@code key.<name>.roles} (required) — the roles the key grants, a comma-delimited list of
 *       wire names ({@code saga:read,saga:write}). An unknown role fails startup.
 *   <li>{@code key.<name>.principal} (optional, default {@code <name>}) — the principal recorded
 *       for audit when the key authenticates.
 * </ul>
 *
 * <p>{@code <name>} is a logical key id, local to the config; it is never sent by clients (they
 * send the secret value in the header).
 */
final class ApiKeyConfig {

  static final String PREFIX = "scalar.db.saga.server.security.apikey.";
  static final String HEADER_KEY = PREFIX + "header";
  static final String KEY_PREFIX = PREFIX + "key.";
  static final String SECRET_SUFFIX = ".secret";
  static final String ROLES_SUFFIX = ".roles";
  static final String PRINCIPAL_SUFFIX = ".principal";
  static final String DEFAULT_HEADER = "X-API-Key";

  private final String header;
  private final List<Definition> definitions;

  private ApiKeyConfig(String header, List<Definition> definitions) {
    this.header = header;
    this.definitions = definitions;
  }

  /** One configured key: the resolved secret plus the principal and roles it grants. */
  static final class Definition {
    private final String principal;
    private final Set<SagaRole> roles;
    private final String secret;

    Definition(String principal, Set<SagaRole> roles, String secret) {
      this.principal = principal;
      this.roles = Set.copyOf(roles);
      this.secret = secret;
    }

    String principal() {
      return principal;
    }

    Set<SagaRole> roles() {
      return roles;
    }

    String secret() {
      return secret;
    }
  }

  /**
   * Parses the API-key configuration.
   *
   * @param resolved the secret-resolved server properties (secret values expanded)
   * @param raw the pre-resolution server properties (references not yet expanded), used to enforce
   *     that each key was supplied as a secret reference
   * @return the parsed configuration
   * @throws IllegalArgumentException if no key is configured, a key is inline rather than a secret
   *     reference, a reference did not resolve (unchanged after resolution), a resolved key is
   *     blank, or a role is unknown/missing
   */
  static ApiKeyConfig from(Properties resolved, Properties raw) {
    String header = valueOrDefault(resolved.getProperty(HEADER_KEY), DEFAULT_HEADER);
    List<Definition> definitions = new ArrayList<>();
    for (String name : keyNames(resolved)) {
      definitions.add(parseDefinition(name, resolved, raw));
    }
    if (definitions.isEmpty()) {
      throw new IllegalArgumentException(
          "The API-key security provider requires at least one key, but no '"
              + KEY_PREFIX
              + "<name>"
              + SECRET_SUFFIX
              + "' is configured.");
    }
    return new ApiKeyConfig(header, List.copyOf(definitions));
  }

  String header() {
    return header;
  }

  List<Definition> definitions() {
    return definitions;
  }

  /** Collects the distinct {@code <name>}s that have a {@code key.<name>.secret} entry, sorted. */
  private static Set<String> keyNames(Properties resolved) {
    Set<String> names = new TreeSet<>();
    for (String key : resolved.stringPropertyNames()) {
      if (key.startsWith(KEY_PREFIX) && key.endsWith(SECRET_SUFFIX)) {
        String name = key.substring(KEY_PREFIX.length(), key.length() - SECRET_SUFFIX.length());
        if (!name.isBlank()) {
          names.add(name);
        }
      }
    }
    return names;
  }

  private static Definition parseDefinition(String name, Properties resolved, Properties raw) {
    String secretKey = KEY_PREFIX + name + SECRET_SUFFIX;
    String rawSecret = raw.getProperty(secretKey);
    if (rawSecret == null || !isSecretReference(rawSecret)) {
      throw new IllegalArgumentException(
          "'"
              + secretKey
              + "' must be a secret reference (e.g. ${env:NAME} or ${file:UTF-8:/path}), not an"
              + " inline value, so a pre-shared key never appears in plaintext config.");
    }
    String secret = resolved.getProperty(secretKey);
    if (secret == null || secret.isBlank()) {
      throw new IllegalArgumentException("'" + secretKey + "' resolved to an empty value.");
    }
    // The reference passed through unchanged: it never expanded (an undefined ${env:...}, which the
    // resolver leaves verbatim). Fail fast rather than silently treating the reference text as the
    // key — otherwise a typo'd or unset variable becomes a literal-string key no client can
    // present.
    if (secret.equals(rawSecret)) {
      throw new IllegalArgumentException(
          "'"
              + secretKey
              + "' secret reference "
              + rawSecret
              + " did not resolve — is the referenced environment variable or file present?");
    }
    Set<SagaRole> roles = parseRoles(name, resolved.getProperty(KEY_PREFIX + name + ROLES_SUFFIX));
    String principal =
        valueOrDefault(resolved.getProperty(KEY_PREFIX + name + PRINCIPAL_SUFFIX), name);
    return new Definition(principal, roles, secret);
  }

  /** Parses a comma-delimited list of role wire names, requiring at least one known role. */
  private static Set<SagaRole> parseRoles(String name, @Nullable String value) {
    String rolesKey = KEY_PREFIX + name + ROLES_SUFFIX;
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("'" + rolesKey + "' must list at least one role.");
    }
    Set<SagaRole> roles = new LinkedHashSet<>();
    for (String token : TextSplitter.split(value, c -> c == ',', true)) {
      SagaRole role =
          SagaRole.fromWireName(token)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Unknown role '"
                              + token
                              + "' in '"
                              + rolesKey
                              + "'. Valid roles: saga:read, saga:write, saga:admin."));
      roles.add(role);
    }
    if (roles.isEmpty()) {
      throw new IllegalArgumentException("'" + rolesKey + "' must list at least one role.");
    }
    return roles;
  }

  /** Whether the whole (trimmed) value is a single {@code ${...}} secret reference. */
  private static boolean isSecretReference(String value) {
    String trimmed = value.trim();
    return trimmed.length() > 3 && trimmed.startsWith("${") && trimmed.endsWith("}");
  }

  private static String valueOrDefault(@Nullable String value, String defaultValue) {
    return (value == null || value.isBlank()) ? defaultValue : value.trim();
  }
}
