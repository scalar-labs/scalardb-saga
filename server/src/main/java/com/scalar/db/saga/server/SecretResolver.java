package com.scalar.db.saga.server;

import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;

/**
 * Resolves secret references in configuration values against a restricted set of lookups.
 *
 * <p>Two reference forms are supported:
 *
 * <ul>
 *   <li>{@code ${file:UTF-8:/path/to/secret}} — the file's contents. The natural fit for a
 *       Kubernetes mounted {@code Secret}: it is not exposed in the process environment and can be
 *       rotated without a restart. <b>Preferred</b> for secrets.
 *   <li>{@code ${env:NAME}} — an environment variable. A convenient fallback.
 * </ul>
 *
 * <p>The substitutor is built with an allowlist of exactly these two lookups and with the default
 * interpolator lookups <b>disabled</b>. This is the security-critical property: the default {@link
 * StringSubstitutor} interpolator also enables {@code ${script:...}}, {@code ${url:...}} and {@code
 * ${dns:...}}, the remote-code-execution / SSRF vectors of CVE-2022-42889 ("Text4Shell"). None of
 * those prefixes are reachable here. Requires Commons Text &ge; 1.10.0 (which also fixes the CVE by
 * default), pinned in the version catalog.
 *
 * <p>An unrecognized reference — an unknown prefix (e.g. the disabled {@code ${script:...}}) or an
 * undefined {@code ${env:NAME}} variable — is left in the value <b>verbatim</b> rather than
 * throwing, so such a reference is not disrupted. A {@code ${file:...}} reference to an unreadable
 * file, by contrast, surfaces the error: a missing secret file should fail startup, not silently
 * resolve to a literal.
 *
 * <p>Interpolation otherwise follows {@link StringSubstitutor} syntax, so a value is not guaranteed
 * verbatim merely because it contains a {@code ${...}} sequence: the default-value delimiter is
 * active ({@code ${x:-y}} yields {@code y} when {@code x} is unresolved), and {@code $$} escapes to
 * a literal {@code $} (so {@code $${env:HOME}} becomes {@code ${env:HOME}}). Only the {@code env}
 * and {@code file} lookups are enabled.
 *
 * <p>Substitution in resolved values is <b>disabled</b>: a secret's contents are treated literally,
 * so a {@code ${...}} sequence inside a resolved secret is never re-interpreted. Otherwise a
 * password containing those characters would be corrupted, and a resolved value that read like
 * {@code ${env:...}} / {@code ${file:...}} would trigger an unintended nested lookup.
 */
final class SecretResolver {

  private final StringSubstitutor substitutor;

  SecretResolver() {
    StringLookupFactory factory = StringLookupFactory.INSTANCE;
    StringLookup fileLookup = factory.fileStringLookup();
    Map<String, StringLookup> lookups =
        Map.of(
            "env",
            factory.environmentVariableStringLookup(),
            "file",
            key -> readFile(fileLookup, key));
    // addDefaultLookups=false excludes script/url/dns/etc. — the CVE-2022-42889 vectors; a null
    // default lookup means an unprefixed ${NAME} is not resolved either.
    StringLookup interpolator = factory.interpolatorStringLookup(lookups, null, false);
    StringSubstitutor substitutor = new StringSubstitutor(interpolator);
    // Treat a resolved secret's contents literally: StringSubstitutor recursively re-scans
    // substituted values by default, so a secret whose value itself contains a ${...} sequence (a
    // password with those characters, or a value that happens to read like ${env:...}) would be
    // corrupted or trigger an unintended nested file/env lookup. Disable that recursion.
    substitutor.setDisableSubstitutionInValues(true);
    this.substitutor = substitutor;
  }

  /**
   * Validates the reference form before the library reads the file, so a malformed reference and an
   * unknown charset are reported as {@link PermanentReferenceException} rather than as whatever the
   * library makes of them.
   *
   * <p>The distinction is what lets {@code --validate-config} soften "that secret is not on this
   * machine" while still failing a reference that is wrong wherever it runs. Without it,
   * commons-text reports both as the same {@code IllegalArgumentException}, and a typo in a
   * reference would pass an offline check and then stop the daemon from starting.
   */
  private static String readFile(StringLookup fileLookup, String key) {
    SecretFileReference.parse(key);
    return fileLookup.lookup(key);
  }

  /**
   * Resolves any {@code ${env:...}} / {@code ${file:...}} references in {@code value}, returning
   * the value with each recognized reference replaced by its resolved secret. A value with no
   * resolvable reference is returned unchanged.
   */
  String resolve(String value) {
    return substitutor.replace(value);
  }

  /**
   * Whether {@code value} is written entirely as one {@code ${env:...}} reference. Paired with a
   * resolution that returned the value unchanged, this is what tells an undefined environment
   * variable from an ordinary literal: such a reference is left verbatim rather than raised, so
   * without this nothing marks it and the reference text stands in as though it were a value.
   *
   * <p>Only {@code env}. A {@code ${file:...}} reference that cannot be read raises instead of
   * passing through, so it is already recorded; an unknown prefix ({@code ${script:...}}) or an
   * unprefixed {@code ${NAME}} resolves on no machine, and calling that "absent here" would soften
   * something wrong everywhere.
   *
   * <p>Whole-value by design: a reference surrounded by other text would have to be located inside
   * the value before it could be named, and the rest of that value may be an inline secret.
   */
  static boolean isEnvReference(String value) {
    String trimmed = value.trim();
    return trimmed.length() > "${env:}".length()
        && trimmed.startsWith("${env:")
        && trimmed.endsWith("}");
  }
}
