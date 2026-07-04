package com.scalar.db.saga.daemon;

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
 * throwing, so resolution never disrupts a value that merely contains a {@code ${...}} sequence. A
 * {@code ${file:...}} reference to an unreadable file, by contrast, surfaces the error: a missing
 * secret file should fail startup, not silently resolve to a literal.
 */
final class SecretResolver {

  private final StringSubstitutor substitutor;

  SecretResolver() {
    StringLookupFactory factory = StringLookupFactory.INSTANCE;
    Map<String, StringLookup> lookups =
        Map.of(
            "env", factory.environmentVariableStringLookup(),
            "file", factory.fileStringLookup());
    // addDefaultLookups=false excludes script/url/dns/etc. — the CVE-2022-42889 vectors; a null
    // default lookup means an unprefixed ${NAME} is not resolved either.
    StringLookup interpolator = factory.interpolatorStringLookup(lookups, null, false);
    this.substitutor = new StringSubstitutor(interpolator);
  }

  /**
   * Resolves any {@code ${env:...}} / {@code ${file:...}} references in {@code value}, returning
   * the value with each recognized reference replaced by its resolved secret. A value with no
   * resolvable reference is returned unchanged.
   */
  String resolve(String value) {
    return substitutor.replace(value);
  }
}
