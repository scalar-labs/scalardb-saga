package com.scalar.db.saga.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;

/**
 * Resolves secret references in service-file values, like {@link SecretResolver} but with {@code
 * ${file:...}} confined to the configured secrets root.
 *
 * <p>The confinement is the point of this class existing separately: service files are a live trust
 * boundary once configuration reload ships — whoever can write one gets its values resolved by the
 * daemon on the next pass, without a restart. An unconfined file lookup would let such a file pair
 * an attacker-controlled {@code base_url} with {@code header.X =
 * ${file:UTF-8:/var/run/secrets/kubernetes.io/serviceaccount/token}} and exfiltrate any
 * process-readable file one request later. Confining resolution to the secrets root (after symlink
 * resolution, regular files only, size-capped) reduces that to the secrets the operator mounted for
 * this purpose.
 *
 * <p>{@code server.properties} itself keeps the unconfined {@link SecretResolver}: it is
 * operator-owned bootstrap configuration, not a reloaded input.
 *
 * <p>The substitutor is built exactly like {@link SecretResolver}'s (allowlisted {@code env} +
 * {@code file} lookups, default interpolator lookups disabled — the CVE-2022-42889 vectors — and no
 * substitution inside resolved values). {@code ${env:...}} remains available but is discouraged in
 * service files (the environment cannot change in a running pod, so it defeats rotation); the
 * parser warns on it.
 */
final class ServiceSecretResolver implements ServiceValueResolver {

  /**
   * A {@code ${file:...}} reference that resolves outside the secrets root, kept distinct from
   * every other resolution failure so it stays fatal even where failures are tolerated.
   *
   * <p>{@code --validate-config} runs where the secrets are usually absent and softens "that file
   * is not on this machine", which says nothing about whether the configuration is right. An escape
   * is the opposite: the service file is reaching somewhere it may never reach, which is as wrong
   * on a laptop as in production, and is exactly what an offline check should catch.
   */
  static final class ContainmentViolationException extends IllegalArgumentException {
    ContainmentViolationException(String message) {
      super(message);
    }
  }

  /**
   * Cap on a {@code ${file:...}} target, matching the cap on the service files themselves: a secret
   * is small, and an unbounded read of a mis-pointed reference (a device node, a huge file) must
   * not stall or exhaust the resolving pass.
   */
  static final long MAX_SECRET_FILE_BYTES = 1024 * 1024;

  private final Path secretsRoot;
  private final StringSubstitutor substitutor;

  ServiceSecretResolver(Path secretsRoot) {
    this.secretsRoot = secretsRoot;
    StringLookupFactory factory = StringLookupFactory.INSTANCE;
    Map<String, StringLookup> lookups =
        Map.of("env", factory.environmentVariableStringLookup(), "file", this::readContainedFile);
    StringLookup interpolator = factory.interpolatorStringLookup(lookups, null, false);
    StringSubstitutor substitutor = new StringSubstitutor(interpolator);
    substitutor.setDisableSubstitutionInValues(true);
    this.substitutor = substitutor;
  }

  /**
   * Resolves any {@code ${env:...}} / {@code ${file:...}} references in {@code value}. A {@code
   * ${file:...}} reference outside the secrets root, or to a missing, non-regular, or oversized
   * file, throws; error messages name paths (they are configuration text) but never file contents.
   *
   * <p>This is the strict implementation, so it never returns an unresolved marker: every {@link
   * Resolution} it returns carries a value.
   */
  @Override
  public Resolution resolve(String value) {
    try {
      return Resolution.of(substitutor.replace(value));
    } catch (UncheckedIOException e) {
      // Unwrap to the message our own lookup composed; the cause chain would re-embed nothing
      // secret (contents are never in these messages), but the flattened form reads as one line.
      IOException cause = e.getCause();
      throw new IllegalArgumentException(cause != null ? cause.getMessage() : e.getMessage(), e);
    }
  }

  /**
   * The {@code file} lookup body: {@code key} is {@code <charset>:<path>} (the same form {@link
   * SecretResolver} documents, e.g. {@code UTF-8:/run/secrets/token}).
   */
  private String readContainedFile(String key) {
    // Every message below redacts the configured root. It is a scalar.db.saga.* value like any
    // other, so it may itself have been written as a ${file:...} reference and arrive here as the
    // secret's plaintext — one the operator never typed anywhere the daemon may echo. These
    // messages reach the reload WARN on every pass that rejects.
    //
    // The reference path stays visible, and deliberately: the operator wrote it in the service
    // file, so quoting it back discloses nothing they kept elsewhere, and it is what makes the
    // error actionable. A path under a secret-valued root would already put that secret in the
    // service file, which is a different problem from this one.
    int colon = key.indexOf(':');
    if (colon <= 0 || colon == key.length() - 1) {
      throw new IllegalArgumentException(
          "A ${file:...} reference in a service file must be ${file:<charset>:<path>}, e.g."
              + " ${file:UTF-8:/run/secrets/token}; got '${file:"
              + key
              + "}'");
    }
    Charset charset = Charset.forName(key.substring(0, colon));
    Path path = Path.of(key.substring(colon + 1));
    try {
      Path realRoot;
      try {
        realRoot = secretsRoot.toRealPath();
      } catch (IOException e) {
        throw new UncheckedIOException(
            new IOException(
                "'"
                    + SagaServerConfig.SECRETS_ROOT_KEY
                    + "' cannot be resolved ("
                    + e.getClass().getSimpleName()
                    + ") "
                    + Redaction.redacted(secretsRoot.toString())
                    + "; ${file:...} references in service files resolve only inside it",
                e));
      }
      // toRealPath resolves symlinks, so a link inside the root pointing outside it lands on the
      // real target and fails the startsWith check — the escape this confinement exists to stop.
      Path real = path.toRealPath();
      if (!real.startsWith(realRoot)) {
        // Not an UncheckedIOException like its neighbours: this one must stay fatal even for a
        // caller that tolerates unresolvable references. See ContainmentViolationException.
        throw new ContainmentViolationException(
            "'"
                + path
                + "' resolves outside '"
                + SagaServerConfig.SECRETS_ROOT_KEY
                + "' "
                + Redaction.redacted(secretsRoot.toString()));
      }
      if (!Files.isRegularFile(real)) {
        throw new UncheckedIOException(new IOException("'" + path + "' is not a regular file"));
      }
      if (Files.size(real) > MAX_SECRET_FILE_BYTES) {
        throw new UncheckedIOException(
            new IOException(
                "'" + path + "' exceeds the " + MAX_SECRET_FILE_BYTES + "-byte secret cap"));
      }
      return Files.readString(real, charset);
    } catch (IOException e) {
      // The cause is named by class rather than quoted: a filesystem exception's message is the
      // path it failed on, which after symlink resolution need not be the one the operator wrote.
      throw new UncheckedIOException(
          new IOException(
              "'" + path + "' cannot be read (" + e.getClass().getSimpleName() + ")", e));
    }
  }
}
