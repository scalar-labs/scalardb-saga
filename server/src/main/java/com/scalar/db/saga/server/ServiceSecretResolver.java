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
final class ServiceSecretResolver {

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
   */
  String resolve(String value) {
    try {
      return substitutor.replace(value);
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
                "secrets_root '"
                    + secretsRoot
                    + "' cannot be resolved ("
                    + e.getMessage()
                    + "); ${file:...} references in service files resolve only inside it",
                e));
      }
      // toRealPath resolves symlinks, so a link inside the root pointing outside it lands on the
      // real target and fails the startsWith check — the escape this confinement exists to stop.
      Path real = path.toRealPath();
      if (!real.startsWith(realRoot)) {
        throw new UncheckedIOException(
            new IOException("'" + path + "' resolves outside secrets_root '" + secretsRoot + "'"));
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
      throw new UncheckedIOException(
          new IOException("'" + path + "' cannot be read (" + e.getMessage() + ")", e));
    }
  }
}
