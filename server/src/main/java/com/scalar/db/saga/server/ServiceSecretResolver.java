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
   * Where {@code path} lands once symlinks are followed, defined for a target that does not exist:
   * the deepest ancestor that does exist is resolved, and the segments walked past are re-attached
   * to it. For a target that exists this is exactly {@link Path#toRealPath}.
   *
   * <p>This is what lets containment be judged for a reference to a file that is not on this
   * machine, which is the ordinary case offline. Comparing such a path as written instead would
   * reject a reference reaching the root through a symlinked ancestor, an everyday container shape
   * ({@code /var/run} is usually a link to {@code /run}).
   *
   * <p>Deliberately not normalized before the walk: {@code /var/run/..} is {@code /var} as text but
   * names {@code /} once {@code /var/run} is followed, so simplifying here would place the
   * reference somewhere it does not land.
   */
  private static Path landingPath(Path path) throws IOException {
    Path absolute = path.toAbsolutePath();
    Path existing = absolute;
    while (existing != null && !Files.exists(existing)) {
      existing = existing.getParent();
    }
    if (existing == null) {
      // No component of the path is on this machine, so there is nothing to follow and the path as
      // written is the best statement of where it lands.
      return absolute;
    }
    return existing.toRealPath().resolve(existing.relativize(absolute));
  }

  /**
   * The containment check for a secrets root that does not resolve: compares the paths as written,
   * after normalizing {@code .} and {@code ..} away.
   *
   * <p>Strictly weaker than the symlink-resolved check, and used only where that one cannot run.
   * {@link Path#startsWith} compares whole path components, so a root of {@code /run/secrets} does
   * not admit {@code /run/secrets-evil}.
   */
  private void requireContainedAsWritten(Path path) {
    if (!path.toAbsolutePath().normalize().startsWith(secretsRoot.toAbsolutePath().normalize())) {
      throw new PermanentReferenceException(
          "'"
              + path
              + "' resolves outside '"
              + SagaServerConfig.SECRETS_ROOT_KEY
              + "' "
              + Redaction.redacted(secretsRoot.toString())
              + ", as written");
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
    SecretFileReference reference = SecretFileReference.parse(key);
    Charset charset = reference.charset();
    Path path = reference.path();
    try {
      Path realRoot;
      try {
        realRoot = secretsRoot.toRealPath();
      } catch (IOException e) {
        // The root is not on this machine, so nothing can be resolved against it and the check
        // below cannot run. Compare the paths as written instead: that cannot see through a
        // symlink, but it still catches a reference plainly pointing somewhere else, which is the
        // mistake an offline check is for. Only ever a fallback — applying it where the root does
        // resolve would reject a reference reaching the root through a symlinked ancestor, which
        // is an ordinary shape (a container's /var/run is usually a link to /run).
        requireContainedAsWritten(path);
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
      // Where the reference lands once symlinks are followed, so a link inside the root pointing
      // outside it is judged by its real target and not by its location — the escape this
      // confinement exists to stop. Computed from the deepest ancestor that exists, because
      // toRealPath needs the target itself and on the machine an offline check runs the secret is
      // usually absent; without that, a reference naming no existing file would reach neither
      // containment check and an escape would pass as merely missing here.
      Path landing = landingPath(path);
      if (!landing.startsWith(realRoot)) {
        // Not an UncheckedIOException like its neighbours: this one must stay fatal even for a
        // caller that tolerates unresolvable references. See PermanentReferenceException.
        throw new PermanentReferenceException(
            "'"
                + path
                + "' resolves outside '"
                + SagaServerConfig.SECRETS_ROOT_KEY
                + "' "
                + Redaction.redacted(secretsRoot.toString()));
      }
      // Containment is settled, so a target that is not here is now only a fact about this
      // machine, and the handler below reports it as one.
      Path real = path.toRealPath();
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
