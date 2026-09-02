package com.scalar.db.saga.server;

import java.nio.file.Path;

/**
 * The {@code --validate-config} resolver: {@link ServiceSecretResolver}'s rules, but a reference it
 * cannot read becomes an unresolved marker rather than an error.
 *
 * <p>Only what depends on <b>this machine</b> is softened: the file is not here, the root is not
 * mounted, the path is a directory, the file is too large. A reference that is wrong wherever it
 * runs — a malformed {@code ${file:...}} form, a charset no JVM knows, a path escaping the secrets
 * root — still fails, because softening it would let the check pass a configuration that can never
 * start a daemon, which is the one thing this command exists to prevent.
 *
 * <p>{@link ServiceSecretResolver.PermanentReferenceException} draws that line, and every one of
 * those checks runs before the file is opened, so the classification does not depend on what
 * happens to be mounted.
 */
final class LenientServiceValueResolver implements ServiceValueResolver {

  private final ServiceSecretResolver strict;

  LenientServiceValueResolver(Path secretsRoot) {
    this.strict = new ServiceSecretResolver(secretsRoot);
  }

  @Override
  public Resolution resolve(String value) {
    try {
      return strict.resolve(value);
    } catch (ServiceSecretResolver.PermanentReferenceException e) {
      throw e;
    } catch (RuntimeException e) {
      return Resolution.unresolved(value, e.getMessage() == null ? e.toString() : e.getMessage());
    }
  }
}
