package com.scalar.db.saga.server;

/**
 * A secret reference that is wrong wherever it runs, kept distinct from every other resolution
 * failure so it stays fatal even where failures are tolerated.
 *
 * <p>{@code --validate-config} runs where the secrets are usually absent, and softens failures that
 * describe <b>this machine</b> rather than the configuration: the file is not here, the root is not
 * mounted, the path is a directory, the file is too large. None of those say anything about whether
 * the configuration is right.
 *
 * <p>These do. A malformed {@code ${file:...}} form and a charset no JVM knows fail identically on
 * a running daemon, so softening them would let an offline check pass a configuration that can
 * never start a server. An escaping path is the same in a different way: a service file reaching
 * somewhere it may never reach is as wrong on a laptop as in production, and is exactly what an
 * offline check should catch.
 */
final class PermanentReferenceException extends IllegalArgumentException {
  PermanentReferenceException(String message) {
    super(message);
  }
}
