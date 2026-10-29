package com.scalar.db.saga.server;

import org.jspecify.annotations.Nullable;

/**
 * Resolves the secret references in a service-file value, on behalf of {@link ServiceFileParser}.
 *
 * <p>Two implementations, and the difference between them is what happens when a reference cannot
 * be read. The daemon uses {@link ServiceSecretResolver}, which throws: a service whose token file
 * is missing must not start serving with a header it could not build. {@code --validate-config}
 * uses {@link LenientServiceValueResolver}, which returns an <b>unresolved marker</b> instead; see
 * that class for why, and for where the line is drawn.
 *
 * <p>An unresolved marker is not a value. Its contract is that the caller <b>skips the checks that
 * are about the value</b> and reports having skipped them; it must never be validated as if it were
 * the operator's real value, in either direction. Passing it would report a configuration as
 * checked when the check never ran, and failing it would report a problem in a value nobody has
 * seen yet.
 */
interface ServiceValueResolver {

  /**
   * Resolves the {@code ${env:...}} and {@code ${file:...}} references in {@code value}.
   *
   * @return the resolved value, or an unresolved marker when this resolver continues without it
   * @throws RuntimeException when a reference cannot be resolved and this resolver treats that as
   *     an error, which is the daemon's behavior
   */
  Resolution resolve(String value);

  /**
   * A resolved value, or a marker saying it could not be resolved and why.
   *
   * @param value the resolved value; for an unresolved marker, the reference text as written, which
   *     stands in only so the parse can continue and is never checked
   * @param unresolvedReason why the value could not be resolved, or {@code null} when it was
   */
  record Resolution(String value, @Nullable String unresolvedReason) {

    static Resolution of(String value) {
      return new Resolution(value, null);
    }

    static Resolution unresolved(String reference, String reason) {
      return new Resolution(reference, reason);
    }
  }
}
