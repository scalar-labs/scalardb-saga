package com.scalar.db.saga.server;

import java.nio.file.Path;

/**
 * The {@code --validate-config} resolver: {@link ServiceSecretResolver}'s rules, but a reference it
 * cannot read becomes an unresolved marker rather than an error.
 *
 * <p>Only the failure is softened, never the confinement. A {@code ${file:...}} that resolves
 * <b>outside</b> the secrets root fails here exactly as it does on the daemon, because that is not
 * a secret the tool happens not to have — it is a service file reaching somewhere it may never
 * reach, which is as wrong on a laptop as it is in production, and is precisely the kind of mistake
 * an offline check should catch. What is softened is the ordinary "that file is not on this
 * machine", which says nothing about whether the configuration is correct.
 *
 * <p>The distinction is the containment check inside {@link ServiceSecretResolver}: it runs before
 * the read, so an escaping reference throws from there and is left to propagate.
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
    } catch (ServiceSecretResolver.ContainmentViolationException e) {
      throw e;
    } catch (RuntimeException e) {
      return Resolution.unresolved(value, e.getMessage() == null ? e.toString() : e.getMessage());
    }
  }
}
