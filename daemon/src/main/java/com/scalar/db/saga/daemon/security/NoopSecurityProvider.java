package com.scalar.db.saga.daemon.security;

import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link SagaSecurityProvider}: authenticates <b>every</b> request as a single
 * full-access identity ({@code principal="anonymous"} holding {@link SagaRole#ADMIN}, which implies
 * every role). Effectively no authentication — appropriate only when the daemon runs on a
 * trusted/isolated network (the deployment posture the server has assumed to date) or in local
 * development.
 *
 * <p>Because it grants full access, it <b>logs a prominent warning</b> at construction so an
 * operator who ships it unintentionally sees that the server is unauthenticated. Select a real
 * provider (JWT, API key) to enforce access control.
 */
public final class NoopSecurityProvider implements SagaSecurityProvider {

  private static final Logger logger = LoggerFactory.getLogger(NoopSecurityProvider.class);

  /**
   * The identity every request resolves to: full access, for audit visibility named "anonymous".
   */
  private static final SagaIdentity ANONYMOUS_ADMIN =
      SagaIdentity.of("anonymous", EnumSet.allOf(SagaRole.class));

  public NoopSecurityProvider() {
    logger.warn(
        "Security is DISABLED: the '{}' provider authenticates every request as a full-access"
            + " administrator, enforcing no access control. Run the daemon only on a trusted,"
            + " network-isolated deployment, or configure a real security provider (JWT or API"
            + " key) to enable authentication.",
        name());
  }

  @Override
  public SagaIdentity authenticate(SagaAuthRequest request) {
    return ANONYMOUS_ADMIN;
  }

  @Override
  public String name() {
    return "noop";
  }
}
