package com.scalar.db.saga.server.api;

import com.scalar.db.saga.server.security.SagaIdentity;
import com.scalar.db.saga.server.security.SagaOperation;
import com.scalar.db.saga.server.security.SagaSecurityHandler;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Objects;

/**
 * A Javalin {@code beforeMatched}-handler that rate-limits per authenticated principal the
 * operations {@link SagaOperation#rateLimited()} marks, to bound how fast one caller can create
 * sagas (a DoS control).
 *
 * <p>It runs after {@link SagaSecurityHandler} (which resolves the caller and stores the {@link
 * SagaIdentity} on the request), and limits only <b>authenticated</b> calls; routes with no
 * resolved identity (the HMAC-authed async callback, the health probe) are skipped, so a
 * participant callback is never throttled by a user's budget. Over-limit requests get {@code 429}
 * via {@link RateLimitExceededException}.
 *
 * <p>Registered on {@code beforeMatched} rather than {@code before} — this is load-bearing, not
 * stylistic. The rate-limit decision is per operation, which is only knowable once a route has
 * matched; and Javalin runs every {@code before} handler ahead of any {@code beforeMatched} one, so
 * a limiter left on {@code before} would read a null identity for every request and skip silently,
 * disabling rate limiting daemon-wide with all its unit tests still green. It must move in lockstep
 * with {@link SagaSecurityHandler}.
 *
 * <p>Registered only when a positive limit is configured; when disabled it is never installed.
 */
public final class RateLimitHandler {

  private final RateLimiter limiter;

  private RateLimitHandler(RateLimiter limiter) {
    this.limiter = limiter;
  }

  /**
   * Registers the rate-limit handler.
   *
   * @param app the Javalin app
   * @param limiter the per-principal limiter; shared with the gRPC transport so a caller's budget
   *     spans both, rather than being counted separately per port
   */
  public static void register(Javalin app, RateLimiter limiter) {
    Objects.requireNonNull(app, "app must not be null");
    Objects.requireNonNull(limiter, "limiter must not be null");
    RateLimitHandler handler = new RateLimitHandler(limiter);
    app.beforeMatched(handler::handle);
  }

  private void handle(Context ctx) {
    if (!SagaOperation.fromRouteRoles(ctx.routeRoles()).rateLimited()) {
      return;
    }
    SagaIdentity identity = ctx.attribute(SagaSecurityHandler.IDENTITY_ATTRIBUTE);
    if (identity == null) {
      // No resolved caller on a rate-limited operation. Reachable only if the security handler did
      // not run or the operation is auth-exempt; there is then no budget to key on, so the call
      // proceeds rather than being blocked by a limit it cannot attribute.
      return;
    }
    if (!limiter.tryAcquire(identity.principal(), System.currentTimeMillis())) {
      throw new RateLimitExceededException(
          "Saga-start rate limit exceeded for principal '" + identity.principal() + "'");
    }
  }
}
