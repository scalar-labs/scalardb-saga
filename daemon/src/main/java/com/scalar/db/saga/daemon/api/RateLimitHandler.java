package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SecurityHandler;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Objects;

/**
 * A Javalin {@code before}-handler that rate-limits saga-start requests per authenticated
 * principal, to bound how fast one caller can create sagas (a DoS control).
 *
 * <p>It runs after {@link SecurityHandler} (which resolves the caller and stores the {@link
 * SagaIdentity} on the request), and limits only <b>authenticated writes</b> — {@code POST}/{@code
 * PUT}, which today are exactly the saga-start endpoints ({@code POST /sagas}, {@code PUT
 * /sagas/{id}}). Reads ({@code GET}) are not limited, and routes with no resolved identity (the
 * HMAC-authed async callback, the health probe) are skipped, so a participant callback is never
 * throttled by a user's budget. Over-limit requests get {@code 429} via {@link
 * RateLimitExceededException}.
 *
 * <p>Registered only when a positive limit is configured; when disabled it is never installed.
 */
public final class RateLimitHandler {

  private final RateLimiter limiter;

  private RateLimitHandler(RateLimiter limiter) {
    this.limiter = limiter;
  }

  /**
   * Registers the rate-limit before-handler.
   *
   * @param app the Javalin app
   * @param limiter the per-principal saga-start limiter; shared with the gRPC transport so a
   *     caller's budget spans both, rather than being counted separately per port
   */
  public static void register(Javalin app, RateLimiter limiter) {
    Objects.requireNonNull(app, "app must not be null");
    Objects.requireNonNull(limiter, "limiter must not be null");
    RateLimitHandler handler = new RateLimitHandler(limiter);
    app.before(handler::handle);
  }

  private void handle(Context ctx) {
    SagaIdentity identity = ctx.attribute(SecurityHandler.IDENTITY_ATTRIBUTE);
    if (identity == null) {
      // No resolved caller: an auth-exempt route (HMAC callback, health probe). Not rate-limited
      // here — the callback must not be throttled by a user's saga-start budget.
      return;
    }
    String method = ctx.method().name();
    if (!method.equals("POST") && !method.equals("PUT")) {
      // Only state-changing saga-start requests are limited; reads are cheap.
      return;
    }
    if (!limiter.tryAcquire(identity.principal(), System.currentTimeMillis())) {
      throw new RateLimitExceededException(
          "Saga-start rate limit exceeded for principal '" + identity.principal() + "'");
    }
  }
}
