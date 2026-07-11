package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Objects;

/**
 * A Javalin {@code before}-handler that rate-limits saga-start requests per authenticated
 * principal, to bound how fast one caller can create sagas (a DoS control).
 *
 * <p>It runs after {@link SagaSecurityHandler} (which resolves the caller and stores the {@link
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
    SagaIdentity identity = ctx.attribute(SagaSecurityHandler.IDENTITY_ATTRIBUTE);
    if (identity == null) {
      // No resolved caller: an auth-exempt route (HMAC callback, health probe). Not rate-limited
      // here — the callback must not be throttled by a user's saga-start budget.
      return;
    }
    if (!isRateLimited(ctx.method().name())) {
      // Only state-changing saga-start requests are limited; reads are cheap.
      return;
    }
    if (!limiter.tryAcquire(identity.principal(), System.currentTimeMillis())) {
      throw new RateLimitExceededException(
          "Saga-start rate limit exceeded for principal '" + identity.principal() + "'");
    }
  }

  /**
   * Whether an HTTP method is rate-limited: {@code POST}/{@code PUT} — today's saga-start endpoints
   * ({@code POST /sagas}, {@code PUT /sagas/{id}}) — are limited; reads ({@code GET}) are not.
   * Mirrors the gRPC interceptor's rate-limit predicate, which limits everything but the read/poll
   * methods. Public (not package-private) so the cross-transport parity test can assert against the
   * real rule from another package rather than re-encoding it.
   */
  public static boolean isRateLimited(String method) {
    return method.equals("POST") || method.equals("PUT");
  }
}
