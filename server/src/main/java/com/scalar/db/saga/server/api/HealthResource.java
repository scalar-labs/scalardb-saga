package com.scalar.db.saga.server.api;

import com.scalar.db.saga.server.security.SagaOperation;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.util.Map;

/**
 * Registers the {@code GET /health} <b>liveness</b> endpoint: it confirms the server process is up
 * and serving (always {@code 200 {"status":"UP"}}). It deliberately does <b>not</b> verify ScalarDB
 * connectivity or recovery status — a dependency-checking <em>readiness</em> probe is a future
 * enhancement.
 */
public final class HealthResource {

  /** The liveness route path. */
  public static final String PATH = "/health";

  private HealthResource() {}

  /**
   * Registers the liveness route on the given app, tagged {@link SagaOperation#HEALTH} so the
   * security layer exempts this infrastructure probe — which carries no user credential — from
   * caller-facing auth. Both {@code GET} and {@code HEAD} are served, since load balancers and
   * uptime monitors commonly probe with {@code HEAD}; the {@code HEAD} handler carries the route's
   * roles, so it is exempted the same way (rather than falling to the {@code 405} the security
   * layer returns for HEAD on a GET-only route).
   *
   * @param app the Javalin app
   */
  public static void register(Javalin app) {
    Handler handler = ctx -> ctx.json(Map.of("status", "UP"));
    app.get(PATH, handler, SagaOperation.HEALTH);
    app.head(PATH, handler, SagaOperation.HEALTH);
  }
}
