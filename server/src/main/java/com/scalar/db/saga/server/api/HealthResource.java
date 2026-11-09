package com.scalar.db.saga.server.api;

import com.scalar.db.saga.server.security.SagaOperation;
import io.javalin.router.JavalinDefaultRoutingApi;
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
   * Registers the liveness route on the given routing configuration, tagged {@link
   * SagaOperation#HEALTH} so the security layer exempts this infrastructure probe — which carries
   * no user credential — from caller-facing auth. Javalin answers {@code HEAD} from the {@code GET}
   * handler with the same roles, so load balancers and uptime monitors that probe with {@code HEAD}
   * are exempted the same way.
   *
   * @param routes the routing configuration the server is created with
   */
  public static void register(JavalinDefaultRoutingApi routes) {
    routes.get(PATH, ctx -> ctx.json(Map.of("status", "UP")), SagaOperation.HEALTH);
  }
}
