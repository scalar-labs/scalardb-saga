package com.scalar.db.saga.daemon.api;

import io.javalin.Javalin;
import java.util.Map;

/**
 * Registers the {@code GET /health} <b>liveness</b> endpoint: it confirms the server process is up
 * and serving (always {@code 200 {"status":"UP"}}). It deliberately does <b>not</b> verify ScalarDB
 * connectivity or recovery status — a dependency-checking <em>readiness</em> probe is a future
 * enhancement.
 */
public final class HealthResource {

  /**
   * The liveness route path. Exposed as a constant so the security layer can exempt this
   * infrastructure probe — which carries no user credential — from caller-facing auth.
   */
  public static final String PATH = "/health";

  private HealthResource() {}

  /**
   * Registers the liveness route on the given app.
   *
   * @param app the Javalin app
   */
  public static void register(Javalin app) {
    app.get(PATH, ctx -> ctx.json(Map.of("status", "UP")));
  }
}
