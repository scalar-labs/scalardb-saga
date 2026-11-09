package com.scalar.db.saga.server.api;

import com.scalar.db.saga.server.security.SagaOperation;
import io.javalin.http.Handler;
import io.javalin.router.JavalinDefaultRoutingApi;

/**
 * Registers a read route on both {@code GET} and {@code HEAD}, with one handler and one operation.
 *
 * <p>Javalin answers a {@code HEAD} that has no handler of its own with a bare {@code 200} as soon
 * as a {@code GET} route exists at the path, without running that route's handler. Such a {@code
 * HEAD} would report {@code 200} for a saga that does not exist and drop the headers the {@code
 * GET} sets. Registering the handler on {@code HEAD} as well runs it, and the server discards the
 * body, so a {@code HEAD} returns the same status and headers as its {@code GET}.
 */
final class ReadRoutes {

  private ReadRoutes() {}

  /**
   * Registers {@code handler} on {@code GET} and {@code HEAD} at {@code path}, tagged {@code
   * operation}.
   *
   * @param routes the routing configuration the server is created with
   * @param path the route path
   * @param handler the handler that answers both methods
   * @param operation the operation the security layer authorizes both methods against
   */
  static void register(
      JavalinDefaultRoutingApi routes, String path, Handler handler, SagaOperation operation) {
    routes.get(path, handler, operation);
    routes.head(path, handler, operation);
  }
}
