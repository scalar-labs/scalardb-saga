package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.SagaStateSnapshot;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Registers the saga lifecycle REST endpoints:
 *
 * <ul>
 *   <li>{@code POST /sagas} — start a saga with a server-generated ID (synchronous by default;
 *       {@code ?async=true} returns {@code 202} immediately)
 *   <li>{@code PUT /sagas/{id}} — start a saga with a client-supplied ID (idempotent; {@code 409}
 *       with the existing snapshot on conflict)
 *   <li>{@code GET /sagas/{id}} — fetch a saga's current state
 * </ul>
 *
 * <p><b>Synchronous outcome contract.</b> A {@code 200} from a synchronous start means the saga
 * <em>executed to a terminal state</em> — it does <b>not</b> imply business success. Callers must
 * inspect the body {@code status}: {@code COMPLETED} (succeeded) vs {@code COMPENSATED}/{@code
 * ESCALATED} (rolled back / stuck). A saga still resolving (e.g. {@code COMPENSATING}) returns
 * {@code 202} — poll {@code GET /sagas/{id}}. Pre-execution problems map to 4xx (unknown definition
 * → 404, duplicate ID → 409, invalid request → 400). This mirrors synchronous workflow APIs such as
 * AWS Step Functions {@code StartSyncExecution} and Netflix Conductor, which return {@code 200} for
 * a failed execution and carry the outcome in the body.
 *
 * <p>Not yet wired: {@code PUT /sagas/{id}/cancel} (needs the engine's {@code cancel} method) and
 * {@code GET /sagas} listing (needs the admin query layer).
 */
public final class SagaResource {

  private SagaResource() {}

  /**
   * Registers the saga lifecycle routes on the given app.
   *
   * @param app the Javalin app
   * @param sagaManager the saga manager the endpoints delegate to
   */
  public static void register(Javalin app, SagaManager sagaManager) {
    app.post(
        "/sagas",
        ctx -> {
          StartSagaRequest request = parseRequest(ctx);
          Map<String, Object> input = request.inputOrEmpty();
          if (isAsync(ctx.queryParam("async"))) {
            String sagaId = sagaManager.startAsync(request.requireSagaName(), input);
            respond(ctx, 202, sagaManager.getStateSnapshot(sagaId));
          } else {
            String sagaId = sagaManager.start(request.requireSagaName(), input);
            respondSync(ctx, sagaManager, sagaId);
          }
        });

    app.put(
        "/sagas/{id}",
        ctx -> {
          String sagaId = ctx.pathParam("id");
          StartSagaRequest request = parseRequest(ctx);
          Map<String, Object> input = request.inputOrEmpty();
          if (isAsync(ctx.queryParam("async"))) {
            sagaManager.startAsync(sagaId, request.requireSagaName(), input);
            respond(ctx, 202, sagaManager.getStateSnapshot(sagaId));
          } else {
            sagaManager.start(sagaId, request.requireSagaName(), input);
            respondSync(ctx, sagaManager, sagaId);
          }
        });

    app.get(
        "/sagas/{id}", ctx -> respond(ctx, 200, sagaManager.getStateSnapshot(ctx.pathParam("id"))));
  }

  /**
   * Renders a synchronous start response: {@code 200} once the saga has reached a terminal state
   * (the body {@code status} carries the business outcome — {@code COMPLETED} vs {@code
   * COMPENSATED}/{@code ESCALATED}), or {@code 202} while it is still resolving ({@code
   * COMPENSATING} / parked {@code RUNNING}) — poll {@code GET /sagas/{id}}.
   */
  private static void respondSync(Context ctx, SagaManager sagaManager, String sagaId) {
    SagaStateSnapshot snapshot = sagaManager.getStateSnapshot(sagaId);
    respond(ctx, snapshot.getStatus().isTerminal() ? 200 : 202, snapshot);
  }

  /** Renders a saga snapshot as the JSON response body with the given HTTP status. */
  private static void respond(Context ctx, int status, SagaStateSnapshot snapshot) {
    ctx.status(status).json(SagaSnapshotResponse.from(snapshot));
  }

  /**
   * Parses the JSON request body, mapping any deserialization failure to a {@code 400}. Catches
   * {@link Exception} because the JSON mapper surfaces parse failures as undeclared checked
   * exceptions.
   */
  private static StartSagaRequest parseRequest(Context ctx) {
    try {
      return ctx.bodyAsClass(StartSagaRequest.class);
    } catch (Exception e) {
      throw new InvalidRequestException("malformed request body");
    }
  }

  /**
   * Parses the {@code ?async} flag. Absent → synchronous (the default). Accepts {@code true}/{@code
   * false} (case-insensitive); any other value is rejected with {@code 400} rather than silently
   * taking the (riskier, thread-pinning) synchronous path.
   */
  private static boolean isAsync(@Nullable String value) {
    if (value == null) {
      return false;
    }
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new InvalidRequestException("query parameter 'async' must be 'true' or 'false'");
  }
}
