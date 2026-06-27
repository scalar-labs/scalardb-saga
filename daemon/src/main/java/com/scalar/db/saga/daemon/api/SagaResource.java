package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
 * <p><b>Bounded synchronous start (opt-in).</b> A synchronous start runs the saga on the engine's
 * (virtual-thread) executor and blocks the request thread only until the saga is terminal — which,
 * with retries/compensation over slow participants, can be long. When {@code
 * scalar.db.saga.server.sync_timeout_millis} is set (default {@code 0} = disabled, i.e. block to
 * terminal), the request instead returns {@code 202} once that bound elapses, while the saga keeps
 * running (poll {@code GET /sagas/{id}}). This caps how long a single request can hold a thread, so
 * a burst of slow synchronous sagas cannot exhaust the request pool. Returning {@code 202} (rather
 * than an error) is the honest outcome — the saga is not cancelled, it is still being processed —
 * and reuses the {@code 202} this endpoint already returns for a non-terminal outcome. The pattern
 * mirrors RFC 7240's {@code Prefer: respond-async, wait=N}, Azure Durable Functions' {@code
 * WaitForCompletionOrCreateCheckStatusResponse}, and Conductor's {@code executeWorkflow} wait
 * timeout.
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
   * @param syncTimeoutMillis the synchronous-start timeout ({@code 0} disables it; see the class
   *     doc's bounded-synchronous-start note)
   */
  public static void register(
      Javalin app, DefaultSagaOrchestrator sagaManager, long syncTimeoutMillis) {
    app.post(
        "/sagas",
        ctx -> {
          StartSagaRequest request = parseRequest(ctx);
          Map<String, Object> input = request.inputOrEmpty();
          if (isAsync(ctx.queryParam("async"))) {
            String sagaId = sagaManager.startAsync(request.requireSagaName(), input);
            respond(ctx, 202, sagaManager.getStateSnapshot(sagaId));
          } else if (syncTimeoutMillis > 0) {
            AtomicReference<SagaStateSnapshot> terminal = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            String sagaId =
                sagaManager.startAsync(
                    request.requireSagaName(), input, terminalSignal(done, terminal));
            respondBoundedSync(ctx, sagaManager, sagaId, done, terminal, syncTimeoutMillis);
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
          } else if (syncTimeoutMillis > 0) {
            AtomicReference<SagaStateSnapshot> terminal = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            sagaManager.startAsync(
                sagaId, request.requireSagaName(), input, terminalSignal(done, terminal));
            respondBoundedSync(ctx, sagaManager, sagaId, done, terminal, syncTimeoutMillis);
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
  private static void respondSync(Context ctx, DefaultSagaOrchestrator sagaManager, String sagaId) {
    SagaStateSnapshot snapshot = sagaManager.getStateSnapshot(sagaId);
    respond(ctx, snapshot.getStatus().isTerminal() ? 200 : 202, snapshot);
  }

  /**
   * A {@link SagaCallback} that captures the terminal snapshot and releases {@code done} when the
   * saga finishes (in any terminal outcome), so a bounded synchronous start can wake as soon as the
   * saga is done rather than always waiting the full timeout.
   */
  private static SagaCallback terminalSignal(
      CountDownLatch done, AtomicReference<SagaStateSnapshot> terminal) {
    return new SagaCallback() {
      @Override
      public void onCompleted(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }

      @Override
      public void onCompensated(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }

      @Override
      public void onEscalated(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }
    };
  }

  /**
   * Renders a <em>bounded</em> synchronous start: waits up to {@code timeoutMillis} for the saga to
   * reach a terminal state. If it does, responds like {@link #respondSync} ({@code 200}/{@code
   * 202}); if the bound elapses first, responds {@code 202} with the in-flight snapshot while the
   * saga keeps running on the engine's executor (the client polls {@code GET /sagas/{id}}). The
   * request thread is therefore held for at most {@code timeoutMillis}, never the saga's full run.
   */
  private static void respondBoundedSync(
      Context ctx,
      DefaultSagaOrchestrator sagaManager,
      String sagaId,
      CountDownLatch done,
      AtomicReference<SagaStateSnapshot> terminal,
      long timeoutMillis) {
    boolean reached;
    try {
      reached = done.await(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // The request thread was interrupted (e.g. shutdown); stop waiting. The saga continues, so
      // 202 is the honest answer.
      Thread.currentThread().interrupt();
      reached = false;
    }
    if (reached) {
      // 'reached' means a terminal callback ran, which sets 'terminal' before counting down.
      SagaStateSnapshot snapshot = Objects.requireNonNull(terminal.get());
      respond(ctx, snapshot.getStatus().isTerminal() ? 200 : 202, snapshot);
    } else {
      respond(ctx, 202, sagaManager.getStateSnapshot(sagaId));
    }
  }

  /** Renders a saga snapshot as the JSON response body with the given HTTP status. */
  private static void respond(Context ctx, int status, SagaStateSnapshot snapshot) {
    ctx.status(status).json(SagaSnapshotResponse.from(snapshot));
  }

  /**
   * Parses the JSON request body, mapping any deserialization failure — or a {@code null} body — to
   * a {@code 400}. Catches {@link Exception} because the JSON mapper surfaces parse failures as
   * undeclared checked exceptions.
   */
  private static StartSagaRequest parseRequest(Context ctx) {
    StartSagaRequest request;
    try {
      request = ctx.bodyAsClass(StartSagaRequest.class);
    } catch (Exception e) {
      throw new InvalidRequestException("malformed request body");
    }
    // A body of the JSON null literal deserializes to null without throwing; reject it cleanly here
    // rather than NPE-ing downstream. The check is outside the try so its message survives.
    if (request == null) {
      throw new InvalidRequestException("request body must not be null");
    }
    return request;
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
