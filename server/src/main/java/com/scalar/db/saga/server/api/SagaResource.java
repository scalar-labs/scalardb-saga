package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import com.scalar.db.saga.server.security.SagaOperation;
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
 *       with the standard error body on conflict — deliberately without the existing snapshot,
 *       which would let an ID-guessing caller read another caller's saga state)
 *   <li>{@code GET /sagas/{id}} — fetch a saga's current state
 * </ul>
 *
 * <p><b>Synchronous outcome contract.</b> A {@code 200} from a synchronous start means the saga
 * <em>executed to a terminal state</em> — it does <b>not</b> imply business success. Callers must
 * inspect the body {@code status}: {@code COMPLETED} (succeeded) vs {@code COMPENSATED}/{@code
 * ESCALATED} (rolled back / stuck). A saga still resolving (e.g. {@code COMPENSATING}) returns
 * {@code 202} — poll {@code GET /sagas/{id}}. This mirrors synchronous workflow APIs such as AWS
 * Step Functions {@code StartSyncExecution} and Netflix Conductor, which return {@code 200} for a
 * failed execution and carry the outcome in the body.
 *
 * <p><b>Which failures are 4xx, and which are not.</b> Only what is checked <em>before</em> the
 * saga is persisted maps to 4xx: unknown definition → {@code 404}, duplicate ID → {@code 409},
 * invalid request → {@code 400}. Everything after that point — step resolution, a failing step,
 * compensation — cannot reach the caller, because execution has already been handed to the engine's
 * executor. A definition whose steps cannot be resolved therefore answers {@code 200} with {@code
 * status: ESCALATED} rather than a 4xx, and leaves a persisted saga that needs manual admin
 * resolution (retention cleanup skips {@code ESCALATED}). This is the same rule gRPC has always
 * followed, and it is the contract {@link com.scalar.db.saga.api.SagaOrchestrator} states for its
 * {@code startAsync} overloads; until 2026-08 the default REST path used the synchronous {@code
 * start} overloads, which did surface that failure as a 4xx.
 *
 * <p><b>No run-to-completion in a single request.</b> The wait bound is unconditional, so a saga
 * that outlives it answers {@code 202} and the client polls {@code GET /sagas/{id}}. There is no
 * long-poll on this surface — no {@code ?wait=} — so past the bound the poll carries no server-side
 * wait. gRPC's {@code AwaitSaga} is a resumable window the Java SDK loops to deliver
 * block-until-terminal; REST has no analogue, and this API exists precisely for consumers who skip
 * that SDK. A REST long-poll bounded by the same policy would close the gap without restoring an
 * unbounded wait; see {@code todos/086}.
 *
 * <p><b>Bounded synchronous start.</b> A synchronous start runs the saga on the engine's
 * (virtual-thread) executor and waits, never longer than the {@code sync.max_wait_millis} ceiling,
 * tightened by {@code sync.timeout_millis} when that is set. The wait ends as soon as the engine
 * stops driving the saga — it reached a terminal state, or it parked on an async step — and at the
 * bound at the latest, after which the request returns {@code 202} while the saga keeps running
 * (poll {@code GET /sagas/{id}}). This caps how long a single request can hold a thread, so a burst
 * of slow synchronous sagas cannot exhaust the request pool. Returning {@code 202} rather than an
 * error is the honest outcome: the saga is not cancelled, it is still being processed, and it
 * reuses the {@code 202} this endpoint already returns for a non-terminal outcome. The pattern
 * mirrors RFC 7240's {@code Prefer: respond-async, wait=N}, Azure Durable Functions' {@code
 * WaitForCompletionOrCreateCheckStatusResponse}, and Conductor's {@code executeWorkflow} wait
 * timeout.
 *
 * <p>The bound is unconditional: an unset {@code sync.timeout_millis} still leaves the ceiling in
 * force.
 *
 * <p>Not yet wired: {@code PUT /sagas/{id}/cancel} (needs the engine's {@code cancel} method). The
 * {@code GET /sagas} listing lives on the admin surface ({@link SagaAdminResource}).
 */
public final class SagaResource {

  private SagaResource() {}

  /**
   * Registers the saga lifecycle routes on the given app.
   *
   * @param app the Javalin app
   * @param orchestrator the saga orchestrator the endpoints delegate to
   * @param syncWaitBoundMillis how long a synchronous start may wait before answering {@code 202},
   *     already resolved from the {@code sync.*} keys. Always finite, so no start can block
   *     indefinitely.
   */
  public static void register(
      Javalin app, SagaOrchestrator orchestrator, long syncWaitBoundMillis) {
    app.post(
        "/sagas",
        ctx -> {
          StartSagaRequest request = parseRequest(ctx);
          Map<String, Object> input = request.inputOrEmpty();
          if (isAsync(ctx.queryParam("async"))) {
            String sagaId = orchestrator.startAsync(request.requireSagaName(), input);
            respond(ctx, 202, orchestrator.getStateSnapshot(sagaId));
          } else {
            AtomicReference<SagaStateSnapshot> outcome = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            String sagaId =
                orchestrator.startAsync(
                    request.requireSagaName(), input, outcomeSignal(done, outcome));
            respondBoundedSync(ctx, orchestrator, sagaId, done, outcome, syncWaitBoundMillis);
          }
        },
        SagaOperation.START_SAGA);

    app.put(
        "/sagas/{id}",
        ctx -> {
          String sagaId = ctx.pathParam("id");
          StartSagaRequest request = parseRequest(ctx);
          Map<String, Object> input = request.inputOrEmpty();
          if (isAsync(ctx.queryParam("async"))) {
            orchestrator.startAsync(sagaId, request.requireSagaName(), input);
            respond(ctx, 202, orchestrator.getStateSnapshot(sagaId));
          } else {
            AtomicReference<SagaStateSnapshot> outcome = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            orchestrator.startAsync(
                sagaId, request.requireSagaName(), input, outcomeSignal(done, outcome));
            respondBoundedSync(ctx, orchestrator, sagaId, done, outcome, syncWaitBoundMillis);
          }
        },
        SagaOperation.START_SAGA);

    app.get(
        "/sagas/{id}",
        ctx -> respond(ctx, 200, orchestrator.getStateSnapshot(ctx.pathParam("id"))),
        SagaOperation.GET_SAGA);

    // A saga's detail (state + timeline). An application read of its own saga — self-service
    // diagnosis of a failure — so it lives here with the other application reads, not on the admin
    // surface; the timeline redacts raw step payloads.
    app.get(
        "/sagas/{id}/detail",
        ctx ->
            ctx.status(200)
                .json(SagaDetailResponse.from(orchestrator.getSagaDetail(ctx.pathParam("id")))),
        SagaOperation.GET_SAGA_DETAIL);
  }

  /**
   * A {@link SagaCallback} that captures the saga's outcome and releases {@code done} as soon as
   * the engine stops driving it — in any terminal outcome, or when it parks on an async step — so a
   * bounded synchronous start wakes then rather than always waiting the full bound.
   *
   * <p>Parking counts: a saga waiting on an external callback has stopped progressing, and holding
   * the request until the bound elapses would answer the same {@code 202} up to a minute later.
   */
  private static SagaCallback outcomeSignal(
      CountDownLatch done, AtomicReference<SagaStateSnapshot> outcome) {
    return new SagaCallback() {
      @Override
      public void onCompleted(SagaStateSnapshot saga) {
        outcome.set(saga);
        done.countDown();
      }

      @Override
      public void onCompensated(SagaStateSnapshot saga) {
        outcome.set(saga);
        done.countDown();
      }

      @Override
      public void onEscalated(SagaStateSnapshot saga) {
        outcome.set(saga);
        done.countDown();
      }

      @Override
      public void onParked(SagaStateSnapshot saga) {
        outcome.set(saga);
        done.countDown();
      }
    };
  }

  /**
   * Renders a bounded synchronous start: waits up to {@code timeoutMillis} for the engine to stop
   * driving the saga, then responds {@code 200} if it reached a terminal state (the body {@code
   * status} carries the business outcome) or {@code 202} otherwise, with the current snapshot,
   * while the saga keeps running on the engine's executor (the client polls {@code GET
   * /sagas/{id}}). A saga that parked on an async step answers {@code 202} as soon as it parks, not
   * when the bound elapses. The request thread is therefore held for at most {@code timeoutMillis},
   * and usually far less.
   */
  private static void respondBoundedSync(
      Context ctx,
      SagaOrchestrator orchestrator,
      String sagaId,
      CountDownLatch done,
      AtomicReference<SagaStateSnapshot> outcome,
      long timeoutMillis) {
    boolean settled;
    try {
      settled = done.await(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // Stop waiting and answer with what we know; the saga continues either way, so 202 is honest.
      // Note shutdown does not land here: Jetty drains in-flight requests rather than interrupting
      // their handlers, so this covers only a deliberate interrupt of the carrying thread.
      Thread.currentThread().interrupt();
      settled = false;
    }
    if (settled) {
      // 'settled' means a callback ran, and every one sets 'outcome' before counting down. The
      // status decides the code: terminal is 200, a parked saga is 202.
      SagaStateSnapshot snapshot = Objects.requireNonNull(outcome.get());
      respond(ctx, snapshot.getStatus().isTerminal() ? 200 : 202, snapshot);
    } else {
      respond(ctx, 202, orchestrator.getStateSnapshot(sagaId));
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
      throw new SagaInvalidRequestException("malformed request body");
    }
    // A body of the JSON null literal deserializes to null without throwing; reject it cleanly here
    // rather than NPE-ing downstream. The check is outside the try so its message survives.
    if (request == null) {
      throw new SagaInvalidRequestException("request body must not be null");
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
    throw new SagaInvalidRequestException("query parameter 'async' must be 'true' or 'false'");
  }
}
