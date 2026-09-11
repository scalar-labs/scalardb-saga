package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.server.SagaWaiterRegistry;
import com.scalar.db.saga.server.security.SagaAuthRequest;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaIdentity;
import com.scalar.db.saga.server.security.SagaRole;
import com.scalar.db.saga.server.security.SagaSecurityHandler;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the two start endpoints' wait behaviour — the subject of {@code todos/076}.
 *
 * <p>Until 2026-08 an unset {@code sync.timeout_millis} (the default) made a synchronous start run
 * the saga <em>inline on the request thread</em>, unbounded, so ~200 concurrent slow sagas
 * exhausted the Jetty pool. Nothing here tested it, which is why the change that removed it broke
 * no test. These tests pin the replacement: every start dispatches to {@code startAsync} and waits
 * only up to the bound the server was configured with.
 */
class SagaResourceStartTest {

  private static final String SAGA_ID = "s1";
  private static final String SAGA_NAME = "order-saga";
  private static final Instant TS = Instant.parse("2026-08-31T10:00:00Z");

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;
  private SagaOrchestrator orchestrator;
  private CompletableFuture<Void> shutdownSignal;
  // Held so a test can settle a saga through it, which is what a resumed drive does.
  private SagaWaiterRegistry waiterRegistry;

  private void startServer(long syncWaitBoundMillis) {
    shutdownSignal = new CompletableFuture<>();
    orchestrator = mock(SagaOrchestrator.class);
    waiterRegistry = new SagaWaiterRegistry();
    app = Javalin.create();
    SagaSecurityHandler.register(app, new RoleHeaderProvider());
    ErrorMapper.register(app);
    SagaResource.register(app, orchestrator, syncWaitBoundMillis, shutdownSignal, waiterRegistry);
    app.start(0);
  }

  @BeforeEach
  void setUp() {
    // A generous bound: tests that care about the bound elapsing set their own.
    startServer(30_000L);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void postSagas_withDefaultConfig_dispatchesToStartAsyncNotStart() throws Exception {
    // Arrange — the saga finishes immediately, via the callback the resource registers.
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation
                  .getArgument(2, SagaCallback.class)
                  .onCompleted(snapshot(SagaStatus.COMPLETED));
              return SAGA_ID;
            });

    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert — the saga never runs on the request thread.
    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class));
    verify(orchestrator, never()).start(any(String.class), anyMap());
  }

  @Test
  void postSagas_sagaCompletesWithinBound_returns200WithTerminalSnapshot() throws Exception {
    // Arrange
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation
                  .getArgument(2, SagaCallback.class)
                  .onCompensated(snapshot(SagaStatus.COMPENSATED));
              return SAGA_ID;
            });

    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert — terminal means 200; the body carries the business outcome, which is a rollback here.
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("COMPENSATED");
  }

  @Test
  void postSagas_boundElapsesFirst_returns202AndLeavesTheSagaRunning() throws Exception {
    // Arrange — a bound that expires before the saga does; the callback is never invoked.
    app.stop();
    startServer(50L);
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenReturn(SAGA_ID);
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.RUNNING));

    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert — the honest answer: still running, not an error, and the saga was not cancelled.
    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).contains("RUNNING");
  }

  @Test
  void postSagas_sagaParksThenFinishesBeforeTheBound_returns200WithTheOutcome() throws Exception {
    // Arrange — the saga parks on an async step, so the engine reports onParked, and finishes
    // before the bound elapses. The resume carries no SagaCallback (and may happen on another
    // replica), so the callback registered here never fires again: what decides the response is
    // the read at bound expiry, which by then sees a completed saga.
    app.stop();
    startServer(300L);
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onParked(snapshot(SagaStatus.WAITING));
              return SAGA_ID;
            });
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.COMPLETED));

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — the outcome the caller asked to wait for, not a 202 delivered in milliseconds.
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("COMPLETED");
    // Both of these fail if the park releases the wait: it would answer at once, well inside the
    // bound, from the parked snapshot and without ever reading the store.
    assertThat(elapsedMillis).isGreaterThanOrEqualTo(250L);
    verify(orchestrator).getStateSnapshot(SAGA_ID);
  }

  @Test
  void postSagas_parkedSagaIsResumedOnThisReplica_returns200WithoutWaitingOutTheBound()
      throws Exception {
    // Arrange — the saga parks, then a resumed drive settles it on this process. That drive carries
    // no SagaCallback, so the registry is the only thing that can wake the waiter. The bound is 30s
    // and the store would report WAITING, so answering COMPLETED promptly is only possible if the
    // registry did the waking.
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onParked(snapshot(SagaStatus.WAITING));
              return SAGA_ID;
            });
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.WAITING));
    CompletableFuture<Void> resume =
        settleOnceWatched(SAGA_ID, () -> snapshot(SagaStatus.COMPLETED));

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    resume.join();

    // Assert — the outcome, delivered when the saga settled rather than when the bound elapsed.
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("COMPLETED");
    assertThat(elapsedMillis).isLessThan(10_000L);
  }

  @Test
  void postSagas_parkedSagaSettlesElsewhere_isSeenByAPollTickBeforeTheBound() throws Exception {
    // Arrange — the saga parks and is then resumed on *another* replica, so nothing on this process
    // notifies the waiter: neither the start callback (dead at the park) nor the registry (the
    // resumed drive ran elsewhere). The poll tick is the only thing that can answer before the
    // bound. A 6s bound derives the 1s floor, so a tick lands well inside it; without the tick this
    // would answer at 6s from the read at bound expiry.
    app.stop();
    startServer(6_000L);
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onParked(snapshot(SagaStatus.WAITING));
              return SAGA_ID;
            });
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.COMPLETED));

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("COMPLETED");
    // Answering this far inside the bound is only possible via a tick.
    assertThat(elapsedMillis).isLessThan(4_000L);
  }

  @Test
  void postSagas_sagaStillParkedWhenTheBoundElapses_returns202() throws Exception {
    // Arrange — the async step has not reported back by the time the bound elapses, so the saga is
    // genuinely unfinished and 202 is the honest answer. This is the case where waiting the bound
    // buys nothing, and it must still answer correctly.
    app.stop();
    startServer(300L);
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onParked(snapshot(SagaStatus.WAITING));
              return SAGA_ID;
            });
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.WAITING));

    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).contains("WAITING");
  }

  @Test
  void postSagas_serverShutsDownMidWait_answers202WithoutWaitingOutTheBound() throws Exception {
    // Arrange — a saga that never settles, against setUp's 30s bound. Shutdown must end the wait:
    // the bound is a maximum, not a promise to wait, and a terminating server cannot advance the
    // saga anyway, so holding the request would answer the same 202 up to 30s later.
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenReturn(SAGA_ID);
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.RUNNING));

    // Act — trip the signal just after the request is in flight.
    long startNanos = System.nanoTime();
    CompletableFuture<HttpResponse<String>> inFlight =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    Thread.sleep(200);
    shutdownSignal.complete(null);
    HttpResponse<String> response = inFlight.get(10, TimeUnit.SECONDS);
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — answered promptly, and honestly: the saga is still running.
    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).contains("RUNNING");
    assertThat(elapsedMillis).isLessThan(10_000L);
  }

  @Test
  void postSagas_shutdownAfterTheSagaCompleted_stillAnswers200() throws Exception {
    // Arrange — the narrow case the short-circuit must not lose: the saga finished, and shutdown
    // wakes the waiter. Re-reading the state is what keeps this a 200 rather than a blanket 202.
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenReturn(SAGA_ID);
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.COMPLETED));

    // Act
    CompletableFuture<HttpResponse<String>> inFlight =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    Thread.sleep(200);
    shutdownSignal.complete(null);
    HttpResponse<String> response = inFlight.get(10, TimeUnit.SECONDS);

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("COMPLETED");
  }

  @Test
  void postSagas_asyncQueryParamGiven_returns202WithoutWaiting() throws Exception {
    // Arrange
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap())).thenReturn(SAGA_ID);
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.RUNNING));

    // Act
    HttpResponse<String> response =
        post("/sagas?async=true", "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert — the no-callback overload, so nothing waits.
    assertThat(response.statusCode()).isEqualTo(202);
    verify(orchestrator).startAsync(eq(SAGA_NAME), anyMap());
  }

  @Test
  void putSagasById_withDefaultConfig_dispatchesToStartAsyncNotStart() throws Exception {
    // Arrange — the client-supplied-ID endpoint gets the same treatment as POST. The void overload
    // needs doAnswer to fire the callback; without it the request would wait out the whole bound.
    doAnswer(
            invocation -> {
              invocation
                  .getArgument(3, SagaCallback.class)
                  .onCompleted(snapshot(SagaStatus.COMPLETED));
              return null;
            })
        .when(orchestrator)
        .startAsync(eq(SAGA_ID), eq(SAGA_NAME), anyMap(), any(SagaCallback.class));

    // Act
    HttpResponse<String> response =
        put("/sagas/" + SAGA_ID, "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).startAsync(eq(SAGA_ID), eq(SAGA_NAME), anyMap(), any(SagaCallback.class));
    verify(orchestrator, never()).start(any(String.class), any(String.class), anyMap());
  }

  @Test
  void postSagas_engineRejectsTheInput_returns400WithoutWaitingOutTheBound() throws Exception {
    // Arrange — input the engine refuses. Validation runs before the saga is persisted, so it
    // throws on the request thread and reaches ErrorMapper, rather than failing on the executor
    // where nothing reports it. Before this, such a request waited out the full bound and answered
    // 202 for a saga that could never run — with setUp's 30s bound, a 30s wait for bad JSON.
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenThrow(new SagaIllegalArgumentException("SagaContext does not allow null values"));

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — the body names which value the context refused. The status and code say only that
    // the input was rejected; the detail is what tells the caller which value to fix.
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body())
        .contains(SagaErrorCode.INVALID_ARGUMENT.code())
        .contains("does not allow null values");
    assertThat(elapsedMillis).isLessThan(5_000L);
  }

  @Test
  void putSagasById_duplicateId_returns409WithoutTheExistingSnapshot() throws Exception {
    // Arrange — startAsync validates the id and persists before dispatching, so a duplicate throws
    // on the request thread. The exception carries the existing saga; the response must not.
    SagaStateSnapshot victim =
        new SagaStateSnapshot(
            SAGA_ID, "someone-elses-saga", SagaStatus.RUNNING, "victim", "v1", TS, TS);
    doThrow(new SagaAlreadyExistsException(SAGA_ID, victim))
        .when(orchestrator)
        .startAsync(eq(SAGA_ID), eq(SAGA_NAME), anyMap(), any(SagaCallback.class));

    // Act
    HttpResponse<String> response =
        put("/sagas/" + SAGA_ID, "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    // Assert — 409 through ErrorMapper, not the 202 branch. The body deliberately omits the
    // existing snapshot: including it would let an ID-guessing caller read another caller's saga
    // state (see this resource's class javadoc). Asserting the absence is the point of the test.
    assertThat(response.statusCode()).isEqualTo(409);
    assertThat(response.body()).doesNotContain("someone-elses-saga");
    assertThat(response.body()).doesNotContain("victim");
    assertThat(response.body()).doesNotContain("RUNNING");
  }

  /**
   * Settles the saga through the registry, but only once the request thread has actually registered
   * its waiter. A notification that arrives first lands on an empty registry and is dropped by
   * design, which the test would discover only when the bound elapsed, seconds later and as the
   * wrong status. {@code isWatching} is the registry's own published signal for this, so the wait
   * is on the condition rather than on a guess at how long registration takes.
   */
  private CompletableFuture<Void> settleOnceWatched(
      String sagaId, Supplier<SagaStateSnapshot> settled) {
    return CompletableFuture.runAsync(
        () -> {
          awaitWatching(sagaId);
          waiterRegistry.onSagaSettled(settled.get());
        });
  }

  private void awaitWatching(String sagaId) {
    for (int attempt = 0; attempt < 1_000; attempt++) {
      if (waiterRegistry.isWatching(sagaId)) {
        return;
      }
      try {
        Thread.sleep(5L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted waiting for a waiter on " + sagaId, e);
      }
    }
    throw new IllegalStateException("no waiter ever registered for saga " + sagaId);
  }

  private SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(SAGA_ID, SAGA_NAME, status, "owner", "v1", TS, TS);
  }

  private HttpResponse<String> post(String path, String body) throws Exception {
    return send("POST", path, body);
  }

  private HttpResponse<String> put(String path, String body) throws Exception {
    return send("PUT", path, body);
  }

  private HttpResponse<String> send(String method, String path, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
            .header("X-Test-Role", "write")
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    return http.send(request, BodyHandlers.ofString());
  }

  /** A stub provider mapping an {@code X-Test-Role} header to an identity holding that role. */
  private static final class RoleHeaderProvider implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      String role = request.header("X-Test-Role").orElse(null);
      if (role == null) {
        throw new SagaAuthenticationException("missing X-Test-Role");
      }
      return switch (role) {
        case "write" -> SagaIdentity.of("writer", Set.of(SagaRole.READ, SagaRole.WRITE));
        default -> throw new SagaAuthenticationException("unknown role: " + role);
      };
    }

    @Override
    public String name() {
      return "role-header-stub";
    }
  }
}
