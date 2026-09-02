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

  private void startServer(long syncWaitBoundMillis) {
    shutdownSignal = new CompletableFuture<>();
    orchestrator = mock(SagaOrchestrator.class);
    app = Javalin.create();
    SagaSecurityHandler.register(app, new RoleHeaderProvider());
    ErrorMapper.register(app);
    SagaResource.register(app, orchestrator, syncWaitBoundMillis, shutdownSignal);
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
  void postSagas_sagaParksOnAnAsyncStep_returns202WithoutWaitingOutTheBound() throws Exception {
    // Arrange — the saga parks instead of finishing. setUp's bound is 30s, so if parking did not
    // release the wait this request would hang for that long; asserting the elapsed time is what
    // distinguishes "answered because it parked" from "answered because the bound elapsed".
    when(orchestrator.startAsync(eq(SAGA_NAME), anyMap(), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onParked(snapshot(SagaStatus.WAITING));
              return SAGA_ID;
            });

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — a parked saga is still running, so 202 with its current state, delivered promptly.
    assertThat(response.statusCode()).isEqualTo(202);
    assertThat(response.body()).contains("WAITING");
    assertThat(elapsedMillis).isLessThan(5_000L);
    // The park answered it; the resource never fell back to reading the snapshot itself.
    verify(orchestrator, never()).getStateSnapshot(SAGA_ID);
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
        .thenThrow(new IllegalArgumentException("SagaContext does not allow null values"));

    // Act
    long startNanos = System.nanoTime();
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\"}");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert
    assertThat(response.statusCode()).isEqualTo(400);
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
