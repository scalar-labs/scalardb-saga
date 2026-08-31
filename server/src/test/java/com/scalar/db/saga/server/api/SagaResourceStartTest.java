package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
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

  private void startServer(long syncWaitBoundMillis) {
    orchestrator = mock(SagaOrchestrator.class);
    app = Javalin.create();
    SagaSecurityHandler.register(app, new RoleHeaderProvider());
    ErrorMapper.register(app);
    SagaResource.register(app, orchestrator, syncWaitBoundMillis);
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
