package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.OperatorContext;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Exercises {@link SagaAdminResource} end-to-end through a real Javalin dispatch behind the real
 * {@link SagaSecurityHandler}, with a header-keyed stub provider (absent header → {@code 401};
 * {@code read}/{@code write}/{@code admin} → an identity holding that role). The orchestrator and
 * admin service are mocked, so the tests isolate routing, RBAC, operator injection, and
 * exception→status mapping.
 *
 * <p>Negative authorization is the priority surface: the destructive admin mutations must be
 * unreachable without {@code saga:admin}.
 */
class SagaAdminResourceTest {

  private static final String SAGA_ID = "s1";
  private static final Instant TS = Instant.parse("2026-07-18T10:00:00Z");

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;
  private SagaAdminService adminService;
  private DefaultSagaOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    adminService = mock(SagaAdminService.class);
    orchestrator = mock(DefaultSagaOrchestrator.class);
    when(orchestrator.adminService()).thenReturn(adminService);
    when(orchestrator.adminService(any(OperatorContext.class), anyLong())).thenReturn(adminService);

    app = Javalin.create();
    SagaSecurityHandler.register(app, new RoleHeaderProvider());
    ErrorMapper.register(app);
    SagaAdminResource.register(app, orchestrator, 1_000L);
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(SAGA_ID, "order-saga", status, "owner", "v1", TS, TS);
  }

  // --- negative authorization (the priority surface) -------------------------

  @Test
  void recover_writeRoleGiven_returns403() throws Exception {
    // Act — a saga:write caller (not admin) hitting a destructive mutation
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "write", "{\"reason\":\"x\"}");

    // Assert — RECOVER_SAGA requires ADMIN; write must not suffice
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void forceComplete_writeRoleGiven_returns403() throws Exception {
    HttpResponse<String> response =
        send("POST", "/sagas/s1/force-complete", "write", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void bulkReset_writeRoleGiven_returns403() throws Exception {
    HttpResponse<String> response =
        send("POST", "/admin/reset-escalated", "write", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void list_noCredentialGiven_returns401() throws Exception {
    HttpResponse<String> response = send("GET", "/sagas", null, null);
    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void list_readRoleGiven_returns403() throws Exception {
    // Act — listing is global enumeration, an operator action, so saga:read must not suffice
    HttpResponse<String> response = send("GET", "/sagas", "read", null);

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void list_adminRoleGiven_returns200() throws Exception {
    // Arrange
    when(adminService.listSagas(any()))
        .thenReturn(new SagaPage<>(List.of(snapshot(SagaStatus.RUNNING)), null));

    // Act
    HttpResponse<String> response = send("GET", "/sagas", "admin", null);

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"sagaId\":\"s1\"");
  }

  // --- operator injection ----------------------------------------------------

  @Test
  void recover_adminRoleGiven_attributesMutationToAuthenticatedPrincipal() throws Exception {
    // Arrange
    when(adminService.recoverSaga(eq(SAGA_ID), any())).thenReturn(snapshot(SagaStatus.COMPENSATED));

    // Act
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "admin", "{\"reason\":\"stuck\"}");

    // Assert — the operator context handed to the factory yields the authenticated principal, so
    // the
    // audit is attributed to the caller, not to a body-supplied value. 1_000L is the drive deadline
    // the resource was registered with.
    assertThat(response.statusCode()).isEqualTo(200);
    ArgumentCaptor<OperatorContext> operator = ArgumentCaptor.forClass(OperatorContext.class);
    verify(orchestrator).adminService(operator.capture(), eq(1_000L));
    assertThat(operator.getValue().currentOperator()).isEqualTo("root");
  }

  // --- outcome + exception mapping -------------------------------------------

  @Test
  void recover_driveSettlesToTerminal_returns200() throws Exception {
    when(adminService.recoverSaga(eq(SAGA_ID), any())).thenReturn(snapshot(SagaStatus.COMPENSATED));
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void recover_driveStillRunning_returns202() throws Exception {
    // A non-terminal snapshot means the bounded drive was abandoned; the saga keeps running
    when(adminService.recoverSaga(eq(SAGA_ID), any()))
        .thenReturn(snapshot(SagaStatus.COMPENSATING));
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(202);
  }

  @Test
  void recover_wrongState_returns422() throws Exception {
    when(adminService.recoverSaga(eq(SAGA_ID), any()))
        .thenThrow(SagaStatePreconditionException.wrongState(SAGA_ID, "RUNNING", "recover"));
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(response.body()).contains(SagaErrorCode.SAGA_WRONG_STATE.code());
  }

  @Test
  void reset_writeRoleGiven_returns403() throws Exception {
    // Act — RESET_ESCALATED requires ADMIN; a saga:write caller must not reach it
    HttpResponse<String> response = send("POST", "/sagas/s1/reset", "write", "{\"reason\":\"x\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
  }

  @Test
  void reset_driveSettlesToTerminal_returns200() throws Exception {
    when(adminService.resetEscalated(eq(SAGA_ID), any()))
        .thenReturn(snapshot(SagaStatus.COMPENSATED));
    HttpResponse<String> response = send("POST", "/sagas/s1/reset", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void reset_driveStillRunning_returns202() throws Exception {
    // A non-terminal snapshot means the bounded drive was abandoned; the saga keeps running
    when(adminService.resetEscalated(eq(SAGA_ID), any()))
        .thenReturn(snapshot(SagaStatus.COMPENSATING));
    HttpResponse<String> response = send("POST", "/sagas/s1/reset", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(202);
  }

  @Test
  void reset_wrongState_returns422() throws Exception {
    // resetEscalated on a non-ESCALATED saga is a precondition failure, not transient
    when(adminService.resetEscalated(eq(SAGA_ID), any()))
        .thenThrow(SagaStatePreconditionException.wrongState(SAGA_ID, "RUNNING", "reset"));
    HttpResponse<String> response = send("POST", "/sagas/s1/reset", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(response.body()).contains(SagaErrorCode.SAGA_WRONG_STATE.code());
  }

  @Test
  void forceComplete_lostCas_returns409() throws Exception {
    when(adminService.forceComplete(eq(SAGA_ID), any()))
        .thenThrow(new SagaConcurrentModificationException(SAGA_ID));
    HttpResponse<String> response =
        send("POST", "/sagas/s1/force-complete", "admin", "{\"reason\":\"x\"}");
    assertThat(response.statusCode()).isEqualTo(409);
  }

  @Test
  void forceComplete_adminRoleGiven_returns200() throws Exception {
    // Arrange — force-complete is terminal (ESCALATED -> COMPLETED), always a settled 200
    when(adminService.forceComplete(eq(SAGA_ID), any())).thenReturn(snapshot(SagaStatus.COMPLETED));

    // Act
    HttpResponse<String> response =
        send("POST", "/sagas/s1/force-complete", "admin", "{\"reason\":\"done downstream\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"sagaId\":\"s1\"").contains("COMPLETED");
  }

  @Test
  void bulkReset_adminRoleGiven_returns200WithItemizedResult() throws Exception {
    // Arrange — one reset, one skipped, and a continuation token
    when(adminService.resetEscalated(any(SagaQuery.class), any()))
        .thenReturn(
            new ResetResult(
                1,
                List.of(
                    new ResetResult.SkippedSaga(
                        "s2", ResetResult.SkipReason.CONCURRENT_MODIFICATION)),
                "next-token"));

    // Act
    HttpResponse<String> response =
        send("POST", "/admin/reset-escalated", "admin", "{\"reason\":\"sweep\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body())
        .contains("\"resetCount\":1")
        .contains("CONCURRENT_MODIFICATION")
        .contains("next-token");
  }

  @Test
  void recover_blankReason_returns400() throws Exception {
    HttpResponse<String> response =
        send("POST", "/sagas/s1/recover", "admin", "{\"reason\":\"  \"}");
    assertThat(response.statusCode()).isEqualTo(400);
    // INVALID_ARGUMENT, the enum's own example for a blank reason — the same code gRPC and
    // embedded callers get, so the transports cannot drift apart on this input again.
    assertThat(response.body()).contains(SagaErrorCode.INVALID_ARGUMENT.code());
  }

  @Test
  void list_invalidStatusFilter_returns400() throws Exception {
    // admin role, so the request reaches query parsing rather than being denied first
    HttpResponse<String> response = send("GET", "/sagas?status=NOPE", "admin", null);
    assertThat(response.statusCode()).isEqualTo(400);
  }

  private HttpResponse<String> send(
      String method, String path, @Nullable String role, @Nullable String body) throws Exception {
    HttpRequest.BodyPublisher publisher =
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body);
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
            .method(method, publisher);
    if (body != null) {
      builder.header("Content-Type", "application/json");
    }
    if (role != null) {
      builder.header("X-Test-Role", role);
    }
    return http.send(builder.build(), BodyHandlers.ofString());
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
        case "read" -> SagaIdentity.of("reader", Set.of(SagaRole.READ));
        case "write" -> SagaIdentity.of("writer", Set.of(SagaRole.WRITE));
        case "admin" -> SagaIdentity.of("root", EnumSet.allOf(SagaRole.class));
        default -> throw new SagaAuthenticationException("unknown role: " + role);
      };
    }

    @Override
    public String name() {
      return "role-header-stub";
    }
  }
}
