package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaNotFoundException;
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
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the {@code GET /sagas/{id}/detail} endpoint, which lives on {@link SagaResource} as an
 * application self-service read (state + timeline) rather than on the admin surface. Gated at
 * {@link SagaRole#READ}: the application that ran the saga diagnoses its own failure without an
 * operator.
 */
class SagaResourceDetailTest {

  private static final String SAGA_ID = "s1";
  private static final Instant TS = Instant.parse("2026-07-18T10:00:00Z");

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;
  private SagaOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    orchestrator = mock(SagaOrchestrator.class);
    app = Javalin.create();
    SagaSecurityHandler.register(app, new RoleHeaderProvider());
    ErrorMapper.register(app);
    SagaResource.register(app, orchestrator, 0L);
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void detail_readRoleGiven_returns200WithTimeline() throws Exception {
    // Arrange
    SagaStateSnapshot snapshot =
        new SagaStateSnapshot(SAGA_ID, "order-saga", SagaStatus.COMPENSATED, "owner", "v1", TS, TS);
    TimelineEvent event =
        new TimelineEvent(TS, "STEP_FAILED", 1, "credit", null, "downstream broke", null);
    when(orchestrator.getSagaDetail(SAGA_ID)).thenReturn(new SagaDetail(snapshot, List.of(event)));

    // Act — an application reads its own saga's detail with a plain read credential
    HttpResponse<String> response = send("GET", "/sagas/s1/detail", "read");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"sagaId\":\"s1\"");
    assertThat(response.body()).contains("downstream broke");
    assertThat(response.body()).contains("\"truncated\":false");
  }

  @Test
  void detail_truncatedTimeline_surfacesFlagInResponse() throws Exception {
    // Arrange — the orchestrator cut the timeline to its configured bound
    SagaStateSnapshot snapshot =
        new SagaStateSnapshot(SAGA_ID, "order-saga", SagaStatus.ESCALATED, "owner", "v1", TS, TS);
    TimelineEvent event =
        new TimelineEvent(TS, "SAGA_ESCALATED", null, null, SagaStatus.ESCALATED, "stuck", null);
    when(orchestrator.getSagaDetail(SAGA_ID))
        .thenReturn(new SagaDetail(snapshot, List.of(event), true));

    // Act
    HttpResponse<String> response = send("GET", "/sagas/s1/detail", "read");

    // Assert
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"truncated\":true");
  }

  @Test
  void detail_noCredentialGiven_returns401() throws Exception {
    HttpResponse<String> response = send("GET", "/sagas/s1/detail", null);
    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void detail_missingSaga_returns404() throws Exception {
    when(orchestrator.getSagaDetail(SAGA_ID)).thenThrow(new SagaNotFoundException(SAGA_ID));
    HttpResponse<String> response = send("GET", "/sagas/s1/detail", "read");
    assertThat(response.statusCode()).isEqualTo(404);
  }

  private HttpResponse<String> send(String method, String path, @Nullable String role)
      throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
            .method(method, HttpRequest.BodyPublishers.noBody());
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
        default -> throw new SagaAuthenticationException("unknown role: " + role);
      };
    }

    @Override
    public String name() {
      return "role-header-stub";
    }
  }
}
