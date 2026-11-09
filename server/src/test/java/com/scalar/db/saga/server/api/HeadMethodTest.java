package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaNotFoundException;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins HEAD handling across the real REST routes, end-to-end against a live Javalin dispatch.
 *
 * <p>A HEAD must run the GET route's handler, under the GET route's operation, and answer with the
 * GET's status and headers and no body. Javalin alone answers a HEAD on a GET route with a bare
 * {@code 200} without running the handler, so these tests check the handler actually ran and
 * compare HEAD with GET, including on a missing saga. {@code TransportPolicyParityTest} checks the
 * registrations; only a live dispatch shows what a request gets back.
 */
class HeadMethodTest {

  private static final String EXISTING = "saga-1";
  private static final String MISSING = "missing";

  private final HttpClient http = HttpClient.newHttpClient();
  private final GrantingProvider provider = new GrantingProvider();
  private final DefaultSagaOrchestrator orchestrator = mock(DefaultSagaOrchestrator.class);
  private final SagaAdminService adminService = mock(SagaAdminService.class);
  private Javalin app;

  @BeforeEach
  void setUp() {
    Instant now = Instant.ofEpochSecond(1_700_000_000L);
    when(orchestrator.getStateSnapshot(EXISTING))
        .thenReturn(
            new SagaStateSnapshot(EXISTING, "transfer", SagaStatus.COMPLETED, "v1", now, now));
    when(orchestrator.getStateSnapshot(MISSING)).thenThrow(new SagaNotFoundException(MISSING));
    when(orchestrator.adminService()).thenReturn(adminService);
    when(adminService.listSagas(any())).thenReturn(new SagaPage<>(List.of(), null));
    app =
        Javalin.create(
            cfg -> {
              SagaSecurityHandler.register(cfg.routes, provider);
              ErrorMapper.register(cfg.routes);
              HealthResource.register(cfg.routes);
              SagaResource.register(
                  cfg.routes,
                  orchestrator,
                  0L,
                  new CompletableFuture<>(),
                  new SagaWaiterRegistry());
              SagaAdminResource.register(cfg.routes, orchestrator, 0L);
              // A route registered with no operation, to prove HEAD meets the same fail-closed
              // rejection of an untagged route as GET does.
              cfg.routes.get("/untagged", ctx -> ctx.result("should never be served"));
            });
    // Loopback, not the wildcard: a wildcard bind can share a port with another suite's
    // loopback server, which then takes the connection and answers this test's requests.
    app.start("127.0.0.1", 0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void head_healthProbe_answersLikeGetWithoutBody() throws Exception {
    // Arrange — a probe carries no credential that grants any role
    provider.granted = EnumSet.noneOf(SagaRole.class);

    // Act — a load balancer or uptime monitor probing liveness with HEAD
    HttpResponse<String> head = send("HEAD", "/health");
    HttpResponse<String> get = send("GET", "/health");

    // Assert — exempt from auth, with the GET's status and content type and no body
    assertThat(head.statusCode()).isEqualTo(200);
    assertThat(head.headers().firstValue("Content-Type"))
        .isPresent()
        .isEqualTo(get.headers().firstValue("Content-Type"));
    assertThat(head.body()).isEmpty();
  }

  @Test
  void head_existingSagaWithRequiredRole_runsGetHandlerAndAnswersLikeGet() throws Exception {
    // Arrange
    provider.granted = EnumSet.of(SagaRole.READ);

    // Act
    HttpResponse<String> head = send("HEAD", "/sagas/" + EXISTING);
    HttpResponse<String> get = send("GET", "/sagas/" + EXISTING);

    // Assert — the GET handler ran for the HEAD too, and HEAD mirrors GET without a body
    verify(orchestrator, times(2)).getStateSnapshot(EXISTING);
    assertThat(head.statusCode()).isEqualTo(200).isEqualTo(get.statusCode());
    assertThat(head.headers().firstValue("Content-Type"))
        .isPresent()
        .isEqualTo(get.headers().firstValue("Content-Type"));
    assertThat(get.body()).isNotEmpty();
    assertThat(head.body()).isEmpty();
  }

  @Test
  void head_missingSaga_returns404LikeGet() throws Exception {
    // Arrange
    provider.granted = EnumSet.of(SagaRole.READ);

    // Act
    HttpResponse<String> head = send("HEAD", "/sagas/" + MISSING);
    HttpResponse<String> get = send("GET", "/sagas/" + MISSING);

    // Assert — HEAD does not report a saga that does not exist
    assertThat(head.statusCode()).isEqualTo(404).isEqualTo(get.statusCode());
    assertThat(head.body()).isEmpty();
  }

  @Test
  void head_getRouteWithoutRequiredRole_returns403WithoutRunningHandler() throws Exception {
    // Arrange — authenticated, but without the READ role the route requires
    provider.granted = EnumSet.noneOf(SagaRole.class);

    // Act
    HttpResponse<String> response = send("HEAD", "/sagas/" + EXISTING);

    // Assert — HEAD is authorized against the GET route's operation, not let through
    assertThat(response.statusCode()).isEqualTo(403);
    verify(orchestrator, never()).getStateSnapshot(EXISTING);
  }

  @Test
  void head_withoutCredential_returns401WithoutRunningHandler() throws Exception {
    // Arrange — the caller presents no valid credential
    provider.authenticated = false;

    // Act
    HttpResponse<String> response = send("HEAD", "/sagas/" + EXISTING);

    // Assert — HEAD is authenticated like GET, before any handler runs
    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).getStateSnapshot(EXISTING);
  }

  @Test
  void head_adminListWithReadRoleOnly_returns403WithoutRunningHandler() throws Exception {
    // Arrange — READ is enough for the saga reads, not for the admin list
    provider.granted = EnumSet.of(SagaRole.READ);

    // Act
    HttpResponse<String> response = send("HEAD", "/sagas");

    // Assert
    assertThat(response.statusCode()).isEqualTo(403);
    verify(adminService, never()).listSagas(any());
  }

  @Test
  void head_adminListWithAdminRole_runsListHandlerAndAnswersLikeGet() throws Exception {
    // Arrange
    provider.granted = EnumSet.of(SagaRole.ADMIN);

    // Act
    HttpResponse<String> head = send("HEAD", "/sagas");
    HttpResponse<String> get = send("GET", "/sagas");

    // Assert — the list handler ran for the HEAD too, and HEAD mirrors GET without a body
    verify(adminService, times(2)).listSagas(any());
    assertThat(head.statusCode()).isEqualTo(200).isEqualTo(get.statusCode());
    assertThat(head.headers().firstValue("Content-Type"))
        .isPresent()
        .isEqualTo(get.headers().firstValue("Content-Type"));
    assertThat(head.body()).isEmpty();
  }

  @Test
  void head_untaggedRoute_returns500() throws Exception {
    // Act
    HttpResponse<String> response = send("HEAD", "/untagged");

    // Assert — an untagged route is a server-side bug, so HEAD fails closed like GET
    assertThat(response.statusCode()).isEqualTo(500);
  }

  @Test
  void get_untaggedRoute_returns500() throws Exception {
    // Act
    HttpResponse<String> response = send("GET", "/untagged");

    // Assert — fail-closed rejection (an untagged route is a server-side bug, so 500)
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body()).doesNotContain("should never be served");
  }

  private HttpResponse<String> send(String method, String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port() + path))
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build(),
        BodyHandlers.ofString());
  }

  /** A provider that authenticates callers, or refuses them, with the roles the test sets. */
  private static final class GrantingProvider implements SagaSecurityProvider {
    private volatile Set<SagaRole> granted = EnumSet.allOf(SagaRole.class);
    private volatile boolean authenticated = true;

    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      if (!authenticated) {
        throw new SagaAuthenticationException("no credential");
      }
      return SagaIdentity.of("test", granted);
    }

    @Override
    public String name() {
      return "test-granting";
    }
  }
}
