package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.server.SagaWaiterRegistry;
import com.scalar.db.saga.server.security.SagaAuthRequest;
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
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins HEAD handling across the real REST routes, end-to-end against a live Javalin dispatch.
 *
 * <p>Javalin answers a HEAD to a GET route from the GET handler and gives it the GET route's roles,
 * so a HEAD is served, authenticated and authorized exactly like its GET. This locks that in at
 * runtime, which {@code TransportPolicyParityTest} cannot, since it inspects registered endpoint
 * metadata rather than per-method dispatch.
 */
class HeadMethodTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private final GrantingProvider provider = new GrantingProvider();
  private Javalin app;

  @BeforeEach
  void setUp() {
    app =
        Javalin.create(
            cfg -> {
              SagaSecurityHandler.register(cfg.routes, provider);
              ErrorMapper.register(cfg.routes);
              HealthResource.register(cfg.routes);
              SagaResource.register(
                  cfg.routes,
                  mock(DefaultSagaOrchestrator.class),
                  0L,
                  new java.util.concurrent.CompletableFuture<>(),
                  new SagaWaiterRegistry());
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
  void head_healthProbe_returns200() throws Exception {
    // Arrange — a probe carries no credential that grants any role
    provider.granted = EnumSet.noneOf(SagaRole.class);

    // Act — a load balancer or uptime monitor probing liveness with HEAD
    HttpResponse<String> response = send("HEAD", "/health");

    // Assert — served like GET, exempt from auth, with the body stripped for HEAD
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEmpty();
  }

  @Test
  void head_getRouteWithRequiredRole_isServedLikeGet() throws Exception {
    // Arrange
    provider.granted = EnumSet.of(SagaRole.READ);

    // Act — HEAD on a route that only registers GET
    HttpResponse<String> response = send("HEAD", "/sagas/saga-1");

    // Assert — the GET handler answers, with the body stripped for HEAD
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEmpty();
  }

  @Test
  void head_getRouteWithoutRequiredRole_returns403() throws Exception {
    // Arrange — authenticated, but without the READ role the GET route requires
    provider.granted = EnumSet.noneOf(SagaRole.class);

    // Act
    HttpResponse<String> response = send("HEAD", "/sagas/saga-1");

    // Assert — HEAD is authorized against the GET route's operation, not let through
    assertThat(response.statusCode()).isEqualTo(403);
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

  /** A provider that authenticates every caller with the roles the test sets. */
  private static final class GrantingProvider implements SagaSecurityProvider {
    private volatile Set<SagaRole> granted = EnumSet.allOf(SagaRole.class);

    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      return SagaIdentity.of("test", granted);
    }

    @Override
    public String name() {
      return "test-granting";
    }
  }
}
