package com.scalar.db.saga.daemon.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.daemon.security.SagaAuthRequest;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.EnumSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins HEAD handling across the real REST routes, end-to-end against a live Javalin dispatch.
 *
 * <p>Regression guard: enforcing security on {@code beforeMatched} (rather than the previous
 * verb-keyed {@code before} handler that mapped HEAD to the GET policy) means a HEAD to a GET-only
 * route lands in Javalin's resource-handler branch with no roles, which without care rendered a
 * misleading {@code 500}. This locks in the intended behavior — {@code /health} serves HEAD ({@code
 * 200}) for infrastructure probes; every other GET route answers HEAD with an honest {@code 405
 * Allow: GET} — which {@code TransportPolicyParityTest} cannot catch, since it inspects registered
 * endpoint metadata rather than runtime per-method dispatch.
 */
class HeadMethodTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;

  @BeforeEach
  void setUp() {
    app = Javalin.create();
    SagaSecurityHandler.register(app, new FullAccessProvider());
    ErrorMapper.register(app);
    HealthResource.register(app);
    SagaResource.register(app, mock(SagaOrchestrator.class), 0L);
    // A route registered with no operation, to prove the HEAD branch did not weaken the fail-closed
    // rejection of an untagged route on its normal (GET) path.
    app.get("/untagged", ctx -> ctx.result("should never be served"));
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void head_healthProbe_returns200() throws Exception {
    // Act — a load balancer or uptime monitor probing liveness with HEAD
    HttpResponse<String> response = send("HEAD", "/health");

    // Assert — served like GET, with the body stripped for HEAD
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEmpty();
  }

  @Test
  void head_getOnlyRoute_returns405WithAllowGet() throws Exception {
    // Act — HEAD on a route that only registers GET
    HttpResponse<String> response = send("HEAD", "/sagas/saga-1");

    // Assert — an honest 405 with Allow: GET, not a misleading 500
    assertThat(response.statusCode()).isEqualTo(405);
    assertThat(response.headers().firstValue("Allow")).hasValue("GET");
  }

  @Test
  void get_untaggedRoute_stillReturns500() throws Exception {
    // Act — the HEAD branch only diverts HEAD; a GET to an untagged route must still fail closed
    HttpResponse<String> response = send("GET", "/untagged");

    // Assert — fail-closed rejection unchanged (an untagged route is a server-side bug, so 500)
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body()).doesNotContain("should never be served");
  }

  private HttpResponse<String> send(String method, String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build(),
        BodyHandlers.ofString());
  }

  /** A provider that grants full access; the tested paths short-circuit before it is consulted. */
  private static final class FullAccessProvider implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      return SagaIdentity.of("test", EnumSet.allOf(SagaRole.class));
    }

    @Override
    public String name() {
      return "test-full-access";
    }
  }
}
