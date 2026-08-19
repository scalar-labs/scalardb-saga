package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HttpEndpoint}: the single per-endpoint owner of the shared {@link HttpExchange}
 * that produces both the code-step {@link SagaHttpClient} and the declarative {@link Step}/{@link
 * TccStep}. The key invariant is that both front-ends ride the SAME {@link HttpExchange} (one
 * client, one policy), verified directly via the package-private accessors.
 */
class HttpEndpointTest {

  private static final Map<Phase, CallSpec> SAGA_PHASES =
      Map.of(
          Phase.EXECUTION, HttpCall.newBuilder("/do").build(),
          Phase.COMPENSATION, HttpCall.newBuilder("/undo").build());
  private static final Map<Phase, CallSpec> TCC_PHASES =
      Map.of(
          Phase.RESERVATION, HttpCall.newBuilder("/reserve").build(),
          Phase.CONFIRMATION, HttpCall.newBuilder("/confirm").build(),
          Phase.CANCELLATION, HttpCall.newBuilder("/cancel").build());

  private static HttpServiceConfig config(String baseUrl) {
    return new HttpServiceConfig(baseUrl, List.of(), -1, null, Map.of());
  }

  private static Map<Phase, CallSpec> asyncSagaPhases() {
    return Map.of(
        Phase.EXECUTION, HttpCall.newBuilder("/do").async(true).build(),
        Phase.COMPENSATION, HttpCall.newBuilder("/undo").build());
  }

  @Test
  void toStep_asyncPhaseWithoutCallbackProvider_throwsSagaDefinitionException() {
    // An async step with no callback URL provider configured cannot be provisioned — fail fast at
    // plan build rather than parking a saga that could never be completed. The step-scoped code,
    // with the step name as a real metadata field: this site knows the step but not the saga, so
    // INVALID_DEFINITION would force it to fake the required saga_name.
    HttpEndpoint endpoint = HttpEndpoint.create(config("http://svc:8080"));

    assertThatThrownBy(() -> endpoint.toStep("debit", asyncSagaPhases()))
        .isInstanceOfSatisfying(
            SagaDefinitionException.class,
            e -> {
              assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_STEP_DEFINITION);
              assertThat(e.getMetadata()).containsEntry("step_name", "debit");
            });
  }

  @Test
  void toStep_asyncPhaseWithCallbackProvider_succeeds() {
    HttpEndpoint endpoint =
        HttpEndpoint.create(config("http://svc:8080"), (sagaId, step) -> "http://cb/x");

    assertThatCode(() -> endpoint.toStep("debit", asyncSagaPhases())).doesNotThrowAnyException();
  }

  @Test
  void create_suppliedRedirectFollowingClientWithAllowlist_throws() {
    // Arrange — an allowlist plus a supplied client that follows redirects (an SSRF-bypass risk).
    HttpClient redirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
    HttpServiceConfig cfg =
        new HttpServiceConfig("http://svc:8080", List.of("svc"), -1, redirecting, Map.of());

    // Act & Assert
    assertThatThrownBy(() -> HttpEndpoint.create(cfg)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void create_suppliedRedirectNeverClientWithAllowlist_ok() {
    // Arrange — a supplied client that does not follow redirects is fine with an allowlist.
    HttpClient nonRedirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    HttpServiceConfig cfg =
        new HttpServiceConfig("http://svc:8080", List.of("svc"), -1, nonRedirecting, Map.of());

    // Act & Assert
    assertThatCode(() -> HttpEndpoint.create(cfg).close()).doesNotThrowAnyException();
  }

  @Test
  void create_suppliedRedirectFollowingClientNoAllowlist_ok() {
    // Arrange — with no allowlist there is nothing to bypass, so a redirect-following client is
    // fine.
    HttpClient redirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
    HttpServiceConfig cfg =
        new HttpServiceConfig("http://svc:8080", List.of(), -1, redirecting, Map.of());

    // Act & Assert
    assertThatCode(() -> HttpEndpoint.create(cfg).close()).doesNotThrowAnyException();
  }

  @Test
  void sagaHttpClientAndDeclarativeStep_sameEndpoint_shareOneHttpExchange() {
    // Arrange
    try (HttpEndpoint endpoint = HttpEndpoint.create(config("http://account-svc:8080"))) {
      // Act
      SagaHttpClient client = endpoint.sagaHttpClient();
      endpoint.toStep("debit", SAGA_PHASES); // returns a step backed by the shared adapter

      // Assert — the SagaHttpClient, the declarative transport adapter, and the endpoint all ride
      // the very same HttpExchange instance (and therefore the same policy).
      HttpExchange shared = endpoint.exchange();
      assertThat(((SagaHttpClientImpl) client).exchange()).isSameAs(shared);
      assertThat(((HttpTransportAdapter) endpoint.transportAdapter()).exchange()).isSameAs(shared);
    }
  }

  @Test
  void sagaHttpClientAndDeclarativeStep_sameEndpoint_shareOnePolicy() {
    // Arrange
    try (HttpEndpoint endpoint = HttpEndpoint.create(config("http://account-svc:8080"))) {
      // Act
      HttpExchange clientExchange = ((SagaHttpClientImpl) endpoint.sagaHttpClient()).exchange();
      HttpExchange adapterExchange =
          ((HttpTransportAdapter) endpoint.transportAdapter()).exchange();

      // Assert — both front-ends classify through the same OutboundHttpPolicy instance.
      assertThat(clientExchange.policy()).isSameAs(adapterExchange.policy());
    }
  }

  @Test
  void create_withMaxBodyBytes_buildsPerEndpointPolicyFromConfig() {
    // Arrange — a non-default body limit must flow from the config into the endpoint's policy.
    HttpServiceConfig config =
        new HttpServiceConfig(
            "http://account-svc:8080", List.of("account-svc"), 4096, null, Map.of());

    // Act
    try (HttpEndpoint endpoint = HttpEndpoint.create(config)) {
      // Assert
      assertThat(endpoint.exchange().policy().maxBodyBytes()).isEqualTo(4096);
    }
  }

  @Test
  void toStep_withSagaPhases_returnsNamedStep() {
    // Arrange
    try (HttpEndpoint endpoint = HttpEndpoint.create(config("http://account-svc:8080"))) {
      // Act
      Step step = endpoint.toStep("debit", SAGA_PHASES);

      // Assert
      assertThat(step.getName()).isEqualTo("debit");
    }
  }

  @Test
  void toTccStep_withTccPhases_returnsNamedTccStep() {
    // Arrange
    try (HttpEndpoint endpoint = HttpEndpoint.create(config("http://booking-svc:8080"))) {
      // Act
      TccStep step = endpoint.toTccStep("seat", TCC_PHASES);

      // Assert
      assertThat(step.getName()).isEqualTo("seat");
    }
  }

  @Test
  void defaultHeaders_applied_onBothDeclarativeAndSagaHttpClientPaths() throws Exception {
    // Arrange — a server that records the Authorization header it sees on each request.
    AtomicReference<String> declarativeAuth = new AtomicReference<>();
    AtomicReference<String> codeAuth = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/declarative",
        ex -> {
          declarativeAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
          respondJson(ex);
        });
    server.createContext(
        "/code",
        ex -> {
          codeAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
          respondJson(ex);
        });
    server.start();
    String baseUrl = "http://localhost:" + server.getAddress().getPort();
    HttpServiceConfig config =
        new HttpServiceConfig(
            baseUrl, List.of(), -1, null, Map.of("Authorization", "Bearer secret"));

    try (HttpEndpoint endpoint = HttpEndpoint.create(config)) {
      // Act — the declarative transport adapter path.
      SagaContext context = new FakeSagaContext("saga-1", Map.of());
      endpoint
          .transportAdapter()
          .call(HttpCall.newBuilder("/declarative").method(HttpMethod.GET).build(), context, "d");

      // Act — the SagaHttpClient (code-step) path, both riding the same exchange.
      SagaCorrelationContext.Correlation previous =
          SagaCorrelationContext.bind("saga-1", "c", 0L, Clock.systemUTC());
      try {
        endpoint.sagaHttpClient().get("/code").send();
      } finally {
        SagaCorrelationContext.restore(previous);
      }

      // Assert — the endpoint default header reached the server on BOTH front-ends.
      assertThat(declarativeAuth.get()).isEqualTo("Bearer secret");
      assertThat(codeAuth.get()).isEqualTo("Bearer secret");
    } finally {
      server.stop(0);
    }
  }

  private static void respondJson(com.sun.net.httpserver.HttpExchange ex) throws IOException {
    byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Test
  void close_frameworkCreatedClient_doesNotThrow() {
    // Act & Assert — a framework-created client is owned and closed here.
    assertThatCode(() -> HttpEndpoint.create(config("http://a:8080")).close())
        .doesNotThrowAnyException();
  }

  @Test
  void close_callerSuppliedClient_leavesItOpen() {
    // Arrange — a caller-supplied client is NOT owned by the endpoint.
    HttpClient supplied = HttpClient.newHttpClient();
    HttpServiceConfig config =
        new HttpServiceConfig("http://a:8080", List.of(), -1, supplied, Map.of());

    // Act
    HttpEndpoint.create(config).close();

    // Assert — still usable because the endpoint left it open (a closed client rejects new requests
    // by throwing; sending to an unroutable URI here would otherwise surface a different error).
    assertThat(supplied.followRedirects()).isNotNull();
  }
}
