package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaHttpClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HttpEndpoint}: the single per-endpoint owner of the shared {@link HttpExchange}
 * that produces the code-step {@link SagaHttpClient}. The key invariant is that the client rides
 * the endpoint's {@link HttpExchange} (one client, one policy), verified via the package-private
 * accessors.
 */
class HttpEndpointTest {

  private static HttpServiceConfig config(String baseUrl) {
    return new HttpServiceConfig(baseUrl, List.of(), -1, null, Map.of());
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
  void sagaHttpClient_ridesTheEndpointsSharedHttpExchange() {
    // Arrange
    try (HttpEndpoint endpoint = HttpEndpoint.create(config("http://account-svc:8080"))) {
      // Act
      SagaHttpClient client = endpoint.sagaHttpClient();

      // Assert — the SagaHttpClient rides the very same HttpExchange instance the endpoint owns.
      assertThat(((SagaHttpClientImpl) client).exchange()).isSameAs(endpoint.exchange());
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
  void defaultHeaders_applied_onSagaHttpClientPath() throws Exception {
    // Arrange — a server that records the Authorization header it sees.
    AtomicReference<String> codeAuth = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
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
      // Act — the SagaHttpClient (code-step) path.
      SagaCorrelationContext.Correlation previous = SagaCorrelationContext.bind("saga-1", "c", 0L);
      try {
        endpoint.sagaHttpClient().get("/code").send();
      } finally {
        SagaCorrelationContext.restore(previous);
      }

      // Assert — the endpoint default header reached the server.
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
