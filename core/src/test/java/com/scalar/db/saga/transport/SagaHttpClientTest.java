package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.SagaHttpResponse;
import com.scalar.db.saga.exception.StepExecutionException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the code-step {@link SagaHttpClient} ({@link SagaHttpClientImpl}) against a local
 * {@link HttpServer} (the same harness as {@code HttpExchangeTest}). Covers the D4 throw-by-default
 * contract, the {@code sendRaw()} escape, policy enforcement on both, and binary bodies.
 */
class SagaHttpClientTest {

  private HttpServer server;
  private String baseUrl;
  private final AtomicReference<String> capturedMethod = new AtomicReference<>();
  private final AtomicReference<String> capturedQuery = new AtomicReference<>();
  private final AtomicReference<String> capturedContentType = new AtomicReference<>();
  private final AtomicReference<String> capturedBody = new AtomicReference<>();
  private final AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();

  /** A typed body for {@link SagaHttpResponse#bodyJson(Class)}. */
  public record Payload(String k) {}

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/ok", ex -> respond(ex, 200, "{\"k\":\"v\"}", Map.of()));
    server.createContext("/fail422", ex -> respond(ex, 422, "{\"error\":\"bad\"}", Map.of()));
    server.createContext("/fail503", ex -> respond(ex, 503, "{}", Map.of()));
    server.createContext(
        "/retryable409", ex -> respond(ex, 409, "{}", Map.of("X-Saga-Retryable", "true")));
    server.createContext(
        "/bytes",
        ex -> {
          byte[] body = {1, 2, 3, 4};
          ex.getResponseHeaders().add("Content-Type", "application/octet-stream");
          ex.sendResponseHeaders(200, body.length);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
          }
        });
    server.createContext(
        "/redirect",
        ex -> {
          ex.getResponseHeaders().add("Location", "http://example.invalid/elsewhere");
          ex.sendResponseHeaders(302, -1);
          ex.close();
        });
    server.createContext(
        "/slow",
        ex -> {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          respond(ex, 200, "{}", Map.of());
        });
    server.createContext(
        "/echo",
        ex -> {
          capturedMethod.set(ex.getRequestMethod());
          capturedQuery.set(ex.getRequestURI().getRawQuery());
          capturedContentType.set(ex.getRequestHeaders().getFirst("Content-Type"));
          // Case-insensitive: HttpServer normalizes header-name case (e.g. X-Trace -> X-trace).
          Map<String, String> hdrs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
          ex.getRequestHeaders().forEach((k, v) -> hdrs.put(k, v.isEmpty() ? "" : v.get(0)));
          capturedHeaders.set(hdrs);
          capturedBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(ex, 200, "{\"k\":\"v\"}", Map.of("X-Resp", "rv"));
        });
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private SagaHttpClient client(OutboundHttpPolicy policy) {
    HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    return new SagaHttpClientImpl(new HttpExchange(http, policy), baseUrl);
  }

  @Test
  void send_boundStepDeadlineElapsesBeforeResponse_throwsRetryable() {
    // Arrange — the server is far slower than the bound step deadline.
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());
    SagaCorrelationContext.Correlation previous =
        SagaCorrelationContext.bind("saga-1", "s", System.currentTimeMillis() + 300L);

    // Act — the per-request timeout is derived from the remaining step deadline.
    Throwable thrown;
    try {
      thrown = catchThrowable(() -> client.get("/slow").send());
    } finally {
      SagaCorrelationContext.restore(previous);
    }

    // Assert — timed out, surfaced as a retryable step failure.
    assertThat(thrown).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) thrown).isRetryable()).isTrue();
  }

  @Test
  void send_2xx_returnsResponse() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    SagaHttpResponse response = client.post("/ok").jsonBody(Map.of("a", "b")).send();

    // Assert
    assertThat(response.status()).isEqualTo(200);
    assertThat(response.bodyJsonObject()).containsEntry("k", "v");
  }

  @Test
  void send_4xx_throwsNonRetryableStepExecutionException() {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    Throwable t = catchThrowable(() -> client.get("/fail422").send());

    // Assert
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void send_5xx_throwsRetryableStepExecutionException() {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    Throwable t = catchThrowable(() -> client.get("/fail503").send());

    // Assert
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void send_xSagaRetryableOverride_makesNonRetryableStatusRetryable() {
    // Arrange — 409 is normally non-retryable, but the header overrides it to retryable
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    Throwable t = catchThrowable(() -> client.get("/retryable409").send());

    // Assert
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void sendRaw_nonSuccessStatus_returnsResponse() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    SagaHttpResponse response = client.get("/fail422").sendRaw();

    // Assert — a received non-2xx does not throw under sendRaw()
    assertThat(response.status()).isEqualTo(422);
    assertThat(response.bodyJsonObject()).containsEntry("error", "bad");
  }

  @Test
  void sendRaw_bodyExceedsLimit_throwsNonRetryable() {
    // Arrange — a 1-byte body limit; the /ok response is larger
    OutboundHttpPolicy policy = OutboundHttpPolicy.newBuilder().maxBodyBytes(1).build();
    SagaHttpClient client = client(policy);

    // Act
    Throwable t = catchThrowable(() -> client.get("/ok").sendRaw());

    // Assert — the body limit is enforced even for sendRaw(); a policy violation is non-retryable
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void sendRaw_disallowedHost_throwsNonRetryable() {
    // Arrange — allowlist excludes localhost
    OutboundHttpPolicy policy = OutboundHttpPolicy.newBuilder().allowedHosts("other-host").build();
    SagaHttpClient client = client(policy);

    // Act
    Throwable t = catchThrowable(() -> client.get("/ok").sendRaw());

    // Assert — SSRF allowlist is enforced even for sendRaw()
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isFalse();
  }

  @Test
  void sendRaw_redirectResponse_isNotFollowed() throws Exception {
    // Arrange — the framework client uses Redirect.NEVER (via HttpEndpoint)
    SagaHttpClient client =
        HttpEndpoint.create(new HttpServiceConfig(baseUrl, List.of(), -1, null, Map.of()))
            .sagaHttpClient();

    // Act
    SagaHttpResponse response = client.get("/redirect").sendRaw();

    // Assert — the 302 is returned, not followed to the disallowed Location host
    assertThat(response.status()).isEqualTo(302);
    assertThat(response.header("Location")).contains("http://example.invalid/elsewhere");
  }

  @Test
  void send_binaryBodyInAndBytesOut_roundTrips() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());
    byte[] payload = {10, 20, 30};

    // Act
    SagaHttpResponse response =
        client.post("/bytes").bytesBody(payload, "application/octet-stream").send();

    // Assert — a byte[] request body is accepted and a byte[] response body is read back
    assertThat(response.bodyBytes()).containsExactly(1, 2, 3, 4);
  }

  @Test
  void send_transportFailure_throwsRetryable() throws IOException {
    // Arrange — bind then release a port so nothing is listening
    HttpServer probe = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    int deadPort = probe.getAddress().getPort();
    probe.start();
    probe.stop(0);
    SagaHttpClient client =
        new SagaHttpClientImpl(
            new HttpExchange(
                HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
                OutboundHttpPolicy.allowAll()),
            "http://localhost:" + deadPort);

    // Act
    Throwable t = catchThrowable(() -> client.get("/x").send());

    // Assert — an IO error always throws and is retryable, regardless of send vs sendRaw
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void sendRaw_transportFailure_throwsRetryable() throws IOException {
    // Arrange
    HttpServer probe = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    int deadPort = probe.getAddress().getPort();
    probe.start();
    probe.stop(0);
    SagaHttpClient client =
        new SagaHttpClientImpl(
            new HttpExchange(
                HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
                OutboundHttpPolicy.allowAll()),
            "http://localhost:" + deadPort);

    // Act
    Throwable t = catchThrowable(() -> client.get("/x").sendRaw());

    // Assert — sendRaw() still throws on a transport failure (no response received)
    assertThat(t).isInstanceOf(StepExecutionException.class);
    assertThat(((StepExecutionException) t).isRetryable()).isTrue();
  }

  @Test
  void send_calledTwice_throwsIllegalStateException() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());
    SagaHttpClient.Request request = client.get("/ok");
    request.send();

    // Act
    Throwable t = catchThrowable(request::send);

    // Assert
    assertThat(t).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void jsonBody_twice_throwsIllegalStateException() {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());
    SagaHttpClient.Request request = client.post("/ok").jsonBody(Map.of("a", 1));

    // Act
    Throwable t = catchThrowable(() -> request.stringBody("x", "text/plain"));

    // Assert — at most one body may be set
    assertThat(t).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patch_sendsPatchMethod() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.patch("/echo").jsonBody(Map.of("a", 1)).send();

    // Assert
    assertThat(capturedMethod.get()).isEqualTo("PATCH");
  }

  @Test
  void header_singleHeader_propagatesToRequest() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.get("/echo").header("X-Trace", "abc").send();

    // Assert
    assertThat(capturedHeaders.get()).containsEntry("X-Trace", "abc");
  }

  @Test
  void headers_mapOfHeaders_allPropagateToRequest() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.get("/echo").headers(Map.of("X-Alpha", "1", "X-Beta", "2")).send();

    // Assert
    assertThat(capturedHeaders.get()).containsEntry("X-Alpha", "1").containsEntry("X-Beta", "2");
  }

  @Test
  void query_singleParam_appendedToUrl() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.get("/echo").query("a", "1").send();

    // Assert
    assertThat(capturedQuery.get()).isEqualTo("a=1");
  }

  @Test
  void query_mapOfParams_allAppendedToUrl() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.get("/echo").query(Map.of("a", "1", "b", "2")).send();

    // Assert
    assertThat(capturedQuery.get()).contains("a=1").contains("b=2");
  }

  @Test
  void formBody_urlEncodesBodyAndSetsContentType() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    client.post("/echo").formBody(Map.of("q", "a b&c")).send();

    // Assert — urlencoded body (space -> '+', '&' -> %26) with the form content type.
    assertThat(capturedBody.get()).isEqualTo("q=a+b%26c");
    assertThat(capturedContentType.get()).isEqualTo("application/x-www-form-urlencoded");
  }

  @Test
  void responseHeaders_returnsResponseHeaderMultimap() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act
    SagaHttpResponse response = client.get("/echo").send();

    // Assert — the response header set by the server is present (header names are
    // case-insensitive).
    Map<String, List<String>> headers = response.headers();
    String key =
        headers.keySet().stream()
            .filter(k -> k.equalsIgnoreCase("X-Resp"))
            .findFirst()
            .orElseThrow();
    assertThat(headers.get(key)).contains("rv");
  }

  @Test
  void bodyJson_typed_deserializesResponseBody() throws Exception {
    // Arrange
    SagaHttpClient client = client(OutboundHttpPolicy.allowAll());

    // Act — /echo returns {"k":"v"}, decoded into the typed Payload.
    SagaHttpResponse response = client.get("/echo").send();
    Payload payload = response.bodyJson(Payload.class);

    // Assert
    assertThat(payload.k()).isEqualTo("v");
  }

  private static void respond(
      com.sun.net.httpserver.HttpExchange ex, int status, String body, Map<String, String> headers)
      throws IOException {
    headers.forEach((name, value) -> ex.getResponseHeaders().add(name, value));
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
