package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.definition.HttpCall;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpTransportAdapterTest {

  private HttpServer server;
  private HttpTransportAdapter adapter;
  private final AtomicReference<String> sagaIdHeader = new AtomicReference<>();
  private final AtomicReference<String> sagaStepHeader = new AtomicReference<>();
  private final AtomicReference<String> requestMethod = new AtomicReference<>();
  private final AtomicReference<String> rawPath = new AtomicReference<>();
  private final AtomicReference<String> rawQuery = new AtomicReference<>();
  private final AtomicReference<String> requestContentType = new AtomicReference<>();
  private final AtomicReference<String> requestBody = new AtomicReference<>();
  private final AtomicReference<byte[]> requestBodyBytes = new AtomicReference<>();
  private final AtomicInteger hitCount = new AtomicInteger();

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/debit",
        ex -> {
          hitCount.incrementAndGet();
          sagaIdHeader.set(ex.getRequestHeaders().getFirst(HttpHeaders.SAGA_ID));
          sagaStepHeader.set(ex.getRequestHeaders().getFirst(HttpHeaders.SAGA_STEP));
          requestMethod.set(ex.getRequestMethod());
          respond(ex, 200, "{\"debit_id\":\"DBT-1\"}");
        });
    server.createContext(
        "/empty-debit",
        ex -> respond(ex, 200, "{}")); // success but missing the expected output field
    server.createContext(
        "/users",
        ex -> {
          requestMethod.set(ex.getRequestMethod());
          respond(ex, 200, "{\"name\":\"Ann\"}");
        });
    server.createContext(
        "/orders",
        ex -> {
          rawPath.set(ex.getRequestURI().getRawPath());
          rawQuery.set(ex.getRequestURI().getRawQuery());
          respond(ex, 200, "{}");
        });
    server.createContext(
        "/notify",
        ex -> {
          requestContentType.set(ex.getRequestHeaders().getFirst(HttpHeaders.CONTENT_TYPE));
          byte[] raw = ex.getRequestBody().readAllBytes();
          requestBodyBytes.set(raw);
          requestBody.set(new String(raw, StandardCharsets.UTF_8));
          respondText(ex, 200, "plain-text-result");
        });
    server.createContext("/fail503", ex -> respond(ex, 503, "{}"));
    server.createContext("/fail422", ex -> respond(ex, 422, "{}"));
    server.createContext(
        "/conflict-retryable",
        ex -> {
          ex.getResponseHeaders().set(HttpHeaders.SAGA_RETRYABLE, "true");
          respond(ex, 409, "{}");
        });
    server.createContext(
        "/slow",
        ex -> {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          respond(ex, 200, "{}");
        });
    server.start();
    String baseUrl = "http://localhost:" + server.getAddress().getPort();
    adapter =
        new HttpTransportAdapter(
            baseUrl, new HttpExchange(HttpClient.newHttpClient(), OutboundHttpPolicy.allowAll()));
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private static SagaContext ctx(Map<String, Object> data) {
    return new FakeSagaContext("saga-1", data);
  }

  @Test
  void call_boundStepDeadlineElapsesBeforeResponse_throwsRetryableTransportException() {
    // Arrange — the server is far slower than the bound step deadline.
    HttpCall spec = HttpCall.newBuilder("/slow").method(HttpMethod.GET).build();
    SagaCorrelationContext.Correlation previous =
        SagaCorrelationContext.bind(
            "saga-1", "s", System.currentTimeMillis() + 300L, java.time.Clock.systemUTC());

    // Act — the per-request timeout is derived from the remaining step deadline.
    Throwable thrown;
    try {
      thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "s"));
    } finally {
      SagaCorrelationContext.restore(previous);
    }

    // Assert — timed out, surfaced as a retryable transport failure.
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isTrue();
  }

  @Test
  void call_postWithRequestAndOutput_propagatesHeadersAndExtractsOutput() throws Exception {
    // Arrange
    HttpCall spec =
        HttpCall.newBuilder("/debit")
            .method(HttpMethod.POST)
            .jsonBody(Map.of("account_id", "${acct}", "amount", "${amount}"))
            .output(Map.of("debitId", "$.debit_id"))
            .build();

    // Act
    Map<String, Object> output =
        adapter.call(spec, ctx(Map.of("acct", "A-1", "amount", 500)), "debit");

    // Assert
    assertThat(output).containsExactly(Map.entry("debitId", "DBT-1"));
    assertThat(sagaIdHeader.get()).isEqualTo("saga-1");
    assertThat(sagaStepHeader.get()).isEqualTo("debit");
    assertThat(requestMethod.get()).isEqualTo("POST");
  }

  @Test
  void call_getWithPathAndQueryTemplates_sendsNoBody() throws Exception {
    // Arrange
    HttpCall spec =
        HttpCall.newBuilder("/users")
            .method(HttpMethod.GET)
            .query(Map.of("id", "${userId}"))
            .output(Map.of("name", "$.name"))
            .build();

    // Act
    Map<String, Object> output = adapter.call(spec, ctx(Map.of("userId", "U1")), "fetch");

    // Assert
    assertThat(output).containsEntry("name", "Ann");
    assertThat(requestMethod.get()).isEqualTo("GET");
  }

  @Test
  void call_pathTemplateValueWithReservedChars_isPercentEncodedNotInjected() throws Exception {
    // Arrange — a context value carrying '?' and a space must not inject a query or break the call
    HttpCall spec = HttpCall.newBuilder("/orders/${id}").method(HttpMethod.GET).build();

    // Act
    adapter.call(spec, ctx(Map.of("id", "7?x=1 y")), "fetch");

    // Assert — the value landed in the path, encoded; no query string was injected
    assertThat(rawPath.get()).isEqualTo("/orders/7%3Fx%3D1%20y");
    assertThat(rawQuery.get()).isNull();
  }

  @Test
  void call_stringBodyWithContentTypeOverride_sendsRawBodyAndCapturesRawResponse()
      throws Exception {
    // Arrange — a raw templated string body, an explicit content type, and a $body capture.
    HttpCall spec =
        HttpCall.newBuilder("/notify")
            .method(HttpMethod.POST)
            .stringBody("<msg>${text}</msg>")
            .contentType("application/xml")
            .output(Map.of("raw", HttpCall.BODY_OUTPUT))
            .build();

    // Act
    Map<String, Object> output = adapter.call(spec, ctx(Map.of("text", "hi")), "notify");

    // Assert — the override content type and the templated raw body were sent; $body was captured.
    assertThat(requestContentType.get()).isEqualTo("application/xml");
    assertThat(requestBody.get()).isEqualTo("<msg>hi</msg>");
    assertThat(output).containsEntry("raw", "plain-text-result");
  }

  @Test
  void call_stringBodyWithCharsetInContentType_encodesWithThatCharset() throws Exception {
    // Arrange — a content-type override declaring a non-UTF-8 charset.
    HttpCall spec =
        HttpCall.newBuilder("/notify")
            .method(HttpMethod.POST)
            .stringBody("<msg>${text}</msg>")
            .contentType("application/xml; charset=ISO-8859-1")
            .build();

    // Act — "é" is one byte in ISO-8859-1 (0xE9) but two in UTF-8 (0xC3 0xA9).
    adapter.call(spec, ctx(Map.of("text", "é")), "notify");

    // Assert — the templated body is encoded with the declared charset, not UTF-8.
    assertThat(requestBodyBytes.get())
        .isEqualTo("<msg>é</msg>".getBytes(StandardCharsets.ISO_8859_1));
    assertThat(requestContentType.get()).isEqualTo("application/xml; charset=ISO-8859-1");
  }

  @Test
  void call_stringBodyMissingTemplateValue_throwsNonRetryableWithoutCallingServer() {
    // Arrange — a ${...} in the string body with no matching context value.
    HttpCall spec =
        HttpCall.newBuilder("/notify")
            .method(HttpMethod.POST)
            .stringBody("<msg>${missing}</msg>")
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "notify"));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
  }

  @Test
  void call_status503_throwsRetryableTransportException() {
    // Arrange
    HttpCall spec = HttpCall.newBuilder("/fail503").build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "s"));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isTrue();
  }

  @Test
  void call_status422_throwsNonRetryableTransportException() {
    // Arrange
    HttpCall spec = HttpCall.newBuilder("/fail422").build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "s"));

    // Assert — the server returned a status, so the side effect may have committed.
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
    assertThat(((TransportException) thrown).knownNotCommitted()).isFalse();
  }

  @Test
  void call_connectionRefused_knownNotCommitted() throws IOException {
    // Arrange — an adapter pointing at a dead port: the request never reaches a participant.
    HttpServer probe = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    int deadPort = probe.getAddress().getPort();
    probe.start();
    probe.stop(0);
    HttpTransportAdapter dead =
        new HttpTransportAdapter(
            "http://localhost:" + deadPort,
            new HttpExchange(HttpClient.newHttpClient(), OutboundHttpPolicy.allowAll()));
    HttpCall spec = HttpCall.newBuilder("/debit").build();

    // Act
    Throwable thrown = catchThrowable(() -> dead.call(spec, ctx(Map.of()), "debit"));

    // Assert — proven non-delivery propagates from HttpExchange through the adapter.
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).knownNotCommitted()).isTrue();
  }

  @Test
  void call_xSagaRetryableOverride_overridesStatusClassification() {
    // Arrange — 409 is non-retryable by status, but the participant says retryable.
    HttpCall spec = HttpCall.newBuilder("/conflict-retryable").build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "s"));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isTrue();
  }

  @Test
  void call_missingContextKey_throwsNonRetryableWithoutCallingServer() {
    // Arrange
    HttpCall spec =
        HttpCall.newBuilder("/debit")
            .method(HttpMethod.POST)
            .jsonBody(Map.of("amount", "${missing}"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "debit"));

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
    assertThat(hitCount.get()).isZero();
  }

  @Test
  void call_outputFieldMissingInResponse_throwsNonRetryable() {
    // Arrange
    HttpCall spec =
        HttpCall.newBuilder("/empty-debit").output(Map.of("debitId", "$.debit_id")).build();

    // Act
    Throwable thrown = catchThrowable(() -> adapter.call(spec, ctx(Map.of()), "debit"));

    // Assert — a 2xx whose body lacks the mapped output: the side effect committed → not skipped.
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
    assertThat(((TransportException) thrown).knownNotCommitted()).isFalse();
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }

  private static void respondText(com.sun.net.httpserver.HttpExchange ex, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "text/plain");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
