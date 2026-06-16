package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.saga.api.HttpCall;
import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaContext;
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
          requestBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
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

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
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

    // Assert
    assertThat(thrown).isInstanceOf(TransportException.class);
    assertThat(((TransportException) thrown).isRetryable()).isFalse();
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
