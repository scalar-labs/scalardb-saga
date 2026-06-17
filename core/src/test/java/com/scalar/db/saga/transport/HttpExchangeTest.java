package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests for the shared {@link com.scalar.db.saga.transport.HttpExchange} contract —
 * especially the failure paths. Both {@code HttpTransportAdapter} and the code-step {@code
 * SagaHttpClient} depend on this behavior.
 */
class HttpExchangeTest {

  private static final List<Map.Entry<String, String>> NO_PARAMS = List.of();

  private HttpServer server;
  private String baseUrl;
  private com.scalar.db.saga.transport.HttpExchange exchange;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/ok", ex -> respond(ex, 200, "{\"k\":\"v\"}"));
    server.createContext(
        "/empty",
        ex -> {
          ex.sendResponseHeaders(200, -1); // 2xx with no body
          ex.close();
        });
    server.createContext("/malformed", ex -> respond(ex, 200, "not json"));
    server.createContext("/fail503", ex -> respond(ex, 503, "{}"));
    server.createContext("/fail422", ex -> respond(ex, 422, "{}"));
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    exchange =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.allowAll());
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void exchange_withDefaultHeaders_sendsThemAndCallerHeaderOverrides() throws Exception {
    // Arrange — a server that echoes the X-Custom header it received.
    java.util.concurrent.atomic.AtomicReference<String> received =
        new java.util.concurrent.atomic.AtomicReference<>();
    server.createContext(
        "/headers",
        ex -> {
          received.set(ex.getRequestHeaders().getFirst("X-Custom"));
          respond(ex, 200, "{}");
        });
    com.scalar.db.saga.transport.HttpExchange withDefaults =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(),
            OutboundHttpPolicy.allowAll(),
            Map.of("X-Custom", "default-value"));

    // Act — no per-call header: the default is sent.
    withDefaults.exchange(
        "GET", baseUrl, "/headers", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null);

    // Assert
    assertThat(received.get()).isEqualTo("default-value");

    // Act — a per-call header of the same name overrides the default (case-insensitive).
    withDefaults.exchange(
        "GET",
        baseUrl,
        "/headers",
        NO_PARAMS,
        List.of(Map.entry("x-custom", "call-value")),
        null,
        null,
        "saga-1",
        "s",
        null);

    // Assert — the caller value wins; the default is not also appended.
    assertThat(received.get()).isEqualTo("call-value");
  }

  @Test
  void exchange_2xxJsonObject_returnsDecodableResponse() throws Exception {
    HttpCallResponse response =
        exchange.exchange(
            "POST",
            baseUrl,
            "/ok",
            NO_PARAMS,
            NO_PARAMS,
            exchange.encodeJson(Map.of("a", "b")),
            "application/json",
            "saga-1",
            "step",
            null);

    assertThat(response.status()).isEqualTo(200);
    assertThat(response.bodyJsonObject()).containsEntry("k", "v");
  }

  @Test
  void exchange_emptyResponseBody_returnsEmptyMap() throws Exception {
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl, "/empty", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null);

    assertThat(response.bodyJsonObject()).isEmpty();
  }

  @Test
  void bodyJsonObject_malformedJsonResponse_throwsNonRetryable() throws Exception {
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl, "/malformed", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null);

    Throwable t = catchThrowable(response::bodyJsonObject);

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_5xxStatus_throwsRetryableWithResponse() {
    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET",
                    baseUrl,
                    "/fail503",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "s",
                    null));

    assertThat(t).isInstanceOf(HttpCallException.class);
    HttpCallException e = (HttpCallException) t;
    assertThat(e.isRetryable()).isTrue();
    assertThat(e.response()).isPresent();
    assertThat(e.response().orElseThrow().status()).isEqualTo(503);
  }

  @Test
  void exchange_4xxStatus_throwsNonRetryable() {
    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET",
                    baseUrl,
                    "/fail422",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "s",
                    null));

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_transportFailure_throwsRetryableWithoutResponse() throws IOException {
    // Bind then immediately release a port so nothing is listening on it.
    HttpServer probe = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    int deadPort = probe.getAddress().getPort();
    probe.start();
    probe.stop(0);
    String deadUrl = "http://localhost:" + deadPort;

    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET", deadUrl, "/x", NO_PARAMS, NO_PARAMS, null, null, "i", "s", null));

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isTrue();
    assertThat(((HttpCallException) t).response()).isEmpty();
  }

  @Test
  void exchange_stepNameWithControlChar_throwsNonRetryable() {
    // A control character in the step name (propagated as the X-Saga-Step header) is rejected by
    // the JDK; it must surface as a non-retryable HttpCallException, not a raw
    // IllegalArgumentException.
    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET",
                    baseUrl,
                    "/ok",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "bad\nstep",
                    null));

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_baseUrlTrailingSlashAndPathNoLeadingSlash_resolvesCorrectly() throws Exception {
    // base URL ends with '/', path has no leading '/': should still resolve to {base}/ok
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl + "/", "ok", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null);

    assertThat(response.bodyJsonObject()).containsEntry("k", "v");
  }

  private static void respond(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
