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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

  @Test
  void exchange_serverStallsMidResponseBody_throwsRetryableWithinTimeout() throws Exception {
    // Arrange — a server that sends the headers (declaring a 100-byte body) and a few body bytes,
    // then stalls without sending the rest. The request timeout must cover the body and fire.
    CountDownLatch release = new CountDownLatch(1);
    server.createContext(
        "/stall",
        ex -> {
          ex.sendResponseHeaders(200, 100); // promise 100 bytes...
          try (OutputStream os = ex.getResponseBody()) {
            os.write("{\"k\":".getBytes(StandardCharsets.UTF_8)); // ...send only a few, then stall
            os.flush();
            if (!release.await(5, TimeUnit.SECONDS)) {
              // The test always releases the latch once it has observed the timeout; tripping this
              // means it did not, so stop holding the body open rather than blocking indefinitely.
              Thread.currentThread().interrupt();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

    // Act — a short per-call timeout; the body never completes.
    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET",
                    baseUrl,
                    "/stall",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "s",
                    Duration.ofMillis(500)));
    release.countDown(); // let the server thread unwind

    // Assert — a mid-body stall surfaces as a retryable transport failure, not a hang.
    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isTrue();
  }

  @Test
  void exchange_responseBodyExceedsLimitWithContentLength_throwsNonRetryable() throws Exception {
    // Arrange — an honest server declares a body larger than the policy limit (fast-fail path).
    server.createContext("/big", ex -> respond(ex, 200, "x".repeat(50)));
    com.scalar.db.saga.transport.HttpExchange limited =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.newBuilder().maxBodyBytes(10).build());

    // Act
    Throwable t =
        catchThrowable(
            () ->
                limited.exchange(
                    "GET", baseUrl, "/big", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null));

    // Assert
    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_responseBodyExceedsLimitChunked_throwsNonRetryable() throws Exception {
    // Arrange — a chunked response (no Content-Length) that overruns the limit; the cap must be
    // enforced by counting bytes as they arrive, not just from the declared length.
    server.createContext(
        "/chunked",
        ex -> {
          ex.sendResponseHeaders(200, 0); // 0 => chunked, undeclared length
          try (OutputStream os = ex.getResponseBody()) {
            os.write("x".repeat(50).getBytes(StandardCharsets.UTF_8));
          }
        });
    com.scalar.db.saga.transport.HttpExchange limited =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.newBuilder().maxBodyBytes(10).build());

    // Act
    Throwable t =
        catchThrowable(
            () ->
                limited.exchange(
                    "GET",
                    baseUrl,
                    "/chunked",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "s",
                    null));

    // Assert
    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_responseBodyExactlyAtLimit_succeeds() throws Exception {
    // Arrange — a body of exactly maxBodyBytes must be accepted: the cap is a strict '>', so
    // '== limit' is allowed. The body below is exactly 16 bytes.
    server.createContext("/exact", ex -> respond(ex, 200, "{\"k\":\"01234567\"}"));
    com.scalar.db.saga.transport.HttpExchange limited =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.newBuilder().maxBodyBytes(16).build());

    // Act
    HttpCallResponse response =
        limited.exchange(
            "GET", baseUrl, "/exact", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s", null);

    // Assert — accepted and returned intact, not rejected as oversized.
    assertThat(response.status()).isEqualTo(200);
    assertThat(response.bodyJsonObject()).containsEntry("k", "01234567");
  }

  @Test
  void exchange_responseBodyOneByteOverLimitChunked_throwsNonRetryable() throws Exception {
    // Arrange — one byte past the limit, sent chunked so the overrun is caught by byte counting
    // (not the Content-Length fast-fail). Pins the '>' boundary at exactly maxBodyBytes + 1.
    server.createContext(
        "/over-by-one",
        ex -> {
          ex.sendResponseHeaders(200, 0); // 0 => chunked, undeclared length
          try (OutputStream os = ex.getResponseBody()) {
            os.write("x".repeat(17).getBytes(StandardCharsets.UTF_8));
          }
        });
    com.scalar.db.saga.transport.HttpExchange limited =
        new com.scalar.db.saga.transport.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.newBuilder().maxBodyBytes(16).build());

    // Act
    Throwable t =
        catchThrowable(
            () ->
                limited.exchange(
                    "GET",
                    baseUrl,
                    "/over-by-one",
                    NO_PARAMS,
                    NO_PARAMS,
                    null,
                    null,
                    "saga-1",
                    "s",
                    null));

    // Assert
    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  private static void respond(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
