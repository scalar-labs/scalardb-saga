package com.scalar.db.saga.invoker;

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
 * Direct tests for the shared {@link com.scalar.db.saga.invoker.HttpExchange} contract — especially
 * the failure paths that {@code HttpServiceInvokerTest} can't reach through the invoker. Both
 * {@code HttpServiceInvoker} and (later) {@code HttpTransportAdapter} depend on this behavior.
 */
class HttpExchangeTest {

  private static final List<Map.Entry<String, String>> NO_PARAMS = List.of();

  private HttpServer server;
  private String baseUrl;
  private com.scalar.db.saga.invoker.HttpExchange exchange;

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
        new com.scalar.db.saga.invoker.HttpExchange(
            HttpClient.newHttpClient(), OutboundHttpPolicy.allowAll());
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
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
            "step");

    assertThat(response.status()).isEqualTo(200);
    assertThat(response.jsonObject()).containsEntry("k", "v");
  }

  @Test
  void exchange_emptyResponseBody_returnsEmptyMap() throws Exception {
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl, "/empty", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s");

    assertThat(response.jsonObject()).isEmpty();
  }

  @Test
  void jsonObject_malformedJsonResponse_throwsNonRetryable() throws Exception {
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl, "/malformed", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s");

    Throwable t = catchThrowable(response::jsonObject);

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_5xxStatus_throwsRetryableWithResponse() {
    Throwable t =
        catchThrowable(
            () ->
                exchange.exchange(
                    "GET", baseUrl, "/fail503", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s"));

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
                    "GET", baseUrl, "/fail422", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s"));

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
                    "GET", deadUrl, "/x", NO_PARAMS, NO_PARAMS, null, null, "i", "s"));

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
                    "bad\nstep"));

    assertThat(t).isInstanceOf(HttpCallException.class);
    assertThat(((HttpCallException) t).isRetryable()).isFalse();
  }

  @Test
  void exchange_baseUrlTrailingSlashAndPathNoLeadingSlash_resolvesCorrectly() throws Exception {
    // base URL ends with '/', path has no leading '/': should still resolve to {base}/ok
    HttpCallResponse response =
        exchange.exchange(
            "GET", baseUrl + "/", "ok", NO_PARAMS, NO_PARAMS, null, null, "saga-1", "s");

    assertThat(response.jsonObject()).containsEntry("k", "v");
  }

  private static void respond(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
