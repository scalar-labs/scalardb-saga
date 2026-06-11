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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests {@link HttpRequestSpec}'s validation contract and body/header handling. */
class HttpRequestSpecTest {

  private HttpServer server;
  private String baseUrl;
  private com.scalar.db.saga.invoker.HttpExchange exchange;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/ok", ex -> respond(ex, "{}"));
    server.createContext(
        "/inspect",
        ex -> {
          String contentType = ex.getRequestHeaders().getFirst(HttpHeaders.CONTENT_TYPE);
          ex.getRequestBody().readAllBytes();
          respond(ex, String.format("{\"contentType\":\"%s\"}", contentType));
        });
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

  private HttpRequestSpec post(String path) {
    return new HttpRequestSpec(exchange, "POST", baseUrl, path, "saga-1", "step");
  }

  @Test
  void header_reservedSagaId_throwsIllegalArgumentException() {
    assertThat(catchThrowable(() -> post("/ok").header(HttpHeaders.SAGA_ID, "x")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void header_reservedContentType_throwsIllegalArgumentException() {
    assertThat(catchThrowable(() -> post("/ok").header("content-type", "text/plain")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void jsonBody_setTwice_throwsIllegalStateException() {
    HttpRequestSpec spec = post("/ok").jsonBody(java.util.Map.of("a", "b"));

    assertThat(catchThrowable(() -> spec.jsonBody(java.util.Map.of("c", "d"))))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rawBody_afterJsonBody_throwsIllegalStateException() {
    HttpRequestSpec spec = post("/ok").jsonBody(java.util.Map.of("a", "b"));

    assertThat(
            catchThrowable(() -> spec.rawBody("x".getBytes(StandardCharsets.UTF_8), "text/plain")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void send_calledTwice_throwsIllegalStateException() throws Exception {
    HttpRequestSpec spec = post("/ok");
    spec.send();

    assertThat(catchThrowable(spec::send)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void jsonBody_sendsApplicationJsonContentType() throws Exception {
    String contentType =
        (String)
            post("/inspect")
                .jsonBody(java.util.Map.of("a", "b"))
                .send()
                .jsonObject()
                .get("contentType");

    assertThat(contentType).isEqualTo("application/json");
  }

  @Test
  void rawBody_sendsGivenContentType() throws Exception {
    String contentType =
        (String)
            post("/inspect")
                .rawBody(
                    "a=1&b=2".getBytes(StandardCharsets.UTF_8), "application/x-www-form-urlencoded")
                .send()
                .jsonObject()
                .get("contentType");

    assertThat(contentType).isEqualTo("application/x-www-form-urlencoded");
  }

  private static void respond(HttpExchange ex, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
