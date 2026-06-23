package com.scalar.db.saga.daemon.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Locks in {@link ErrorMapper}'s exception-resolution behavior against a real Javalin dispatch.
 *
 * <p>The catch-all {@code Exception} handler must <b>not</b> swallow Javalin's built-in {@link
 * io.javalin.http.HttpResponseException}s (e.g. {@link BadRequestResponse}). In Javalin 6.x {@code
 * ExceptionMapper} pre-registers a handler for {@code HttpResponseException} and resolves the
 * nearest handler up the class hierarchy, so {@code HttpResponseException} (1 hop) wins over {@code
 * Exception} (3 hops) and the intended status is preserved. A Javalin upgrade that changed that
 * resolution order — silently mapping client errors to {@code 500} — would fail this test.
 *
 * <p>Binds an ephemeral port (no ScalarDB, no {@code SagaServer}); it exercises only {@code
 * ErrorMapper} on a bare app.
 */
class ErrorMapperTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private Javalin app;

  @BeforeEach
  void setUp() {
    app = Javalin.create();
    ErrorMapper.register(app);
    app.get(
        "/bad-request",
        ctx -> {
          throw new BadRequestResponse();
        });
    app.get(
        "/boom",
        ctx -> {
          throw new RuntimeException("internal detail");
        });
    app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void httpResponseException_keepsItsStatus_notSwallowedByCatchAll() throws Exception {
    HttpResponse<String> response = get("/bad-request");
    assertThat(response.statusCode()).isEqualTo(400);
  }

  @Test
  void uncaughtException_mapsToGeneric500_withoutLeakingMessage() throws Exception {
    HttpResponse<String> response = get("/boom");
    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body()).contains("Internal server error").doesNotContain("internal detail");
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).GET().build(),
        BodyHandlers.ofString());
  }
}
