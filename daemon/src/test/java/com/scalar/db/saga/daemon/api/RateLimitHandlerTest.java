package com.scalar.db.saga.daemon.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link RateLimitHandler} end-to-end against a real Javalin dispatch. A stub before
 * handler stands in for {@link SagaSecurityHandler} by planting an identity on the request; {@code
 * null} identity models an auth-exempt route.
 */
class RateLimitHandlerTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private @Nullable Javalin app;

  private Javalin start(int limit, @Nullable SagaIdentity identity) {
    app = Javalin.create();
    // Stand in for SagaSecurityHandler: plant (or omit) the resolved identity.
    app.before(
        ctx -> {
          if (identity != null) {
            ctx.attribute(SagaSecurityHandler.IDENTITY_ATTRIBUTE, identity);
          }
        });
    RateLimitHandler.register(app, new RateLimiter(limit, 60_000L));
    ErrorMapper.register(app);
    app.post("/sagas", ctx -> ctx.result("created"));
    app.get("/sagas/x", ctx -> ctx.result("read"));
    return app.start(0);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void post_overLimit_returns429() throws Exception {
    // Arrange — limit 2/min for the authenticated principal
    start(2, SagaIdentity.of("alice", Set.of(SagaRole.WRITE)));

    // Act / Assert — first two allowed, third throttled
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
    HttpResponse<String> third = send("POST", "/sagas");
    assertThat(third.statusCode()).isEqualTo(429);
    assertThat(third.body()).contains("TOO_MANY_REQUESTS");
  }

  @Test
  void get_notLimited_evenBeyondLimit() throws Exception {
    // Arrange
    start(1, SagaIdentity.of("alice", Set.of(SagaRole.WRITE)));

    // Act / Assert — reads are never throttled
    for (int i = 0; i < 5; i++) {
      assertThat(send("GET", "/sagas/x").statusCode()).isEqualTo(200);
    }
  }

  @Test
  void post_noIdentity_notLimited() throws Exception {
    // Arrange — an auth-exempt route has no resolved identity
    start(1, null);

    // Act / Assert — not throttled (e.g. so an HMAC callback is never blocked by a user's budget)
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
  }

  private HttpResponse<String> send(String method, String path) throws Exception {
    int port = Objects.requireNonNull(app, "app not started").port();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build();
    return http.send(request, BodyHandlers.ofString());
  }
}
