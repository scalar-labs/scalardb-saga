package com.scalar.db.saga.daemon.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.daemon.security.SagaAuthRequest;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaOperation;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityHandler;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import com.scalar.db.saga.exception.SagaErrorCode;
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
 * Exercises {@link RateLimitHandler} end-to-end against a real Javalin dispatch, wired behind the
 * <b>real</b> {@link SagaSecurityHandler} rather than a stand-in that plants an identity.
 *
 * <p>That wiring is the point of the test, not incidental setup. The limiter is only reachable
 * because it reads the identity the security handler resolved, and both must therefore sit on the
 * same Javalin stage: Javalin runs every {@code before} handler ahead of any {@code beforeMatched}
 * one, so a limiter left on {@code before} while the authenticator moved to {@code beforeMatched}
 * would read a null identity on every request and skip silently — rate limiting disabled
 * daemon-wide, with a policy unit test still green. Only a test that drives both handlers through a
 * real request catches that.
 */
class RateLimitHandlerTest {

  private final HttpClient http = HttpClient.newHttpClient();
  private @Nullable Javalin app;

  /** Registers the real security handler and the limiter, in the order {@code SagaServer} uses. */
  private Javalin startAuthenticated(int limit, SagaIdentity identity) {
    app = Javalin.create();
    SagaSecurityHandler.register(app, new FixedIdentityProvider(identity));
    RateLimitHandler.register(app, new RateLimiter(limit, 60_000L));
    ErrorMapper.register(app);
    registerRoutes(app);
    return app.start(0);
  }

  /** Registers the limiter with no upstream authenticator, so no identity is ever resolved. */
  private Javalin startWithoutAuth(int limit) {
    app = Javalin.create();
    RateLimitHandler.register(app, new RateLimiter(limit, 60_000L));
    ErrorMapper.register(app);
    registerRoutes(app);
    return app.start(0);
  }

  private static void registerRoutes(Javalin app) {
    app.post("/sagas", ctx -> ctx.result("created"), SagaOperation.START_SAGA);
    app.get("/sagas/x", ctx -> ctx.result("read"), SagaOperation.GET_SAGA);
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void rateLimitedOperation_overLimit_returns429() throws Exception {
    // Arrange — limit 2/min for the authenticated principal
    startAuthenticated(2, SagaIdentity.of("alice", Set.of(SagaRole.WRITE)));

    // Act / Assert — first two allowed, third throttled
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
    assertThat(send("POST", "/sagas").statusCode()).isEqualTo(200);
    HttpResponse<String> third = send("POST", "/sagas");
    assertThat(third.statusCode()).isEqualTo(429);
    assertThat(third.body()).contains(SagaErrorCode.RATE_LIMIT_EXCEEDED.code());
  }

  @Test
  void unlimitedOperation_notLimited_evenBeyondLimit() throws Exception {
    // Arrange
    startAuthenticated(1, SagaIdentity.of("alice", Set.of(SagaRole.WRITE)));

    // Act / Assert — reads are never throttled
    for (int i = 0; i < 5; i++) {
      assertThat(send("GET", "/sagas/x").statusCode()).isEqualTo(200);
    }
  }

  @Test
  void rateLimitedOperation_withNoUpstreamAuthenticator_notLimited() throws Exception {
    // Arrange — the limiter alone, so no identity is ever resolved
    startWithoutAuth(1);

    // Act / Assert — a limit that cannot be attributed to a principal is not enforced, rather than
    // blocking the call (this is what keeps an HMAC callback off a user's budget)
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

  /** A provider that authenticates every request as one fixed identity. */
  private record FixedIdentityProvider(SagaIdentity identity) implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      return identity;
    }

    @Override
    public String name() {
      return "fixed-identity-stub";
    }
  }
}
