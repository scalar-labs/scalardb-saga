package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link CallbackResource} against a real Javalin dispatch (ephemeral port, no ScalarDB)
 * — HMAC auth, resume wiring, and the idempotent/error mappings — with a mocked orchestrator.
 */
class CallbackResourceTest {

  private static final String SECRET = "test-callback-secret";
  private static final String SAGA_ID = "saga-1";
  private static final String STEP = "debit";
  private static final String IAT = "1700000000";

  private final HttpClient http = HttpClient.newHttpClient();
  private DefaultSagaOrchestrator orchestrator;
  private Javalin app;

  @BeforeEach
  void setUp() {
    orchestrator = mock(DefaultSagaOrchestrator.class);
    // TTL disabled (maxAge = 0) for the base fixture; the TTL tests start their own app.
    app = startApp(0, Clock.systemUTC());
  }

  private Javalin startApp(long maxAgeSeconds, Clock clock) {
    Javalin a = Javalin.create();
    ErrorMapper.register(a);
    CallbackResource.register(a, orchestrator, SECRET, maxAgeSeconds, clock);
    a.start(0);
    return a;
  }

  @AfterEach
  void tearDown() {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void validToken_resumesStepAndReturns200() throws Exception {
    when(orchestrator.completeStepAsync(eq(SAGA_ID), eq(STEP), any()))
        .thenReturn(snapshot(SagaStatus.RUNNING));

    HttpResponse<String> response =
        post("?token=" + validToken() + "&iat=" + IAT, "{\"paymentId\":\"p1\"}");

    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).completeStepAsync(eq(SAGA_ID), eq(STEP), eq(Map.of("paymentId", "p1")));
  }

  @Test
  void emptyBody_completesWithEmptyOutput() throws Exception {
    when(orchestrator.completeStepAsync(eq(SAGA_ID), eq(STEP), any()))
        .thenReturn(snapshot(SagaStatus.RUNNING));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "");

    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).completeStepAsync(eq(SAGA_ID), eq(STEP), eq(Map.of()));
  }

  @Test
  void invalidToken_returns401_andDoesNotResume() throws Exception {
    HttpResponse<String> response = post("?token=deadbeef&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void wrongLengthToken_returns401_andDoesNotResume() throws Exception {
    // Valid lowercase-hex charset but not the 64-char HMAC length: rejected by the shape check
    // before any HMAC is computed.
    String tooLong = "a".repeat(128);

    HttpResponse<String> response = post("?token=" + tooLong + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void nonHexToken_returns401_andDoesNotResume() throws Exception {
    // Correct length (64) but uppercase — outside the lowercase-hex charset.
    String nonHex = "A".repeat(64);

    HttpResponse<String> response = post("?token=" + nonHex + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void missingToken_returns401() throws Exception {
    HttpResponse<String> response = post("?iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void missingIat_returns401() throws Exception {
    HttpResponse<String> response = post("?token=" + validToken(), "{}");

    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void tokenForDifferentStep_returns401() throws Exception {
    // A token computed over a different step name must not authorize this step.
    String tokenForOtherStep =
        HmacUtils.hmacSha256Hex(SECRET, HmacUtils.callbackSignedData(SAGA_ID, "credit", IAT));

    HttpResponse<String> response = post("?token=" + tokenForOtherStep + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void wrongStepName_propagatesAs400() throws Exception {
    when(orchestrator.completeStepAsync(any(), any(), any()))
        .thenThrow(new IllegalArgumentException("not the parked step"));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(400);
  }

  @Test
  void duplicateOrLateCallback_notWaiting_isIdempotent200() throws Exception {
    // A saga already resumed (or swept) → completeStepAsync throws IllegalStateException; the route
    // returns the saga's current state instead of failing the duplicate callback.
    when(orchestrator.completeStepAsync(any(), any(), any()))
        .thenThrow(new IllegalStateException("saga not WAITING"));
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.RUNNING));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).getStateSnapshot(SAGA_ID);
  }

  @Test
  void callbackLostRaceToTimeoutSweep_isIdempotent200() throws Exception {
    // The timeout sweep resolved the parked step first → completeStepAsync throws
    // SagaConcurrentModificationException; the route returns the saga's current state, not an
    // error.
    when(orchestrator.completeStepAsync(any(), any(), any()))
        .thenThrow(new SagaConcurrentModificationException(SAGA_ID));
    when(orchestrator.getStateSnapshot(SAGA_ID)).thenReturn(snapshot(SagaStatus.COMPENSATING));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).getStateSnapshot(SAGA_ID);
  }

  @Test
  void expiredToken_beyondMaxAge_returns401() throws Exception {
    // A 60s TTL with a clock one hour past the token's iat: the (validly-signed) token is expired.
    app.stop();
    app =
        startApp(
            60, Clock.fixed(Instant.ofEpochSecond(Long.parseLong(IAT) + 3600), ZoneOffset.UTC));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(401);
    verify(orchestrator, never()).completeStepAsync(any(), any(), any());
  }

  @Test
  void freshToken_withinMaxAge_resumesAndReturns200() throws Exception {
    // A 60s TTL with a clock 30s past the token's iat: within the TTL, so it resumes as normal.
    app.stop();
    app =
        startApp(60, Clock.fixed(Instant.ofEpochSecond(Long.parseLong(IAT) + 30), ZoneOffset.UTC));
    when(orchestrator.completeStepAsync(eq(SAGA_ID), eq(STEP), any()))
        .thenReturn(snapshot(SagaStatus.RUNNING));

    HttpResponse<String> response = post("?token=" + validToken() + "&iat=" + IAT, "{}");

    assertThat(response.statusCode()).isEqualTo(200);
    verify(orchestrator).completeStepAsync(eq(SAGA_ID), eq(STEP), any());
  }

  private String validToken() {
    return HmacUtils.hmacSha256Hex(SECRET, HmacUtils.callbackSignedData(SAGA_ID, STEP, IAT));
  }

  private HttpResponse<String> post(String query, String body) throws Exception {
    URI uri =
        URI.create(
            "http://localhost:"
                + app.port()
                + "/sagas/"
                + SAGA_ID
                + "/steps/"
                + STEP
                + "/complete"
                + query);
    return http.send(
        HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        BodyHandlers.ofString());
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    return new SagaStateSnapshot(SAGA_ID, "order-saga", status, "engine-1", "v1", now, now);
  }
}
