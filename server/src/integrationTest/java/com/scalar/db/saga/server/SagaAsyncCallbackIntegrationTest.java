package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * End-to-end async step completion over HTTP: a declarative step marked {@code async} parks the
 * saga on a participant {@code 202 Accepted}, and an HMAC-authenticated callback resumes it.
 *
 * <p>The daemon must know its own externally-reachable URL to mint the callback URL, so — unlike
 * the other daemon ITs that bind an ephemeral port — this test pre-allocates a free port and pins
 * the daemon (and its {@code callback.base_url}) to it.
 */
class SagaAsyncCallbackIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA_NAME = "payment";
  private static final String SECRET = "integration-callback-secret";

  private static final String EAGER_SAGA_NAME = "payment-eager";

  private final AtomicReference<String> capturedCallbackUrl = new AtomicReference<>();
  private final AtomicReference<String> capturedFailureUrl = new AtomicReference<>();
  private final AtomicInteger chargeCalls = new AtomicInteger();
  private final AtomicInteger undoCalls = new AtomicInteger();
  private final AtomicReference<String> eagerCallbackUrl = new AtomicReference<>();
  private final AtomicReference<HttpResponse<String>> eagerCallbackResponse =
      new AtomicReference<>();
  private final HttpClient http = HttpClient.newHttpClient();

  private static final String DEFINITION =
      withService(
          """
          { "name": "payment", "mode": "SAGA", "steps": [
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge", "async": true },
              "compensation": { "method": "POST", "path": "/charge-undo" } } ] }
          """);

  // A participant that calls back before it answers the step, so the callback is certain to arrive
  // before the saga records its park.
  private static final String EAGER_DEFINITION =
      withService(
          """
          { "name": "payment-eager", "mode": "SAGA", "steps": [
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge-eager", "async": true },
              "compensation": { "method": "POST", "path": "/charge-undo" } } ] }
          """);

  @Override
  protected void configureParticipant(HttpServer participant) {
    // The async step: capture the callback URL the daemon injected, then accept (202) — the result
    // arrives later via the callback rather than in this response.
    participant.createContext(
        "/charge",
        ex -> {
          capturedCallbackUrl.set(ex.getRequestHeaders().getFirst("X-Saga-Callback-Url"));
          capturedFailureUrl.set(ex.getRequestHeaders().getFirst("X-Saga-Failure-Url"));
          chargeCalls.incrementAndGet();
          respond(ex, 202, "{}");
        });
    participant.createContext(
        "/charge-eager",
        ex -> {
          String url = ex.getRequestHeaders().getFirst("X-Saga-Callback-Url");
          eagerCallbackUrl.set(url);
          try {
            eagerCallbackResponse.set(postAbsolute(url, "{\"paymentId\":\"P-2\"}"));
          } catch (Exception e) {
            throw new IOException(e);
          }
          respond(ex, 202, "{}");
        });
    participant.createContext(
        "/charge-undo",
        ex -> {
          undoCalls.incrementAndGet();
          respond(ex, 200, "{}");
        });
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
    writeDefinition(definitionsDir, EAGER_SAGA_NAME, EAGER_DEFINITION);
  }

  @Override
  protected void configureProperties(Properties props) {
    // Pin the daemon to a known port so it can mint a callback URL that points back at itself.
    int daemonPort = freePort();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, String.valueOf(daemonPort));
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://localhost:" + daemonPort);
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, SECRET);
    // A parked saga no longer ends a synchronous start's wait, so each start below runs the bound
    // to completion before answering WAITING. At the 60s default that is a minute of wall clock per
    // test for an assertion about the status, not about how long it took to arrive. Two seconds
    // proves the same thing. See todos/105.
    props.setProperty(SagaServerConfig.SYNC_MAX_WAIT_MILLIS_KEY, "2000");
  }

  @Test
  void asyncStep_parksOn202_thenResumesAndCompletesOnCallback() throws Exception {
    // Start the saga — the async step returns 202, so the sync start parks and returns WAITING.
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");

    // The daemon minted a signed callback URL and handed it to the participant.
    String callbackUrl =
        Objects.requireNonNull(capturedCallbackUrl.get(), "the participant was never called");
    assertThat(callbackUrl)
        .contains("/sagas/" + sagaId + "/steps/charge/complete?token=")
        .contains("&iat=");

    // The participant reports completion by POSTing the (signed) callback URL with the step output.
    HttpResponse<String> callback = postAbsolute(callbackUrl, "{\"paymentId\":\"P-1\"}");
    assertThat(callback.statusCode()).isEqualTo(200);

    // The saga resumed from the parked step and, having no further steps, completed.
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }

  @Test
  void callback_withTamperedToken_isRejected401_andSagaStaysWaiting() throws Exception {
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");

    // Mutate iat (part of the signed data) so the recomputed HMAC no longer matches the token.
    String tampered =
        Objects.requireNonNull(capturedCallbackUrl.get(), "the participant was never called")
            .replace("&iat=", "&iat=9");
    HttpResponse<String> callback = postAbsolute(tampered, "{}");

    assertThat(callback.statusCode()).isEqualTo(401);
    assertThat(status(get("/sagas/" + sagaId))).isEqualTo("WAITING");
  }

  @Test
  void callback_beforeTheParkIsRecorded_isRefusedRetryablyThenCompletesOnRetry() throws Exception {
    // The participant calls back before it has even answered the step, so the saga has not parked.
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + EAGER_SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");

    // That early callback was refused with a status a generic HTTP retry acts on, not acked and
    // lost: before the fix it answered 200 and the saga waited on it until its timeout.
    HttpResponse<String> early =
        Objects.requireNonNull(eagerCallbackResponse.get(), "the participant was never called");
    assertThat(early.statusCode()).isEqualTo(503);
    assertThat(early.headers().firstValue("Retry-After")).isPresent();
    assertThat(early.body()).contains("DB-SAGA-20007");

    // The participant's retry of the same callback completes the step.
    HttpResponse<String> retry =
        postAbsolute(Objects.requireNonNull(eagerCallbackUrl.get()), "{\"paymentId\":\"P-2\"}");
    assertThat(retry.statusCode()).isEqualTo(200);
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }

  @Test
  void asyncStep_participantReportsFailure_compensatesAndRecordsWhy() throws Exception {
    // Arrange
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");
    String failureUrl =
        Objects.requireNonNull(capturedFailureUrl.get(), "the participant got no failure URL");
    assertThat(failureUrl).contains("/sagas/" + sagaId + "/steps/charge/fail?token=");

    // Act — the participant reports the step failed, without asking for a retry.
    HttpResponse<String> failure = postAbsolute(failureUrl, "{\"error\":\"card declined\"}");

    // Assert — the only step is at the pivot, so the saga rolls back, including the failed step.
    assertThat(failure.statusCode()).isEqualTo(200);
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPENSATED");
    assertThat(undoCalls.get()).isEqualTo(1);
    assertThat(chargeCalls.get()).isEqualTo(1);
    assertThat(get("/sagas/" + sagaId + "/detail").body()).contains("card declined");
  }

  @Test
  void asyncStep_participantReportsRetryableFailure_reissuesTheStepThenCompletes()
      throws Exception {
    // Arrange
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    String failureUrl =
        Objects.requireNonNull(capturedFailureUrl.get(), "the participant got no failure URL");

    // Act — a retryable failure re-issues the step, which calls the participant again.
    HttpResponse<String> failure =
        http.send(
            HttpRequest.newBuilder(URI.create(failureUrl))
                .header("X-Saga-Retryable", "true")
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString("{\"error\":\"approver busy\"}"))
                .build(),
            BodyHandlers.ofString());
    assertThat(failure.statusCode()).isEqualTo(200);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (chargeCalls.get() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertThat(chargeCalls.get()).isEqualTo(2);
    String secondCallbackUrl = Objects.requireNonNull(capturedCallbackUrl.get());
    // The re-issued step parks again before its callback can complete it; a callback that beats
    // the park is refused retryably, so retry it as a participant would.
    HttpResponse<String> callback = postAbsolute(secondCallbackUrl, "{\"paymentId\":\"P-3\"}");
    while (callback.statusCode() == 503 && System.nanoTime() < deadline) {
      Thread.sleep(50);
      callback = postAbsolute(secondCallbackUrl, "{\"paymentId\":\"P-3\"}");
    }

    // Assert
    assertThat(callback.statusCode()).isEqualTo(200);
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
    assertThat(undoCalls.get()).isZero();
    // The reported reason survives the re-issue, though no step failure was recorded.
    assertThat(get("/sagas/" + sagaId + "/detail").body()).contains("approver busy");
  }

  private HttpResponse<String> postAbsolute(String url, String body) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body))
            .build(),
        BodyHandlers.ofString());
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
