package com.scalar.db.saga.daemon;

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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * End-to-end async step completion over HTTP for a <b>declarative TCC</b> saga: a TCC step marked
 * {@code async} parks the saga on a participant {@code 202 Accepted}, and an HMAC-authenticated
 * callback resumes it. Covers both forward phases a TCC step can park on:
 *
 * <ul>
 *   <li>an async <b>reservation</b> parks (callback keyed on {@code s1.reserve}); the callback
 *       resumes it and the sync confirm rolls forward to {@code COMPLETED};
 *   <li>an async <b>confirmation</b> parks after a sync reserve (callback keyed on {@code
 *       s1.confirm}); the callback resumes it to {@code COMPLETED}.
 * </ul>
 *
 * <p>The engine names a TCC step's parked {@code STEP_PENDING} marker with a phase-qualified name
 * ({@code s1.reserve} / {@code s1.confirm}), so the callback URL — keyed on the same name — must
 * carry that suffix to resolve to the parked step. This is the TCC counterpart to {@link
 * SagaAsyncCallbackIntegrationTest} (the SAGA-mode version).
 *
 * <p>The daemon must know its own externally-reachable URL to mint the callback URL, so — unlike
 * the other daemon ITs that bind an ephemeral port — this test pre-allocates a free port and pins
 * the daemon (and its {@code callback_base_url}) to it.
 */
class SagaTccAsyncCallbackIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String SECRET = "integration-tcc-callback-secret";

  private final AtomicReference<String> reserveCallbackUrl = new AtomicReference<>();
  private final AtomicReference<String> confirmCallbackUrl = new AtomicReference<>();
  private final HttpClient http = HttpClient.newHttpClient();

  // Single TCC step whose reservation is async: the reserve parks on 202, the confirm is sync.
  private static final String ASYNC_RESERVE_DEF =
      withService(
          """
          { "name": "tcc-async-reserve", "mode": "TCC", "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-async", "async": true },
              "confirmation": { "method": "POST", "path": "/confirm-sync" },
              "cancellation": { "method": "POST", "path": "/cancel-r" } } ] }
          """);

  // Single TCC step whose confirmation is async: the reserve is sync, the confirm parks on 202.
  private static final String ASYNC_CONFIRM_DEF =
      withService(
          """
          { "name": "tcc-async-confirm", "mode": "TCC", "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-sync" },
              "confirmation": { "method": "POST", "path": "/confirm-async", "async": true },
              "cancellation": { "method": "POST", "path": "/cancel-c" } } ] }
          """);

  @Override
  protected void configureParticipant(HttpServer participant) {
    // The async reserve: capture the callback URL the daemon injected, then accept (202) — the
    // reservation's result arrives later via the callback rather than in this response.
    participant.createContext(
        "/reserve-async",
        ex -> {
          reserveCallbackUrl.set(ex.getRequestHeaders().getFirst("X-Saga-Callback-Url"));
          respond(ex, 202, "{}");
        });
    // The async confirm: capture its callback URL, then accept (202).
    participant.createContext(
        "/confirm-async",
        ex -> {
          confirmCallbackUrl.set(ex.getRequestHeaders().getFirst("X-Saga-Callback-Url"));
          respond(ex, 202, "{}");
        });
    route(participant, "/confirm-sync", 200);
    route(participant, "/reserve-sync", 200);
    route(participant, "/cancel-r", 200); // not exercised on the happy path
    route(participant, "/cancel-c", 200); // not exercised on the happy path
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, "tcc-async-reserve", ASYNC_RESERVE_DEF);
    writeDefinition(definitionsDir, "tcc-async-confirm", ASYNC_CONFIRM_DEF);
  }

  @Override
  protected void configureProperties(Properties props) {
    // Pin the daemon to a known port so it can mint a callback URL that points back at itself.
    int daemonPort = freePort();
    props.setProperty(SagaServerConfig.PORT_KEY, String.valueOf(daemonPort));
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://localhost:" + daemonPort);
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, SECRET);
  }

  @Test
  void asyncReservation_parksOn202_thenResumesAndConfirmsOnCallback() throws Exception {
    // Start the saga — the async reservation returns 202, so the sync start parks and returns
    // WAITING (the reserve is the pivot step).
    HttpResponse<String> start = post("/sagas", "{\"sagaName\":\"tcc-async-reserve\"}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");

    // The callback URL is keyed on the phase-qualified reserve name, matching the parked marker —
    // a bare "s1" here would fail to resolve to the parked "s1.reserve" step.
    String callbackUrl = reserveCallbackUrl.get();
    assertThat(callbackUrl)
        .isNotNull()
        .contains("/sagas/" + sagaId + "/steps/s1.reserve/complete?token=")
        .contains("&iat=");

    // The participant reports the reservation done via the signed callback URL.
    HttpResponse<String> callback = postAbsolute(callbackUrl, "{\"reservationId\":\"R-1\"}");
    assertThat(callback.statusCode()).isEqualTo(200);

    // The saga resumed from the parked reserve, ran the sync confirm, and completed — no cancel.
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
    assertThat(hits("/confirm-sync")).isEqualTo(1);
    assertThat(hits("/cancel-r")).isZero();
  }

  @Test
  void asyncConfirmation_parksOn202_thenResumesAndCompletesOnCallback() throws Exception {
    // Start the saga — the reserve is sync (200), the confirmation returns 202, so the start parks
    // on the confirm (post-pivot) and returns WAITING.
    HttpResponse<String> start = post("/sagas", "{\"sagaName\":\"tcc-async-confirm\"}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");
    assertThat(hits("/reserve-sync")).isEqualTo(1);

    // The callback URL is keyed on the phase-qualified confirm name, matching the parked marker.
    String callbackUrl = confirmCallbackUrl.get();
    assertThat(callbackUrl)
        .isNotNull()
        .contains("/sagas/" + sagaId + "/steps/s1.confirm/complete?token=")
        .contains("&iat=");

    // The participant reports the confirmation done via the signed callback URL.
    HttpResponse<String> callback = postAbsolute(callbackUrl, "{}");
    assertThat(callback.statusCode()).isEqualTo(200);

    // The saga resumed from the parked confirm and, having no further steps, completed — no cancel.
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
    assertThat(hits("/cancel-c")).isZero();
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
