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
import java.util.Properties;
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

  private final AtomicReference<String> capturedCallbackUrl = new AtomicReference<>();
  private final HttpClient http = HttpClient.newHttpClient();

  private static final String DEFINITION =
      withService(
          """
          { "name": "payment", "mode": "SAGA", "steps": [
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge", "async": true },
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
          respond(ex, 202, "{}");
        });
    route(participant, "/charge-undo", 200); // compensation (not exercised on the happy path)
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
  }

  @Override
  protected void configureProperties(Properties props) {
    // Pin the daemon to a known port so it can mint a callback URL that points back at itself.
    int daemonPort = freePort();
    props.setProperty(SagaServerConfig.HTTP_PORT_KEY, String.valueOf(daemonPort));
    props.setProperty(SagaServerConfig.CALLBACK_BASE_URL_KEY, "http://localhost:" + daemonPort);
    props.setProperty(SagaServerConfig.CALLBACK_SECRET_KEY, SECRET);
  }

  @Test
  void asyncStep_parksOn202_thenResumesAndCompletesOnCallback() throws Exception {
    // Start the saga — the async step returns 202, so the sync start parks and returns WAITING.
    HttpResponse<String> start =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    String sagaId = MAPPER.readTree(start.body()).get("sagaId").asText();
    assertThat(status(start)).isEqualTo("WAITING");

    // The daemon minted a signed callback URL and handed it to the participant.
    String callbackUrl = capturedCallbackUrl.get();
    assertThat(callbackUrl)
        .isNotNull()
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
    String tampered = capturedCallbackUrl.get().replace("&iat=", "&iat=9");
    HttpResponse<String> callback = postAbsolute(tampered, "{}");

    assertThat(callback.statusCode()).isEqualTo(401);
    assertThat(status(get("/sagas/" + sagaId))).isEqualTo("WAITING");
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
