package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What a refused start looks like on the wire.
 *
 * <p>Deliberately thin. The cap's semantics are proven against the engine in {@code
 * AdmissionControlIntegrationTest} and do not need re-proving per transport — one budget serves
 * REST, gRPC and embedded alike, by construction. What only a real request can show is the shape of
 * the refusal: the status, the error code in the body, the {@code Retry-After} header, and that a
 * full engine does not start answering unrelated questions with 503.
 */
class SagaAdmissionIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA_NAME = "capped-saga";

  private static final String DEFINITION =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/slow" },
              "compensation": { "method": "POST", "path": "/reverse" } } ] }
          """
              .replace("$name", SAGA_NAME));

  /** Held by the participant so one saga stays in flight, saturating a cap of 1. */
  private final CountDownLatch release = new CountDownLatch(1);

  /** Opens once the held saga is genuinely executing, so the cap is known to be full. */
  private final CountDownLatch inFlight = new CountDownLatch(1);

  @Override
  protected void configureProperties(Properties props) {
    props.setProperty(SagaServerConfig.MAX_CONCURRENT_SAGA_EXECUTIONS_KEY, "1");
  }

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext(
        "/slow",
        exchange -> {
          inFlight.countDown();
          try {
            if (!release.await(30, TimeUnit.SECONDS)) {
              throw new IllegalStateException("the test never released the participant");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          respond(exchange, 200, "{}");
        });
    route(participant, "/reverse", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
  }

  @AfterEach
  void releaseParticipant() {
    release.countDown(); // never leave a drive pinned
  }

  /** Starts a saga asynchronously and leaves it in flight, occupying the only seat. */
  private void saturate() throws Exception {
    HttpResponse<String> held =
        post("/sagas?async=true", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    assertThat(held.statusCode()).isEqualTo(202);
    // The seat is taken at admission, but waiting for the participant proves the drive is really
    // running rather than merely accepted.
    assertThat(inFlight.await(30, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void start_atTheCap_returns503WithTheCodeAndRetryAfter() throws Exception {
    // Arrange
    saturate();

    // Act
    HttpResponse<String> refused =
        post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");

    // Assert — the status a client keys retries off, the code it keys typed handling off, and the
    // hint that keeps a retry loop from becoming a hot loop.
    assertThat(refused.statusCode()).isEqualTo(503);
    assertThat(refused.body()).contains("DB-SAGA-20006");
    assertThat(refused.headers().firstValue("Retry-After")).contains("1");
  }

  @Test
  void start_refusedSaga_wasNeverPersisted() throws Exception {
    // The wire half of the claim the engine test makes: the caller's ID is still free afterwards.
    // Arrange
    saturate();
    String body = "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}";

    // Act
    HttpResponse<String> refused = put("/sagas/refused-saga", body);

    // Assert
    assertThat(refused.statusCode()).isEqualTo(503);
    assertThat(get("/sagas/refused-saga").statusCode()).isEqualTo(404);
  }

  @Test
  void start_unknownSagaAtTheCap_returns404NotOverloaded() throws Exception {
    // A full engine must not start answering "does this saga exist?" with "try again later".
    // Arrange
    saturate();

    // Act
    HttpResponse<String> unknown = post("/sagas", "{\"sagaName\":\"no-such-saga\",\"input\":{}}");

    // Assert
    assertThat(unknown.statusCode()).isEqualTo(404);
  }

  @Test
  void readsAtTheCap_areUnaffected() throws Exception {
    // The cap bounds new starts, not the whole API: a saturated engine still answers reads, which
    // is what an operator needs most while it is saturated.
    // Arrange
    saturate();

    // Act & Assert
    assertThat(get("/sagas/does-not-exist").statusCode()).isEqualTo(404);
    assertThat(get("/health").statusCode()).isEqualTo(200);
  }
}
