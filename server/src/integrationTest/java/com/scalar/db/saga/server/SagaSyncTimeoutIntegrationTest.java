package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the <b>bounded synchronous start</b> ({@code
 * scalar.db.saga.server.sync.timeout_millis}). With a small sync timeout configured: a saga whose
 * participant answers quickly finishes within the bound and returns {@code 200}; a saga whose
 * participant is slower than the bound returns {@code 202} (the request thread is released) and the
 * saga keeps running on the engine's executor, reaching {@code COMPLETED} once the participant
 * finally answers.
 */
class SagaSyncTimeoutIntegrationTest extends ServerIntegrationTestSupport {

  private static final long SYNC_TIMEOUT_MILLIS = 500;
  private static final String FAST_SAGA = "fast-saga";
  private static final String SLOW_SAGA = "slow-saga";

  private static final String FAST_DEF =
      withService(
          """
          { "name": "fast-saga", "mode": "SAGA", "steps": [
            { "name": "s", "service": "$svc",
              "execution":    { "method": "POST", "path": "/fast" },
              "compensation": { "method": "POST", "path": "/undo" } } ] }
          """);
  private static final String SLOW_DEF =
      withService(
          """
          { "name": "slow-saga", "mode": "SAGA", "steps": [
            { "name": "s", "service": "$svc",
              "execution":    { "method": "POST", "path": "/slow" },
              "compensation": { "method": "POST", "path": "/undo" } } ] }
          """);

  @Override
  protected void configureProperties(Properties props) {
    props.setProperty(SagaServerConfig.SYNC_TIMEOUT_MILLIS_KEY, Long.toString(SYNC_TIMEOUT_MILLIS));
  }

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/fast", 200);
    route(participant, "/undo", 200);
    // Answers after the sync timeout (so a synchronous start must hand back 202) but soon enough to
    // complete within pollUntilTerminal's window.
    participant.createContext(
        "/slow",
        ex -> {
          try {
            Thread.sleep(SYNC_TIMEOUT_MILLIS * 2);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          respond(ex, 200, "{}");
        });
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, FAST_SAGA, FAST_DEF);
    writeDefinition(definitionsDir, SLOW_SAGA, SLOW_DEF);
  }

  @Test
  void startSync_completesWithinTimeout_returns200() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + FAST_SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
  }

  @Test
  void startSync_exceedsTimeout_returns202AndKeepsRunning() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SLOW_SAGA + "\"}");

    // The participant is slower than the sync timeout, so the request is released with 202 rather
    // than blocked to terminal — and the saga is still running, not abandoned.
    assertThat(post.statusCode()).isEqualTo(202);
    assertThat(status(post)).isEqualTo("RUNNING");

    // It continues on the engine's executor and reaches COMPLETED once the slow participant
    // answers.
    String sagaId = MAPPER.readTree(post.body()).get("sagaId").asText();
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }
}
