package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Liveness coverage: a started {@link SagaServer} answers {@code GET /health} with {@code UP}. Uses
 * {@link DaemonIntegrationTestSupport} only to bring a server up (with one registered definition,
 * as the daemon now refuses to start with none); the saga itself is never invoked.
 */
class SagaServerHealthIntegrationTest extends DaemonIntegrationTestSupport {

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext("/x", ex -> respond(ex, 200, "{}"));
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(
        definitionsDir,
        "saga",
        "{\"name\":\"saga\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"s\",\"service\":\""
            + SERVICE
            + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/x\"},"
            + "\"compensation\":{\"method\":\"POST\",\"path\":\"/x\"}}]}");
  }

  @Test
  void health_serverRunning_returnsUp() throws Exception {
    HttpResponse<String> response = get("/health");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("UP");
  }
}
