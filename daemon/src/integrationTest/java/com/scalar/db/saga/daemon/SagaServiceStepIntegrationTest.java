package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the daemon's <b>outbound service-step transport</b> — a declarative {@code
 * service} step's call actually reaches the participant over HTTP. Asserts that the participant is
 * hit on execution and, on a downstream failure, hit again on compensation (with the captured
 * output threaded into the compensation body) — i.e. that the daemon→participant wiring works.
 *
 * <p>Counterpart: {@link SagaRestApiIntegrationTest} asserts on the <b>inbound</b> REST contract.
 * Both share {@link DaemonIntegrationTestSupport}; this one adds participant-hit assertions.
 */
class SagaServiceStepIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String COMPLETING_SAGA = "declarative-saga";
  private static final String COMPENSATING_SAGA = "declarative-compensating-saga";

  // Single step that captures output and completes.
  private static final String COMPLETING_DEF =
      "{\"name\":\""
          + COMPLETING_SAGA
          + "\",\"mode\":\"SAGA\",\"steps\":[{\"name\":\"debit\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/debit\","
          + "\"output\":{\"debitId\":\"$.debit_id\"}},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/reverse\","
          + "\"jsonBody\":{\"id\":\"${debitId}\"}}}]}";

  // debit succeeds, charge returns 422 → backward recovery compensates debit via /reverse.
  private static final String COMPENSATING_DEF =
      "{\"name\":\""
          + COMPENSATING_SAGA
          + "\",\"mode\":\"SAGA\",\"recoveryStrategy\":\"BACKWARD\","
          + "\"defaultRetryPolicy\":{\"maxAttempts\":1,\"initialIntervalMillis\":1},\"steps\":["
          + "{\"name\":\"debit\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/debit\","
          + "\"output\":{\"debitId\":\"$.debit_id\"}},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/reverse\","
          + "\"jsonBody\":{\"id\":\"${debitId}\"}}},"
          + "{\"name\":\"charge\",\"service\":\""
          + SERVICE
          + "\",\"execution\":{\"method\":\"POST\",\"path\":\"/charge\"},"
          + "\"compensation\":{\"method\":\"POST\",\"path\":\"/void\"}}]}";

  private final AtomicInteger debitHits = new AtomicInteger();
  private final AtomicInteger reverseHits = new AtomicInteger();

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext(
        "/debit",
        ex -> {
          debitHits.incrementAndGet();
          respond(ex, 200, "{\"debit_id\":\"DBT-1\"}");
        });
    participant.createContext(
        "/reverse",
        ex -> {
          reverseHits.incrementAndGet();
          respond(ex, 200, "{}");
        });
    participant.createContext("/charge", ex -> respond(ex, 422, "{}"));
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, COMPLETING_SAGA, COMPLETING_DEF);
    writeDefinition(definitionsDir, COMPENSATING_SAGA, COMPENSATING_DEF);
  }

  @Test
  void startSync_declarativeServiceStep_callsParticipantAndCompletes() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPLETING_SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPLETED");
    assertThat(debitHits.get()).isEqualTo(1);
  }

  @Test
  void startSync_declarativeBusinessFailure_compensatesViaParticipant() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPENSATING_SAGA + "\"}");

    // charge returned 422 → debit compensated via /reverse → cleanly rolled back.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(post.body()).get("status").asText()).isEqualTo("COMPENSATED");
    assertThat(debitHits.get()).isEqualTo(1);
    assertThat(reverseHits.get()).isEqualTo(1);
  }
}
