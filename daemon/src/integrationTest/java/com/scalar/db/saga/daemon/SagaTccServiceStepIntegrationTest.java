package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of <b>declarative TCC</b> over the daemon: a {@code mode: "TCC"} definition
 * is parsed, registered, and driven through the REST API, and the participant is hit on the
 * reserve/confirm/cancel phases over HTTP. Covers the four distinctive TCC outcomes:
 *
 * <ul>
 *   <li>all reserves then all confirms → {@code COMPLETED};
 *   <li>a reservation (try) fails → prior reserves are cancelled → {@code COMPENSATED};
 *   <li>a confirmation fails permanently → past the pivot there is no rollback, so the saga stays
 *       {@code RUNNING} and nothing is cancelled;
 *   <li>a confirmation fails transiently → the step's {@code retryPolicy} rolls it forward to
 *       {@code COMPLETED} (still no cancellation).
 * </ul>
 *
 * <p>Counterpart to {@link SagaServiceStepIntegrationTest} (the SAGA-mode version); both share
 * {@link DaemonIntegrationTestSupport}. All phases hit the single participant ({@value #SERVICE})
 * on distinct paths.
 */
class SagaTccServiceStepIntegrationTest extends DaemonIntegrationTestSupport {

  // Two steps, both reserve+confirm succeed.
  private static final String COMPLETE_DEF =
      withService(
          """
          { "name": "tcc-complete", "mode": "TCC", "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-1" },
              "confirmation": { "method": "POST", "path": "/confirm-1" },
              "cancellation": { "method": "POST", "path": "/cancel-1" } },
            { "name": "s2", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-2" },
              "confirmation": { "method": "POST", "path": "/confirm-2" },
              "cancellation": { "method": "POST", "path": "/cancel-2" } } ] }
          """);

  // s1 reserves, s2's reservation returns 422 → s1 is cancelled.
  private static final String RESERVE_FAIL_DEF =
      withService(
          """
          { "name": "tcc-reserve-fail", "mode": "TCC",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-1" },
              "confirmation": { "method": "POST", "path": "/confirm-1" },
              "cancellation": { "method": "POST", "path": "/cancel-1" } },
            { "name": "s2", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-2-fail" },
              "confirmation": { "method": "POST", "path": "/confirm-2" },
              "cancellation": { "method": "POST", "path": "/cancel-2" } } ] }
          """);

  // Single step: reserve succeeds, confirm fails permanently (503) → past the pivot, stays RUNNING.
  private static final String CONFIRM_FAIL_DEF =
      withService(
          """
          { "name": "tcc-confirm-fail", "mode": "TCC",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-1" },
              "confirmation": { "method": "POST", "path": "/confirm-fail" },
              "cancellation": { "method": "POST", "path": "/cancel-1" } } ] }
          """);

  // Single step: reserve succeeds, confirm is flaky (503, 503, 200) → retry rolls it forward.
  private static final String CONFIRM_RETRY_DEF =
      withService(
          """
          { "name": "tcc-confirm-retry", "mode": "TCC",
            "defaultRetryPolicy": { "maxAttempts": 5, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "reservation":  { "method": "POST", "path": "/reserve-1" },
              "confirmation": { "method": "POST", "path": "/confirm-flaky" },
              "cancellation": { "method": "POST", "path": "/cancel-1" } } ] }
          """);

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/reserve-1", 200);
    route(participant, "/reserve-2", 200);
    route(participant, "/reserve-2-fail", 422);
    route(participant, "/confirm-1", 200);
    route(participant, "/confirm-2", 200);
    route(participant, "/confirm-fail", 503);
    routeFlaky(participant, "/confirm-flaky", 2, 503, 200);
    route(participant, "/cancel-1", 200);
    route(participant, "/cancel-2", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, "tcc-complete", COMPLETE_DEF);
    writeDefinition(definitionsDir, "tcc-reserve-fail", RESERVE_FAIL_DEF);
    writeDefinition(definitionsDir, "tcc-confirm-fail", CONFIRM_FAIL_DEF);
    writeDefinition(definitionsDir, "tcc-confirm-retry", CONFIRM_RETRY_DEF);
  }

  @Test
  void startSync_tccReservesConfirmsAndCompletes() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"tcc-complete\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
    assertThat(hits("/reserve-1")).isEqualTo(1);
    assertThat(hits("/reserve-2")).isEqualTo(1);
    assertThat(hits("/confirm-1")).isEqualTo(1);
    assertThat(hits("/confirm-2")).isEqualTo(1);
    assertThat(hits("/cancel-1")).isZero();
    assertThat(hits("/cancel-2")).isZero();
  }

  @Test
  void startSync_tccReserveFailure_cancelsPriorReserves() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"tcc-reserve-fail\"}");

    // s2's reservation returned 422 → s1 (already reserved) is cancelled; nothing is confirmed.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPENSATED");
    assertThat(hits("/reserve-1")).isEqualTo(1);
    assertThat(hits("/reserve-2-fail")).isEqualTo(1);
    assertThat(hits("/cancel-1")).isEqualTo(1);
    assertThat(hits("/confirm-1")).isZero();
  }

  @Test
  void startAsync_tccConfirmFailure_staysRunningWithoutCancelling() throws Exception {
    HttpResponse<String> post = post("/sagas?async=true", "{\"sagaName\":\"tcc-confirm-fail\"}");
    assertThat(post.statusCode()).isEqualTo(202);
    String sagaId = MAPPER.readTree(post.body()).get("sagaId").asText();

    // The reserve succeeds but the confirm fails permanently. The confirm is past the pivot, so the
    // engine does NOT cancel — the saga stays RUNNING (it can only roll forward), and no
    // cancellation is ever invoked.
    awaitHit("/confirm-fail");
    assertThat(status(get("/sagas/" + sagaId))).isEqualTo("RUNNING");
    assertThat(hits("/reserve-1")).isGreaterThanOrEqualTo(1);
    assertThat(hits("/cancel-1")).isZero();
  }

  @Test
  void startSync_tccConfirmTransientFailure_retriesForwardAndCompletes() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"tcc-confirm-retry\"}");

    // The confirm fails twice (503) then succeeds; the retryPolicy rolls it forward to COMPLETED —
    // a confirm failure rolls forward, never back, so the cancellation is never invoked.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
    assertThat(hits("/confirm-flaky")).isEqualTo(3);
    assertThat(hits("/cancel-1")).isZero();
  }
}
