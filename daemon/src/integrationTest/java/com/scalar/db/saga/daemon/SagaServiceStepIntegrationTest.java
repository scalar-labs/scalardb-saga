package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the daemon's <b>outbound service-step transport</b> — a declarative {@code
 * service} step's call actually reaches the participant over HTTP. Asserts that the participant is
 * hit on execution and, on a downstream failure, hit again on compensation (a saga-id-keyed undo —
 * the correlation header identifies the work, not the failed step's own output) — i.e. that the
 * daemon→participant wiring works. Covers default completion, BACKWARD compensation, and MIXED
 * recovery with a {@code pivot} step (a pre-pivot failure compensates → {@code COMPENSATED}; a
 * post-pivot failure rolls forward → stays {@code RUNNING}).
 *
 * <p>Counterpart: {@link SagaRestApiIntegrationTest} asserts on the <b>inbound</b> REST contract.
 * Both share {@link DaemonIntegrationTestSupport}; this one adds participant-hit assertions.
 */
class SagaServiceStepIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String COMPLETING_SAGA = "declarative-saga";
  private static final String COMPENSATING_SAGA = "declarative-compensating-saga";
  private static final String MIXED_BEFORE_PIVOT = "mixed-before-pivot";
  private static final String MIXED_AFTER_PIVOT = "mixed-after-pivot";
  private static final String MISMATCHED_BODY_SAGA = "declarative-mismatched-body";

  // Single step that captures output and completes.
  private static final String COMPLETING_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "debit", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit", "output": { "debitId": "$.debit_id" } },
              "compensation": { "method": "POST", "path": "/reverse" } } ] }
          """
              .replace("$name", COMPLETING_SAGA));

  // debit succeeds, charge returns 422 → backward recovery compensates debit via /reverse.
  private static final String COMPENSATING_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "BACKWARD",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "debit", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit", "output": { "debitId": "$.debit_id" } },
              "compensation": { "method": "POST", "path": "/reverse" } },
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge" },
              "compensation": { "method": "POST", "path": "/void" } } ] }
          """
              .replace("$name", COMPENSATING_SAGA));

  // MIXED recovery, 3 steps with a pivot. The pivot's execution fails (before the commit point), so
  // the completed pre-pivot step (s1) is compensated and the saga rolls back.
  private static final String MIXED_BEFORE_PIVOT_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "MIXED",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/m/s1" },
              "compensation": { "method": "POST", "path": "/m/s1-undo" } },
            { "name": "pivot", "service": "$svc", "pivot": true,
              "execution":    { "method": "POST", "path": "/m/pivot-fail" },
              "compensation": { "method": "POST", "path": "/m/pivot-undo" } },
            { "name": "s3", "service": "$svc",
              "execution":    { "method": "POST", "path": "/m/s3" },
              "compensation": { "method": "POST", "path": "/m/s3-undo" } } ] }
          """
              .replace("$name", MIXED_BEFORE_PIVOT));

  // MIXED recovery, 3 steps with a pivot. A post-pivot step (s3) fails: past the pivot there is no
  // rollback — the saga stays RUNNING (recovery rolls forward) and nothing is compensated.
  private static final String MIXED_AFTER_PIVOT_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "MIXED",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/m/s1" },
              "compensation": { "method": "POST", "path": "/m/s1-undo" } },
            { "name": "pivot", "service": "$svc", "pivot": true,
              "execution":    { "method": "POST", "path": "/m/pivot" },
              "compensation": { "method": "POST", "path": "/m/pivot-undo" } },
            { "name": "s3", "service": "$svc",
              "execution":    { "method": "POST", "path": "/m/s3-fail" },
              "compensation": { "method": "POST", "path": "/m/s3-undo" } } ] }
          """
              .replace("$name", MIXED_AFTER_PIVOT));

  // debit succeeds; charge returns 200 but its body lacks the mapped output field, so output
  // extraction fails AFTER the participant committed → the committed charge is compensated via
  // /void (the bug's "known committed" case: a 2xx whose body does not match the output mapping).
  private static final String MISMATCHED_BODY_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "BACKWARD",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "debit", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit", "output": { "debitId": "$.debit_id" } },
              "compensation": { "method": "POST", "path": "/reverse" } },
            { "name": "charge", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge-badbody", "output": { "chargeId": "$.charge_id" } },
              "compensation": { "method": "POST", "path": "/void" } } ] }
          """
              .replace("$name", MISMATCHED_BODY_SAGA));

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200, "{\"debit_id\":\"DBT-1\"}");
    route(participant, "/reverse", 200);
    route(participant, "/charge", 422);
    route(participant, "/void", 200); // charge's compensation: a 422 may have committed
    route(participant, "/charge-badbody", 200, "{}"); // 2xx but missing the mapped output field

    // MIXED-recovery (pivot) endpoints.
    route(participant, "/m/s1", 200);
    route(participant, "/m/s1-undo", 200);
    route(participant, "/m/pivot", 200);
    route(participant, "/m/pivot-fail", 422);
    route(participant, "/m/pivot-undo", 200);
    route(participant, "/m/s3", 200);
    route(participant, "/m/s3-fail", 422);
    route(participant, "/m/s3-undo", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, COMPLETING_SAGA, COMPLETING_DEF);
    writeDefinition(definitionsDir, COMPENSATING_SAGA, COMPENSATING_DEF);
    writeDefinition(definitionsDir, MIXED_BEFORE_PIVOT, MIXED_BEFORE_PIVOT_DEF);
    writeDefinition(definitionsDir, MIXED_AFTER_PIVOT, MIXED_AFTER_PIVOT_DEF);
    writeDefinition(definitionsDir, MISMATCHED_BODY_SAGA, MISMATCHED_BODY_DEF);
  }

  @Test
  void startSync_declarativeServiceStep_callsParticipantAndCompletes() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPLETING_SAGA + "\"}");

    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPLETED");
    assertThat(hits("/debit")).isEqualTo(1);
  }

  @Test
  void startSync_declarativeBusinessFailure_compensatesViaParticipant() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPENSATING_SAGA + "\"}");

    // charge returned 422 (the side effect may have committed), so charge is compensated via /void
    // AND debit via /reverse (compensate from the failed step, not just the prior ones).
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPENSATED");
    assertThat(hits("/debit")).isEqualTo(1);
    assertThat(hits("/void")).isEqualTo(1);
    assertThat(hits("/reverse")).isEqualTo(1);
  }

  @Test
  void startSync_declarative2xxMismatchedOutput_compensatesCommittedStep() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + MISMATCHED_BODY_SAGA + "\"}");

    // charge returned 200 (committed) but its body lacked the mapped output field, so output
    // extraction failed. The committed charge is still compensated via /void — it is NOT orphaned —
    // and debit via /reverse.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPENSATED");
    assertThat(hits("/debit")).isEqualTo(1);
    assertThat(hits("/charge-badbody")).isEqualTo(1);
    assertThat(hits("/void")).isEqualTo(1);
    assertThat(hits("/reverse")).isEqualTo(1);
  }

  @Test
  void startSync_mixedRecoveryFailureBeforePivot_compensates() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + MIXED_BEFORE_PIVOT + "\"}");

    // The pivot's execution returned 422 (it may have committed before responding), so the pivot IS
    // compensated via /m/pivot-undo along with the completed pre-pivot step; s3 never runs.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPENSATED");
    assertThat(hits("/m/s1-undo")).isEqualTo(1);
    assertThat(hits("/m/pivot-undo")).isEqualTo(1);
  }

  @Test
  void startAsync_mixedRecoveryFailureAfterPivot_staysRunning() throws Exception {
    HttpResponse<String> post =
        post("/sagas?async=true", "{\"sagaName\":\"" + MIXED_AFTER_PIVOT + "\"}");
    assertThat(post.statusCode()).isEqualTo(202);
    String sagaId = MAPPER.readTree(post.body()).get("sagaId").asText();

    // A post-pivot step failed: past the commit point there is no rollback — the saga stays RUNNING
    // (recovery rolls forward) and nothing is compensated.
    awaitHit("/m/s3-fail");
    assertThat(status(get("/sagas/" + sagaId))).isEqualTo("RUNNING");
    assertThat(hits("/m/s1-undo")).isZero();
    assertThat(hits("/m/pivot-undo")).isZero();
  }
}
