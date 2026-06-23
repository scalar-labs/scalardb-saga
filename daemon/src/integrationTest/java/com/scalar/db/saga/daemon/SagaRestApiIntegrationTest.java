package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the daemon's <b>client-facing REST contract</b> — how the {@link
 * SagaServer} responds to HTTP callers: synchronous vs {@code ?async} start, client-supplied IDs
 * via {@code PUT} ({@code 409} + the existing snapshot on conflict), status {@code GET} ({@code
 * 404} when unknown), request validation ({@code 400} for a missing {@code sagaName}, a malformed
 * body, or an unrecognized {@code ?async} value), and the synchronous outcome status codes ({@code
 * 200} for a terminal state, {@code 202} while still resolving). The sagas are minimal declarative
 * service steps whose only job is to reach a known state; this test asserts only on the daemon's
 * responses.
 *
 * <p>Counterpart: {@link SagaServiceStepIntegrationTest} asserts on the <b>outbound</b> side — that
 * the participant is actually called. Both share {@link DaemonIntegrationTestSupport}.
 */
class SagaRestApiIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String SAGA_NAME = "saga";
  private static final String COMPENSATING_SAGA = "compensating-saga";
  private static final String COMPENSATION_FAILING_SAGA = "compensation-failing-saga";

  // One step that completes against the participant.
  private static final String DEFINITION =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit" },
              "compensation": { "method": "POST", "path": "/reverse" } } ] }
          """
              .replace("$name", SAGA_NAME));

  // s1 succeeds, s2 (/charge) returns 422 → backward recovery compensates s1 cleanly via /reverse.
  private static final String COMPENSATING_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "BACKWARD",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit" },
              "compensation": { "method": "POST", "path": "/reverse" } },
            { "name": "s2", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge" },
              "compensation": { "method": "POST", "path": "/void" } } ] }
          """
              .replace("$name", COMPENSATING_SAGA));

  // Like above, but s1's compensation (/reverse-fail) fails → saga stuck COMPENSATING.
  private static final String COMPENSATION_FAILING_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "recoveryStrategy": "BACKWARD",
            "defaultRetryPolicy": { "maxAttempts": 1, "initialIntervalMillis": 1 }, "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit" },
              "compensation": { "method": "POST", "path": "/reverse-fail" } },
            { "name": "s2", "service": "$svc",
              "execution":    { "method": "POST", "path": "/charge" },
              "compensation": { "method": "POST", "path": "/void" } } ] }
          """
              .replace("$name", COMPENSATION_FAILING_SAGA));

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200);
    route(participant, "/reverse", 200);
    route(participant, "/charge", 422);
    route(participant, "/reverse-fail", 500);
    route(participant, "/void", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
    writeDefinition(definitionsDir, COMPENSATING_SAGA, COMPENSATING_DEF);
    writeDefinition(definitionsDir, COMPENSATION_FAILING_SAGA, COMPENSATION_FAILING_DEF);
  }

  @Test
  void startSync_completesAndIsQueryable() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");

    assertThat(post.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(post.body());
    assertThat(body.get("status").asText()).isEqualTo("COMPLETED");
    String sagaId = body.get("sagaId").asText();
    assertThat(sagaId).isNotBlank();

    HttpResponse<String> get = get("/sagas/" + sagaId);
    assertThat(get.statusCode()).isEqualTo(200);
    assertThat(status(get)).isEqualTo("COMPLETED");
  }

  @Test
  void startAsync_returns202AndEventuallyCompletes() throws Exception {
    HttpResponse<String> post =
        post("/sagas?async=true", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");

    assertThat(post.statusCode()).isEqualTo(202);
    String sagaId = MAPPER.readTree(post.body()).get("sagaId").asText();

    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }

  @Test
  void startWithClientSuppliedIdTwice_returns409WithExisting() throws Exception {
    String body = "{\"sagaName\":\"" + SAGA_NAME + "\"}";

    HttpResponse<String> first = put("/sagas/order-1", body);
    assertThat(first.statusCode()).isEqualTo(200);

    HttpResponse<String> second = put("/sagas/order-1", body);
    assertThat(second.statusCode()).isEqualTo(409);
    JsonNode conflict = MAPPER.readTree(second.body());
    assertThat(conflict.get("error").asText()).isEqualTo("SAGA_ALREADY_EXISTS");
    assertThat(conflict.get("message").asText()).isNotBlank();
    assertThat(conflict.get("sagaId").asText()).isEqualTo("order-1");
    assertThat(conflict.get("existing").get("sagaId").asText()).isEqualTo("order-1");
  }

  @Test
  void getUnknownSaga_returns404() throws Exception {
    HttpResponse<String> get = get("/sagas/does-not-exist");
    assertThat(get.statusCode()).isEqualTo(404);
    assertThat(MAPPER.readTree(get.body()).get("error").asText()).isEqualTo("SAGA_NOT_FOUND");
  }

  @Test
  void putWithInvalidSagaId_returns400() throws Exception {
    // A client-supplied id outside [a-zA-Z0-9._-]{1,128} is a client error (400), not a 500.
    HttpResponse<String> put =
        put("/sagas/" + "a".repeat(129), "{\"sagaName\":\"" + SAGA_NAME + "\"}");

    assertThat(put.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(put.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void postWithoutSagaName_returns400() throws Exception {
    HttpResponse<String> post = post("/sagas", "{}");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void postWithMalformedBody_returns400() throws Exception {
    HttpResponse<String> post = post("/sagas", "not-json");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void postWithUnrecognizedAsyncValue_returns400() throws Exception {
    HttpResponse<String> post =
        post("/sagas?async=1", "{\"sagaName\":\"" + SAGA_NAME + "\",\"input\":{}}");
    assertThat(post.statusCode()).isEqualTo(400);
    assertThat(MAPPER.readTree(post.body()).get("error").asText()).isEqualTo("BAD_REQUEST");
  }

  @Test
  void startSync_businessFailure_returns200WithCompensated() throws Exception {
    HttpResponse<String> post = post("/sagas", "{\"sagaName\":\"" + COMPENSATING_SAGA + "\"}");

    // The saga ran to a terminal state (cleanly rolled back) → 200, with the outcome in the body.
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(status(post)).isEqualTo("COMPENSATED");
  }

  @Test
  void startSync_compensationFailure_returns202WithCompensating() throws Exception {
    HttpResponse<String> post =
        post("/sagas", "{\"sagaName\":\"" + COMPENSATION_FAILING_SAGA + "\"}");

    // Compensation itself failed → saga is non-terminal (still resolving) → 202.
    assertThat(post.statusCode()).isEqualTo(202);
    assertThat(status(post)).isEqualTo("COMPENSATING");
  }
}
