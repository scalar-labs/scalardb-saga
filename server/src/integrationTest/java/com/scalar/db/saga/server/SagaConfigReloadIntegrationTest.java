package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * End-to-end reload scenarios against a live daemon: onboarding a service and definition without a
 * restart, rejection keeping the previous configuration serving, secret rotation propagating
 * through {@code ${file:...}}, and un-bumped edits being refused. Tests mutate the watched
 * directories and drive a deterministic pass via {@code reloadNow()} — the scheduler that would
 * otherwise run the pass on an interval has its own unit coverage.
 */
class SagaConfigReloadIntegrationTest extends ServerIntegrationTestSupport {

  private static final String BASE_SAGA = "base-saga";
  private static final String ONBOARDED_SAGA = "onboarded-saga";
  private static final String ONBOARDED_SERVICE = "onboarded";

  private final AtomicReference<String> observedAuthorization = new AtomicReference<>();

  private static final String BASE_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "call", "service": "$svc",
              "execution":    { "method": "POST", "path": "/base" },
              "compensation": { "method": "POST", "path": "/base-undo" } } ] }
          """
              .replace("$name", BASE_SAGA));

  private static final String ONBOARDED_DEF =
      """
      { "name": "$name", "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/onboarded" },
          "compensation": { "method": "POST", "path": "/onboarded-undo" } } ] }
      """
          .replace("$name", ONBOARDED_SAGA)
          .replace("$svc", ONBOARDED_SERVICE);

  @Override
  protected void configureParticipant(HttpServer participant) {
    participant.createContext(
        "/base",
        exchange -> {
          observedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          respond(exchange, 200, "{}");
        });
    route(participant, "/base-undo", 200);
    route(participant, "/onboarded", 200);
    route(participant, "/onboarded-undo", 200);
    route(participant, "/v2", 200);
    route(participant, "/v2-undo", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, BASE_SAGA, BASE_DEF);
  }

  private String startSaga(String sagaName) throws Exception {
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + sagaName + "\"}");
    assertThat(response.statusCode()).isEqualTo(200);
    return status(response);
  }

  @Test
  void reload_newServiceAndDefinitionFiles_sagaRunsEndToEndWithoutRestart() throws Exception {
    // Arrange — onboard a new service (same participant, its own service file) and a definition
    // that references it, both after boot
    Properties onboarded = new Properties();
    onboarded.setProperty("base_url", participantBaseUrl());
    writeService(ONBOARDED_SERVICE, onboarded);
    writeDefinition(definitionsDir(), ONBOARDED_SAGA, ONBOARDED_DEF);

    // Act
    boolean applied = reloadNow();

    // Assert — the onboarded saga runs end to end on the live daemon
    assertThat(applied).isTrue();
    assertThat(startSaga(ONBOARDED_SAGA)).isEqualTo("COMPLETED");
    assertThat(hits("/onboarded")).isEqualTo(1);
  }

  @Test
  void reload_danglingReference_keepsServingOldConfigThenAppliesTheFix() throws Exception {
    // Arrange — a new definition referencing a service that has no file
    writeDefinition(definitionsDir(), ONBOARDED_SAGA, ONBOARDED_DEF);

    // Act — the pass rejects; the previously applied configuration keeps serving
    boolean rejected = !reloadNow();

    // Assert
    assertThat(rejected).isTrue();
    assertThat(startSaga(BASE_SAGA)).isEqualTo("COMPLETED");

    // Act — fix by supplying the service file; the next pass applies
    Properties onboarded = new Properties();
    onboarded.setProperty("base_url", participantBaseUrl());
    writeService(ONBOARDED_SERVICE, onboarded);

    assertThat(reloadNow()).isTrue();
    assertThat(startSaga(ONBOARDED_SAGA)).isEqualTo("COMPLETED");
  }

  @Test
  void reload_rotatedSecretFile_nextSagaSendsTheNewHeader() throws Exception {
    // Arrange — re-point the base service's Authorization header at a mounted secret
    Files.writeString(secretsDir().resolve("token"), "Bearer before-rotation");
    Properties base = new Properties();
    base.setProperty("base_url", participantBaseUrl());
    base.setProperty("header.Authorization", "${file:UTF-8:" + secretsDir().resolve("token") + "}");
    writeService(SERVICE, base);
    assertThat(reloadNow()).isTrue();
    assertThat(startSaga(BASE_SAGA)).isEqualTo("COMPLETED");
    assertThat(observedAuthorization.get()).isEqualTo("Bearer before-rotation");

    // Act — rotate the SECRET only; no watched service file changes
    Files.writeString(secretsDir().resolve("token"), "Bearer after-rotation");
    assertThat(reloadNow()).isTrue();

    // Assert — the next saga's outbound call carries the rotated credential
    assertThat(startSaga(BASE_SAGA)).isEqualTo("COMPLETED");
    assertThat(observedAuthorization.get()).isEqualTo("Bearer after-rotation");
  }

  @Test
  void reload_unBumpedDefinitionEdit_rejectedUntilVersionBumped() throws Exception {
    // Arrange — edit the base definition's path without bumping its version
    // "/base" -> "/v2" also rewrites "/base-undo" to "/v2-undo"; both routes exist.
    String edited = BASE_DEF.replace("/base", "/v2");
    writeDefinition(definitionsDir(), BASE_SAGA, edited);

    // Act & Assert — rejected; the old definition keeps serving (the edited path is never hit)
    assertThat(reloadNow()).isFalse();
    assertThat(startSaga(BASE_SAGA)).isEqualTo("COMPLETED");
    assertThat(hits("/v2")).isZero();

    // Act — bump the version; the pass applies and the new path serves
    String bumped =
        edited.replace(
            "\"name\": \"" + BASE_SAGA + "\"",
            "\"name\": \"" + BASE_SAGA + "\", \"version\": \"2.0\"");
    writeDefinition(definitionsDir(), BASE_SAGA, bumped);

    assertThat(reloadNow()).isTrue();
    assertThat(startSaga(BASE_SAGA)).isEqualTo("COMPLETED");
    assertThat(hits("/v2")).isEqualTo(1);
  }
}
