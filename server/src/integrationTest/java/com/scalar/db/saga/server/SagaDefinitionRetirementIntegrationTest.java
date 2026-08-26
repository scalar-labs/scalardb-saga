package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.exception.SagaDefinitionDisabledException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end retirement against a live daemon: what an operator does to take a saga out of service,
 * and what a caller sees afterwards.
 *
 * <p>Two things need a real server rather than a unit test. First, the refusal has to survive the
 * trip to a client — the mapper tables prove a {@link SagaDefinitionDisabledException} becomes 422
 * and {@code FAILED_PRECONDITION}, and the engine tests prove a retired saga throws one, but
 * nothing proves the two halves connect through a resource, an interceptor, and a future. Second,
 * the retirement has to <b>converge</b>: an operator disables the saga and deletes the services
 * only it used in one change, and the reload has to settle on that rather than reject it every
 * interval with the retirement never completing.
 */
class SagaDefinitionRetirementIntegrationTest extends ServerIntegrationTestSupport {

  private static final String RETIRING_SAGA = "retiring-saga";
  private static final String KEEPER_SAGA = "keeper-saga";

  /** The service only the retiring saga calls, so it can be deleted alongside the retirement. */
  private static final String LEGACY_SERVICE = "legacy";

  private static final String RETIRING_DEF =
      """
      { "name": "$name", "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/legacy" },
          "compensation": { "method": "POST", "path": "/legacy-undo" } } ] }
      """
          .replace("$name", RETIRING_SAGA)
          .replace("$svc", LEGACY_SERVICE);

  /** The same saga, retired: a NEW version carrying the marker. */
  private static final String RETIRED_DEF =
      """
      { "name": "$name", "version": "2.0", "disabled": true, "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/legacy" },
          "compensation": { "method": "POST", "path": "/legacy-undo" } } ] }
      """
          .replace("$name", RETIRING_SAGA)
          .replace("$svc", LEGACY_SERVICE);

  /** A second saga, so retiring the first never empties the candidate set. */
  private static final String KEEPER_DEF =
      withService(
          """
          { "name": "$name", "mode": "SAGA", "steps": [
            { "name": "call", "service": "$svc",
              "execution":    { "method": "POST", "path": "/keep" },
              "compensation": { "method": "POST", "path": "/keep-undo" } } ] }
          """
              .replace("$name", KEEPER_SAGA));

  private GrpcSagaOrchestratorClient client;

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/legacy", 200);
    route(participant, "/legacy-undo", 200);
    route(participant, "/keep", 200);
    route(participant, "/keep-undo", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, RETIRING_SAGA, RETIRING_DEF);
    writeDefinition(definitionsDir, KEEPER_SAGA, KEEPER_DEF);
  }

  @Override
  protected void configureServices(Map<String, Properties> services) {
    Properties legacy = new Properties();
    legacy.setProperty("base_url", participantBaseUrl());
    services.put(LEGACY_SERVICE, legacy);
  }

  @BeforeEach
  void createClient() {
    client = GrpcSagaOrchestratorClient.create("localhost:" + grpcPort());
  }

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  /** Publishes the retirement and applies it, as an operator's commit plus one reload would. */
  private void retire() throws IOException {
    writeDefinition(definitionsDir(), RETIRING_SAGA, RETIRED_DEF);
    assertThat(reloadNow()).isTrue();
  }

  @Test
  void start_afterRetirementOverRest_refusedAsPreconditionFailedWithTheCode() throws Exception {
    // Arrange — the saga starts normally before it is retired
    assertThat(post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}").statusCode())
        .isEqualTo(200);
    retire();

    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}");

    // Assert — 422 rather than 409: retrying the same start never succeeds
    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(MAPPER.readTree(response.body()).get("errorCode").asText())
        .isEqualTo(SagaErrorCodeText.DISABLED);
  }

  @Test
  void start_afterRetirementOverGrpc_reconstructsTheTypedException() throws Exception {
    // Arrange
    retire();

    // Act & Assert — the client rebuilds the typed exception from the wire code, so this covers
    // the trailer and the registry round-trip as well as the status
    assertThatThrownBy(() -> client.start(RETIRING_SAGA, Map.of()))
        .isInstanceOf(SagaDefinitionDisabledException.class)
        .satisfies(
            e -> {
              SagaDefinitionDisabledException disabled = (SagaDefinitionDisabledException) e;
              assertThat(disabled.getSagaName()).isEqualTo(RETIRING_SAGA);
              assertThat(disabled.getVersion()).isEqualTo("2.0");
            });
  }

  @Test
  void retirementAndServiceRemovalInOneChange_convergesAndRefusesStarts() throws Exception {
    // The single-commit retirement: mark the saga disabled AND delete the service only it used.
    // A retired definition is exempt from the service cross-check, so the pass that completes the
    // retirement is not rejected by the definition that no longer runs.
    // Arrange
    writeDefinition(definitionsDir(), RETIRING_SAGA, RETIRED_DEF);
    Files.delete(servicesDir().resolve(LEGACY_SERVICE + ".properties"));

    // Act
    boolean applied = reloadNow();

    // Assert — it applies, and STAYS applied: a pass that rejected here would repeat every
    // interval with the retirement never completing
    assertThat(applied).isTrue();
    assertThat(reloadNow()).isTrue();
    assertThat(post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}").statusCode())
        .isEqualTo(422);
    // The saga that was not retired is untouched by any of it
    assertThat(post("/sagas", "{\"sagaName\":\"" + KEEPER_SAGA + "\"}").statusCode())
        .isEqualTo(200);
  }

  @Test
  void retirementThenFileDeletion_convergesQuietly() throws Exception {
    // The runbook's sequence, completed: disable, let it apply, then delete the file. The
    // vanished-file warning is for delete-WITHOUT-disable, so this path must stay clean.
    // Arrange
    retire();

    // Act
    Files.delete(definitionsDir().resolve(RETIRING_SAGA + ".json"));

    // Assert
    assertThat(reloadNow()).isTrue();
    assertThat(reloadNow()).isTrue();
    // Still refused: deleting the file retires nothing, the registered disabled version does
    assertThat(post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}").statusCode())
        .isEqualTo(422);
  }

  @Test
  void sagaStartedBeforeRetirement_remainsReadableAfterwards() throws Exception {
    // Retirement refuses new starts and must not touch what already ran.
    // Arrange
    HttpResponse<String> started = post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}");
    assertThat(started.statusCode()).isEqualTo(200);
    String sagaId = MAPPER.readTree(started.body()).get("sagaId").asText();

    // Act
    retire();

    // Assert
    HttpResponse<String> fetched = get("/sagas/" + sagaId);
    assertThat(fetched.statusCode()).isEqualTo(200);
    assertThat(status(fetched)).isEqualTo("COMPLETED");
  }

  /** The wire code string, kept here so the test states it literally rather than deriving it. */
  private static final class SagaErrorCodeText {
    private static final String DISABLED = "DB-SAGA-10403";

    private SagaErrorCodeText() {}
  }
}
