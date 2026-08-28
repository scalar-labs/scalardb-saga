package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.exception.SagaDefinitionNotServedException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end retirement against a live daemon: what an operator does to take a saga out of service,
 * and what a caller sees afterwards.
 *
 * <p>Three things need a real server rather than a unit test. The refusal has to survive the trip
 * to a client — the mapper tables prove the exception becomes 422 and {@code FAILED_PRECONDITION},
 * and the engine tests prove an unserved saga throws one, but nothing proves the two halves connect
 * through a resource, an interceptor and a future. The registration has to <b>outlive</b> the file,
 * which is what separates this from a 404 and what makes a saga still running finish. And the
 * refusal has to survive a <b>restart</b>: the served set is rebuilt from files at boot, so nothing
 * carries a retirement across a process except the absence of the file itself.
 */
class SagaDefinitionRetirementIntegrationTest extends ServerIntegrationTestSupport {

  private static final String RETIRING_SAGA = "retiring-saga";
  private static final String KEEPER_SAGA = "keeper-saga";

  /** The service only the retiring saga calls, so it can be deleted alongside the retirement. */
  private static final String LEGACY_SERVICE = "legacy";

  private static final String RETIRING_VERSION = "1.0";

  /**
   * A service NOTHING else names, so removing it is rejected only by the version this pass adopts.
   * Sharing {@code legacy} would let the retiring saga's own cross-check cause the rejection.
   */
  private static final String ROLLBACK_ONLY_SERVICE = "rollback-only";

  private static final String RETIRING_DEF =
      """
      { "name": "$name", "version": "$ver", "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/legacy" },
          "compensation": { "method": "POST", "path": "/legacy-undo" } } ] }
      """
          .replace("$name", RETIRING_SAGA)
          .replace("$ver", RETIRING_VERSION)
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

  private static final String SLOW_SAGA = "slow-saga";

  private static final String ROLLED_BACK_SAGA = "rolled-back-saga";

  private static final String SLOW_DEF =
      """
      { "name": "$name", "version": "1.0", "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/slow" },
          "compensation": { "method": "POST", "path": "/legacy-undo" } } ] }
      """
          .replace("$name", SLOW_SAGA)
          .replace("$svc", LEGACY_SERVICE);

  /** The rolled-back saga on the shared service, which outlives the rollback. */
  private static String rolledBackDefOnSharedService(String version) {
    return withService(
        """
        { "name": "$name", "version": "$ver", "mode": "SAGA", "steps": [
          { "name": "call", "service": "$svc",
            "execution":    { "method": "POST", "path": "/keep" },
            "compensation": { "method": "POST", "path": "/keep-undo" } } ] }
        """
            .replace("$name", ROLLED_BACK_SAGA)
            .replace("$ver", version));
  }

  /** The same saga moved onto a service only it names, which the rollback then tries to drop. */
  private static String rolledBackDefOnLegacy(String version) {
    return """
      { "name": "$name", "version": "$ver", "mode": "SAGA", "steps": [
        { "name": "call", "service": "$svc",
          "execution":    { "method": "POST", "path": "/legacy" },
          "compensation": { "method": "POST", "path": "/legacy-undo" } } ] }
      """
        .replace("$name", ROLLED_BACK_SAGA)
        .replace("$ver", version)
        .replace("$svc", ROLLBACK_ONLY_SERVICE);
  }

  /** The wire code string, kept here so the test states it literally rather than deriving it. */
  private static final String NOT_SERVED_CODE = "DB-SAGA-10403";

  /** Held to keep a saga genuinely in flight while its definition file is deleted. */
  private final CountDownLatch releaseSlowStep = new CountDownLatch(1);

  private final CountDownLatch slowStepEntered = new CountDownLatch(1);

  private GrpcSagaOrchestratorClient client;

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/legacy", 200);
    route(participant, "/legacy-undo", 200);
    route(participant, "/keep", 200);
    route(participant, "/keep-undo", 200);
    // A route that blocks until the test releases it, so a saga can be held mid-step across a
    // retirement. The runbook's promise is that such a saga finishes; nothing else can show it.
    participant.createContext(
        "/slow",
        exchange -> {
          slowStepEntered.countDown();
          try {
            releaseSlowStep.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          exchange.sendResponseHeaders(200, 0);
          try (OutputStream body = exchange.getResponseBody()) {
            body.write("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }
        });
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
    Properties rollbackOnly = new Properties();
    rollbackOnly.setProperty("base_url", participantBaseUrl());
    services.put(ROLLBACK_ONLY_SERVICE, rollbackOnly);
  }

  @BeforeEach
  void createClient() {
    client = GrpcSagaOrchestratorClient.create("localhost:" + grpcPort());
  }

  @AfterEach
  void releaseAnyHeldStep() {
    releaseSlowStep.countDown();
  }

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  /** Retires the saga, as an operator's commit plus one reload would: delete the file. */
  private void retire() throws IOException {
    Files.delete(definitionsDir().resolve(RETIRING_SAGA + ".json"));
    assertThat(reloadNow()).isTrue();
  }

  private HttpResponse<String> startRetiring() throws Exception {
    return post("/sagas", "{\"sagaName\":\"" + RETIRING_SAGA + "\"}");
  }

  @Test
  void start_afterItsFileIsDeletedOverRest_refusedAsPreconditionFailedWithTheCode()
      throws Exception {
    // Arrange — the saga starts normally while its file is in place
    assertThat(startRetiring().statusCode()).isEqualTo(200);
    retire();

    // Act
    HttpResponse<String> response = startRetiring();

    // Assert — 422 rather than 409: retrying the same start against this replica never succeeds
    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(MAPPER.readTree(response.body()).get("errorCode").asText())
        .isEqualTo(NOT_SERVED_CODE);
  }

  @Test
  void start_afterItsFileIsDeletedOverGrpc_reconstructsTheTypedException() throws Exception {
    // Arrange
    retire();

    // Act & Assert — the client rebuilds the typed exception from the wire code, so this covers
    // the trailer and the registry round-trip as well as the status
    assertThatThrownBy(() -> client.start(RETIRING_SAGA, Map.of()))
        .isInstanceOf(SagaDefinitionNotServedException.class)
        .satisfies(
            e ->
                assertThat(((SagaDefinitionNotServedException) e).getSagaName())
                    .isEqualTo(RETIRING_SAGA));
  }

  @Test
  void start_pinnedToTheVersionOfADeletedDefinition_isRefusedToo() throws Exception {
    // Being served is a property of the NAME. The version is still registered and still resolves,
    // so without the name check this pin would run a saga the operator took out of service.
    // Arrange
    retire();

    // Act & Assert
    assertThatThrownBy(
            () -> client.start(new SagaDefinitionId(RETIRING_SAGA, RETIRING_VERSION), Map.of()))
        .isInstanceOf(SagaDefinitionNotServedException.class);
  }

  @Test
  void start_sagaNameNeverRegistered_isNotFoundRatherThanNotServed() throws Exception {
    // The other side of the distinction: a name nobody ever registered is a different problem with
    // a different fix, and must not be reported as a saga this daemon declines to serve.
    // Act
    HttpResponse<String> response = post("/sagas", "{\"sagaName\":\"no-such-saga\"}");

    // Assert
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(MAPPER.readTree(response.body()).get("errorCode").asText())
        .isNotEqualTo(NOT_SERVED_CODE);
  }

  @Test
  void start_afterTheFileIsRestored_servesTheSagaAgain() throws Exception {
    // Retirement is reversible by reverting the change that caused it, with nothing to register and
    // no version to invent — the registration was never removed.
    // Arrange
    retire();
    assertThat(startRetiring().statusCode()).isEqualTo(422);

    // Act
    writeDefinition(definitionsDir(), RETIRING_SAGA, RETIRING_DEF);
    assertThat(reloadNow()).isTrue();

    // Assert
    assertThat(startRetiring().statusCode()).isEqualTo(200);
  }

  @Test
  void restart_afterItsFileIsDeleted_stillRefusesTheSaga() throws Exception {
    // Nothing carries a retirement across a process except the absence of the file: the served set
    // is rebuilt from the directory at boot. The registration is still in the store, so a replica
    // that boots without the file has to refuse rather than serve what it finds registered.
    // Arrange
    retire();

    // Act
    restartServer();

    // Assert
    HttpResponse<String> response = startRetiring();
    assertThat(response.statusCode()).isEqualTo(422);
    assertThat(MAPPER.readTree(response.body()).get("errorCode").asText())
        .isEqualTo(NOT_SERVED_CODE);
  }

  @Test
  void sagaStartedBeforeRetirement_remainsReadableAfterwards() throws Exception {
    // Retirement refuses new starts and must not touch what already ran. The definition stays in
    // the store, which is what lets this saga still be resolved by the version it recorded.
    // Arrange
    HttpResponse<String> started = startRetiring();
    assertThat(started.statusCode()).isEqualTo(200);
    String sagaId = MAPPER.readTree(started.body()).get("sagaId").asText();

    // Act
    retire();

    // Assert
    HttpResponse<String> fetched = get("/sagas/" + sagaId);
    assertThat(fetched.statusCode()).isEqualTo(200);
    assertThat(status(fetched)).isEqualTo("COMPLETED");
  }

  @Test
  void sagaInFlightWhenItsFileIsDeleted_finishesAndStaysRecoverable() throws Exception {
    // The promise the exception javadoc, the start gate and the runbook all make: retirement stops
    // NEW starts and leaves work already running alone. It holds because a running saga resolves
    // its definition by the version recorded at its start, never through the served-set gate — so
    // only a saga genuinely mid-step across the deletion can show it.
    // Arrange — a saga whose step blocks in the participant
    writeDefinition(definitionsDir(), SLOW_SAGA, SLOW_DEF);
    assertThat(reloadNow()).isTrue();
    HttpResponse<String> accepted =
        post("/sagas?async=true", "{\"sagaName\":\"" + SLOW_SAGA + "\"}");
    assertThat(accepted.statusCode()).isEqualTo(202);
    String sagaId = MAPPER.readTree(accepted.body()).get("sagaId").asText();
    assertThat(slowStepEntered.await(30, TimeUnit.SECONDS)).isTrue();

    // Act — retire it while that step is still in the participant
    Files.delete(definitionsDir().resolve(SLOW_SAGA + ".json"));
    assertThat(reloadNow()).isTrue();
    assertThat(post("/sagas", "{\"sagaName\":\"" + SLOW_SAGA + "\"}").statusCode()).isEqualTo(422);
    releaseSlowStep.countDown();

    // Assert — the in-flight saga runs to a terminal state despite the retirement
    assertThat(pollUntilTerminal(sagaId)).isEqualTo("COMPLETED");
  }

  @Test
  void definitionFileRolledBack_guardsTheServingVersionsServices() throws Exception {
    // Rollback adoption against the REAL store. Re-registering older content is a no-op, so the
    // newer version keeps serving whatever the pass tracks — which is why asserting on what runs
    // proves nothing. What adoption changes is which version the guards reason about: it must be
    // the one serving, or removing a service only that version needs sails through.
    // Arrange — v1 uses the shared service, v2 moved the saga onto a service only it names
    writeDefinition(definitionsDir(), ROLLED_BACK_SAGA, rolledBackDefOnSharedService("1.0"));
    assertThat(reloadNow()).isTrue();
    writeDefinition(definitionsDir(), ROLLED_BACK_SAGA, rolledBackDefOnLegacy("2.0"));
    assertThat(reloadNow()).isTrue();

    // Act — roll back to a version that does not name that service, and drop it
    writeDefinition(definitionsDir(), ROLLED_BACK_SAGA, rolledBackDefOnSharedService("1.0"));
    Files.delete(servicesDir().resolve(ROLLBACK_ONLY_SERVICE + ".properties"));

    // Assert — rejected, because the adopted v2 still names it. Tracking the file's v1 instead
    // would accept this and strand the version that actually runs.
    assertThat(reloadNow()).isFalse();
    assertThat(post("/sagas", "{\"sagaName\":\"" + ROLLED_BACK_SAGA + "\"}").statusCode())
        .isEqualTo(200);
  }

  @Test
  void definitionAndItsServiceDeletedInOneChange_convergesAndRefusesStarts() throws Exception {
    // Retiring a saga and dropping the service only it used is one change and has to settle as
    // one: a pass rejected here would repeat every interval with the retirement never completing.
    // Arrange
    Files.delete(definitionsDir().resolve(RETIRING_SAGA + ".json"));
    Files.delete(servicesDir().resolve(LEGACY_SERVICE + ".properties"));

    // Act
    boolean applied = reloadNow();

    // Assert — it applies, and STAYS applied
    assertThat(applied).isTrue();
    assertThat(reloadNow()).isTrue();
    assertThat(startRetiring().statusCode()).isEqualTo(422);
    // The saga that was not retired is untouched by any of it
    assertThat(post("/sagas", "{\"sagaName\":\"" + KEEPER_SAGA + "\"}").statusCode())
        .isEqualTo(200);
  }
}
