package com.scalar.db.saga.daemon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the <b>gRPC transport</b>: a real {@link GrpcSagaOrchestratorClient}
 * drives a live {@link SagaServer} over the wire (real engine + sqlite store + a declarative
 * service-step saga), exercising the full client → gRPC server → engine → store round-trip. This is
 * also what proves the grpc-java and protobuf runtime versions agree on serialization in practice.
 *
 * <p>Mirrors {@link SagaRestApiIntegrationTest} (same fixture and happy-path definition) but
 * through the gRPC client instead of HTTP. The last test asserts a saga started over gRPC is
 * visible over REST — both transports delegate to the same orchestrator.
 */
class SagaGrpcIntegrationTest extends DaemonIntegrationTestSupport {

  private static final String SAGA_NAME = "saga";

  // One step that completes against the participant (reaches COMPLETED).
  private static final String DEFINITION =
      withService(
          """
          { "name": "saga", "mode": "SAGA", "steps": [
            { "name": "s1", "service": "$svc",
              "execution":    { "method": "POST", "path": "/debit" },
              "compensation": { "method": "POST", "path": "/reverse" } } ] }
          """);

  private GrpcSagaOrchestratorClient client;

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200);
    route(participant, "/reverse", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
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

  @Test
  void startSync_blocksUntilTerminalAndIsQueryable() {
    // Act — a synchronous start blocks until the saga is terminal.
    String sagaId = client.start(SAGA_NAME, Map.of());

    // Assert
    assertThat(sagaId).isNotBlank();
    assertThat(client.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void startAsync_returnsImmediatelyAndEventuallyCompletes() throws Exception {
    // Act
    String sagaId = client.startAsync(SAGA_NAME, Map.of());

    // Assert
    assertThat(sagaId).isNotBlank();
    assertThat(awaitTerminal(sagaId)).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void getStateSnapshot_returnsAllFieldsWithEmptyOwnerId() {
    String sagaId = client.start(SAGA_NAME, Map.of());

    SagaStateSnapshot snapshot = client.getStateSnapshot(sagaId);

    assertThat(snapshot.getSagaId()).isEqualTo(sagaId);
    assertThat(snapshot.getSagaName()).isEqualTo(SAGA_NAME);
    assertThat(snapshot.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(snapshot.getDefinitionVersion()).isNotBlank();
    assertThat(snapshot.getCreatedAt()).isNotNull();
    assertThat(snapshot.getUpdatedAt()).isNotNull();
    // owner_id is a server-internal field dropped from the wire; the client fills it with "".
    assertThat(snapshot.getOwnerId()).isEmpty();
  }

  @Test
  void startClientSuppliedIdTwice_throwsAlreadyExistsCarryingExistingState() {
    // Arrange — the first start (client-supplied id) blocks to terminal.
    client.start("order-1", SAGA_NAME, Map.of());

    // Act + Assert — the duplicate id conflicts; the client re-fetches and carries the existing
    // state.
    assertThatThrownBy(() -> client.start("order-1", SAGA_NAME, Map.of()))
        .isInstanceOfSatisfying(
            SagaAlreadyExistsException.class,
            e -> {
              assertThat(e.getSagaId()).isEqualTo("order-1");
              assertThat(e.getExisting()).isNotNull();
              assertThat(e.getExisting().getSagaId()).isEqualTo("order-1");
              assertThat(e.getExisting().getStatus()).isEqualTo(SagaStatus.COMPLETED);
            });
  }

  @Test
  void getStateSnapshot_unknownId_throwsSagaNotFound() {
    assertThatThrownBy(() -> client.getStateSnapshot("does-not-exist"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void sagaStartedOverGrpc_isVisibleOverRest() throws Exception {
    // Act — start via gRPC, then read the same saga via the REST transport.
    String sagaId = client.start(SAGA_NAME, Map.of());

    // Assert — both transports see the same saga in the same store.
    HttpResponse<String> rest = get("/sagas/" + sagaId);
    assertThat(rest.statusCode()).isEqualTo(200);
    JsonNode body = MAPPER.readTree(rest.body());
    assertThat(body.get("sagaId").asText()).isEqualTo(sagaId);
    assertThat(body.get("status").asText()).isEqualTo("COMPLETED");
  }

  /** Polls {@code getStateSnapshot} until the saga reaches a terminal status, then returns it. */
  private SagaStatus awaitTerminal(String sagaId) throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      SagaStatus status = client.getStateSnapshot(sagaId).getStatus();
      if (status.isTerminal()) {
        return status;
      }
      Thread.sleep(40);
    }
    throw new AssertionError("Saga " + sagaId + " did not reach a terminal status in time");
  }
}
