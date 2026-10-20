package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the <b>application gRPC client against a real, authenticated daemon</b>: a
 * real {@link GrpcSagaOrchestratorClient} drives a live {@link SagaServer} configured with the
 * {@code apikey} security provider, so its calls run through the actual {@code
 * SagaSecurityInterceptor}. The callback test is the one that matters most: the watch behind a
 * {@link SagaCallback} issues its calls through a stub of its own, and only a real interceptor
 * shows that the credential reaches those calls too.
 */
class SagaGrpcAuthIntegrationTest extends ServerIntegrationTestSupport {

  private static final String SAGA_NAME = "saga";
  private static final String DEFINITION =
      withService(
          """
        { "name": "saga", "mode": "SAGA", "steps": [
          { "name": "s1", "service": "$svc",
            "execution":    { "method": "POST", "path": "/debit" },
            "compensation": { "method": "POST", "path": "/reverse" } } ] }""");

  private final List<GrpcSagaOrchestratorClient> clients = new ArrayList<>();

  @Override
  protected void configureParticipant(HttpServer participant) {
    route(participant, "/debit", 200);
    route(participant, "/reverse", 200);
  }

  @Override
  protected void writeDefinitions(Path definitionsDir) throws IOException {
    writeDefinition(definitionsDir, SAGA_NAME, DEFINITION);
  }

  @Override
  protected void configureProperties(Properties props) {
    enableApiKeyProvider(props);
  }

  @AfterEach
  void closeClients() {
    clients.forEach(GrpcSagaOrchestratorClient::close);
  }

  /** A client presenting {@code apiKey} in the configured header, or none when {@code null}. */
  private GrpcSagaOrchestratorClient client(@Nullable String apiKey) {
    GrpcSagaOrchestratorClient.Builder builder =
        GrpcSagaOrchestratorClient.newBuilder().target("localhost:" + grpcPort());
    if (apiKey != null) {
      builder.callCredentials(
          GrpcSagaOrchestratorClient.staticHeaderCredentials(API_KEY_HEADER, apiKey));
    }
    GrpcSagaOrchestratorClient client = builder.build();
    clients.add(client);
    return client;
  }

  @Test
  void writeKey_start_completesAndIsReadable() {
    // Arrange
    GrpcSagaOrchestratorClient client = client(WRITE_KEY);

    // Act
    String sagaId = client.start(SAGA_NAME, Map.of());

    // Assert — saga:write covers the start and, through saga:read, the read.
    assertThat(client.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void writeKey_startAsyncWithCallback_callsOnCompleted() throws Exception {
    // Arrange
    CompletableFuture<SagaStateSnapshot> completed = new CompletableFuture<>();
    SagaCallback callback =
        new SagaCallback() {
          @Override
          public void onCompleted(SagaStateSnapshot saga) {
            completed.complete(saga);
          }

          @Override
          public void onCompensated(SagaStateSnapshot saga) {
            completed.completeExceptionally(new AssertionError("compensated"));
          }

          @Override
          public void onEscalated(SagaStateSnapshot saga) {
            completed.completeExceptionally(new AssertionError("escalated"));
          }
        };

    // Act
    String sagaId = client(WRITE_KEY).startAsync(SAGA_NAME, Map.of(), callback);

    // Assert — an unauthenticated watch would stop on UNAUTHENTICATED and never call back.
    assertThat(completed.get(30, TimeUnit.SECONDS).getSagaId()).isEqualTo(sagaId);
  }

  @Test
  void noCredential_start_isUnauthenticated() {
    assertThatThrownBy(() -> client(null).start(SAGA_NAME, Map.of()))
        .isInstanceOf(SagaUnauthenticatedException.class);
  }
}
