package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import com.scalar.db.saga.server.SagaServer;
import com.scalar.db.saga.server.SagaServerConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.jspecify.annotations.Nullable;

/**
 * The server mode: boots the real {@link SagaServer} in-process on ephemeral loopback ports —
 * declarative service-step definitions calling a local {@link Participant} — and drives it through
 * {@link GrpcSagaOrchestratorClient} over real gRPC. One command benchmarks the full daemon
 * round-trip, client to store, with no separately managed processes.
 */
final class ServerHarness implements BenchHarness {

  // The server config keys are package-private constants of SagaServerConfig, so the daemon's
  // documented property names are spelled out here; SagaServerConfig.load rejects unknown
  // scalar.db.saga.server.* keys, which keeps these literals from silently drifting.
  private static final String HOST_KEY = "scalar.db.saga.server.host";
  private static final String HTTP_PORT_KEY = "scalar.db.saga.server.http.port";
  private static final String GRPC_PORT_KEY = "scalar.db.saga.server.grpc.port";
  private static final String DEFINITIONS_PATH_KEY = "scalar.db.saga.server.definitions_path";
  private static final String SERVICE_BASE_URL_KEY =
      "scalar.db.saga.server.service." + BenchmarkDefinitions.SERVICE + ".base_url";

  private final GrpcSagaOrchestratorClient client;
  private final SagaServer server;
  private final Participant participant;
  private final @Nullable Path tempDbPath;
  private final @Nullable Path definitionsDir;
  private final String description;

  private ServerHarness(
      GrpcSagaOrchestratorClient client,
      SagaServer server,
      Participant participant,
      @Nullable Path tempDbPath,
      @Nullable Path definitionsDir,
      String description) {
    this.client = client;
    this.server = server;
    this.participant = participant;
    this.tempDbPath = tempDbPath;
    this.definitionsDir = definitionsDir;
    this.description = description;
  }

  /**
   * Starts the participant and the daemon, then connects the client.
   *
   * @param propertiesFile ScalarDB store properties, or {@code null} for a throwaway SQLite file
   * @param overrides extra store or {@code scalar.db.saga.server.*} properties applied on top
   * @param sagaName the definition name to write and start
   * @param stepCount sequential service steps per saga
   * @param stepDelayMillis how long the participant holds each call
   * @param clientDeadlineMillis per-call gRPC deadline for the client; {@code 0} for none
   */
  static ServerHarness create(
      @Nullable Path propertiesFile,
      Map<String, String> overrides,
      String sagaName,
      int stepCount,
      long stepDelayMillis,
      long clientDeadlineMillis) {
    Participant participant = null;
    Path definitionsDir = null;
    HarnessSupport.StoreSetup store = HarnessSupport.storeProperties(propertiesFile);
    SagaServer server = null;
    try {
      participant = Participant.start(stepDelayMillis);
      definitionsDir = Files.createTempDirectory("saga-bench-defs-");
      Files.writeString(
          definitionsDir.resolve(sagaName + ".json"),
          BenchmarkDefinitions.serviceDefinitionJson(sagaName, stepCount));

      Properties props = store.properties();
      props.setProperty(HOST_KEY, "127.0.0.1");
      props.setProperty(HTTP_PORT_KEY, "0");
      props.setProperty(GRPC_PORT_KEY, "0");
      props.setProperty(DEFINITIONS_PATH_KEY, definitionsDir.toString());
      props.setProperty(SERVICE_BASE_URL_KEY, participant.baseUrl());
      HarnessSupport.applyOverrides(props, overrides);

      server = new SagaServer(SagaServerConfig.load(props)).start();

      GrpcSagaOrchestratorClient.Builder clientBuilder =
          GrpcSagaOrchestratorClient.newBuilder().target("127.0.0.1:" + server.grpcPort());
      if (clientDeadlineMillis > 0) {
        clientBuilder.defaultDeadlineMillis(clientDeadlineMillis);
      }
      GrpcSagaOrchestratorClient client = clientBuilder.build();
      return new ServerHarness(
          client,
          server,
          participant,
          store.tempDbPath(),
          definitionsDir,
          "in-process server (gRPC on 127.0.0.1:"
              + server.grpcPort()
              + ", store "
              + store.description()
              + ")");
    } catch (IOException e) {
      closeQuietly(server, participant, store.tempDbPath(), definitionsDir);
      throw new UncheckedIOException("failed to start the in-process server harness", e);
    } catch (RuntimeException e) {
      closeQuietly(server, participant, store.tempDbPath(), definitionsDir);
      throw e;
    }
  }

  private static void closeQuietly(
      @Nullable SagaServer server,
      @Nullable Participant participant,
      @Nullable Path tempDbPath,
      @Nullable Path definitionsDir) {
    if (server != null) {
      server.close();
    }
    if (participant != null) {
      participant.close();
    }
    HarnessSupport.deleteQuietly(tempDbPath);
    HarnessSupport.deleteRecursivelyQuietly(definitionsDir);
  }

  @Override
  public SagaOrchestrator orchestrator() {
    return client;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public long duplicateStepExecutions() {
    return participant.duplicateExecutions();
  }

  @Override
  public void close() {
    try {
      client.close();
    } finally {
      closeQuietly(server, participant, tempDbPath, definitionsDir);
    }
  }
}
