package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;

/**
 * The external-server mode: a {@link GrpcSagaOrchestratorClient} against a daemon someone else
 * runs. The benchmark definition must already be registered there — {@code --print-definition}
 * emits the JSON to install — and step duplicates are not observable from here (the participants
 * are not ours).
 */
final class GrpcHarness implements BenchHarness {

  private final GrpcSagaOrchestratorClient client;
  private final String description;

  private GrpcHarness(GrpcSagaOrchestratorClient client, String description) {
    this.client = client;
    this.description = description;
  }

  static GrpcHarness create(String target, boolean tls, long clientDeadlineMillis) {
    GrpcSagaOrchestratorClient.Builder builder =
        GrpcSagaOrchestratorClient.newBuilder().target(target);
    if (tls) {
      builder.useTransportSecurity();
    }
    if (clientDeadlineMillis > 0) {
      builder.defaultDeadlineMillis(clientDeadlineMillis);
    }
    return new GrpcHarness(builder.build(), "external server (gRPC " + target + ")");
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
    return -1;
  }

  @Override
  public void close() {
    client.close();
  }
}
