package example;

import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shows how to drive sagas through the client SDK's gRPC client, which implements the same {@code
 * SagaOrchestrator} interface as the embedded engine, so application code runs unchanged embedded
 * or remote.
 *
 * <p>Runs against the getting-started stack ({@code docker compose up -d --wait} in the parent
 * directory): {@code ../../gradlew run}
 */
public final class SagaClientExample {

  public static void main(String[] args) throws Exception {
    String target = args.length > 0 ? args[0] : "localhost:12051";

    try (GrpcSagaOrchestratorClient client = GrpcSagaOrchestratorClient.create(target)) {
      // A synchronous start blocks until the saga is terminal; the status carries the outcome.
      String sagaId = client.start("order-saga", orderInput("o-9001"));
      SagaStatus outcome = client.getStateSnapshot(sagaId).getStatus(); // COMPLETED

      // A rolled-back saga is a status, not an exception: the engine compensates the completed
      // steps and reports COMPENSATED.
      String failingId = client.start("order-saga-failing", orderInput("o-9002"));
      SagaStatus rolledBack = client.getStateSnapshot(failingId).getStatus(); // COMPENSATED

      // The durable record the engine wrote while executing and unwinding it: SAGA_STARTED,
      // STEP_COMPLETED..., STEP_FAILED, SAGA_COMPENSATING, STEP_COMPENSATED..., SAGA_COMPENSATED.
      SagaDetail detail = client.getSagaDetail(failingId);
      List<TimelineEvent> timeline = detail.getTimeline();

      // An asynchronous start returns as soon as the saga is accepted; poll for the outcome.
      String asyncId = client.startAsync("order-saga", orderInput("o-9003"));
      SagaStateSnapshot snapshot = client.getStateSnapshot(asyncId);
      while (!snapshot.getStatus().isTerminal()) {
        Thread.sleep(200L);
        snapshot = client.getStateSnapshot(asyncId);
      }
    }
  }

  private static Map<String, Object> orderInput(String orderId) {
    Map<String, Object> input = new HashMap<>();
    input.put("orderId", orderId);
    input.put("amount", "100");
    input.put("item", "widget");
    input.put("quantity", "2");
    return input;
  }

  private SagaClientExample() {}
}
