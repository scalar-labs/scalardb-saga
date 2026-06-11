package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.engine.ServiceInvokerRegistryTest.FakeSagaContext;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ServiceInvokerStepTest {

  @Test
  void execute_wrapsSagaContextWithStepName_andPropagatesData() throws Exception {
    // Arrange — capture the ServiceCallContext the invoker receives
    AtomicReference<ServiceCallContext> captured = new AtomicReference<>();
    AtomicReference<String> capturedMethod = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult execute(String operation, ServiceCallContext context) {
            capturedMethod.set(operation);
            captured.set(context);
            return StepResult.empty();
          }

          @Override
          public void compensate(String operation, ServiceCallContext context) {}

          @Override
          public boolean supportsExecute(String operation) {
            return true;
          }

          @Override
          public boolean supportsCompensate(String operation) {
            return true;
          }
        };
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", invoker));
    Step step = registry.toStep("debit", "account", "debitMethod");

    // Act — the engine hands a plain SagaContext (no step name)
    step.execute(new FakeSagaContext("saga-9", Map.of("accountId", "A-1")));

    // Assert — the wrapper adds the step name and delegates data + saga id
    assertThat(capturedMethod.get()).isEqualTo("debitMethod");
    ServiceCallContext ctx = Objects.requireNonNull(captured.get());
    assertThat(ctx.getStepName()).isEqualTo("debit");
    assertThat(ctx.getSagaId()).isEqualTo("saga-9");
    assertThat(ctx.get("accountId", String.class)).contains("A-1");
  }

  @Test
  void compensate_wrapsSagaContextWithStepName() throws Exception {
    // Arrange
    AtomicReference<ServiceCallContext> captured = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult execute(String operation, ServiceCallContext context) {
            return StepResult.empty();
          }

          @Override
          public void compensate(String operation, ServiceCallContext context) {
            captured.set(context);
          }

          @Override
          public boolean supportsExecute(String operation) {
            return true;
          }

          @Override
          public boolean supportsCompensate(String operation) {
            return true;
          }
        };
    Step step =
        ServiceInvokerRegistry.of(Map.of("account", invoker)).toStep("debit", "account", "reverse");

    // Act
    step.compensate(new FakeSagaContext("saga-9", Map.of()));

    // Assert
    assertThat(Objects.requireNonNull(captured.get()).getStepName()).isEqualTo("debit");
  }
}
