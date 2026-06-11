package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.engine.ServiceInvokerRegistryTest.FakeSagaContext;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ServiceInvokerTccStepTest {

  @Test
  void reserve_wrapsSagaContextWithStepName_andPropagatesData() throws Exception {
    // Arrange — capture the ServiceCallContext the invoker receives
    AtomicReference<ServiceCallContext> captured = new AtomicReference<>();
    AtomicReference<String> capturedOperation = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult reserve(String operation, ServiceCallContext context) {
            capturedOperation.set(operation);
            captured.set(context);
            return StepResult.empty();
          }

          @Override
          public boolean supportsReserve(String operation) {
            return true;
          }
        };
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", invoker));
    TccStep step = registry.toTccStep("reserve", "inventory", "reserveStock");

    // Act — the engine hands a plain SagaContext (no step name)
    step.reserve(new FakeSagaContext("saga-9", Map.of("itemId", "I-1")));

    // Assert — the wrapper adds the step name and delegates data + saga id
    assertThat(capturedOperation.get()).isEqualTo("reserveStock");
    ServiceCallContext ctx = Objects.requireNonNull(captured.get());
    assertThat(ctx.getStepName()).isEqualTo("reserve");
    assertThat(ctx.getSagaId()).isEqualTo("saga-9");
    assertThat(ctx.get("itemId", String.class)).contains("I-1");
  }

  @Test
  void confirm_wrapsSagaContextWithStepName() throws Exception {
    // Arrange
    AtomicReference<ServiceCallContext> captured = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public void confirm(String operation, ServiceCallContext context) {
            captured.set(context);
          }

          @Override
          public boolean supportsConfirm(String operation) {
            return true;
          }
        };
    TccStep step =
        ServiceInvokerRegistry.of(Map.of("inventory", invoker))
            .toTccStep("reserve", "inventory", "reserveStock");

    // Act
    step.confirm(new FakeSagaContext("saga-9", Map.of()));

    // Assert
    assertThat(Objects.requireNonNull(captured.get()).getStepName()).isEqualTo("reserve");
  }

  @Test
  void cancel_wrapsSagaContextWithStepName() throws Exception {
    // Arrange
    AtomicReference<ServiceCallContext> captured = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public void cancel(String operation, ServiceCallContext context) {
            captured.set(context);
          }

          @Override
          public boolean supportsCancel(String operation) {
            return true;
          }
        };
    TccStep step =
        ServiceInvokerRegistry.of(Map.of("inventory", invoker))
            .toTccStep("reserve", "inventory", "reserveStock");

    // Act
    step.cancel(new FakeSagaContext("saga-9", Map.of()));

    // Assert
    assertThat(Objects.requireNonNull(captured.get()).getStepName()).isEqualTo("reserve");
  }

  @Test
  void reserve_invokerThrowsStepExecutionException_propagates() {
    // Arrange
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult reserve(String operation, ServiceCallContext context)
              throws StepExecutionException {
            throw new StepExecutionException("reserve failed", false);
          }

          @Override
          public boolean supportsReserve(String operation) {
            return true;
          }
        };
    TccStep step =
        ServiceInvokerRegistry.of(Map.of("inventory", invoker))
            .toTccStep("reserve", "inventory", "reserveStock");

    // Act & Assert
    assertThatThrownBy(() -> step.reserve(new FakeSagaContext("saga-9", Map.of())))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void confirm_invokerThrowsStepExecutionException_propagates() {
    // Arrange
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public void confirm(String operation, ServiceCallContext context)
              throws StepExecutionException {
            throw new StepExecutionException("confirm failed", false);
          }

          @Override
          public boolean supportsConfirm(String operation) {
            return true;
          }
        };
    TccStep step =
        ServiceInvokerRegistry.of(Map.of("inventory", invoker))
            .toTccStep("reserve", "inventory", "reserveStock");

    // Act & Assert
    assertThatThrownBy(() -> step.confirm(new FakeSagaContext("saga-9", Map.of())))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void cancel_invokerThrowsStepCompensationException_propagates() {
    // Arrange
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public void cancel(String operation, ServiceCallContext context)
              throws StepCompensationException {
            throw new StepCompensationException("cancel failed");
          }

          @Override
          public boolean supportsCancel(String operation) {
            return true;
          }
        };
    TccStep step =
        ServiceInvokerRegistry.of(Map.of("inventory", invoker))
            .toTccStep("reserve", "inventory", "reserveStock");

    // Act & Assert
    assertThatThrownBy(() -> step.cancel(new FakeSagaContext("saga-9", Map.of())))
        .isInstanceOf(StepCompensationException.class);
  }
}
