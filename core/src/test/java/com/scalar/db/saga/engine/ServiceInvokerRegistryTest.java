package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ServiceInvokerRegistryTest {

  private static final ServiceCallContext CTX = new FakeServiceCallContext("saga-1", "debit");

  @Test
  void execute_registeredService_dispatchesToInvoker() throws Exception {
    // Arrange
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult execute(String operation, ServiceCallContext context) {
            return StepResult.of("calledMethod", operation);
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

    // Act
    StepResult result = registry.execute("account", "debit", CTX);

    // Assert
    assertThat(result.getOutput()).containsEntry("calledMethod", "debit");
  }

  @Test
  void execute_unknownService_throwsStepExecutionException() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of());

    assertThatThrownBy(() -> registry.execute("missing", "m", CTX))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void compensate_unknownService_throwsStepCompensationException() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of());

    assertThatThrownBy(() -> registry.compensate("missing", "m", CTX))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void close_closesEachRegisteredInvoker() {
    // Arrange
    AtomicBoolean closed = new AtomicBoolean(false);
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult execute(String operation, ServiceCallContext context) {
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

          @Override
          public void close() {
            closed.set(true);
          }
        };
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", invoker));

    // Act
    registry.close();

    // Assert
    assertThat(closed).isTrue();
  }

  @Test
  void contains_reflectsRegistrations() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", noopInvoker()));

    assertThat(registry.contains("account")).isTrue();
    assertThat(registry.contains("missing")).isFalse();
  }

  @Test
  void supportsExecute_delegatesToInvokerAndHandlesUnknownService() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", noopInvoker()));

    assertThat(registry.supportsExecute("account", "known")).isTrue();
    assertThat(registry.supportsExecute("account", "unknown")).isFalse();
    assertThat(registry.supportsExecute("missing", "known")).isFalse();
  }

  @Test
  void supportsCompensate_delegatesToInvokerAndHandlesUnknownService() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", noopInvoker()));

    assertThat(registry.supportsCompensate("account", "known")).isTrue();
    assertThat(registry.supportsCompensate("account", "unknown")).isFalse();
    assertThat(registry.supportsCompensate("missing", "known")).isFalse();
  }

  @Test
  void toStep_returnsStepNamedAfterStep() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("account", noopInvoker()));

    Step step = registry.toStep("debit", "account", "debitMethod");

    assertThat(step.getName()).isEqualTo("debit");
  }

  @Test
  void reserve_registeredService_dispatchesToInvoker() throws Exception {
    // Arrange
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult reserve(String operation, ServiceCallContext context) {
            return StepResult.of("reservedOperation", operation);
          }

          @Override
          public boolean supportsReserve(String operation) {
            return true;
          }
        };
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", invoker));

    // Act
    StepResult result = registry.reserve("inventory", "reserveStock", CTX);

    // Assert
    assertThat(result.getOutput()).containsEntry("reservedOperation", "reserveStock");
  }

  @Test
  void reserve_unknownService_throwsStepExecutionException() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of());

    assertThatThrownBy(() -> registry.reserve("missing", "m", CTX))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void confirm_unknownService_throwsStepExecutionException() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of());

    assertThatThrownBy(() -> registry.confirm("missing", "m", CTX))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void cancel_unknownService_throwsStepCompensationException() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of());

    assertThatThrownBy(() -> registry.cancel("missing", "m", CTX))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void supportsReserve_delegatesToInvokerAndHandlesUnknownService() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", tccInvoker()));

    assertThat(registry.supportsReserve("inventory", "known")).isTrue();
    assertThat(registry.supportsReserve("inventory", "unknown")).isFalse();
    assertThat(registry.supportsReserve("missing", "known")).isFalse();
  }

  @Test
  void supportsConfirm_delegatesToInvokerAndHandlesUnknownService() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", tccInvoker()));

    assertThat(registry.supportsConfirm("inventory", "known")).isTrue();
    assertThat(registry.supportsConfirm("inventory", "unknown")).isFalse();
    assertThat(registry.supportsConfirm("missing", "known")).isFalse();
  }

  @Test
  void supportsCancel_delegatesToInvokerAndHandlesUnknownService() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", tccInvoker()));

    assertThat(registry.supportsCancel("inventory", "known")).isTrue();
    assertThat(registry.supportsCancel("inventory", "unknown")).isFalse();
    assertThat(registry.supportsCancel("missing", "known")).isFalse();
  }

  @Test
  void toTccStep_returnsStepNamedAfterStep() {
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", tccInvoker()));

    TccStep step = registry.toTccStep("reserve", "inventory", "reserveStock");

    assertThat(step.getName()).isEqualTo("reserve");
  }

  private static ServiceInvoker noopInvoker() {
    return new ServiceInvoker() {
      @Override
      public StepResult execute(String operation, ServiceCallContext context) {
        return StepResult.empty();
      }

      @Override
      public void compensate(String operation, ServiceCallContext context) {}

      @Override
      public boolean supportsExecute(String operation) {
        return "known".equals(operation);
      }

      @Override
      public boolean supportsCompensate(String operation) {
        return "known".equals(operation);
      }
    };
  }

  private static ServiceInvoker tccInvoker() {
    return new ServiceInvoker() {
      @Override
      public StepResult reserve(String operation, ServiceCallContext context) {
        return StepResult.empty();
      }

      @Override
      public void confirm(String operation, ServiceCallContext context) {}

      @Override
      public void cancel(String operation, ServiceCallContext context) {}

      @Override
      public boolean supportsReserve(String operation) {
        return "known".equals(operation);
      }

      @Override
      public boolean supportsConfirm(String operation) {
        return "known".equals(operation);
      }

      @Override
      public boolean supportsCancel(String operation) {
        return "known".equals(operation);
      }
    };
  }

  /** Minimal {@link ServiceCallContext} for tests. */
  static final class FakeServiceCallContext implements ServiceCallContext {
    private final String sagaId;
    private final String stepName;
    private final Map<String, Object> data;

    FakeServiceCallContext(String sagaId, String stepName) {
      this(sagaId, stepName, Map.of());
    }

    FakeServiceCallContext(String sagaId, String stepName, Map<String, Object> data) {
      this.sagaId = sagaId;
      this.stepName = stepName;
      this.data = data;
    }

    @Override
    public String getStepName() {
      return stepName;
    }

    @Override
    public String getSagaId() {
      return sagaId;
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> type) {
      return Optional.ofNullable(data.get(key)).map(type::cast);
    }
  }

  /** Minimal {@link SagaContext} (no step name) for the step-adapter test. */
  static final class FakeSagaContext implements SagaContext {
    private final String sagaId;
    private final Map<String, Object> data;

    FakeSagaContext(String sagaId, Map<String, Object> data) {
      this.sagaId = sagaId;
      this.data = data;
    }

    @Override
    public String getSagaId() {
      return sagaId;
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> type) {
      return Optional.ofNullable(data.get(key)).map(type::cast);
    }
  }
}
