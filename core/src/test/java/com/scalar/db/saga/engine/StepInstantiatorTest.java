package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class StepInstantiatorTest {

  private static final ServiceInvokerRegistry EMPTY_REGISTRY = ServiceInvokerRegistry.of(Map.of());

  @Test
  void instantiate_classStep_resolvesViaStepResolver() {
    // Arrange
    Step resolved = noopStep("debit");
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> resolved, EMPTY_REGISTRY);

    // Act
    Step step = instantiator.instantiate(classStep("debit", "com.example.DebitStep"), Step.class);

    // Assert
    assertThat(step).isSameAs(resolved);
  }

  @Test
  void instantiate_classStepNotExpectedType_throwsSagaDefinitionException() {
    // Arrange — resolver returns a Step, but the TCC path expects a TccStep
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep("x"), EMPTY_REGISTRY);

    // Act & Assert
    assertThatThrownBy(
            () -> instantiator.instantiate(classStep("x", "com.example.X"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStep_dispatchesToRegisteredInvoker() throws Exception {
    // Arrange — registry with a capturing invoker
    AtomicReference<String> capturedMethod = new AtomicReference<>();
    AtomicReference<ServiceCallContext> capturedContext = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult execute(String operation, ServiceCallContext context) {
            capturedMethod.set(operation);
            capturedContext.set(context);
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
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    // Act
    Step step =
        instantiator.instantiate(serviceStep("debit", "account", "debitMethod"), Step.class);
    step.execute(new FakeSagaContext("saga-7"));

    // Assert — the ServiceStep dispatched to the invoker with the method and step name
    assertThat(step.getName()).isEqualTo("debit");
    assertThat(capturedMethod.get()).isEqualTo("debitMethod");
    assertThat(Objects.requireNonNull(capturedContext.get()).getStepName()).isEqualTo("debit");
  }

  @Test
  void instantiate_serviceStepAsTccStep_throwsSagaDefinitionException() {
    // Register the service so resolution passes the wiring checks and reaches the type guard.
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", fakeInvoker(true, true)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepUnregisteredService_throwsSagaDefinitionException() {
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), EMPTY_REGISTRY);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "missing", "m"), Step.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepMethodWithoutAction_throwsSagaDefinitionException() {
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", fakeInvoker(false, true)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), Step.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepMethodWithoutCompensation_throwsSagaDefinitionException() {
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", fakeInvoker(true, false)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), Step.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepAsTccStep_dispatchesToRegisteredInvoker() throws Exception {
    // Arrange — registry with a capturing TCC invoker
    AtomicReference<String> capturedOperation = new AtomicReference<>();
    AtomicReference<ServiceCallContext> capturedContext = new AtomicReference<>();
    ServiceInvoker invoker =
        new ServiceInvoker() {
          @Override
          public StepResult reserve(String operation, ServiceCallContext context) {
            capturedOperation.set(operation);
            capturedContext.set(context);
            return StepResult.empty();
          }

          @Override
          public void confirm(String operation, ServiceCallContext context) {}

          @Override
          public void cancel(String operation, ServiceCallContext context) {}

          @Override
          public boolean supportsReserve(String operation) {
            return true;
          }

          @Override
          public boolean supportsConfirm(String operation) {
            return true;
          }

          @Override
          public boolean supportsCancel(String operation) {
            return true;
          }
        };
    ServiceInvokerRegistry registry = ServiceInvokerRegistry.of(Map.of("inventory", invoker));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    // Act
    TccStep step =
        instantiator.instantiate(
            serviceStep("reserve", "inventory", "reserveStock"), TccStep.class);
    step.reserve(new FakeSagaContext("saga-7"));

    // Assert — the ServiceStep dispatched to the invoker with the operation and step name
    assertThat(step.getName()).isEqualTo("reserve");
    assertThat(capturedOperation.get()).isEqualTo("reserveStock");
    assertThat(Objects.requireNonNull(capturedContext.get()).getStepName()).isEqualTo("reserve");
  }

  @Test
  void instantiate_serviceStepTccMissingReserve_throwsSagaDefinitionException() {
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", tccInvoker(false, true, true)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepTccMissingConfirm_throwsSagaDefinitionException() {
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", tccInvoker(true, false, true)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_serviceStepTccMissingCancel_throwsSagaDefinitionException() {
    ServiceInvokerRegistry registry =
        ServiceInvokerRegistry.of(Map.of("svc", tccInvoker(true, true, false)));
    StepInstantiator instantiator =
        new StepInstantiator((name, className) -> noopStep(name), registry);

    assertThatThrownBy(() -> instantiator.instantiate(serviceStep("x", "svc", "m"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  private static StepDefinition classStep(String name, String stepClass) {
    return SagaDefinition.newBuilder("s", SagaMode.SAGA)
        .step(name, stepClass)
        .add()
        .build()
        .getSteps()
        .get(0);
  }

  private static StepDefinition serviceStep(String name, String service, String operation) {
    return SagaDefinition.newBuilder("s", SagaMode.SAGA)
        .serviceStep(name, service, operation)
        .add()
        .build()
        .getSteps()
        .get(0);
  }

  private static ServiceInvoker fakeInvoker(boolean executeSupported, boolean compensateSupported) {
    return new ServiceInvoker() {
      @Override
      public StepResult execute(String operation, ServiceCallContext context) {
        return StepResult.empty();
      }

      @Override
      public void compensate(String operation, ServiceCallContext context) {}

      @Override
      public boolean supportsExecute(String operation) {
        return executeSupported;
      }

      @Override
      public boolean supportsCompensate(String operation) {
        return compensateSupported;
      }
    };
  }

  private static ServiceInvoker tccInvoker(
      boolean reserveSupported, boolean confirmSupported, boolean cancelSupported) {
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
        return reserveSupported;
      }

      @Override
      public boolean supportsConfirm(String operation) {
        return confirmSupported;
      }

      @Override
      public boolean supportsCancel(String operation) {
        return cancelSupported;
      }
    };
  }

  private static Step noopStep(String name) {
    return new Step() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public StepResult execute(SagaContext context) {
        return StepResult.empty();
      }

      @Override
      public void compensate(SagaContext context) {}
    };
  }

  private static final class FakeSagaContext implements SagaContext {
    private final String sagaId;

    FakeSagaContext(String sagaId) {
      this.sagaId = sagaId;
    }

    @Override
    public String getSagaId() {
      return sagaId;
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> type) {
      return Optional.empty();
    }
  }
}
