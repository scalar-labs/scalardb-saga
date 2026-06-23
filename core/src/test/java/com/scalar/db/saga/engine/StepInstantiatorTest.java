package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StepInstantiatorTest {

  private static final HttpEndpointRegistry EMPTY_ENDPOINTS = HttpEndpointRegistry.create(Map.of());

  @Test
  void instantiate_classStep_resolvesViaStepResolver() {
    // Arrange
    Step resolved = noopStep("debit");
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> resolved, EMPTY_ENDPOINTS);

    // Act
    Step step = instantiator.instantiate(classStep("debit", "com.example.DebitStep"), Step.class);

    // Assert
    assertThat(step).isSameAs(resolved);
  }

  @Test
  void instantiate_classStepNotExpectedType_throwsSagaDefinitionException() {
    // Arrange — resolver returns a Step, but the TCC path expects a TccStep
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep("x"), EMPTY_ENDPOINTS);

    // Act & Assert
    assertThatThrownBy(
            () -> instantiator.instantiate(classStep("x", "com.example.X"), TccStep.class))
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
}
