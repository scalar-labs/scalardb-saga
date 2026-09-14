package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import org.junit.jupiter.api.Test;

class BenchmarkDefinitionsTest {

  @Test
  public void stepName_indexGiven_returnsStableName() {
    assertThat(BenchmarkDefinitions.stepName(0)).isEqualTo("step-0");
    assertThat(BenchmarkDefinitions.stepName(12)).isEqualTo("step-12");
  }

  @Test
  public void embeddedDefinition_threeSteps_buildsClassStepsInOrder() {
    // Act
    SagaDefinition definition = BenchmarkDefinitions.embeddedDefinition("bench", 3);

    // Assert
    assertThat(definition.getName()).isEqualTo("bench");
    assertThat(definition.getSteps()).hasSize(3);
    assertThat(definition.getSteps().get(0).getName()).isEqualTo("step-0");
    assertThat(definition.getSteps().get(2).getName()).isEqualTo("step-2");
    assertThat(definition.getSteps().get(0))
        .isInstanceOf(SagaDefinition.ClassStep.class)
        .extracting(step -> ((SagaDefinition.ClassStep) step).getStepClass())
        .isEqualTo(BenchmarkStep.class.getName());
  }

  @Test
  public void embeddedDefinition_zeroSteps_throwsException() {
    assertThatThrownBy(() -> BenchmarkDefinitions.embeddedDefinition("bench", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void serviceDefinitionJson_twoSteps_parsesAsServiceStepDefinition() {
    // Act
    String json = BenchmarkDefinitions.serviceDefinitionJson("bench", 2);
    SagaDefinition definition = SagaDefinitionParser.parseJson(json);

    // Assert
    assertThat(definition.getName()).isEqualTo("bench");
    assertThat(definition.getSteps()).hasSize(2);
    assertThat(definition.getSteps().get(1))
        .isInstanceOf(SagaDefinition.ServiceStep.class)
        .extracting(step -> ((SagaDefinition.ServiceStep) step).getService())
        .isEqualTo(BenchmarkDefinitions.SERVICE);
  }

  @Test
  public void serviceDefinitionJson_zeroSteps_throwsException() {
    assertThatThrownBy(() -> BenchmarkDefinitions.serviceDefinitionJson("bench", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
