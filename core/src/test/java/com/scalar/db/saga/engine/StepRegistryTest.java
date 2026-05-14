package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StepRegistryTest {

  private StepRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new StepRegistry();
  }

  @Test
  void register_stepGiven_registersSuccessfully() {
    // Arrange
    Step step = mock(Step.class);

    // Act
    registry.register("step1", step);

    // Assert
    assertThat(registry.getStep("step1")).isSameAs(step);
  }

  @Test
  void register_tccStepGiven_registersSuccessfully() {
    // Arrange
    TccStep step = mock(TccStep.class);

    // Act
    registry.register("tcc1", step);

    // Assert
    assertThat(registry.getTccStep("tcc1")).isSameAs(step);
  }

  @Test
  void getStep_registeredStepGiven_returnsStep() {
    // Arrange
    Step step = mock(Step.class);
    registry.register("step1", step);

    // Act & Assert
    assertThat(registry.getStep("step1")).isSameAs(step);
  }

  @Test
  void getStep_unregisteredNameGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> registry.getStep("unknown"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getTccStep_unregisteredNameGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> registry.getTccStep("unknown"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void register_duplicateNameGiven_throwsException() {
    // Arrange
    Step step1 = mock(Step.class);
    Step step2 = mock(Step.class);
    registry.register("dup", step1);

    // Act & Assert
    assertThatThrownBy(() -> registry.register("dup", step2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void register_nullNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> registry.register(null, mock(Step.class)))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void register_nullStepGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> registry.register("name", (Step) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void getStep_wrongType_throwsIllegalStateException() {
    // Arrange
    registry.register("tcc1", mock(TccStep.class));

    // Act & Assert
    assertThatThrownBy(() -> registry.getStep("tcc1")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void getTccStep_wrongType_throwsIllegalStateException() {
    // Arrange
    registry.register("step1", mock(Step.class));

    // Act & Assert
    assertThatThrownBy(() -> registry.getTccStep("step1"))
        .isInstanceOf(IllegalStateException.class);
  }
}
