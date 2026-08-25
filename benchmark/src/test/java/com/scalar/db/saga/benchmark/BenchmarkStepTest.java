package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepExecutionException;
import org.junit.jupiter.api.Test;

class BenchmarkStepTest {

  private static SagaContext contextFor(String sagaId) {
    SagaContext context = mock(SagaContext.class);
    when(context.getSagaId()).thenReturn(sagaId);
    return context;
  }

  @Test
  public void constructor_blankNameGiven_throwsException() {
    assertThatThrownBy(() -> new BenchmarkStep(" ", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_negativeDelayGiven_throwsException() {
    assertThatThrownBy(() -> new BenchmarkStep("step-0", -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getName_returnsConfiguredName() {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-7", 0);

    // Act & Assert
    assertThat(step.getName()).isEqualTo("step-7");
  }

  @Test
  public void execute_distinctSagas_countsNoDuplicates() throws Exception {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 0);

    // Act
    StepResult result = step.execute(contextFor("saga-1"));
    step.execute(contextFor("saga-2"));

    // Assert
    assertThat(result.getOutput()).isEmpty();
    assertThat(step.executions()).isEqualTo(2);
    assertThat(step.duplicateExecutions()).isZero();
  }

  @Test
  public void execute_sameSagaTwice_countsDuplicate() throws Exception {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 0);

    // Act
    step.execute(contextFor("saga-1"));
    step.execute(contextFor("saga-1"));

    // Assert
    assertThat(step.executions()).isEqualTo(2);
    assertThat(step.duplicateExecutions()).isEqualTo(1);
  }

  @Test
  public void execute_withDelay_sleepsAtLeastTheDelay() throws Exception {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 30);
    long before = System.nanoTime();

    // Act
    step.execute(contextFor("saga-1"));

    // Assert
    assertThat((System.nanoTime() - before) / 1_000_000).isGreaterThanOrEqualTo(30);
  }

  @Test
  public void execute_interrupted_throwsStepExecutionException() {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 1_000);
    Thread.currentThread().interrupt();

    // Act & Assert
    try {
      assertThatThrownBy(() -> step.execute(contextFor("saga-1")))
          .isInstanceOf(StepExecutionException.class);
      assertThat(Thread.interrupted()).as("interrupt flag restored").isTrue();
    } finally {
      // Ensure the flag never leaks into other tests even on assertion failure.
      Thread.interrupted();
    }
  }

  @Test
  public void compensate_counts() {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 0);

    // Act
    step.compensate(contextFor("saga-1"));

    // Assert
    assertThat(step.compensations()).isEqualTo(1);
  }

  @Test
  public void compensate_interrupted_swallowsAndRestoresFlag() {
    // Arrange
    BenchmarkStep step = new BenchmarkStep("step-0", 1_000);
    Thread.currentThread().interrupt();

    // Act
    step.compensate(contextFor("saga-1"));

    // Assert
    try {
      assertThat(Thread.interrupted()).as("interrupt flag restored").isTrue();
    } finally {
      Thread.interrupted();
    }
  }
}
