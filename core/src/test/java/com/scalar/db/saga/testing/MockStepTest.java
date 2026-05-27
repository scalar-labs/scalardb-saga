package com.scalar.db.saga.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MockStepTest {

  private static SagaContext contextWith(String sagaId) {
    return new SagaContext() {
      @Override
      public String getSagaId() {
        return sagaId;
      }

      @Override
      public <T> Optional<T> get(String key, Class<T> type) {
        return Optional.empty();
      }
    };
  }

  @Test
  void getName_returnsConfiguredName() {
    // Arrange
    MockStep step = MockStep.newBuilder("payment").build();

    // Act & Assert
    assertThat(step.getName()).isEqualTo("payment");
  }

  @Test
  void execute_withDefaults_returnsEmptyResultAndTracksHistory() throws StepExecutionException {
    // Arrange
    MockStep step = MockStep.newBuilder("step1").build();

    // Act
    StepResult result = step.execute(contextWith("saga-1"));

    // Assert
    assertThat(result).isEqualTo(StepResult.empty());
    assertThat(step.getExecutions()).containsExactly("saga-1");
    assertThat(step.getExecutionCount()).isEqualTo(1);
  }

  @Test
  void execute_withCustomResult_returnsConfiguredResult() throws StepExecutionException {
    // Arrange
    StepResult expected = StepResult.of("txId", "abc-123");
    MockStep step = MockStep.newBuilder("payment").executeReturns(expected).build();

    // Act
    StepResult result = step.execute(contextWith("saga-1"));

    // Assert
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void execute_withExecuteAction_invokesAction() throws StepExecutionException {
    // Arrange
    MockStep step =
        MockStep.newBuilder("step1")
            .executeAction(ctx -> StepResult.of("id", ctx.getSagaId()))
            .build();

    // Act
    StepResult result = step.execute(contextWith("saga-42"));

    // Assert
    assertThat(result.getOutput()).containsEntry("id", "saga-42");
  }

  @Test
  void execute_withFailure_throwsConfiguredException() {
    // Arrange
    StepExecutionException failure = new StepExecutionException("timeout", true);
    MockStep step = MockStep.newBuilder("step1").executeFails(failure).build();

    // Act & Assert
    assertThatThrownBy(() -> step.execute(contextWith("saga-1")))
        .isInstanceOf(StepExecutionException.class);
    assertThat(step.getExecutions()).containsExactly("saga-1");
  }

  @Test
  void compensate_withDefaults_tracksHistoryWithoutThrowing() throws StepCompensationException {
    // Arrange
    MockStep step = MockStep.newBuilder("step1").build();

    // Act
    step.compensate(contextWith("saga-1"));

    // Assert
    assertThat(step.getCompensations()).containsExactly("saga-1");
    assertThat(step.getCompensationCount()).isEqualTo(1);
  }

  @Test
  void compensate_withFailure_throwsConfiguredException() {
    // Arrange
    MockStep step =
        MockStep.newBuilder("step1")
            .compensateFails(new StepCompensationException("compensate failed"))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> step.compensate(contextWith("saga-1")))
        .isInstanceOf(StepCompensationException.class);
    assertThat(step.getCompensations()).containsExactly("saga-1");
  }

  @Test
  void execute_multipleInvocations_tracksAll() throws StepExecutionException {
    // Arrange
    MockStep step = MockStep.newBuilder("step1").build();

    // Act
    step.execute(contextWith("saga-1"));
    step.execute(contextWith("saga-2"));
    step.execute(contextWith("saga-3"));

    // Assert
    assertThat(step.getExecutions()).containsExactly("saga-1", "saga-2", "saga-3");
    assertThat(step.getExecutionCount()).isEqualTo(3);
  }

  @Test
  void getExecutions_returnsCopy_notLiveView() throws StepExecutionException {
    // Arrange
    MockStep step = MockStep.newBuilder("step1").build();
    step.execute(contextWith("saga-1"));
    var snapshot = step.getExecutions();

    // Act
    step.execute(contextWith("saga-2"));

    // Assert
    assertThat(snapshot).containsExactly("saga-1");
    assertThat(step.getExecutions()).containsExactly("saga-1", "saga-2");
  }
}
