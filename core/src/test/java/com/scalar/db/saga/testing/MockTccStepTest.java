package com.scalar.db.saga.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MockTccStepTest {

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
    MockTccStep step = MockTccStep.newBuilder("inventory").build();

    // Act & Assert
    assertThat(step.getName()).isEqualTo("inventory");
  }

  @Test
  void reserve_withDefaults_returnsEmptyResultAndTracksHistory() throws StepExecutionException {
    // Arrange
    MockTccStep step = MockTccStep.newBuilder("step1").build();

    // Act
    StepResult result = step.reserve(contextWith("saga-1"));

    // Assert
    assertThat(result).isEqualTo(StepResult.empty());
    assertThat(step.getReservations()).containsExactly("saga-1");
  }

  @Test
  void reserve_withCustomResult_returnsConfiguredResult() throws StepExecutionException {
    // Arrange
    StepResult expected = StepResult.of("reserved", true);
    MockTccStep step = MockTccStep.newBuilder("step1").reserveReturns(expected).build();

    // Act
    StepResult result = step.reserve(contextWith("saga-1"));

    // Assert
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void reserve_withReserveAction_invokesAction() throws StepExecutionException {
    // Arrange
    MockTccStep step =
        MockTccStep.newBuilder("step1")
            .reserveAction(ctx -> StepResult.of("id", ctx.getSagaId()))
            .build();

    // Act
    StepResult result = step.reserve(contextWith("saga-42"));

    // Assert
    assertThat(result.getOutput()).containsEntry("id", "saga-42");
  }

  @Test
  void reserve_withFailure_throwsConfiguredException() {
    // Arrange
    MockTccStep step =
        MockTccStep.newBuilder("step1")
            .reserveFails(new StepExecutionException("reserve failed", false))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> step.reserve(contextWith("saga-1")))
        .isInstanceOf(StepExecutionException.class);
    assertThat(step.getReservations()).containsExactly("saga-1");
  }

  @Test
  void confirm_withDefaults_tracksHistoryWithoutThrowing() throws StepExecutionException {
    // Arrange
    MockTccStep step = MockTccStep.newBuilder("step1").build();

    // Act
    step.confirm(contextWith("saga-1"));

    // Assert
    assertThat(step.getConfirmations()).containsExactly("saga-1");
  }

  @Test
  void confirm_withFailure_throwsConfiguredException() {
    // Arrange
    MockTccStep step =
        MockTccStep.newBuilder("step1")
            .confirmFails(new StepExecutionException("confirm failed", false))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> step.confirm(contextWith("saga-1")))
        .isInstanceOf(StepExecutionException.class);
  }

  @Test
  void cancel_withDefaults_tracksHistoryWithoutThrowing() throws StepCompensationException {
    // Arrange
    MockTccStep step = MockTccStep.newBuilder("step1").build();

    // Act
    step.cancel(contextWith("saga-1"));

    // Assert
    assertThat(step.getCancellations()).containsExactly("saga-1");
  }

  @Test
  void cancel_withFailure_throwsConfiguredException() {
    // Arrange
    MockTccStep step =
        MockTccStep.newBuilder("step1")
            .cancelFails(new StepCompensationException("cancel failed"))
            .build();

    // Act & Assert
    assertThatThrownBy(() -> step.cancel(contextWith("saga-1")))
        .isInstanceOf(StepCompensationException.class);
  }

  @Test
  void allPhases_trackIndependently() throws Exception {
    // Arrange
    MockTccStep step = MockTccStep.newBuilder("step1").build();

    // Act
    step.reserve(contextWith("saga-1"));
    step.confirm(contextWith("saga-1"));
    step.reserve(contextWith("saga-2"));
    step.cancel(contextWith("saga-2"));

    // Assert
    assertThat(step.getReservations()).containsExactly("saga-1", "saga-2");
    assertThat(step.getConfirmations()).containsExactly("saga-1");
    assertThat(step.getCancellations()).containsExactly("saga-2");
  }
}
