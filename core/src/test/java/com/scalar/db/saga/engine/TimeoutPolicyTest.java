package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TimeoutPolicyTest {

  // --- calculateSagaDeadline ---

  @Test
  void calculateSagaDeadline_positiveTimeoutGiven_returnsNowPlusTimeout() {
    // Arrange
    long nowMillis = 10_000;
    long timeoutMillis = 5_000;

    // Act
    long deadline = TimeoutPolicy.calculateSagaDeadline(timeoutMillis, nowMillis);

    // Assert
    assertThat(deadline).isEqualTo(15_000);
  }

  @Test
  void calculateSagaDeadline_zeroTimeoutGiven_returnsZero() {
    // Arrange & Act
    long deadline = TimeoutPolicy.calculateSagaDeadline(0, 10_000);

    // Assert
    assertThat(deadline).isEqualTo(0);
  }

  // --- calculateStepDeadline ---

  @Test
  void calculateStepDeadline_bothTimeoutsGiven_returnsMinimum() {
    // Arrange
    long stepTimeoutMillis = 2_000;
    long sagaDeadline = 11_000; // e.g., now(10_000) + 1_000 remaining
    long nowMillis = 10_000;

    // Act
    long stepDeadline =
        TimeoutPolicy.calculateStepDeadline(stepTimeoutMillis, sagaDeadline, nowMillis);

    // Assert — step would be 12_000, saga is 11_000, so min is 11_000
    assertThat(stepDeadline).isEqualTo(11_000);
  }

  @Test
  void calculateStepDeadline_stepTimeoutShorterThanSagaRemaining_returnsStepDeadline() {
    // Arrange
    long stepTimeoutMillis = 1_000;
    long sagaDeadline = 20_000; // plenty of saga time remaining
    long nowMillis = 10_000;

    // Act
    long stepDeadline =
        TimeoutPolicy.calculateStepDeadline(stepTimeoutMillis, sagaDeadline, nowMillis);

    // Assert — step deadline = 11_000 < saga deadline = 20_000
    assertThat(stepDeadline).isEqualTo(11_000);
  }

  @Test
  void calculateStepDeadline_onlyStepTimeoutGiven_returnsStepDeadline() {
    // Arrange
    long stepTimeoutMillis = 3_000;
    long sagaDeadline = 0; // no saga timeout
    long nowMillis = 10_000;

    // Act
    long stepDeadline =
        TimeoutPolicy.calculateStepDeadline(stepTimeoutMillis, sagaDeadline, nowMillis);

    // Assert
    assertThat(stepDeadline).isEqualTo(13_000);
  }

  @Test
  void calculateStepDeadline_onlySagaDeadlineGiven_returnsSagaDeadline() {
    // Arrange
    long stepTimeoutMillis = 0; // no step timeout
    long sagaDeadline = 15_000;
    long nowMillis = 10_000;

    // Act
    long stepDeadline =
        TimeoutPolicy.calculateStepDeadline(stepTimeoutMillis, sagaDeadline, nowMillis);

    // Assert
    assertThat(stepDeadline).isEqualTo(15_000);
  }

  @Test
  void calculateStepDeadline_neitherTimeoutGiven_returnsZero() {
    // Arrange & Act
    long stepDeadline = TimeoutPolicy.calculateStepDeadline(0, 0, 10_000);

    // Assert
    assertThat(stepDeadline).isEqualTo(0);
  }

  // --- isSagaTimedOut ---

  @Test
  void isSagaTimedOut_nowAfterDeadline_returnsTrue() {
    // Arrange & Act & Assert
    assertThat(TimeoutPolicy.isSagaTimedOut(10_000, 10_001)).isTrue();
  }

  @Test
  void isSagaTimedOut_nowAtDeadline_returnsFalse() {
    // Arrange & Act & Assert
    assertThat(TimeoutPolicy.isSagaTimedOut(10_000, 10_000)).isFalse();
  }

  @Test
  void isSagaTimedOut_nowBeforeDeadline_returnsFalse() {
    // Arrange & Act & Assert
    assertThat(TimeoutPolicy.isSagaTimedOut(10_000, 9_999)).isFalse();
  }

  @Test
  void isSagaTimedOut_zeroDeadlineGiven_returnsFalse() {
    // Arrange & Act & Assert
    assertThat(TimeoutPolicy.isSagaTimedOut(0, 999_999)).isFalse();
  }
}
