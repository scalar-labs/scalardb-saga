package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.Step;
import org.junit.jupiter.api.Test;

class StepWithPolicyTest {

  @Test
  void constructor_validArgsGiven_setsFields() {
    // Arrange
    Step step = mock(Step.class);
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act
    StepWithPolicy entry = new StepWithPolicy(step, policy, 5000);

    // Assert
    assertThat(entry.step()).isSameAs(step);
    assertThat(entry.retryPolicy()).isSameAs(policy);
    assertThat(entry.stepTimeoutMillis()).isEqualTo(5000);
  }

  @Test
  void constructor_zeroTimeoutGiven_succeeds() {
    // Arrange
    Step step = mock(Step.class);
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act
    StepWithPolicy entry = new StepWithPolicy(step, policy, 0);

    // Assert
    assertThat(entry.stepTimeoutMillis()).isEqualTo(0);
  }

  @Test
  void constructor_negativeTimeoutGiven_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepWithPolicy(mock(Step.class), RetryPolicy.defaultPolicy(), -1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
