package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.definition.RetryPolicy;
import org.junit.jupiter.api.Test;

class StepWithPolicyTest {

  @Test
  void constructor_validArgsGiven_setsFields() {
    // Arrange
    Step step = mock(Step.class);
    RetryPolicy policy = RetryPolicy.defaultPolicy();
    RetryPolicy compensationPolicy = RetryPolicy.compensationDefault();

    // Act
    StepWithPolicy entry = new StepWithPolicy(step, policy, compensationPolicy, 5000, 600_000);

    // Assert
    assertThat(entry.step()).isSameAs(step);
    assertThat(entry.executionRetryPolicy()).isSameAs(policy);
    assertThat(entry.compensationRetryPolicy()).isSameAs(compensationPolicy);
    assertThat(entry.stepTimeoutMillis()).isEqualTo(5000);
    assertThat(entry.callbackTimeoutMillis()).isEqualTo(600_000);
  }

  @Test
  void constructor_zeroTimeoutGiven_succeeds() {
    // Arrange
    Step step = mock(Step.class);
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act
    StepWithPolicy entry =
        new StepWithPolicy(step, policy, RetryPolicy.compensationDefault(), 0, 0);

    // Assert
    assertThat(entry.stepTimeoutMillis()).isEqualTo(0);
    assertThat(entry.callbackTimeoutMillis()).isEqualTo(0);
  }

  @Test
  void constructor_negativeTimeoutGiven_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(
            () ->
                new StepWithPolicy(
                    mock(Step.class),
                    RetryPolicy.defaultPolicy(),
                    RetryPolicy.compensationDefault(),
                    -1,
                    0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeCallbackTimeoutGiven_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(
            () ->
                new StepWithPolicy(
                    mock(Step.class),
                    RetryPolicy.defaultPolicy(),
                    RetryPolicy.compensationDefault(),
                    0,
                    -1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
