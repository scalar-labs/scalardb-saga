package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  @Test
  void defaultPolicy_called_returnsExpectedValues() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(3);
    assertThat(policy.getInitialIntervalMillis()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMillis()).isEqualTo(30_000);
  }

  @Test
  void compensationDefault_called_returnsExpectedValues() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.compensationDefault();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(3);
    assertThat(policy.getInitialIntervalMillis()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMillis()).isEqualTo(10_000);
  }

  @Test
  void confirmDefault_called_returnsExpectedValues() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.confirmDefault();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(10);
    assertThat(policy.getInitialIntervalMillis()).isEqualTo(500);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMillis()).isEqualTo(60_000);
  }

  @Test
  void builder_withCustomValues_setsAllFields() {
    // Arrange & Act
    RetryPolicy policy =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMillis(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMillis(10_000)
            .build();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(5);
    assertThat(policy.getInitialIntervalMillis()).isEqualTo(2000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(1.5);
    assertThat(policy.getMaxIntervalMillis()).isEqualTo(10_000);
  }

  @Test
  void builder_withDefaults_matchesDefaultPolicy() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.newBuilder().build();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(3);
    assertThat(policy.getInitialIntervalMillis()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMillis()).isEqualTo(30_000);
  }

  @Test
  void build_withZeroMaxAttempts_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().maxAttempts(0).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withNegativeMaxAttempts_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().maxAttempts(-1).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withZeroInitialIntervalMs_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().initialIntervalMillis(0).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withNegativeInitialIntervalMs_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().initialIntervalMillis(-100).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withBackoffMultiplierLessThanOne_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().backoffMultiplier(0.5).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withMaxIntervalLessThanInitial_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(
            () ->
                RetryPolicy.newBuilder()
                    .initialIntervalMillis(5000)
                    .maxIntervalMillis(1000)
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void equals_sameValues_returnsTrue() {
    // Arrange
    RetryPolicy a =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMillis(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMillis(10_000)
            .build();
    RetryPolicy b =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMillis(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMillis(10_000)
            .build();

    // Act & Assert
    assertThat(a).isEqualTo(b);
  }

  @Test
  void equals_differentMaxAttempts_returnsFalse() {
    // Arrange
    RetryPolicy a = RetryPolicy.newBuilder().maxAttempts(3).build();
    RetryPolicy b = RetryPolicy.newBuilder().maxAttempts(5).build();

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_differentBackoffMultiplier_returnsFalse() {
    // Arrange
    RetryPolicy a = RetryPolicy.newBuilder().backoffMultiplier(1.5).build();
    RetryPolicy b = RetryPolicy.newBuilder().backoffMultiplier(2.0).build();

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Arrange
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act & Assert
    assertThat(policy).isNotEqualTo(null);
  }

  @Test
  void hashCode_equalObjects_sameHashCode() {
    // Arrange
    RetryPolicy a = RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMillis(500).build();
    RetryPolicy b = RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMillis(500).build();

    // Act & Assert
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  // --- sleepWithBackoff ---

  @Test
  void sleepWithBackoff_called_returnsNextInterval() throws InterruptedException {
    // Arrange
    RetryPolicy policy =
        RetryPolicy.newBuilder()
            .initialIntervalMillis(100)
            .backoffMultiplier(2.0)
            .maxIntervalMillis(10_000)
            .build();

    // Act
    long nextInterval = policy.sleepWithBackoff(100);

    // Assert — next = min(100 * 2.0, 10_000) = 200
    assertThat(nextInterval).isEqualTo(200);
  }

  @Test
  void sleepWithBackoff_intervalExceedsMax_returnsCappedInterval() throws InterruptedException {
    // Arrange
    RetryPolicy policy =
        RetryPolicy.newBuilder()
            .initialIntervalMillis(100)
            .backoffMultiplier(2.0)
            .maxIntervalMillis(500)
            .build();

    // Act
    long nextInterval = policy.sleepWithBackoff(400);

    // Assert — next = min(400 * 2.0, 500) = 500
    assertThat(nextInterval).isEqualTo(500);
  }

  @Test
  void sleepWithBackoff_calledRepeatedly_jitterBoundsHold() throws InterruptedException {
    // Arrange
    RetryPolicy policy =
        RetryPolicy.newBuilder()
            .initialIntervalMillis(100)
            .backoffMultiplier(2.0)
            .maxIntervalMillis(10_000)
            .build();

    // Act & Assert — run multiple times to verify jitter doesn't cause issues
    long interval = 2;
    for (int i = 0; i < 5; i++) {
      long nextInterval = policy.sleepWithBackoff(interval);
      assertThat(nextInterval).isGreaterThan(0);
      assertThat(nextInterval).isLessThanOrEqualTo(policy.getMaxIntervalMillis());
      interval = nextInterval;
    }
  }

  @Test
  void sleepWithBackoff_zeroIntervalGiven_throwsIllegalArgumentException() {
    // Arrange
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act & Assert
    assertThatThrownBy(() -> policy.sleepWithBackoff(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void sleepWithBackoff_negativeIntervalGiven_throwsIllegalArgumentException() {
    // Arrange
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act & Assert
    assertThatThrownBy(() -> policy.sleepWithBackoff(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void toString_called_containsFieldValues() {
    // Arrange
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act
    String result = policy.toString();

    // Assert
    assertThat(result).contains("maxAttempts=3");
    assertThat(result).contains("initialIntervalMillis=1000");
    assertThat(result).contains("backoffMultiplier=2.0");
    assertThat(result).contains("maxIntervalMillis=30000");
  }
}
