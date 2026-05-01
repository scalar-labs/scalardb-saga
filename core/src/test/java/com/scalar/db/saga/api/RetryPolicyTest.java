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
    assertThat(policy.getInitialIntervalMs()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMs()).isEqualTo(30_000);
  }

  @Test
  void compensationDefault_called_returnsExpectedValues() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.compensationDefault();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(3);
    assertThat(policy.getInitialIntervalMs()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMs()).isEqualTo(10_000);
  }

  @Test
  void confirmDefault_called_returnsExpectedValues() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.confirmDefault();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(10);
    assertThat(policy.getInitialIntervalMs()).isEqualTo(500);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMs()).isEqualTo(60_000);
  }

  @Test
  void builder_withCustomValues_setsAllFields() {
    // Arrange & Act
    RetryPolicy policy =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMs(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMs(10_000)
            .build();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(5);
    assertThat(policy.getInitialIntervalMs()).isEqualTo(2000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(1.5);
    assertThat(policy.getMaxIntervalMs()).isEqualTo(10_000);
  }

  @Test
  void builder_withDefaults_matchesDefaultPolicy() {
    // Arrange & Act
    RetryPolicy policy = RetryPolicy.newBuilder().build();

    // Assert
    assertThat(policy.getMaxAttempts()).isEqualTo(3);
    assertThat(policy.getInitialIntervalMs()).isEqualTo(1000);
    assertThat(policy.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(policy.getMaxIntervalMs()).isEqualTo(30_000);
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
    assertThatThrownBy(() -> RetryPolicy.newBuilder().initialIntervalMs(0).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void build_withNegativeInitialIntervalMs_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> RetryPolicy.newBuilder().initialIntervalMs(-100).build())
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
            () -> RetryPolicy.newBuilder().initialIntervalMs(5000).maxIntervalMs(1000).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void equals_sameValues_returnsTrue() {
    // Arrange
    RetryPolicy a =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMs(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMs(10_000)
            .build();
    RetryPolicy b =
        RetryPolicy.newBuilder()
            .maxAttempts(5)
            .initialIntervalMs(2000)
            .backoffMultiplier(1.5)
            .maxIntervalMs(10_000)
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
    RetryPolicy a = RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMs(500).build();
    RetryPolicy b = RetryPolicy.newBuilder().maxAttempts(5).initialIntervalMs(500).build();

    // Act & Assert
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void toString_called_containsFieldValues() {
    // Arrange
    RetryPolicy policy = RetryPolicy.defaultPolicy();

    // Act
    String result = policy.toString();

    // Assert
    assertThat(result).contains("maxAttempts=3");
    assertThat(result).contains("initialIntervalMs=1000");
    assertThat(result).contains("backoffMultiplier=2.0");
    assertThat(result).contains("maxIntervalMs=30000");
  }
}
