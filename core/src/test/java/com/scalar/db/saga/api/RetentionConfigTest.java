package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class RetentionConfigTest {

  private static final Clock FIXED_CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  // =========================================================================
  // Valid construction
  // =========================================================================

  @Test
  void constructor_allValidParams_createsConfig() {
    // Act
    RetentionConfig config = new RetentionConfig(Duration.ofDays(3), 120, 5000, 20, FIXED_CLOCK);

    // Assert
    assertThat(config.retentionPeriod()).isEqualTo(Duration.ofDays(3));
    assertThat(config.cleanupIntervalSeconds()).isEqualTo(120);
    assertThat(config.batchSize()).isEqualTo(5000);
    assertThat(config.maxConcurrentPurges()).isEqualTo(20);
    assertThat(config.clock()).isEqualTo(FIXED_CLOCK);
  }

  // =========================================================================
  // Validation — retentionPeriod
  // =========================================================================

  @Test
  @SuppressWarnings("NullAway")
  void constructor_nullRetentionPeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(null, 60, 10_000, 10, FIXED_CLOCK))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_zeroRetentionPeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ZERO, 60, 10_000, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeRetentionPeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(-1), 60, 10_000, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — cleanupIntervalSeconds
  // =========================================================================

  @Test
  void constructor_zeroCleanupIntervalSeconds_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 0, 10_000, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeCleanupIntervalSeconds_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), -1, 10_000, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — batchSize
  // =========================================================================

  @Test
  void constructor_zeroBatchSize_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 60, 0, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeBatchSize_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 60, -1, 10, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — maxConcurrentPurges
  // =========================================================================

  @Test
  void constructor_zeroMaxConcurrentPurges_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 60, 10_000, 0, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeMaxConcurrentPurges_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 60, 10_000, -1, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — clock
  // =========================================================================

  @Test
  @SuppressWarnings("NullAway")
  void constructor_nullClock_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RetentionConfig(Duration.ofDays(7), 60, 10_000, 10, null))
        .isInstanceOf(NullPointerException.class);
  }

  // =========================================================================
  // defaults()
  // =========================================================================

  @Test
  void defaults_noArgs_returnsDefaultConfig() {
    // Act
    RetentionConfig config = RetentionConfig.defaults();

    // Assert
    assertThat(config.retentionPeriod()).isEqualTo(Duration.ofDays(7));
    assertThat(config.cleanupIntervalSeconds()).isEqualTo(60);
    assertThat(config.batchSize()).isEqualTo(10_000);
    assertThat(config.maxConcurrentPurges()).isEqualTo(10);
  }

  @Test
  void defaults_clockGiven_returnsDefaultConfigWithClock() {
    // Act
    RetentionConfig config = RetentionConfig.defaults(FIXED_CLOCK);

    // Assert
    assertThat(config.retentionPeriod()).isEqualTo(Duration.ofDays(7));
    assertThat(config.cleanupIntervalSeconds()).isEqualTo(60);
    assertThat(config.batchSize()).isEqualTo(10_000);
    assertThat(config.maxConcurrentPurges()).isEqualTo(10);
    assertThat(config.clock()).isEqualTo(FIXED_CLOCK);
  }
}
