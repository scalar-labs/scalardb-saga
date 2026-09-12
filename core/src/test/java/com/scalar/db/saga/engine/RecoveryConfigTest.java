package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class RecoveryConfigTest {

  private static final Clock FIXED_CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  // =========================================================================
  // Valid construction
  // =========================================================================

  @Test
  void constructor_allValidParams_createsConfig() {
    // Act
    RecoveryConfig config =
        new RecoveryConfig(30_000, 10, Duration.ofHours(2), 500, 5, FIXED_CLOCK);

    // Assert
    assertThat(config.stalenessThresholdMillis()).isEqualTo(30_000);
    assertThat(config.intervalSeconds()).isEqualTo(10);
    assertThat(config.compensationGracePeriod()).isEqualTo(Duration.ofHours(2));
    assertThat(config.maxRecoveriesPerSweep()).isEqualTo(500);
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(5);
    assertThat(config.clock()).isEqualTo(FIXED_CLOCK);
  }

  // =========================================================================
  // Validation — stalenessThresholdMillis
  // =========================================================================

  @Test
  void constructor_zeroStalenessThresholdMillis_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(0, 10, Duration.ofHours(2), 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeStalenessThresholdMillis_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(-1, 10, Duration.ofHours(2), 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — intervalSeconds
  // =========================================================================

  @Test
  void constructor_zeroIntervalSeconds_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, 0, Duration.ofHours(2), 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeIntervalSeconds_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, -1, Duration.ofHours(2), 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — compensationGracePeriod
  // =========================================================================

  @Test
  @SuppressWarnings("NullAway")
  void constructor_nullCompensationGracePeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(30_000, 10, null, 500, 5, FIXED_CLOCK))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_zeroCompensationGracePeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(30_000, 10, Duration.ZERO, 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeCompensationGracePeriod_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, 10, Duration.ofHours(-1), 500, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — maxRecoveriesPerSweep
  // =========================================================================

  @Test
  void constructor_zeroMaxRecoveriesPerSweep_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(30_000, 10, Duration.ofHours(2), 0, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeMaxRecoveriesPerSweep_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, 10, Duration.ofHours(2), -1, 5, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — maxConcurrentRecoveries
  // =========================================================================

  @Test
  void constructor_zeroMaxConcurrentRecoveries_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, 10, Duration.ofHours(2), 500, 0, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_negativeMaxConcurrentRecoveries_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> new RecoveryConfig(30_000, 10, Duration.ofHours(2), 500, -1, FIXED_CLOCK))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // =========================================================================
  // Validation — clock
  // =========================================================================

  @Test
  @SuppressWarnings("NullAway")
  void constructor_nullClock_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new RecoveryConfig(30_000, 10, Duration.ofHours(2), 500, 5, null))
        .isInstanceOf(NullPointerException.class);
  }

  // =========================================================================
  // defaults()
  // =========================================================================

  @Test
  void defaults_noArgs_returnsDefaultConfig() {
    // Act
    RecoveryConfig config = RecoveryConfig.defaults();

    // Assert
    assertThat(config.stalenessThresholdMillis()).isEqualTo(60_000);
    assertThat(config.intervalSeconds()).isEqualTo(30);
    assertThat(config.compensationGracePeriod()).isEqualTo(Duration.ofHours(4));
    assertThat(config.maxRecoveriesPerSweep()).isEqualTo(1000);
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(10);
  }

  @Test
  void defaults_clockGiven_returnsDefaultConfigWithClock() {
    // Act
    RecoveryConfig config = RecoveryConfig.defaults(FIXED_CLOCK);

    // Assert
    assertThat(config.stalenessThresholdMillis()).isEqualTo(60_000);
    assertThat(config.intervalSeconds()).isEqualTo(30);
    assertThat(config.compensationGracePeriod()).isEqualTo(Duration.ofHours(4));
    assertThat(config.maxRecoveriesPerSweep()).isEqualTo(1000);
    assertThat(config.maxConcurrentRecoveries()).isEqualTo(10);
    assertThat(config.clock()).isEqualTo(FIXED_CLOCK);
  }
}
