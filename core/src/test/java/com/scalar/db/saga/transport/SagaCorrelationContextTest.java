package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SagaCorrelationContextTest {

  @AfterEach
  void clearBinding() {
    // Prevent a binding leaking to the next test on this (sequential) thread.
    SagaCorrelationContext.restore(null);
  }

  @Test
  void current_noBinding_returnsEmptyCorrelationWithNoDeadline() {
    // Act
    SagaCorrelationContext.Correlation current = SagaCorrelationContext.current();

    // Assert
    assertThat(current.sagaId()).isEmpty();
    assertThat(current.stepName()).isEmpty();
    assertThat(current.deadlineMillis()).isZero();
  }

  @Test
  void bind_setsCorrelationAndReturnsPrevious() {
    // Arrange
    Clock clock = Clock.systemUTC();

    // Act
    SagaCorrelationContext.Correlation previous =
        SagaCorrelationContext.bind("saga-1", "step-1", 123L, clock);

    // Assert
    assertThat(previous).isNull();
    assertThat(SagaCorrelationContext.current())
        .isEqualTo(new SagaCorrelationContext.Correlation("saga-1", "step-1", 123L, clock));
  }

  @Test
  void restore_previousBinding_restoresIt() {
    // Arrange
    SagaCorrelationContext.bind("outer", "o", 0L, Clock.systemUTC());
    SagaCorrelationContext.Correlation outer = SagaCorrelationContext.current();
    SagaCorrelationContext.Correlation toRestore =
        SagaCorrelationContext.bind("inner", "i", 999L, Clock.systemUTC());

    // Act
    SagaCorrelationContext.restore(toRestore);

    // Assert
    assertThat(SagaCorrelationContext.current()).isEqualTo(outer);
  }

  @Test
  void remaining_unbound_returnsNull() {
    // Act & Assert — NONE carries a zero deadline.
    assertThat(SagaCorrelationContext.remaining()).isNull();
  }

  @Test
  void remaining_noDeadlineBound_returnsNull() {
    // Arrange
    SagaCorrelationContext.bind("saga-1", "s", 0L, Clock.systemUTC());

    // Act & Assert
    assertThat(SagaCorrelationContext.remaining()).isNull();
  }

  @Test
  void remaining_futureDeadline_returnsPositiveDurationWithinBudget() {
    // Arrange
    long budgetMillis = 10_000L;
    SagaCorrelationContext.bind(
        "saga-1", "s", System.currentTimeMillis() + budgetMillis, Clock.systemUTC());

    // Act
    Duration remaining = SagaCorrelationContext.remaining();

    // Assert
    assertThat(remaining).isNotNull();
    assertThat(remaining).isGreaterThan(Duration.ZERO);
    assertThat(remaining).isLessThanOrEqualTo(Duration.ofMillis(budgetMillis));
  }

  @Test
  void remaining_passedDeadline_returnsFlooredOneMilli() {
    // Arrange — a deadline already in the past.
    SagaCorrelationContext.bind(
        "saga-1", "s", System.currentTimeMillis() - 5_000L, Clock.systemUTC());

    // Act & Assert — floored at 1ms so the JDK accepts a positive timeout and the call fails fast.
    assertThat(SagaCorrelationContext.remaining()).isEqualTo(Duration.ofMillis(1));
  }

  @Test
  void remaining_usesBoundClock_notWallClock() {
    // Arrange — a fixed clock whose "now" is epoch+1s; the deadline is 5s after that. remaining()
    // must be computed against the bound clock, independent of wall-clock time (this is what the
    // configurable-Clock fix guarantees — the old System.currentTimeMillis() version would floor to
    // 1ms).
    Clock fixed = Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC);
    SagaCorrelationContext.bind("saga-1", "s", 6_000L, fixed);

    // Act & Assert
    assertThat(SagaCorrelationContext.remaining()).isEqualTo(Duration.ofMillis(5_000L));
  }
}
