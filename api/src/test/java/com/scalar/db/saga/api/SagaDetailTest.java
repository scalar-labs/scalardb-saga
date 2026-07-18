package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SagaDetailTest {

  private static final Instant TS = Instant.parse("2026-07-13T10:00:00Z");

  private static SagaStateSnapshot snapshot() {
    return new SagaStateSnapshot("saga-1", "order", SagaStatus.ESCALATED, "owner", "v1", TS, TS);
  }

  private static TimelineEvent event() {
    return new TimelineEvent(TS, "SAGA_STARTED", null, null, SagaStatus.RUNNING, null, null);
  }

  @Test
  void constructor_snapshotAndTimelineGiven_exposesBoth() {
    // Act
    SagaDetail detail = new SagaDetail(snapshot(), List.of(event()));

    // Assert
    assertThat(detail.getSnapshot()).isEqualTo(snapshot());
    assertThat(detail.getTimeline()).containsExactly(event());
  }

  @Test
  void getTimeline_always_isUnmodifiable() {
    // Arrange
    SagaDetail detail = new SagaDetail(snapshot(), List.of(event()));

    // Act & Assert
    assertThatThrownBy(() -> detail.getTimeline().add(event()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void constructor_timelineMutatedAfterConstruction_isNotReflected() {
    // Arrange
    List<TimelineEvent> mutable = new ArrayList<>();
    mutable.add(event());
    SagaDetail detail = new SagaDetail(snapshot(), mutable);

    // Act — mutate the source list after construction
    mutable.add(event());

    // Assert — the defensive copy is unaffected
    assertThat(detail.getTimeline()).hasSize(1);
  }

  @Test
  void equals_sameFields_returnsTrueAndHashMatches() {
    // Arrange
    SagaDetail a = new SagaDetail(snapshot(), List.of(event()));
    SagaDetail b = new SagaDetail(snapshot(), List.of(event()));

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSnapshotGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDetail(null, List.of()))
        .isInstanceOf(NullPointerException.class);
  }
}
