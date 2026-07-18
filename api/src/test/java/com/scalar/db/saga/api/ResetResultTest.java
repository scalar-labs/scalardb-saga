package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.ResetResult.SkipReason;
import com.scalar.db.saga.api.ResetResult.SkippedSaga;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResetResultTest {

  @Test
  void constructor_countResetAndSkippedGiven_exposesAll() {
    // Arrange
    List<SkippedSaga> skipped =
        List.of(
            new SkippedSaga("s-1", SkipReason.CONCURRENT_MODIFICATION),
            new SkippedSaga("s-2", SkipReason.DEFINITION_NOT_FOUND));

    // Act
    ResetResult r = new ResetResult(47, skipped, "next-token");

    // Assert
    assertThat(r.getResetCount()).isEqualTo(47);
    assertThat(r.getSkipped()).containsExactlyElementsOf(skipped);
    assertThat(r.getSkippedCount()).isEqualTo(2);
    assertThat(r.getNextPageToken()).isEqualTo("next-token");
    assertThat(r.hasMore()).isTrue();
  }

  @Test
  void getSkipped_exposesReasonPerSaga() {
    // Arrange
    ResetResult r =
        new ResetResult(0, List.of(new SkippedSaga("s-9", SkipReason.DEFINITION_NOT_FOUND)), null);

    // Assert
    SkippedSaga only = r.getSkipped().get(0);
    assertThat(only.getSagaId()).isEqualTo("s-9");
    assertThat(only.getReason()).isEqualTo(SkipReason.DEFINITION_NOT_FOUND);
  }

  @Test
  void hasMore_nullTokenGiven_returnsFalse() {
    // Act & Assert
    assertThat(new ResetResult(1, List.of(), null).hasMore()).isFalse();
  }

  @Test
  void getSkipped_always_isUnmodifiable() {
    // Arrange
    ResetResult r = new ResetResult(0, List.of(), null);

    // Act & Assert
    assertThatThrownBy(
            () -> r.getSkipped().add(new SkippedSaga("x", SkipReason.CONCURRENT_MODIFICATION)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void constructor_skippedMutatedAfterConstruction_isNotReflected() {
    // Arrange
    List<SkippedSaga> mutable = new ArrayList<>();
    mutable.add(new SkippedSaga("s-1", SkipReason.CONCURRENT_MODIFICATION));
    ResetResult r = new ResetResult(0, mutable, null);

    // Act
    mutable.add(new SkippedSaga("s-2", SkipReason.DEFINITION_NOT_FOUND));

    // Assert
    assertThat(r.getSkipped()).hasSize(1);
  }

  @Test
  void constructor_negativeResetCountGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new ResetResult(-1, List.of(), null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSkippedGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new ResetResult(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equals_sameFields_returnsTrueAndHashMatches() {
    // Arrange
    List<SkippedSaga> skipped = List.of(new SkippedSaga("s-1", SkipReason.CONCURRENT_MODIFICATION));
    ResetResult a = new ResetResult(5, skipped, "t");
    ResetResult b = new ResetResult(5, skipped, "t");

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void skippedSaga_equals_sameFields_returnsTrue() {
    // Arrange
    SkippedSaga a = new SkippedSaga("s-1", SkipReason.DEFINITION_NOT_FOUND);
    SkippedSaga b = new SkippedSaga("s-1", SkipReason.DEFINITION_NOT_FOUND);

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @SuppressWarnings("NullAway")
  @Test
  void skippedSaga_nullSagaIdGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SkippedSaga(null, SkipReason.CONCURRENT_MODIFICATION))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void skippedSaga_detailGiven_exposesItAndNullByDefault() {
    // Arrange
    SkippedSaga withDetail =
        new SkippedSaga("s-1", SkipReason.CORRUPT_EVENT_STREAM, "Unknown event type: X");
    SkippedSaga withoutDetail = new SkippedSaga("s-1", SkipReason.DEFINITION_NOT_FOUND);

    // Act & Assert
    assertThat(withDetail.getDetail()).isEqualTo("Unknown event type: X");
    assertThat(withoutDetail.getDetail()).isNull();
  }

  @Test
  void skippedSaga_differentDetailGiven_areNotEqual() {
    // Arrange — same saga and reason, but a different detail must not compare equal
    SkippedSaga a = new SkippedSaga("s-1", SkipReason.CORRUPT_EVENT_STREAM, "bad payload");
    SkippedSaga b = new SkippedSaga("s-1", SkipReason.CORRUPT_EVENT_STREAM, "unknown type");
    SkippedSaga noDetail = new SkippedSaga("s-1", SkipReason.CORRUPT_EVENT_STREAM);

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
    assertThat(a).isNotEqualTo(noDetail);
  }
}
