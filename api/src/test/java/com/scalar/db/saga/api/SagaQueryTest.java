package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class SagaQueryTest {

  @Test
  public void newBuilder_withDefaults_usesDefaultPageSizeAndNullFilters() {
    // Act
    SagaQuery query = SagaQuery.newBuilder().build();

    // Assert
    assertThat(query.getPageSize()).isEqualTo(SagaQuery.DEFAULT_PAGE_SIZE);
    assertThat(query.getStatus()).isNull();
    assertThat(query.getUpdatedAfter()).isNull();
    assertThat(query.getUpdatedBefore()).isNull();
    assertThat(query.getPageToken()).isNull();
  }

  @Test
  public void build_withAllOptions_setsAllFields() {
    // Arrange
    Instant after = Instant.parse("2026-01-01T00:00:00Z");
    Instant before = Instant.parse("2026-02-01T00:00:00Z");

    // Act
    SagaQuery query =
        SagaQuery.newBuilder()
            .status(SagaStatus.ESCALATED)
            .updatedAfter(after)
            .updatedBefore(before)
            .pageSize(50)
            .pageToken("tok")
            .build();

    // Assert
    assertThat(query.getStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(query.getUpdatedAfter()).isEqualTo(after);
    assertThat(query.getUpdatedBefore()).isEqualTo(before);
    assertThat(query.getPageSize()).isEqualTo(50);
    assertThat(query.getPageToken()).isEqualTo("tok");
  }

  @Test
  public void pageSize_zeroGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaQuery.newBuilder().pageSize(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void pageSize_negativeGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaQuery.newBuilder().pageSize(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void pageSize_aboveMaxGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaQuery.newBuilder().pageSize(SagaQuery.MAX_PAGE_SIZE + 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void pageSize_atBoundsGiven_accepted() {
    // Act & Assert — both bounds are valid
    assertThat(SagaQuery.newBuilder().pageSize(1).build().getPageSize()).isEqualTo(1);
    assertThat(SagaQuery.newBuilder().pageSize(SagaQuery.MAX_PAGE_SIZE).build().getPageSize())
        .isEqualTo(SagaQuery.MAX_PAGE_SIZE);
  }

  @Test
  public void equals_sameFieldsGiven_areEqual() {
    // Arrange
    SagaQuery a = SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(10).build();
    SagaQuery b = SagaQuery.newBuilder().status(SagaStatus.RUNNING).pageSize(10).build();

    // Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  @Test
  public void equals_differentFieldsGiven_areNotEqual() {
    // Arrange
    SagaQuery a = SagaQuery.newBuilder().status(SagaStatus.RUNNING).build();
    SagaQuery b = SagaQuery.newBuilder().status(SagaStatus.COMPLETED).build();

    // Assert
    assertThat(a).isNotEqualTo(b);
  }
}
