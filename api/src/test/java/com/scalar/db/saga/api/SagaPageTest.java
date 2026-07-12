package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SagaPageTest {

  @Test
  @SuppressWarnings("NullAway") // intentionally passing null to verify the defensive null-check
  public void constructor_nullItemsGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaPage<>(null, "tok")).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasMore_withTokenGiven_returnsTrue() {
    // Arrange
    SagaPage<String> page = new SagaPage<>(List.of("a"), "next");

    // Assert
    assertThat(page.hasMore()).isTrue();
    assertThat(page.getNextPageToken()).isEqualTo("next");
  }

  @Test
  public void hasMore_withoutTokenGiven_returnsFalse() {
    // Arrange
    SagaPage<String> page = new SagaPage<>(List.of("a"), null);

    // Assert
    assertThat(page.hasMore()).isFalse();
    assertThat(page.getNextPageToken()).isNull();
  }

  @Test
  public void getItems_returnsUnmodifiableList() {
    // Arrange
    SagaPage<String> page = new SagaPage<>(List.of("a"), null);

    // Act & Assert
    assertThatThrownBy(() -> page.getItems().add("b"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void constructor_defensivelyCopiesItems() {
    // Arrange
    List<String> source = new ArrayList<>(List.of("a"));
    SagaPage<String> page = new SagaPage<>(source, null);

    // Act — mutating the source must not affect the page
    source.add("b");

    // Assert
    assertThat(page.getItems()).containsExactly("a");
  }

  @Test
  public void equals_sameContentGiven_areEqual() {
    // Arrange
    SagaPage<String> a = new SagaPage<>(List.of("a", "b"), "tok");
    SagaPage<String> b = new SagaPage<>(List.of("a", "b"), "tok");

    // Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }
}
