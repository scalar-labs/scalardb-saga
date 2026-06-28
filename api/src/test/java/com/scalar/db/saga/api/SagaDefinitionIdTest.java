package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaDefinitionIdTest {

  @Test
  void constructor_validNameAndVersionGiven_createsSagaDefinitionId() {
    // Act
    SagaDefinitionId id = new SagaDefinitionId("MoneyTransfer", "1.0");

    // Assert
    assertThat(id.name()).isEqualTo("MoneyTransfer");
    assertThat(id.version()).isEqualTo("1.0");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId(null, "1.0"))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullVersionGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("transfer", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_blankNameGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("  ", "1.0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_blankVersionGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("transfer", "  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_unicodeWhitespaceNameGiven_throwsIllegalArgumentException() {
    // Arrange — U+3000 (ideographic space) is whitespace above U+0020, which String.trim() does not
    // strip; the blank check must still reject it (exact String.isBlank() parity under --release
    // 8).
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("\u3000", "1.0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_unicodeWhitespaceVersionGiven_throwsIllegalArgumentException() {
    // Arrange — U+3000 (ideographic space) is whitespace above U+0020, which String.trim() does not
    // strip; the blank check must still reject it (exact String.isBlank() parity under --release
    // 8).
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("transfer", "\u3000"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_nameWithColonGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("ns:transfer", "1.0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_versionWithColonGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionId("transfer", "1:0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void toString_returnsReadableFormat() {
    // Arrange
    SagaDefinitionId id = new SagaDefinitionId("MoneyTransfer", "1.0");

    // Act & Assert
    assertThat(id).hasToString("MoneyTransfer (v1.0)");
  }

  @Test
  void equals_sameNameAndVersionGiven_returnsTrue() {
    // Arrange
    SagaDefinitionId id1 = new SagaDefinitionId("transfer", "1.0");
    SagaDefinitionId id2 = new SagaDefinitionId("transfer", "1.0");

    // Act & Assert
    assertThat(id1).isEqualTo(id2);
    assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
  }

  @Test
  void equals_differentVersionGiven_returnsFalse() {
    // Arrange
    SagaDefinitionId id1 = new SagaDefinitionId("transfer", "1.0");
    SagaDefinitionId id2 = new SagaDefinitionId("transfer", "2.0");

    // Act & Assert
    assertThat(id1).isNotEqualTo(id2);
  }
}
