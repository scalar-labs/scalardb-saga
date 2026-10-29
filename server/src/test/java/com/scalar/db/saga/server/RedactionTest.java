package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RedactionTest {

  @Test
  void redacted_valueGiven_reportsLengthWithoutEchoingValue() {
    // Act
    String described = Redaction.redacted("s3cr3t-plaintext");

    // Assert
    assertThat(described).isEqualTo("(value redacted, 16 chars)");
    assertThat(described).doesNotContain("s3cr3t");
  }

  @Test
  void redacted_emptyValueGiven_reportsZeroChars() {
    // Act / Assert
    assertThat(Redaction.redacted("")).isEqualTo("(value redacted, 0 chars)");
  }
}
