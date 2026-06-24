package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TransportExceptionTest {

  @Test
  void knownNotCommitted_default_isFalse() {
    // Assert — an unproven failure is treated as possibly committed (the safe default).
    assertThat(new TransportException("boom", false).knownNotCommitted()).isFalse();
    assertThat(new TransportException("boom", new RuntimeException(), false).knownNotCommitted())
        .isFalse();
  }

  @Test
  void constructor_knownNotCommittedGiven_exposesFlag() {
    // Act
    TransportException e = new TransportException("refused", new RuntimeException(), true, true);

    // Assert — knownNotCommitted is orthogonal to retryable.
    assertThat(e.knownNotCommitted()).isTrue();
    assertThat(e.isRetryable()).isTrue();
  }
}
