package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HttpCallExceptionTest {

  @Test
  void knownNotCommitted_default_isFalse() {
    // Assert — a status response or ambiguous failure may have committed (the safe default).
    assertThat(new HttpCallException("boom", false).knownNotCommitted()).isFalse();
    assertThat(new HttpCallException("boom", new RuntimeException(), false).knownNotCommitted())
        .isFalse();
  }

  @Test
  void constructor_knownNotCommittedGiven_exposesFlag() {
    // Act
    HttpCallException e = new HttpCallException("refused", new RuntimeException(), true, true);

    // Assert — knownNotCommitted is orthogonal to retryable.
    assertThat(e.knownNotCommitted()).isTrue();
    assertThat(e.isRetryable()).isTrue();
  }
}
