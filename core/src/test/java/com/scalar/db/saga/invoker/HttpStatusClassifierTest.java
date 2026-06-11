package com.scalar.db.saga.invoker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HttpStatusClassifierTest {

  @Test
  void isSuccess_2xxGiven_returnsTrue() {
    assertThat(HttpStatusClassifier.isSuccess(200)).isTrue();
    assertThat(HttpStatusClassifier.isSuccess(204)).isTrue();
    assertThat(HttpStatusClassifier.isSuccess(299)).isTrue();
  }

  @Test
  void isSuccess_non2xxGiven_returnsFalse() {
    assertThat(HttpStatusClassifier.isSuccess(199)).isFalse();
    assertThat(HttpStatusClassifier.isSuccess(300)).isFalse();
    assertThat(HttpStatusClassifier.isSuccess(404)).isFalse();
    assertThat(HttpStatusClassifier.isSuccess(500)).isFalse();
  }

  @Test
  void isRetryable_retryable4xxAndAll5xxGiven_returnsTrue() {
    assertThat(HttpStatusClassifier.isRetryable(408)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(429)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(500)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(502)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(503)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(504)).isTrue();
    assertThat(HttpStatusClassifier.isRetryable(599)).isTrue();
  }

  @Test
  void isRetryable_otherClientErrorsGiven_returnsFalse() {
    assertThat(HttpStatusClassifier.isRetryable(400)).isFalse();
    assertThat(HttpStatusClassifier.isRetryable(401)).isFalse();
    assertThat(HttpStatusClassifier.isRetryable(404)).isFalse();
    assertThat(HttpStatusClassifier.isRetryable(409)).isFalse();
    assertThat(HttpStatusClassifier.isRetryable(422)).isFalse();
  }
}
