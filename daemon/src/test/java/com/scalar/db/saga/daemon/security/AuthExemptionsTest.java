package com.scalar.db.saga.daemon.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AuthExemptionsTest {

  @Test
  void none_exemptsNothing() {
    // Arrange
    AuthExemptions exemptions = AuthExemptions.none();

    // Assert
    assertThat(exemptions.isExempt("/health")).isFalse();
    assertThat(exemptions.isExempt("/sagas/abc")).isFalse();
  }

  @Test
  void isExempt_literalPathGiven_matchesExactly() {
    // Arrange
    AuthExemptions exemptions = AuthExemptions.of("/health");

    // Assert
    assertThat(exemptions.isExempt("/health")).isTrue();
    assertThat(exemptions.isExempt("/healthz")).isFalse();
    assertThat(exemptions.isExempt("/health/sub")).isFalse();
  }

  @Test
  void isExempt_singleSegmentParamGiven_matchesOneSegmentOnly() {
    // Arrange — Javalin {param} matches exactly one path segment
    AuthExemptions exemptions = AuthExemptions.of("/sagas/{id}/steps/{stepName}/complete");

    // Assert — matches concrete values
    assertThat(exemptions.isExempt("/sagas/abc/steps/reserve/complete")).isTrue();
    assertThat(exemptions.isExempt("/sagas/123/steps/pay/complete")).isTrue();
  }

  @Test
  void isExempt_paramSegmentGiven_doesNotMatchAcrossSlashes() {
    // Arrange
    AuthExemptions exemptions = AuthExemptions.of("/sagas/{id}/steps/{stepName}/complete");

    // Assert — {id} must not swallow an extra segment, and a missing/extra segment fails
    assertThat(exemptions.isExempt("/sagas/a/b/steps/reserve/complete")).isFalse();
    assertThat(exemptions.isExempt("/sagas/abc/steps/reserve")).isFalse();
    assertThat(exemptions.isExempt("/sagas/abc/steps/reserve/complete/extra")).isFalse();
  }

  @Test
  void isExempt_wildcardSegmentGiven_matchesAcrossSlashes() {
    // Arrange — Javalin <param> matches one-or-more segments
    AuthExemptions exemptions = AuthExemptions.of("/files/<path>");

    // Assert
    assertThat(exemptions.isExempt("/files/a")).isTrue();
    assertThat(exemptions.isExempt("/files/a/b/c")).isTrue();
    assertThat(exemptions.isExempt("/files")).isFalse();
  }

  @Test
  void isExempt_trailingSlash_matchesRegardless() {
    // Arrange
    AuthExemptions exemptions = AuthExemptions.of("/health");

    // Assert — a trailing slash on the request path does not defeat the exemption
    assertThat(exemptions.isExempt("/health/")).isTrue();
  }

  @Test
  void isExempt_multiplePatternsGiven_matchesAny() {
    // Arrange
    AuthExemptions exemptions =
        AuthExemptions.of("/health", "/sagas/{id}/steps/{stepName}/complete");

    // Assert
    assertThat(exemptions.isExempt("/health")).isTrue();
    assertThat(exemptions.isExempt("/sagas/x/steps/y/complete")).isTrue();
    assertThat(exemptions.isExempt("/sagas/x")).isFalse();
  }

  @Test
  void isExempt_nonMatchingPath_returnsFalse() {
    // Arrange
    AuthExemptions exemptions =
        AuthExemptions.of("/health", "/sagas/{id}/steps/{stepName}/complete");

    // Assert — a sibling saga route is not exempt
    assertThat(exemptions.isExempt("/sagas/abc")).isFalse();
    assertThat(exemptions.isExempt("/sagas")).isFalse();
  }
}
