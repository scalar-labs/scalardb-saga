package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class LoopbackHostTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "localhost",
        "LOCALHOST",
        "127.0.0.1",
        "127.0.0.0",
        "127.255.255.255",
        "127.1.2.3",
        "::1",
        "[::1]"
      })
  void isLoopback_loopbackLiteral_returnsTrue(String host) {
    // Act / Assert
    assertThat(LoopbackHost.isLoopback(host)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Spoofable DNS names that merely start with "127." — the bypass this parse closes.
        "127.attacker.org",
        "127.0.0.1.evil.com",
        "127.foo",
        // Non-loopback and malformed IPv4.
        "128.0.0.1",
        "10.0.0.1",
        "0.0.0.0",
        "example.com",
        "127.0.0",
        "127.0.0.1.1",
        "127.0.0.256",
        "127.0.0.-1",
        "127.0.0.",
        "127..0.1",
        "127.0.0.01x",
        "1270.0.0.1"
      })
  void isLoopback_nonLoopbackHost_returnsFalse(String host) {
    // Act / Assert
    assertThat(LoopbackHost.isLoopback(host)).isFalse();
  }

  @ParameterizedTest
  @NullAndEmptySource
  void isLoopback_nullOrEmpty_returnsFalse(String host) {
    // Act / Assert
    assertThat(LoopbackHost.isLoopback(host)).isFalse();
  }
}
