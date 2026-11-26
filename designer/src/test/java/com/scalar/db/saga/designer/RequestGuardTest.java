package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RequestGuardTest {

  private final RequestGuard loopback = new RequestGuard("127.0.0.1");

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1:7070", "localhost:7070", "LOCALHOST", "[::1]:7070"})
  void refusal_getWithALoopbackHost_letsItThrough(String host) {
    // Act Assert
    assertThat(loopback.refusal("GET", host, null, null)).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"evil.example:7070", "evil.example", "127.0.0.1.evil.example:7070"})
  void refusal_foreignHost_refusesItAsRebinding(String host) {
    // Act Assert
    assertThat(loopback.refusal("GET", host, null, null)).isEqualTo("Unexpected Host header");
  }

  @Test
  void refusal_missingHost_refusesIt() {
    // Act Assert
    assertThat(loopback.refusal("GET", null, null, null)).isNotNull();
    assertThat(loopback.refusal("GET", " ", null, null)).isNotNull();
  }

  @Test
  void refusal_putFromTheDesignersOwnPage_letsItThrough() {
    // Act Assert
    assertThat(
            loopback.refusal(
                "PUT",
                "127.0.0.1:7070",
                "http://127.0.0.1:7070",
                "application/json; charset=utf-8"))
        .isNull();
  }

  @Test
  void refusal_postWithNoOrigin_letsItThrough() {
    // Act Assert
    assertThat(loopback.refusal("POST", "127.0.0.1:7070", null, "application/json")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"text/plain", "application/x-www-form-urlencoded", "multipart/form-data"})
  void refusal_postWithASimpleContentType_refusesIt(String contentType) {
    // Act Assert
    assertThat(loopback.refusal("POST", "127.0.0.1:7070", null, contentType))
        .isEqualTo("A request that changes something must be JSON");
  }

  @Test
  void refusal_postWithNoContentType_refusesIt() {
    // Act Assert
    assertThat(loopback.refusal("POST", "127.0.0.1:7070", null, null)).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"http://evil.example", "null", "http://localhost:7070", "https://127.0.0.1:7070"})
  void refusal_postFromAnotherOrigin_refusesIt(String origin) {
    // Act Assert
    assertThat(loopback.refusal("POST", "127.0.0.1:7070", origin, "application/json"))
        .isEqualTo("Cross-origin request");
  }

  @Test
  void refusal_boundToANamedAddress_acceptsThatNameToo() {
    // Arrange
    RequestGuard guard = new RequestGuard("192.168.1.5");

    // Act Assert
    assertThat(guard.refusal("GET", "192.168.1.5:7070", null, null)).isNull();
    assertThat(guard.refusal("GET", "evil.example", null, null)).isNotNull();
  }

  @Test
  void refusal_boundToAnIpv6Address_acceptsItInBrackets() {
    // Arrange
    RequestGuard guard = new RequestGuard("fe80::1");

    // Act Assert
    assertThat(guard.refusal("GET", "[fe80::1]:7070", null, null)).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.0.0.0", "::"})
  void refusal_withAWildcardBind_refusesARebindingPageWhoseOriginMatchesItsHost(String bind) {
    // Arrange
    RequestGuard guard = new RequestGuard(bind, List.of(), List.of(address("192.168.1.5")));

    // Act Assert
    assertThat(
            guard.refusal(
                "PUT", "evil.example:7070", "http://evil.example:7070", "application/json"))
        .isEqualTo("Unexpected Host header");
    assertThat(guard.refusal("GET", "evil.example:7070", null, null))
        .isEqualTo("Unexpected Host header");
  }

  @Test
  void refusal_withAWildcardBind_acceptsLoopbackAndTheMachinesOwnAddresses() {
    // Arrange
    RequestGuard guard =
        new RequestGuard(
            "0.0.0.0", List.of(), List.of(address("192.168.1.5"), address("fe80::1%5")));

    // Act Assert
    assertThat(guard.refusal("GET", "127.0.0.1:7070", null, null)).isNull();
    assertThat(guard.refusal("GET", "192.168.1.5:7070", null, null)).isNull();
    assertThat(guard.refusal("GET", "[fe80::1]:7070", null, null)).isNull();
  }

  @Test
  void refusal_withAnAllowedHost_acceptsThatName() {
    // Arrange
    RequestGuard guard = new RequestGuard("0.0.0.0", List.of("Dev-VM.local"), List.of());

    // Act Assert
    assertThat(
            guard.refusal(
                "PUT", "dev-vm.local:7070", "http://dev-vm.local:7070", "application/json"))
        .isNull();
    assertThat(guard.refusal("GET", "other.local:7070", null, null)).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0"})
  void isWildcard_wildcardAddressGiven_returnsTrue(String host) {
    // Act Assert
    assertThat(RequestGuard.isWildcard(host)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1", "::1", "192.168.1.5", "localhost"})
  void isWildcard_specificAddressGiven_returnsFalse(String host) {
    // Act Assert
    assertThat(RequestGuard.isWildcard(host)).isFalse();
  }

  private static InetAddress address(String literal) {
    return InetAddress.ofLiteral(literal);
  }
}
