package com.scalar.db.saga.transport;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The constructor is the chokepoint every endpoint-construction path flows through (the
 * orchestrator builder and every configuration swap), so its base-URL validation is what guarantees
 * a malformed or misleading URL can never reach an endpoint. Messages must not echo the value: on
 * the server's reload path it may have resolved from a secret reference.
 */
class HttpServiceConfigTest {

  private static HttpServiceConfig config(String baseUrl) {
    return new HttpServiceConfig(baseUrl, List.of(), -1, null, Map.of());
  }

  @Test
  void constructor_validHttpAndHttpsUrls_accepted() {
    assertThatCode(
            () -> {
              config("http://account-svc:8080");
              config("https://account-svc.internal");
            })
        .doesNotThrowAnyException();
  }

  @Test
  void constructor_userInfoInBaseUrl_throwsWithoutEchoingTheValue() {
    // The SSRF shape: http://svc@evil.example resolves to evil.example, not svc.
    assertThatThrownBy(() -> config("http://payment@evil.example"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("user-info")
        .hasMessageNotContaining("evil.example");
  }

  @Test
  void constructor_nonHttpScheme_throws() {
    assertThatThrownBy(() -> config("ftp://account-svc"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_missingScheme_throws() {
    assertThatThrownBy(() -> config("account-svc:8080"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_missingHost_throws() {
    assertThatThrownBy(() -> config("http://")).isInstanceOf(IllegalArgumentException.class);
  }
}
