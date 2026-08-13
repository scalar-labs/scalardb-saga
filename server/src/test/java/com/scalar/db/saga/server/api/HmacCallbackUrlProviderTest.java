package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class HmacCallbackUrlProviderTest {

  private static final String SECRET = "test-secret";
  private static final Clock FIXED =
      Clock.fixed(Instant.ofEpochSecond(1_700_000_000L), ZoneOffset.UTC);

  @Test
  void callbackUrl_buildsSignedUrl_verifiableWithTheSameSecret() {
    HmacCallbackUrlProvider provider =
        new HmacCallbackUrlProvider("http://daemon:8080", SECRET, FIXED);

    String url = Objects.requireNonNull(provider.callbackUrl("saga-1", "debit"));

    assertThat(url)
        .startsWith("http://daemon:8080/sagas/saga-1/steps/debit/complete?token=")
        .endsWith("&iat=1700000000");
    // The token verifies against the same secret + signed data the receiving route recomputes.
    String token = url.substring(url.indexOf("token=") + "token=".length(), url.indexOf("&iat="));
    String signedData = HmacUtils.callbackSignedData("saga-1", "debit", "1700000000");
    assertThat(HmacUtils.verify(SECRET, signedData, token)).isTrue();
  }

  @Test
  void callbackUrl_encodesPathSegments() {
    HmacCallbackUrlProvider provider = new HmacCallbackUrlProvider("http://d", SECRET, FIXED);

    String url = provider.callbackUrl("saga/1", "deb it");

    assertThat(url).contains("/sagas/saga%2F1/steps/deb%20it/complete");
  }

  @Test
  void callbackUrl_mintsAFreshTokenPerCall() {
    // A later issue time yields a different iat → different token, so a re-drive re-provisions.
    Clock later = Clock.fixed(Instant.ofEpochSecond(1_700_000_001L), ZoneOffset.UTC);

    String first = new HmacCallbackUrlProvider("http://d", SECRET, FIXED).callbackUrl("s", "x");
    String second = new HmacCallbackUrlProvider("http://d", SECRET, later).callbackUrl("s", "x");

    assertThat(first).isNotEqualTo(second);
  }
}
