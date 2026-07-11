package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.transport.CallbackUrlProvider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;

/**
 * The daemon's {@link CallbackUrlProvider}: mints a signed callback URL for an async step —
 *
 * <pre>{@code {baseUrl}/sagas/{sagaId}/steps/{stepName}/complete?token={hmac}&iat={epochSeconds}}
 * </pre>
 *
 * where {@code token} is {@link HmacUtils#hmacSha256Hex} over {@link HmacUtils#callbackSignedData}.
 * The verifying counterpart is {@link CallbackResource}, so the two must agree on the signed-data
 * layout. A fresh {@code iat} (and therefore a fresh token) is minted on every call, so a re-drive
 * re-provisions a valid URL.
 */
public final class HmacCallbackUrlProvider implements CallbackUrlProvider {

  private final String baseUrl;
  private final String secret;
  private final Clock clock;

  /**
   * @param baseUrl the daemon's externally-reachable base URL (no trailing slash)
   * @param secret the HMAC callback secret (must match the verifying route's)
   * @param clock the clock for the {@code iat} issue time
   */
  public HmacCallbackUrlProvider(String baseUrl, String secret, Clock clock) {
    this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null");
    this.secret = Objects.requireNonNull(secret, "secret must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public String callbackUrl(String sagaId, String stepName) {
    long iat = clock.instant().getEpochSecond();
    String token =
        HmacUtils.hmacSha256Hex(
            secret, HmacUtils.callbackSignedData(sagaId, stepName, Long.toString(iat)));
    return baseUrl
        + "/sagas/"
        + encodePathSegment(sagaId)
        + "/steps/"
        + encodePathSegment(stepName)
        + "/complete?token="
        + token
        + "&iat="
        + iat;
  }

  /**
   * Encodes a path segment. {@link URLEncoder} targets query strings (encoding space as {@code +}),
   * so the {@code +} is restored to the path-correct {@code %20} — matching how the receiving route
   * decodes its path parameters.
   */
  private static String encodePathSegment(String segment) {
    return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
