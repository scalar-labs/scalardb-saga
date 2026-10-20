package com.scalar.db.saga.server.api;

import com.scalar.db.saga.transport.CallbackUrlProvider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;

/**
 * The daemon's {@link CallbackUrlProvider}: mints the signed URLs an async step's participant calls
 * back on, to complete the step or to report that it failed —
 *
 * <pre>{@code {baseUrl}/sagas/{sagaId}/steps/{stepName}/complete?token={hmac}&iat={epochSeconds}}
 * {@code {baseUrl}/sagas/{sagaId}/steps/{stepName}/fail?token={hmac}&iat={epochSeconds}}
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
   * Creates a provider that signs with {@code secret} and stamps {@code iat} from {@code clock}.
   *
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
    return signedUrl(sagaId, stepName, "complete");
  }

  @Override
  public String failureUrl(String sagaId, String stepName) {
    return signedUrl(sagaId, stepName, "fail");
  }

  /**
   * A signed URL for {@code action} on the step. The token covers the saga, the step and the issue
   * time, not the action, so either route accepts it; holding the step's URL already grants both.
   */
  private String signedUrl(String sagaId, String stepName, String action) {
    long iat = clock.instant().getEpochSecond();
    String token =
        HmacUtils.hmacSha256Hex(
            secret, HmacUtils.callbackSignedData(sagaId, stepName, Long.toString(iat)));
    return baseUrl
        + "/sagas/"
        + encodePathSegment(sagaId)
        + "/steps/"
        + encodePathSegment(stepName)
        + "/"
        + action
        + "?token="
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
