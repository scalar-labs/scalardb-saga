package com.scalar.db.saga.daemon.api;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * HMAC-SHA256 helpers for the async-callback token: {@link #hmacSha256Hex} computes the signature,
 * {@link #verify} checks a presented one in constant time.
 *
 * <p>The callback token binds a parked step to its issuance — the HMAC over {@link
 * #callbackSignedData} — so only a party holding the coordinator's callback secret can complete the
 * step. This is the first crypto in the repo; it deliberately uses only the JDK's JCE (no
 * third-party dependency).
 */
final class HmacUtils {

  private static final String HMAC_SHA256 = "HmacSHA256";
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private HmacUtils() {}

  /**
   * The canonical data signed by a callback token, binding {@code sagaId}, {@code stepName}, and
   * {@code iat}. The two variable-length fields are length-prefixed ({@code <len>:<value>}) so the
   * {@code :} separators are unambiguous even when a field contains a {@code :} — {@code sagaId} is
   * client-supplied free text and step names are not constrained to exclude {@code :}. Without the
   * prefix, distinct tuples could collide (e.g. {@code (a:b, c)} and {@code (a, b:c)} both yield
   * {@code a:b:c:iat}), letting a token minted for one saga or step validate for another. {@code
   * iat} is trailing so it needs no prefix. Both the minting side ({@link HmacCallbackUrlProvider})
   * and the verifying side ({@link CallbackResource}) must sign the string produced here, so they
   * agree on the layout.
   */
  static String callbackSignedData(String sagaId, String stepName, String iat) {
    return sagaId.length() + ":" + sagaId + ":" + stepName.length() + ":" + stepName + ":" + iat;
  }

  /**
   * Computes the lowercase-hex HMAC-SHA256 of {@code data} keyed by {@code secret} (both UTF-8).
   */
  static String hmacSha256Hex(String secret, String data) {
    try {
      Mac mac = Mac.getInstance(HMAC_SHA256);
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_SHA256));
      return toHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      // HmacSHA256 is a required JCE algorithm and any non-empty byte[] is a valid key, so neither
      // branch is reachable at runtime.
      throw new IllegalStateException("Failed to compute HMAC-SHA256", e);
    }
  }

  /**
   * Returns whether {@code providedHex} equals the HMAC-SHA256 of {@code data} keyed by {@code
   * secret}, compared with constant-time {@link MessageDigest#isEqual} so response timing does not
   * leak the expected signature.
   */
  static boolean verify(String secret, String data, String providedHex) {
    byte[] expected = hmacSha256Hex(secret, data).getBytes(StandardCharsets.UTF_8);
    byte[] provided = providedHex.getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(expected, provided);
  }

  private static String toHex(byte[] bytes) {
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int v = bytes[i] & 0xFF;
      out[i * 2] = HEX[v >>> 4];
      out[i * 2 + 1] = HEX[v & 0x0F];
    }
    return new String(out);
  }
}
