package com.scalar.db.saga.transport;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.Nullable;

/** HTTP header names and constants used by the participant protocol. */
final class HttpHeaders {

  static final String SAGA_ID = "X-Saga-Id";
  static final String SAGA_STEP = "X-Saga-Step";
  static final String SAGA_RETRYABLE = "X-Saga-Retryable";
  static final String CONTENT_TYPE = "Content-Type";
  static final String CONTENT_LENGTH = "Content-Length";
  static final String APPLICATION_JSON = "application/json";

  private HttpHeaders() {}

  /**
   * Returns the charset named by the {@code charset} parameter of a {@code Content-Type} value
   * (e.g. {@code charset=ISO-8859-1}), defaulting to {@link StandardCharsets#UTF_8} when the
   * content type is null, declares no charset, or names an unknown/malformed one. Used to encode a
   * string request body and decode a response body consistently with the declared header.
   */
  static Charset charsetOf(@Nullable String contentType) {
    String charset = contentType == null ? null : parameter(contentType, "charset");
    if (charset != null && !charset.isEmpty()) {
      try {
        return Charset.forName(charset);
      } catch (IllegalArgumentException e) {
        // IllegalCharsetNameException (malformed) / UnsupportedCharsetException (unknown) — both
        // IllegalArgumentException; fall through to the UTF-8 default below.
      }
    }
    return StandardCharsets.UTF_8;
  }

  /**
   * Returns the value of media-type parameter {@code name} (case-insensitive, unquoted), or null.
   * Parameters are split on {@code ';'} outside quoted strings (honoring backslash-escaped quotes)
   * and matched by name, so a stray {@code charset=} inside another parameter's value (e.g. a
   * {@code boundary}) is ignored, and optional whitespace around {@code '='} is tolerated.
   */
  private static @Nullable String parameter(String contentType, String name) {
    boolean inQuotes = false;
    int paramStart = -1; // -1 until the first ';' — skips the media type itself
    for (int i = 0; i <= contentType.length(); i++) {
      char c = i < contentType.length() ? contentType.charAt(i) : ';'; // synthetic ';' flushes last
      if (c == '\\' && inQuotes && i + 1 < contentType.length()) {
        i++; // skip the backslash-escaped char (quoted-pair) so it can't toggle inQuotes
      } else if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ';' && !inQuotes) {
        if (paramStart >= 0) {
          String segment = contentType.substring(paramStart, i);
          int eq = segment.indexOf('=');
          if (eq >= 0 && segment.substring(0, eq).trim().equalsIgnoreCase(name)) {
            String value = segment.substring(eq + 1).trim();
            if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }
            return value;
          }
        }
        paramStart = i + 1;
      }
    }
    return null;
  }
}
