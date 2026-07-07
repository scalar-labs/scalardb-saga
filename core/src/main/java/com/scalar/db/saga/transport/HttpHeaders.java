package com.scalar.db.saga.transport;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.Nullable;

/** HTTP header-name constants and Content-Type utilities used by the participant protocol. */
final class HttpHeaders {

  static final String SAGA_ID = "X-Saga-Id";
  static final String SAGA_STEP = "X-Saga-Step";
  static final String SAGA_RETRYABLE = "X-Saga-Retryable";
  // The callback URL a participant POSTs to when an async step completes (injected for async
  // steps).
  static final String SAGA_CALLBACK_URL = "X-Saga-Callback-Url";
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
          String match = matchParameter(contentType.substring(paramStart, i), name);
          if (match != null) {
            return match;
          }
        }
        paramStart = i + 1;
      }
    }
    // Unterminated quoted string: the synthetic ';' was swallowed by the still-open quote, so the
    // tail was never flushed. The quote is known malformed, so re-split the tail on ';' literally
    // to recover a valid parameter (e.g. charset) that follows an unterminated earlier parameter.
    if (inQuotes && paramStart >= 0) {
      for (int segStart = paramStart, i = paramStart; i <= contentType.length(); i++) {
        if (i == contentType.length() || contentType.charAt(i) == ';') {
          String match = matchParameter(contentType.substring(segStart, i), name);
          if (match != null) {
            return match;
          }
          segStart = i + 1;
        }
      }
    }
    return null;
  }

  /**
   * Returns the unquoted value of {@code segment} (a single {@code name=value} parameter) when its
   * name matches {@code name} case-insensitively, or null. A surrounding pair of double quotes is
   * stripped; a lone leading quote (from an unterminated quoted string) is also stripped so a
   * malformed value can still be parsed.
   */
  private static @Nullable String matchParameter(String segment, String name) {
    int eq = segment.indexOf('=');
    if (eq < 0 || !segment.substring(0, eq).trim().equalsIgnoreCase(name)) {
      return null;
    }
    String value = segment.substring(eq + 1).trim();
    if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1).trim();
    } else if (value.startsWith("\"")) {
      value = value.substring(1).trim(); // strip an unterminated opening quote
    }
    return value;
  }
}
