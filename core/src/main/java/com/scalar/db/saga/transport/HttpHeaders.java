package com.scalar.db.saga.transport;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
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
   * Returns the charset named in a {@code Content-Type} value (e.g. {@code charset=ISO-8859-1}),
   * defaulting to {@link StandardCharsets#UTF_8} when the content type is null, declares no
   * charset, or names an unknown/malformed one. Used to encode a string request body and decode a
   * response body consistently with the declared header.
   */
  static Charset charsetOf(@Nullable String contentType) {
    if (contentType != null) {
      String value = contentType.toLowerCase(Locale.ROOT);
      int start = value.indexOf("charset=");
      if (start >= 0) {
        String name = value.substring(start + "charset=".length()).trim();
        int end = name.indexOf(';'); // strip any trailing parameters
        if (end >= 0) {
          name = name.substring(0, end).trim();
        }
        name = name.replace("\"", ""); // strip optional quotes
        try {
          if (!name.isEmpty()) {
            return Charset.forName(name);
          }
        } catch (IllegalArgumentException e) {
          // forName throws IllegalCharsetNameException (malformed) or UnsupportedCharsetException
          // (unknown) — both IllegalArgumentException; fall through to the UTF-8 default below.
        }
      }
    }
    return StandardCharsets.UTF_8;
  }
}
