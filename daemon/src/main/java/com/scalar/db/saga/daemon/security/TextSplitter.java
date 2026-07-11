package com.scalar.db.saga.daemon.security;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * A small string-splitting helper shared by the security config and providers. Splits on a
 * delimiter predicate rather than {@code String.split}, avoiding that method's trailing-empty-token
 * behavior (and the Error Prone {@code StringSplitter} warning).
 */
final class TextSplitter {

  private TextSplitter() {}

  /**
   * Splits {@code value} on every character matching {@code isDelimiter}, optionally {@code
   * trim}ming each token, and always dropping empty tokens.
   *
   * @param value the string to split
   * @param isDelimiter matches a delimiter character (e.g. {@code Character::isWhitespace} or
   *     {@code c -> c == ','})
   * @param trim whether to trim each token before the empty check
   * @return the non-empty tokens, in order
   */
  static List<String> split(String value, IntPredicate isDelimiter, boolean trim) {
    List<String> tokens = new ArrayList<>();
    int start = 0;
    for (int i = 0; i <= value.length(); i++) {
      if (i == value.length() || isDelimiter.test(value.charAt(i))) {
        String token = value.substring(start, i);
        if (trim) {
          token = token.trim();
        }
        if (!token.isEmpty()) {
          tokens.add(token);
        }
        start = i + 1;
      }
    }
    return tokens;
  }
}
