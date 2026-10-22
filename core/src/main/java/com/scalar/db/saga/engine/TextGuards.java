package com.scalar.db.saga.engine;

/** Shaping applied to free text from outside before the engine records or shows it. */
final class TextGuards {

  private TextGuards() {}

  /**
   * Replaces ISO control characters (newlines included) with spaces; log-forging defense. Spaces
   * rather than removal so that multi-line text keeps its word boundaries once flattened. Applied
   * to an operator's reason and to a participant's reported failure.
   */
  static String oneLine(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      sb.append(Character.isISOControl(c) ? ' ' : c);
    }
    return sb.toString();
  }

  /**
   * The first {@code max} chars of {@code value}, or all of it when it fits. The cut backs off one
   * char rather than splitting a surrogate pair, which would leave an unencodable lone surrogate at
   * the boundary.
   */
  static String cut(String value, int max) {
    if (value.length() <= max) {
      return value;
    }
    int end = Character.isHighSurrogate(value.charAt(max - 1)) ? max - 1 : max;
    return value.substring(0, end);
  }
}
