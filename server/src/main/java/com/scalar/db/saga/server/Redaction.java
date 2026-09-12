package com.scalar.db.saga.server;

import java.util.regex.Pattern;

/**
 * Describes a rejected config value without echoing it.
 *
 * <p>Secret references are resolved for every key before parsing, so a reference pasted onto the
 * wrong key arrives at the parsers as the secret's plaintext; echoing it would write the secret to
 * the log, which is readable far more widely than the secret itself.
 *
 * <p>The rule for every parser in this module: a message that rejects a resolved value must name
 * the key, describe the value via {@link #redacted}, and throw without the parse exception as cause
 * — the cause's own message embeds the raw input. Shared by every config parser in the module so
 * the log-format contract cannot drift between them.
 *
 * <p>{@link #oneLine} covers the other direction: strings the parsers quote back (property keys,
 * file names) come from mounted files, and a newline inside one splits the log line it lands in
 * into what reads as a separate record.
 */
public final class Redaction {

  private static final Pattern LINE_BREAK = Pattern.compile("\\s*\\R\\s*");

  private Redaction() {}

  /** Describes {@code value} without echoing it, e.g. {@code (value redacted, 12 chars)}. */
  public static String redacted(String value) {
    return "(value redacted, " + value.length() + " chars)";
  }

  /**
   * Collapses every line break in {@code value} to a single space, so the string occupies exactly
   * one log record.
   *
   * <p>Two callers need it. A message quoting something read from a mounted file could otherwise be
   * forged into what reads as a separate record: {@code Properties.load} performs escape processing
   * on keys, so a key written as {@code a\nb} arrives carrying a real newline. And a wrapped parse
   * error puts its source location on a second line, which would split a startup report in two and
   * leave a log pipeline keying off the first line alone.
   */
  public static String oneLine(String value) {
    return LINE_BREAK.matcher(value.strip()).replaceAll(" ");
  }
}
