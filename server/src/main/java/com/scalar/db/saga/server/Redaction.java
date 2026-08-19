package com.scalar.db.saga.server;

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
 */
public final class Redaction {

  private Redaction() {}

  /** Describes {@code value} without echoing it, e.g. {@code (value redacted, 12 chars)}. */
  public static String redacted(String value) {
    return "(value redacted, " + value.length() + " chars)";
  }
}
