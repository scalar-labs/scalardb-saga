package com.scalar.db.saga.designer;

/**
 * A request the designer cannot act on at all: a malformed body, an unknown file, a file name it
 * refuses. Answered with {@code 400}, or the status given. Problems with the definition itself are
 * never this: they are data, returned as {@link Problem}s.
 */
final class BadRequestException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final int status;
  private final String reason;

  BadRequestException(String message) {
    this(400, message);
  }

  BadRequestException(int status, String message) {
    super(message);
    this.status = status;
    this.reason = message;
  }

  /** The message, never null, for the response body. */
  String reason() {
    return reason;
  }

  int status() {
    return status;
  }
}
