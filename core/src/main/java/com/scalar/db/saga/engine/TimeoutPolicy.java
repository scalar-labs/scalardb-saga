package com.scalar.db.saga.engine;

/**
 * Utility for per-step and per-saga deadline calculation.
 *
 * <p>All methods accept {@code nowMillis} (current time in milliseconds since epoch) to keep the
 * logic pure and testable. The caller obtains the current time via {@link
 * System#currentTimeMillis()}.
 *
 * <p>A deadline value of {@code <= 0} means "no timeout".
 */
final class TimeoutPolicy {

  private TimeoutPolicy() {}

  /**
   * Computes the saga-level deadline.
   *
   * @param sagaTimeoutMillis saga timeout in milliseconds ({@code <= 0} means no timeout)
   * @param nowMillis current time in milliseconds since epoch
   * @return the absolute deadline, or {@code 0} if no saga timeout is configured
   */
  static long calculateSagaDeadline(long sagaTimeoutMillis, long nowMillis) {
    return sagaTimeoutMillis > 0 ? nowMillis + sagaTimeoutMillis : 0;
  }

  /**
   * Computes the per-step deadline as the minimum of the step timeout and the saga deadline.
   *
   * @param stepTimeoutMillis step timeout in milliseconds ({@code <= 0} means no step-level
   *     timeout)
   * @param sagaDeadline the saga-level deadline ({@code <= 0} means no saga timeout)
   * @param nowMillis current time in milliseconds since epoch
   * @return the absolute step deadline, or {@code 0} if neither timeout is configured
   */
  static long calculateStepDeadline(long stepTimeoutMillis, long sagaDeadline, long nowMillis) {
    if (stepTimeoutMillis > 0 && sagaDeadline > 0) {
      return Math.min(nowMillis + stepTimeoutMillis, sagaDeadline);
    } else if (stepTimeoutMillis > 0) {
      return nowMillis + stepTimeoutMillis;
    } else {
      return sagaDeadline; // <= 0 means no timeout
    }
  }

  /**
   * Checks whether the saga has exceeded its deadline.
   *
   * @param sagaDeadline the saga-level deadline ({@code <= 0} means no saga timeout)
   * @param nowMillis current time in milliseconds since epoch
   * @return {@code true} if the deadline is set and the current time has exceeded it
   */
  static boolean isSagaTimedOut(long sagaDeadline, long nowMillis) {
    return sagaDeadline > 0 && nowMillis > sagaDeadline;
  }
}
