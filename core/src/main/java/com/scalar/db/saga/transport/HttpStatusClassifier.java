package com.scalar.db.saga.transport;

/**
 * Classifies HTTP status codes per the participant HTTP protocol: {@code 2xx} success; {@code 408},
 * {@code 429} and all {@code 5xx} retryable; every other {@code 4xx} non-retryable.
 */
final class HttpStatusClassifier {

  static boolean isSuccess(int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  static boolean isRetryable(int statusCode) {
    // 408 (Request Timeout) and 429 (Too Many Requests) are the transient exceptions to the
    // "4xx = client error = non-retryable" rule — a retry may succeed once the timeout/rate
    // limit clears.
    if (statusCode == 408 || statusCode == 429) {
      return true;
    }
    // All 5xx are server-side errors, treated as transient.
    return statusCode >= 500 && statusCode < 600;
  }

  private HttpStatusClassifier() {}
}
