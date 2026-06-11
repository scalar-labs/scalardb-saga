package com.scalar.db.saga.invoker;

/** HTTP header names and constants used by the participant protocol. */
final class HttpHeaders {

  static final String SAGA_ID = "X-Saga-Id";
  static final String SAGA_STEP = "X-Saga-Step";
  static final String SAGA_RETRYABLE = "X-Saga-Retryable";
  static final String CONTENT_TYPE = "Content-Type";
  static final String CONTENT_LENGTH = "Content-Length";
  static final String APPLICATION_JSON = "application/json";

  private HttpHeaders() {}
}
