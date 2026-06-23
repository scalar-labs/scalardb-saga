package com.scalar.db.saga.api;

/**
 * An HTTP verb for a {@link SagaHttpClient} request. {@link #GET} and {@link #DELETE} carry no
 * request body (parameters go in the path/query); {@link #POST}, {@link #PUT}, and {@link #PATCH}
 * carry a request body.
 */
public enum HttpMethod {
  GET,
  POST,
  PUT,
  PATCH,
  DELETE;

  /** Whether this verb sends a request body. {@code false} for {@link #GET} and {@link #DELETE}. */
  public boolean hasBody() {
    return this == POST || this == PUT || this == PATCH;
  }
}
