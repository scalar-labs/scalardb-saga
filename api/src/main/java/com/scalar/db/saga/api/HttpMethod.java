package com.scalar.db.saga.api;

/**
 * The HTTP verb a declarative {@code HttpCall} uses. {@link #GET} and {@link #DELETE} carry no
 * request body (parameters go in the path/query); {@link #POST}, {@link #PUT}, and {@link #PATCH}
 * carry a request body — either a JSON object or a raw string with an explicit content type (see
 * {@code HttpCall}).
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
