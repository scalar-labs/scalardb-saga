package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.StepExecutionException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The response of a {@link SagaHttpClient} call: status code, headers, and body, with typed
 * accessors. Header lookups are case-insensitive (per the HTTP spec).
 *
 * <p>Obtained from {@link SagaHttpClient.Request#send()} (only ever 2xx — non-2xx throws) or {@link
 * SagaHttpClient.Request#sendRaw()} (any status). The body is read once (subject to the endpoint's
 * configured size limit) and can be viewed as a JSON object ({@link #bodyJsonObject()}), a JSON
 * array ({@link #bodyJsonArray()}), a typed value ({@link #bodyJson(Class)}), charset-decoded text
 * ({@link #bodyString()}), or raw bytes ({@link #bodyBytes()}).
 *
 * <p>Implementations are immutable and safe to read from multiple threads.
 */
public interface SagaHttpResponse {

  /**
   * The HTTP status code.
   *
   * @return the status code, for example {@code 200}
   */
  int status();

  /**
   * The response headers, keyed by header name (case-insensitive) to the list of values for that
   * name.
   *
   * @return every response header with all of its values
   */
  Map<String, List<String>> headers();

  /**
   * The first value of the named response header (case-insensitive), if present.
   *
   * @param name the header name, matched case-insensitively
   * @return the first value, or empty if the response carries no such header
   */
  Optional<String> header(String name);

  /**
   * Decodes the body as a JSON object. An empty body yields an empty map.
   *
   * @return the object's members, keyed by name
   * @throws StepExecutionException (non-retryable) if the body is not a JSON object
   */
  Map<String, Object> bodyJsonObject() throws StepExecutionException;

  /**
   * Decodes the body as a JSON array. An empty body yields an empty list.
   *
   * @return the array's elements
   * @throws StepExecutionException (non-retryable) if the body is not a JSON array
   */
  List<Object> bodyJsonArray() throws StepExecutionException;

  /**
   * Decodes the body as a value of the given type using the framework's hardened JSON mapper.
   *
   * @param <T> the type to decode into
   * @param type the class of {@code T}
   * @return the decoded value
   * @throws StepExecutionException (non-retryable) if the body cannot be decoded as {@code type}
   */
  <T> T bodyJson(Class<T> type) throws StepExecutionException;

  /**
   * The body decoded as text using the charset from the response {@code Content-Type} (defaulting
   * to UTF-8). Prefer this over {@code new String(bodyBytes())}, which uses the platform default
   * charset.
   *
   * @return the body as text
   */
  String bodyString();

  /**
   * The raw response body bytes.
   *
   * @return the body exactly as received
   */
  byte[] bodyBytes();
}
