package com.scalar.db.saga.invoker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A fluent, single-use builder for one HTTP call, obtained from an {@link HttpCallContext} verb
 * ({@code get}/{@code post}/{@code put}/{@code patch}/{@code delete}). Configure the body, headers,
 * and query parameters, then call {@link #send()} once.
 *
 * <pre>{@code
 * Map<String,Object> body =
 *     http.post("/charge")
 *         .header("Authorization", "Bearer " + token)
 *         .jsonBody(Map.of("amount", 2000))
 *         .send()
 *         .jsonObject();
 * }</pre>
 *
 * <p><b>Body:</b> set at most one of {@link #jsonBody(Object)} (any JSON value — object, array,
 * scalar, or POJO; framework sets {@code Content-Type: application/json}) or {@link
 * #rawBody(byte[], String)} (caller-supplied bytes and {@code Content-Type}). Setting both, or
 * calling either twice, throws {@link IllegalStateException}.
 *
 * <p><b>Headers:</b> the framework manages {@code X-Saga-Id}, {@code X-Saga-Step}, and {@code
 * Content-Type}; attempting to set them via {@link #header(String, String)} throws {@link
 * IllegalArgumentException}.
 *
 * <p>Not thread-safe and not reusable: {@link #send()} may be called only once.
 */
public final class HttpRequestSpec {

  private final HttpExchange exchange;
  private final String httpMethod;
  private final String baseUrl;
  private final String path;
  private final String sagaId;
  private final String stepName;
  private final List<Map.Entry<String, String>> headers = new ArrayList<>();
  private final List<Map.Entry<String, String>> queryParams = new ArrayList<>();
  private @Nullable Object jsonBody;
  private byte @Nullable [] rawBody;
  private @Nullable String rawContentType;
  private boolean bodySet;
  private boolean sent;

  HttpRequestSpec(
      HttpExchange exchange,
      String httpMethod,
      String baseUrl,
      String path,
      String sagaId,
      String stepName) {
    this.exchange = exchange;
    this.httpMethod = httpMethod;
    this.baseUrl = baseUrl;
    this.path = path;
    this.sagaId = sagaId;
    this.stepName = stepName;
  }

  /** Sets the request body to any JSON value (sent as {@code application/json}). */
  public HttpRequestSpec jsonBody(Object value) {
    checkNotSent();
    Objects.requireNonNull(value, "value must not be null");
    checkNoBody();
    this.jsonBody = value;
    this.bodySet = true;
    return this;
  }

  /** Sets a raw request body with an explicit {@code Content-Type}. */
  public HttpRequestSpec rawBody(byte[] bytes, String contentType) {
    checkNotSent();
    Objects.requireNonNull(bytes, "bytes must not be null");
    Objects.requireNonNull(contentType, "contentType must not be null");
    checkNoBody();
    this.rawBody = bytes.clone();
    this.rawContentType = contentType;
    this.bodySet = true;
    return this;
  }

  /**
   * Adds a request header. Repeatable for multi-valued headers. The framework-managed {@code
   * X-Saga-Id}/{@code X-Saga-Step}/{@code Content-Type} headers must not be set here.
   */
  public HttpRequestSpec header(String name, String value) {
    checkNotSent();
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(value, "value must not be null");
    if (isReserved(name)) {
      throw new IllegalArgumentException(
          "header '" + name + "' is managed by the framework and must not be set");
    }
    headers.add(Map.entry(name, value));
    return this;
  }

  /** Adds a query parameter (URL-encoded and appended to the path). Repeatable. */
  public HttpRequestSpec queryParam(String name, String value) {
    checkNotSent();
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(value, "value must not be null");
    queryParams.add(Map.entry(name, value));
    return this;
  }

  /**
   * Sends the request and returns the response.
   *
   * @return the response on a 2xx status
   * @throws HttpCallException on a non-2xx response (carrying the response), a policy violation, or
   *     a transport error
   * @throws IllegalStateException if this request has already been sent
   */
  public HttpCallResponse send() throws HttpCallException {
    checkNotSent();
    sent = true;
    byte[] body = null;
    String contentType = null;
    if (jsonBody != null) {
      body = exchange.encodeJson(jsonBody);
      contentType = HttpHeaders.APPLICATION_JSON;
    } else if (rawBody != null) {
      body = rawBody;
      contentType = rawContentType;
    }
    return exchange.exchange(
        httpMethod, baseUrl, path, queryParams, headers, body, contentType, sagaId, stepName);
  }

  private void checkNotSent() {
    if (sent) {
      throw new IllegalStateException("this request has already been sent; create a new one");
    }
  }

  private void checkNoBody() {
    if (bodySet) {
      throw new IllegalStateException("a request body has already been set");
    }
  }

  private static boolean isReserved(String name) {
    return name.equalsIgnoreCase(HttpHeaders.SAGA_ID)
        || name.equalsIgnoreCase(HttpHeaders.SAGA_STEP)
        || name.equalsIgnoreCase(HttpHeaders.CONTENT_TYPE);
  }
}
