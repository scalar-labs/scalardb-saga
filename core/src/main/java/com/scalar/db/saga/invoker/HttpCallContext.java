package com.scalar.db.saga.invoker;

/**
 * Handed to {@link HttpServiceInvoker} operation lambdas to make an HTTP call. Each verb returns a
 * fluent {@link HttpRequestSpec} on which you set the body, headers, and query parameters before
 * calling {@link HttpRequestSpec#send()}. The framework adds the {@code X-Saga-Id} and {@code
 * X-Saga-Step} correlation headers (and {@code Content-Type} for a JSON body), enforces the
 * outbound policy (SSRF allowlist + body limits), and classifies the response status.
 *
 * <pre>{@code
 * Map<String,Object> result = http.post("/debit").jsonBody(Map.of("accountId", id)).send().jsonObject();
 * }</pre>
 */
public final class HttpCallContext {

  private final HttpExchange exchange;
  private final String baseUrl;
  private final String sagaId;
  private final String stepName;

  HttpCallContext(HttpExchange exchange, String baseUrl, String sagaId, String stepName) {
    this.exchange = exchange;
    this.baseUrl = baseUrl;
    this.sagaId = sagaId;
    this.stepName = stepName;
  }

  /** Begins a GET request to {@code path} (resolved against the invoker's base URL). */
  public HttpRequestSpec get(String path) {
    return request("GET", path);
  }

  /** Begins a POST request to {@code path} (resolved against the invoker's base URL). */
  public HttpRequestSpec post(String path) {
    return request("POST", path);
  }

  /** Begins a PUT request to {@code path} (resolved against the invoker's base URL). */
  public HttpRequestSpec put(String path) {
    return request("PUT", path);
  }

  /** Begins a PATCH request to {@code path} (resolved against the invoker's base URL). */
  public HttpRequestSpec patch(String path) {
    return request("PATCH", path);
  }

  /** Begins a DELETE request to {@code path} (resolved against the invoker's base URL). */
  public HttpRequestSpec delete(String path) {
    return request("DELETE", path);
  }

  private HttpRequestSpec request(String httpMethod, String path) {
    return new HttpRequestSpec(exchange, httpMethod, baseUrl, path, sagaId, stepName);
  }
}
