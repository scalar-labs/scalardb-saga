package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;

/**
 * A fluent HTTP client injected into a code {@link Step}/{@link TccStep} for calling a remote
 * participant, built on the framework's shared HTTP engine for one {@code httpEndpoint(name,
 * baseUrl)}. It propagates the saga correlation headers ({@code X-Saga-Id}/{@code X-Saga-Step}),
 * enforces the endpoint's outbound policy (SSRF allowlist + body limits + no redirects), and
 * classifies the response status into the engine's retryable/non-retryable contract.
 *
 * <p>Inject it by declaring a constructor parameter of this type annotated with {@link Named}:
 *
 * <pre>{@code
 * public final class DebitStep implements Step {
 *   private final SagaHttpClient http;
 *
 *   public DebitStep(@Named("account-svc") SagaHttpClient http) {
 *     this.http = http;
 *   }
 *
 *   @Override
 *   public StepResult execute(SagaContext context) throws StepExecutionException {
 *     // Collections.singletonMap keeps this snippet compilable on Java 8; on Java 9+ prefer
 *     // Map.of("amount", 2000) — and Map.of(k1, v1, k2, v2, ...) for multiple fields.
 *     Map<String, Object> body =
 *         http.post("/debit")
 *             .jsonBody(Collections.singletonMap("amount", 2000))
 *             .send()
 *             .bodyJsonObject();
 *     return StepResult.of("debitId", body.get("debit_id"));
 *   }
 * }
 * }</pre>
 *
 * <p><b>Thread-safety:</b> a {@code SagaHttpClient} is an application-level singleton shared across
 * concurrent saga executions (the same contract as a {@link Step}); it must be safe to use from
 * multiple threads. Each {@link Request} returned from a verb method is single-use and is not
 * shared.
 *
 * <p><b>Correlation scope:</b> the {@code X-Saga-Id}/{@code X-Saga-Step} headers are bound to the
 * thread the engine runs the step on. Call this client directly on that thread; a call issued from
 * a thread the step spawns itself (e.g. its own executor) falls back to empty correlation rather
 * than failing.
 */
public interface SagaHttpClient {

  /** Begins a GET request to {@code path} (resolved against the endpoint base URL). */
  Request get(String path);

  /** Begins a POST request to {@code path} (resolved against the endpoint base URL). */
  Request post(String path);

  /** Begins a PUT request to {@code path} (resolved against the endpoint base URL). */
  Request put(String path);

  /** Begins a DELETE request to {@code path} (resolved against the endpoint base URL). */
  Request delete(String path);

  /** Begins a PATCH request to {@code path} (resolved against the endpoint base URL). */
  Request patch(String path);

  /** Begins a request with an explicit {@code method} to {@code path}. */
  Request method(HttpMethod method, String path);

  /**
   * A fluent, single-use builder for one HTTP request, obtained from a {@link SagaHttpClient} verb
   * method. Configure the body, headers, and query parameters, then call {@link #send()} (throws on
   * non-2xx) or {@link #sendRaw()} (returns any status) once.
   *
   * <p><b>Body:</b> set at most one of {@link #jsonBody(Object)}, {@link #stringBody(String,
   * String)}, {@link #bytesBody(byte[], String)}, or {@link #formBody(Map)}. Setting more than one,
   * or setting the same body twice, throws {@link IllegalStateException}.
   *
   * <p>Not thread-safe and not reusable: a terminal method may be called only once.
   */
  interface Request {

    /** Adds a request header. Repeatable for multi-valued headers. */
    Request header(String name, String value);

    /** Adds all entries of {@code headers} as request headers. */
    Request headers(Map<String, String> headers);

    /** Adds a query parameter (URL-encoded and appended to the path). Repeatable. */
    Request query(String name, String value);

    /** Adds all entries of {@code params} as query parameters. */
    Request query(Map<String, String> params);

    /** Sets the request body to any JSON value (sent as {@code application/json}). */
    Request jsonBody(Object value);

    /**
     * Sets a string request body with an explicit {@code Content-Type}. The body is encoded using
     * the charset named in {@code contentType} (defaulting to UTF-8 when none is declared, or when
     * the named charset is unknown/unsupported).
     */
    Request stringBody(String body, String contentType);

    /** Sets a raw byte request body with an explicit {@code Content-Type}. */
    Request bytesBody(byte[] body, String contentType);

    /**
     * Sets a URL-encoded form body ({@code application/x-www-form-urlencoded}) from {@code form}'s
     * entries.
     */
    Request formBody(Map<String, String> form);

    /**
     * Sends the request and returns the response on a 2xx status.
     *
     * @return the response (always 2xx)
     * @throws StepExecutionException on a non-2xx response (retryable per the status classifier and
     *     the {@code X-Saga-Retryable} override), a policy violation (non-retryable), or a
     *     transport error / timeout (retryable)
     * @throws IllegalStateException if this request has already been sent
     */
    SagaHttpResponse send() throws StepExecutionException;

    /**
     * Sends the request and returns the response for any status, including non-2xx. The outbound
     * policy (SSRF allowlist, body limits, no redirects) is still enforced; only the
     * throw-on-non-2xx convenience of {@link #send()} is relaxed.
     *
     * @return the response (any status)
     * @throws StepExecutionException on a policy violation (non-retryable) or a transport error /
     *     timeout (retryable); a received non-2xx response does <em>not</em> throw
     * @throws IllegalStateException if this request has already been sent
     */
    SagaHttpResponse sendRaw() throws StepExecutionException;
  }
}
