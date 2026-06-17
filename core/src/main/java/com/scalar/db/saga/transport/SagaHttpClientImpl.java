package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.SagaHttpResponse;
import com.scalar.db.saga.exception.StepExecutionException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.jspecify.annotations.Nullable;

/**
 * Internal {@link SagaHttpClient} for one {@code httpEndpoint(name, baseUrl)}, backed by the shared
 * {@link HttpExchange} (one per endpoint, shared with the declarative transport adapter). It is an
 * application singleton: thread-safe and reused across concurrent saga executions. Per-call saga
 * correlation ({@code X-Saga-Id}/{@code X-Saga-Step}) is read from {@link SagaCorrelationContext},
 * which the engine binds on the step-execution thread.
 *
 * <p>{@link Request#send()} maps a non-2xx {@link HttpCallException} to a {@link
 * StepExecutionException} preserving its {@code retryable} flag (so a code step's propagated
 * non-2xx is retried/compensated exactly like a declarative step). {@link Request#sendRaw()}
 * returns a received non-2xx response instead, but still surfaces transport/policy failures as
 * exceptions.
 */
final class SagaHttpClientImpl implements SagaHttpClient {

  private final HttpExchange exchange;
  private final String baseUrl;

  SagaHttpClientImpl(HttpExchange exchange, String baseUrl) {
    this.exchange = exchange;
    this.baseUrl = baseUrl;
  }

  /** The shared {@link HttpExchange} this client rides (package-private; for identity tests). */
  HttpExchange exchange() {
    return exchange;
  }

  @Override
  public Request get(String path) {
    return method(HttpMethod.GET, path);
  }

  @Override
  public Request post(String path) {
    return method(HttpMethod.POST, path);
  }

  @Override
  public Request put(String path) {
    return method(HttpMethod.PUT, path);
  }

  @Override
  public Request delete(String path) {
    return method(HttpMethod.DELETE, path);
  }

  @Override
  public Request patch(String path) {
    return method(HttpMethod.PATCH, path);
  }

  @Override
  public Request method(HttpMethod method, String path) {
    return new RequestImpl(method.name(), path);
  }

  private final class RequestImpl implements Request {

    private final String httpMethod;
    private final String path;
    private final List<Map.Entry<String, String>> headers = new ArrayList<>();
    private final List<Map.Entry<String, String>> queryParams = new ArrayList<>();
    private @Nullable Object jsonBody;
    private byte @Nullable [] rawBody;
    private @Nullable String contentType;
    private boolean bodySet;
    private boolean sent;

    private RequestImpl(String httpMethod, String path) {
      this.httpMethod = httpMethod;
      this.path = path;
    }

    @Override
    public Request header(String name, String value) {
      checkNotSent();
      headers.add(Map.entry(name, value));
      return this;
    }

    @Override
    public Request headers(Map<String, String> headers) {
      checkNotSent();
      headers.forEach((name, value) -> this.headers.add(Map.entry(name, value)));
      return this;
    }

    @Override
    public Request query(String name, String value) {
      checkNotSent();
      queryParams.add(Map.entry(name, value));
      return this;
    }

    @Override
    public Request query(Map<String, String> params) {
      checkNotSent();
      params.forEach((name, value) -> queryParams.add(Map.entry(name, value)));
      return this;
    }

    @Override
    public Request jsonBody(Object value) {
      checkNotSent();
      checkNoBody();
      this.jsonBody = value;
      this.bodySet = true;
      return this;
    }

    @Override
    public Request stringBody(String body, String contentType) {
      checkNotSent();
      checkNoBody();
      this.rawBody = body.getBytes(StandardCharsets.UTF_8);
      this.contentType = contentType;
      this.bodySet = true;
      return this;
    }

    @Override
    public Request bytesBody(byte[] body, String contentType) {
      checkNotSent();
      checkNoBody();
      this.rawBody = body.clone();
      this.contentType = contentType;
      this.bodySet = true;
      return this;
    }

    @Override
    public Request formBody(Map<String, String> form) {
      checkNotSent();
      checkNoBody();
      StringJoiner joiner = new StringJoiner("&");
      form.forEach(
          (name, value) ->
              joiner.add(
                  URLEncoder.encode(name, StandardCharsets.UTF_8)
                      + "="
                      + URLEncoder.encode(value, StandardCharsets.UTF_8)));
      this.rawBody = joiner.toString().getBytes(StandardCharsets.UTF_8);
      this.contentType = "application/x-www-form-urlencoded";
      this.bodySet = true;
      return this;
    }

    @Override
    public SagaHttpResponse send() throws StepExecutionException {
      // send() lets the non-2xx classification from the exchange propagate as a retryable-tagged
      // StepExecutionException.
      return new SagaHttpResponseImpl(exchangeOrThrow());
    }

    @Override
    public SagaHttpResponse sendRaw() throws StepExecutionException {
      // sendRaw() returns a received non-2xx response; only transport/policy failures (no response)
      // still throw. The exchange enforces SSRF allowlist, body limits, and Redirect.NEVER either
      // way — sendRaw() relaxes only the 2xx-throw convenience.
      try {
        return new SagaHttpResponseImpl(exchangeOrThrow());
      } catch (StepExecutionException e) {
        if (e.getCause() instanceof HttpCallException hce && hce.response().isPresent()) {
          return new SagaHttpResponseImpl(hce.response().orElseThrow());
        }
        throw e;
      }
    }

    private HttpCallResponse exchangeOrThrow() throws StepExecutionException {
      checkNotSent();
      sent = true;
      byte[] body = null;
      String resolvedContentType = null;
      try {
        if (jsonBody != null) {
          body = exchange.encodeJson(jsonBody);
          resolvedContentType = HttpHeaders.APPLICATION_JSON;
        } else if (rawBody != null) {
          body = rawBody;
          resolvedContentType = contentType;
        }
        SagaCorrelationContext.Correlation correlation = SagaCorrelationContext.current();
        // Bound this call to the step's remaining deadline (bound by the engine on this thread); a
        // null remaining (no deadline bound) falls back to the exchange's default per-request
        // timeout.
        @Nullable Duration remaining = SagaCorrelationContext.remaining();
        return exchange.exchange(
            httpMethod,
            baseUrl,
            path,
            queryParams,
            headers,
            body,
            resolvedContentType,
            correlation.sagaId(),
            correlation.stepName(),
            remaining);
      } catch (HttpCallException e) {
        throw new StepExecutionException(e, e.isRetryable());
      }
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
  }
}
