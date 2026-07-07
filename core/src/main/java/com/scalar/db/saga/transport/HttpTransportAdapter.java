package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.HttpCall;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The built-in {@link TransportAdapter} for {@link CallSpec.Transport#HTTP}. Resolves an {@link
 * HttpCall}'s {@code ${key}} path/query/jsonBody/stringBody templates against the saga context and
 * performs the call through the shared {@link HttpExchange} (correlation headers, status→retryable
 * classification, SSRF allowlist + body limits, endpoint default headers). The request body is
 * either the flat {@code jsonBody} map serialized as JSON or the raw templated {@code stringBody}
 * string (sent with the call's {@code contentType}, defaulting to {@code application/json}). The
 * response is reduced to the call's {@code output} mapping ({@code $.path} JSON navigation and the
 * {@code $body} raw-body token).
 */
final class HttpTransportAdapter implements TransportAdapter {

  private static final int HTTP_ACCEPTED = 202;

  private final String baseUrl;
  private final HttpExchange exchange;
  private final @Nullable CallbackUrlProvider callbackUrlProvider;

  HttpTransportAdapter(String baseUrl, HttpExchange exchange) {
    this(baseUrl, exchange, null);
  }

  HttpTransportAdapter(
      String baseUrl, HttpExchange exchange, @Nullable CallbackUrlProvider callbackUrlProvider) {
    this.baseUrl = baseUrl;
    this.exchange = exchange;
    this.callbackUrlProvider = callbackUrlProvider;
  }

  /** The shared {@link HttpExchange} this adapter rides (package-private; for identity tests). */
  HttpExchange exchange() {
    return exchange;
  }

  @Override
  public StepResult call(CallSpec spec, SagaContext context, String stepName)
      throws TransportException {
    if (!(spec instanceof HttpCall http)) {
      throw new TransportException(
          "HttpTransportAdapter cannot handle transport " + spec.transport(), false);
    }

    String path = DeclarativeExpressions.resolvePath(http.getPath(), context);
    List<Map.Entry<String, String>> query = new ArrayList<>();
    DeclarativeExpressions.resolveStringMap(http.getQuery(), context)
        .forEach((name, value) -> query.add(Map.entry(name, value)));

    byte[] body = null;
    String contentType = null;
    if (http.getMethod().hasBody()
        && (!http.getJsonBody().isEmpty() || http.getStringBody() != null)) {
      String overrideContentType = http.getContentType();
      if (http.getStringBody() != null) {
        // Raw/templated string body: sent verbatim with the override content type (or JSON
        // default), encoded with that content type's charset.
        contentType =
            overrideContentType != null ? overrideContentType : HttpHeaders.APPLICATION_JSON;
        body =
            DeclarativeExpressions.resolveString(http.getStringBody(), context)
                .getBytes(HttpHeaders.charsetOf(contentType));
      } else {
        // Flat-map body serialized as JSON; the content type defaults to application/json but a
        // declared override (e.g. a JSON variant content type) takes precedence.
        Map<String, Object> requestBody =
            DeclarativeExpressions.resolveObjectMap(http.getJsonBody(), context);
        try {
          body = exchange.encodeJson(requestBody);
        } catch (HttpCallException e) {
          throw toTransportException(e);
        }
        contentType =
            overrideContentType != null ? overrideContentType : HttpHeaders.APPLICATION_JSON;
      }
    }

    // For an async step, hand the participant a callback URL so it can complete the step later. The
    // provider is null in embedded mode / when async completion is not configured (such a step is
    // rejected at registration, so a null here means a non-async step).
    List<Map.Entry<String, String>> headers = new ArrayList<>();
    if (http.isAsync() && callbackUrlProvider != null) {
      String callbackUrl = callbackUrlProvider.callbackUrl(context.getSagaId(), stepName);
      if (callbackUrl != null) {
        headers.add(Map.entry(HttpHeaders.SAGA_CALLBACK_URL, callbackUrl));
      }
    }

    // Bound this call to the step's remaining deadline (bound by the engine on this thread); a null
    // remaining (no deadline bound) falls back to the exchange's default per-request timeout.
    @Nullable Duration remaining = SagaCorrelationContext.remaining();
    HttpCallResponse response;
    try {
      response =
          exchange.exchange(
              http.getMethod().name(),
              baseUrl,
              path,
              query,
              headers,
              body,
              contentType,
              context.getSagaId(),
              stepName,
              remaining);
    } catch (HttpCallException e) {
      throw toTransportException(e);
    }

    // A 202 Accepted signals async acceptance: an async step parks (pending) and resumes on the
    // callback. A 202 for a non-async step is a contract violation (the participant went async for
    // a
    // step not declared async) — a non-retryable error rather than a silent (empty-output) success.
    if (response.status() == HTTP_ACCEPTED) {
      if (http.isAsync()) {
        return StepResult.pending();
      }
      throw new TransportException(
          "participant returned 202 Accepted for non-async step '" + stepName + "'", false);
    }
    return StepResult.of(DeclarativeExpressions.extractOutput(http.getOutput(), response));
  }

  private static TransportException toTransportException(HttpCallException e) {
    return new TransportException(
        "HTTP transport error: " + e.getMessage(), e, e.isRetryable(), e.knownNotCommitted());
  }
}
