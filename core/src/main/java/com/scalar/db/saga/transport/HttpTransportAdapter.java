package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.CallSpec;
import com.scalar.db.saga.api.HttpCall;
import com.scalar.db.saga.api.SagaContext;
import java.nio.charset.StandardCharsets;
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

  private final String baseUrl;
  private final HttpExchange exchange;

  HttpTransportAdapter(String baseUrl, HttpExchange exchange) {
    this.baseUrl = baseUrl;
    this.exchange = exchange;
  }

  /** The shared {@link HttpExchange} this adapter rides (package-private; for identity tests). */
  HttpExchange exchange() {
    return exchange;
  }

  @Override
  public Map<String, Object> call(CallSpec spec, SagaContext context, String stepName)
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
        // default).
        body =
            DeclarativeExpressions.resolveString(http.getStringBody(), context)
                .getBytes(StandardCharsets.UTF_8);
        contentType =
            overrideContentType != null ? overrideContentType : HttpHeaders.APPLICATION_JSON;
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
              List.of(),
              body,
              contentType,
              context.getSagaId(),
              stepName,
              remaining);
    } catch (HttpCallException e) {
      throw toTransportException(e);
    }

    return DeclarativeExpressions.extractOutput(http.getOutput(), response);
  }

  private static TransportException toTransportException(HttpCallException e) {
    return new TransportException("HTTP transport error: " + e.getMessage(), e, e.isRetryable());
  }
}
