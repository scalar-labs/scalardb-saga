package com.scalar.db.saga.invoker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * The shared HTTP machinery behind {@code HttpServiceInvoker} (and, in a later change, the
 * declarative {@code HttpTransportAdapter}): JSON encoding, correlation-header propagation,
 * status→retryable classification (including the {@code X-Saga-Retryable} override), and {@link
 * OutboundHttpPolicy} (SSRF allowlist + body limits). Keeping it in one place means both front-ends
 * behave identically. Response decoding lives in {@link HttpCallResponse}.
 */
final class HttpExchange {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final HttpClient client;
  private final ObjectMapper mapper;
  private final OutboundHttpPolicy policy;
  private final Duration timeout;

  HttpExchange(HttpClient client, OutboundHttpPolicy policy) {
    this(client, hardenedMapper(), policy, DEFAULT_TIMEOUT);
  }

  HttpExchange(
      HttpClient client, ObjectMapper mapper, OutboundHttpPolicy policy, Duration timeout) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
  }

  /** Encodes {@code value} as a JSON body using the hardened mapper. */
  byte[] encodeJson(Object value) throws HttpCallException {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new HttpCallException("Failed to encode request body", e, false);
    }
  }

  /**
   * Sends an HTTP request to {@code baseUrl}{@code /}{@code path} (with {@code queryParams}
   * appended), propagating the saga correlation headers plus the caller's {@code headers}, and
   * returns the response.
   *
   * <p>When {@code body} is non-null it is sent with the given {@code contentType}; when null no
   * body is sent.
   *
   * @return the response on a 2xx status
   * @throws HttpCallException on a non-2xx response (carrying the {@link
   *     HttpCallException#response() response}), an oversized body, a disallowed host, a malformed
   *     URI/header, or a transport error
   */
  HttpCallResponse exchange(
      String httpMethod,
      String baseUrl,
      String path,
      List<Map.Entry<String, String>> queryParams,
      List<Map.Entry<String, String>> headers,
      byte @Nullable [] body,
      @Nullable String contentType,
      String sagaId,
      String stepName)
      throws HttpCallException {
    URI uri = buildUri(baseUrl, path, queryParams);
    if (!policy.isAllowed(uri)) {
      throw new HttpCallException("Host not allowed by policy: " + uri.getHost(), false);
    }

    if (body != null && body.length > policy.maxBodyBytes()) {
      throw new HttpCallException(
          "Request body exceeds limit (" + body.length + " > " + policy.maxBodyBytes() + ")",
          false);
    }

    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri).timeout(timeout);
    try {
      requestBuilder.header(HttpHeaders.SAGA_ID, sagaId).header(HttpHeaders.SAGA_STEP, stepName);
      for (Map.Entry<String, String> header : headers) {
        requestBuilder.header(header.getKey(), header.getValue());
      }
      if (body != null && contentType != null) {
        requestBuilder.header(HttpHeaders.CONTENT_TYPE, contentType);
      }
    } catch (IllegalArgumentException e) {
      // A control character in a header value (a saga correlation header or a caller-supplied one)
      // makes the JDK reject it. Surface it as non-retryable, like a malformed URI, rather than
      // letting a raw IllegalArgumentException escape.
      throw new HttpCallException("Malformed HTTP header for " + uri, e, false);
    }

    if (body != null && contentType != null) {
      requestBuilder.method(httpMethod, HttpRequest.BodyPublishers.ofByteArray(body));
    } else {
      requestBuilder.method(httpMethod, HttpRequest.BodyPublishers.noBody());
    }

    HttpRequest httpRequest = requestBuilder.build();

    HttpResponse<InputStream> response;
    try {
      response = client.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
    } catch (IOException e) {
      throw new HttpCallException("HTTP call failed: " + uri, e, true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HttpCallException("HTTP call interrupted: " + uri, e, true);
    }

    long maxBodyBytes = policy.maxBodyBytes();
    // Fail fast when an honest server declares an oversized body up front.
    if (response.headers().firstValueAsLong(HttpHeaders.CONTENT_LENGTH).orElse(-1L)
        > maxBodyBytes) {
      throw new HttpCallException("Response body exceeds limit (> " + maxBodyBytes + ")", false);
    }

    // Read at most maxBodyBytes + 1 bytes: enough to detect an oversized (or chunked/undeclared)
    // body and reject it WITHOUT ever buffering the whole stream. Closing the stream early
    // (try-with-resources) cancels the remaining download.
    int readCap = (int) Math.min(maxBodyBytes + 1, Integer.MAX_VALUE);
    byte[] responseBody;
    try (InputStream bodyStream = response.body()) {
      responseBody = bodyStream.readNBytes(readCap);
    } catch (IOException e) {
      throw new HttpCallException("Failed to read response body: " + uri, e, true);
    }
    if (responseBody.length > maxBodyBytes) {
      throw new HttpCallException("Response body exceeds limit (> " + maxBodyBytes + ")", false);
    }

    int status = response.statusCode();
    HttpCallResponse callResponse =
        new HttpCallResponse(status, response.headers().map(), responseBody, mapper);
    if (HttpStatusClassifier.isSuccess(status)) {
      return callResponse;
    }
    throw new HttpCallException(
        "HTTP " + status + " from " + uri, classifyFailure(response), callResponse);
  }

  private static boolean classifyFailure(HttpResponse<?> response) {
    Optional<String> override = response.headers().firstValue(HttpHeaders.SAGA_RETRYABLE);
    if (override.isPresent()) {
      String value = override.get().trim();
      if (value.equalsIgnoreCase("true")) {
        return true;
      }
      if (value.equalsIgnoreCase("false")) {
        return false;
      }
    }
    return HttpStatusClassifier.isRetryable(response.statusCode());
  }

  private static URI buildUri(
      String baseUrl, String path, List<Map.Entry<String, String>> queryParams)
      throws HttpCallException {
    String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    String suffix = path.startsWith("/") ? path : "/" + path;
    StringBuilder url = new StringBuilder(base).append(suffix);
    if (!queryParams.isEmpty()) {
      char separator = suffix.indexOf('?') >= 0 ? '&' : '?';
      for (Map.Entry<String, String> param : queryParams) {
        url.append(separator)
            .append(URLEncoder.encode(param.getKey(), StandardCharsets.UTF_8))
            .append('=')
            .append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8));
        separator = '&';
      }
    }
    try {
      return URI.create(url.toString());
    } catch (IllegalArgumentException e) {
      // A malformed base URL or path (illegal URI characters) is a definition error, not a
      // transient failure — surface it as a non-retryable HttpCallException rather than letting
      // the raw IllegalArgumentException escape as a generic "Unexpected error" in the engine.
      throw new HttpCallException("Malformed request URI: " + url, e, false);
    }
  }

  private static ObjectMapper hardenedMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    // Defense in depth against polymorphic-deserialization gadgets (off by default in Jackson 2.x).
    objectMapper.deactivateDefaultTyping();
    return objectMapper;
  }
}
