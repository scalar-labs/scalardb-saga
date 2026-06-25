package com.scalar.db.saga.transport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodySubscriber;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLHandshakeException;
import org.jspecify.annotations.Nullable;

/**
 * The shared HTTP machinery behind the declarative {@code HttpTransportAdapter} and the code-step
 * {@code SagaHttpClient}: JSON encoding, correlation-header propagation, status→retryable
 * classification (including the {@code X-Saga-Retryable} override), and {@link OutboundHttpPolicy}
 * (SSRF allowlist + body limits). Keeping it in one place means all front-ends behave identically.
 * Response decoding lives in {@link HttpCallResponse}. The per-request timeout may be supplied per
 * call so the single shared, immutable instance can honor each step's remaining deadline.
 *
 * <p>Endpoint default headers (set on the {@code httpEndpoint(...)} sub-builder, never persisted in
 * a definition — the channel for auth/secrets) are applied to every request through this exchange,
 * so both the declarative {@code HttpTransportAdapter} and the code-step {@code SagaHttpClient} get
 * them for free. Precedence per header name: a caller-supplied per-call header overrides a default
 * header of the same name; the framework correlation headers ({@code X-Saga-Id}/{@code
 * X-Saga-Step}) are always set last and win.
 */
final class HttpExchange {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final HttpClient client;
  private final ObjectMapper mapper;
  private final OutboundHttpPolicy policy;
  private final Duration timeout;
  private final Map<String, String> defaultHeaders;

  HttpExchange(HttpClient client, OutboundHttpPolicy policy) {
    this(client, hardenedMapper(), policy, DEFAULT_TIMEOUT, Map.of());
  }

  HttpExchange(HttpClient client, OutboundHttpPolicy policy, Map<String, String> defaultHeaders) {
    this(client, hardenedMapper(), policy, DEFAULT_TIMEOUT, defaultHeaders);
  }

  HttpExchange(
      HttpClient client,
      ObjectMapper mapper,
      OutboundHttpPolicy policy,
      Duration timeout,
      Map<String, String> defaultHeaders) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    this.defaultHeaders = Map.copyOf(defaultHeaders);
  }

  /**
   * The outbound policy (SSRF allowlist + body limit) this exchange enforces; for identity tests.
   */
  OutboundHttpPolicy policy() {
    return policy;
  }

  /** Encodes {@code value} as a JSON body using the hardened mapper. */
  byte[] encodeJson(Object value) throws HttpCallException {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      // Pre-send failure → the request was never sent, so nothing committed.
      throw new HttpCallException("Failed to encode request body", e, false, true);
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
   * <p>{@code requestTimeout} bounds this single call — both the wait for headers (the JDK {@link
   * HttpRequest} timeout) and the response-body read (a hard deadline on the async send's future,
   * since the JDK timeout alone covers only the headers). It is passed per call rather than stored,
   * so the shared, immutable exchange instance can serve calls with different remaining step
   * deadlines without any per-call mutable state. When {@code null}, the exchange's default
   * per-request timeout is used.
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
      String stepName,
      @Nullable Duration requestTimeout)
      throws HttpCallException {
    URI uri = buildUri(baseUrl, path, queryParams);
    if (!policy.isAllowed(uri)) {
      throw new HttpCallException(
          "Host not allowed by policy: " + uri.getHost(), null, false, true);
    }

    if (body != null && body.length > policy.maxBodyBytes()) {
      throw new HttpCallException(
          "Request body exceeds limit (" + body.length + " > " + policy.maxBodyBytes() + ")",
          null,
          false,
          true);
    }

    Duration effectiveTimeout = requestTimeout != null ? requestTimeout : timeout;
    HttpRequest.Builder requestBuilder;
    try {
      requestBuilder = HttpRequest.newBuilder(uri).timeout(effectiveTimeout);
      // Merge by header name so a caller-supplied per-call header overrides an endpoint default of
      // the same name; the JDK header() appends (not replaces), so dedupe here before applying.
      // Case-insensitive per the HTTP spec. The framework correlation/content-type headers are set
      // afterward and always win.
      Map<String, String> merged = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      merged.putAll(defaultHeaders);
      for (Map.Entry<String, String> header : headers) {
        merged.put(header.getKey(), header.getValue());
      }
      merged.forEach(requestBuilder::header);
      requestBuilder.header(HttpHeaders.SAGA_ID, sagaId).header(HttpHeaders.SAGA_STEP, stepName);
      if (body != null && contentType != null) {
        requestBuilder.header(HttpHeaders.CONTENT_TYPE, contentType);
      }
    } catch (IllegalArgumentException e) {
      // newBuilder rejects a non-http(s) scheme or a non-absolute URI; header() rejects a control
      // character in a value (a saga correlation header or a caller-supplied one). Both are
      // definition errors — surface them as non-retryable rather than letting a raw
      // IllegalArgumentException escape.
      throw new HttpCallException("Invalid request URI or header for " + uri, e, false, true);
    }

    // contentType independently gates the Content-Type header above; here we only care whether
    // there is a body to send. Sending a body without a content type is valid HTTP — never drop it.
    if (body != null) {
      requestBuilder.method(httpMethod, HttpRequest.BodyPublishers.ofByteArray(body));
    } else {
      requestBuilder.method(httpMethod, HttpRequest.BodyPublishers.noBody());
    }

    HttpRequest httpRequest = requestBuilder.build();

    long maxBodyBytes = policy.maxBodyBytes();
    // The JDK request timeout (set above) only bounds time-to-headers — it is disarmed once the
    // response status arrives, so it does NOT cover the body. To bound the whole exchange (a
    // mid-body stall included), read the body via sendAsync and impose effectiveTimeout as a hard
    // deadline on the future, cancelling the in-flight request if it elapses. The limitedBytes
    // subscriber still caps the buffered body at maxBodyBytes.
    CompletableFuture<HttpResponse<byte[]>> future =
        client.sendAsync(httpRequest, limitedBytes(maxBodyBytes));
    HttpResponse<byte[]> response;
    try {
      response = future.get(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new HttpCallException("HTTP call timed out: " + uri, e, true);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null && hasCause(cause, BodyTooLargeException.class)) {
        throw new HttpCallException("Response body exceeds limit (> " + maxBodyBytes + ")", false);
      }
      if (cause != null && hasCause(cause, NumberFormatException.class)) {
        // A malformed Content-Length (e.g. "abc") makes the JDK reject the response while framing
        // the body — firstValueAsLong throws NumberFormatException in the body handler. This is a
        // deterministic server protocol violation that no retry will fix, so classify it as
        // non-retryable rather than hammering a broken server up to the policy's max attempts.
        throw new HttpCallException("Malformed Content-Length from " + uri, cause, false);
      }
      // A failure proven never to have reached the participant (connection refused, DNS, TLS
      // handshake, connect-timeout) cannot have committed a side effect, so mark it. A mid-flight
      // reset, read failure, or any ambiguous I/O may have committed, so it stays false (in-doubt).
      boolean knownNotCommitted = cause != null && isProvenNonDelivery(cause);
      throw new HttpCallException(
          "HTTP call failed: " + uri, cause != null ? cause : e, true, knownNotCommitted);
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new HttpCallException("HTTP call interrupted: " + uri, e, true);
    }
    byte[] responseBody = response.body();

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

  /**
   * A {@link BodyHandler} that buffers the response into a {@code byte[]} but cancels the download
   * once more than {@code maxBodyBytes} have arrived, failing with {@link BodyTooLargeException}.
   * Unlike {@code BodyHandlers.ofByteArray()} (unbounded), it preserves the memory cap.
   */
  private static BodyHandler<byte[]> limitedBytes(long maxBodyBytes) {
    // A byte[] response can hold at most Integer.MAX_VALUE bytes, so clamp the cap there; this
    // keeps
    // the onComplete length sum within int range even if a caller configures a larger maxBodyBytes.
    long cap = Math.min(maxBodyBytes, Integer.MAX_VALUE);
    return responseInfo -> {
      // Fail fast when an honest server declares an oversized body up front.
      boolean declaredTooLarge =
          responseInfo.headers().firstValueAsLong(HttpHeaders.CONTENT_LENGTH).orElse(-1L) > cap;
      return new LimitedBodySubscriber(cap, declaredTooLarge);
    };
  }

  /**
   * Accumulates the response body, rejecting it once it exceeds {@code maxBodyBytes}. Each chunk is
   * copied into a right-sized array and concatenated at {@link #onComplete}; on exceeding the limit
   * the subscription is cancelled and the body stage fails with {@link BodyTooLargeException}.
   */
  private static final class LimitedBodySubscriber implements BodySubscriber<byte[]> {

    // These fields need no synchronization: Flow guarantees the subscriber's signals
    // (onSubscribe/onNext*/onComplete|onError) are delivered serially with a happens-before between
    // consecutive signals, so writes in one signal are visible to the next even across carrier
    // threads. Do not add volatile/locks or reuse a subscriber instance across subscriptions.
    private final long maxBodyBytes;
    private final boolean declaredTooLarge;
    private final CompletableFuture<byte[]> body = new CompletableFuture<>();
    private final List<byte[]> buffers = new ArrayList<>();
    private Flow.@Nullable Subscription subscription;
    private long received;

    LimitedBodySubscriber(long maxBodyBytes, boolean declaredTooLarge) {
      this.maxBodyBytes = maxBodyBytes;
      this.declaredTooLarge = declaredTooLarge;
    }

    @Override
    public CompletionStage<byte[]> getBody() {
      return body;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.subscription = subscription;
      if (declaredTooLarge) {
        subscription.cancel();
        body.completeExceptionally(new BodyTooLargeException());
        return;
      }
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(List<ByteBuffer> items) {
      for (ByteBuffer buffer : items) {
        int remaining = buffer.remaining();
        received += remaining;
        if (received > maxBodyBytes) {
          Flow.Subscription current = subscription;
          if (current != null) {
            current.cancel();
          }
          buffers.clear();
          body.completeExceptionally(new BodyTooLargeException());
          return;
        }
        // Copy each chunk into a right-sized array rather than retaining the delivered ByteBuffers:
        // the JDK may hand us a buffer whose backing array is larger than the bytes read (e.g. a
        // 16KB read buffer holding a few bytes of a trickled response), and retaining those would
        // pin far more than maxBodyBytes. Copying keeps the retained heap bounded by maxBodyBytes —
        // the cap's purpose. (Deliberate divergence from the JDK's ofByteArray, which retains the
        // buffers because it is uncapped.)
        if (remaining > 0) {
          byte[] bytes = new byte[remaining];
          buffer.get(bytes);
          buffers.add(bytes);
        }
      }
    }

    @Override
    public void onError(Throwable throwable) {
      buffers.clear();
      body.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      int total = 0;
      for (byte[] buffer : buffers) {
        total += buffer.length;
      }
      byte[] result = new byte[total];
      int offset = 0;
      for (byte[] buffer : buffers) {
        System.arraycopy(buffer, 0, result, offset, buffer.length);
        offset += buffer.length;
      }
      buffers.clear();
      body.complete(result);
    }
  }

  /** Internal marker raised by {@link LimitedBodySubscriber} when the body exceeds the limit. */
  private static final class BodyTooLargeException extends IOException {}

  /** Returns whether {@code type} appears anywhere in {@code throwable}'s cause chain. */
  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
  }

  /**
   * Whether {@code cause} proves the request never reached the participant — connection refused
   * ({@link ConnectException}), host unreachable via firewall/routing ({@link
   * NoRouteToHostException}), DNS failure ({@link UnknownHostException}), TLS handshake failure
   * ({@link SSLHandshakeException}), or a <em>connect</em> (not read) timeout ({@link
   * HttpConnectTimeoutException}). Such a failure cannot have committed a side effect, so the
   * engine may skip the failed step's compensation. Everything else — a mid-flight reset, a read
   * timeout, a body-read failure, or any ambiguous {@link IOException} — may have committed and is
   * not proven.
   *
   * <p>{@link SSLHandshakeException} is safe to treat as non-delivery because JSSE raises it only
   * for {@code handshakeOnly} alerts (bad cert, hostname mismatch, no common protocol, {@code
   * handshake_failure}), which arise only during the <em>initial</em> handshake — and that
   * completes before any request bytes are sent, so it is always pre-send. This client does not
   * force TLS 1.3 / HTTP-2, so a TLS 1.2 connection can still receive a server-initiated
   * renegotiation ({@code hello_request}) after the request was sent; but the JDK {@code
   * HttpClient} does not perform renegotiation — it rejects {@code hello_request} as an unexpected
   * message, which surfaces as {@code SSLProtocolException} (JDK-8208642), a sibling of {@code
   * SSLHandshakeException} that this method does not match, so it correctly stays in-doubt.
   * (Caveat: a non-default JSSE provider that actively renegotiates could throw {@code
   * SSLHandshakeException} post-delivery; not covered.)
   */
  private static boolean isProvenNonDelivery(Throwable cause) {
    return hasCause(cause, ConnectException.class)
        || hasCause(cause, NoRouteToHostException.class)
        || hasCause(cause, UnknownHostException.class)
        || hasCause(cause, SSLHandshakeException.class)
        || hasCause(cause, HttpConnectTimeoutException.class);
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
      throw new HttpCallException("Malformed request URI: " + url, e, false, true);
    }
  }

  private static ObjectMapper hardenedMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    // Defense in depth against polymorphic-deserialization gadgets (off by default in Jackson 2.x).
    objectMapper.deactivateDefaultTyping();
    return objectMapper;
  }
}
