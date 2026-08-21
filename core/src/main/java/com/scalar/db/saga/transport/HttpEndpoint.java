package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaHttpClient;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The per-endpoint unit of the shared HTTP machinery for one {@code httpEndpoint(name, baseUrl)}:
 * it carries ONE {@link HttpExchange} + {@link OutboundHttpPolicy} + {@link HttpClient} and
 * produces BOTH the {@link SagaHttpClient} for code steps (via {@link #sagaHttpClient()}) and the
 * declarative {@link #transportAdapter()}. Both ride the same {@link HttpExchange}, so a code step
 * and a declarative step against the same endpoint share one client, one policy, and one
 * status-classification path — the "one engine per endpoint" invariant.
 *
 * <p>Lifecycle is owned by the {@link HttpEndpointManager}, which creates endpoints from
 * configuration, reuses them across swaps while their topology is unchanged (absorbing header
 * rotation in place via {@link #updateDefaultHeaders}), and retires them gracefully via {@link
 * #shutdown()} on topology change or removal.
 *
 * <p>A framework-created {@link HttpClient} uses {@link HttpClient.Redirect#NEVER} (an allowed host
 * must not 302 to a disallowed one, bypassing the SSRF allowlist) and is shut down at retirement; a
 * caller-supplied client is never shut down here (the caller owns its lifecycle).
 *
 * <p>This lives in the {@code transport} package (not {@code engine}) so it can construct the
 * package-private {@link HttpExchange}/{@link OutboundHttpPolicy}/{@link SagaHttpClientImpl} and
 * the package-private {@link HttpTransportAdapter}; the {@link HttpEndpointManager} holds one of
 * these per endpoint name.
 */
final class HttpEndpoint implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(HttpEndpoint.class);

  /** Grace {@link #close()} gives in-flight exchanges before force-stopping the owned client. */
  private static final Duration CLOSE_GRACE = Duration.ofSeconds(2);

  /**
   * Connect-phase timeout for a framework-created client, so a black-holed host fails fast instead
   * of waiting out the (deadline-derived) per-request timeout. The overall call is still bounded by
   * that per-request timeout; this only shortens the connect phase.
   */
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

  private final HttpExchange exchange;
  private final String baseUrl;
  private final HttpClient client;
  private final boolean ownsClient;
  private final TransportAdapter transportAdapter;

  private HttpEndpoint(
      HttpExchange exchange,
      String baseUrl,
      HttpClient client,
      boolean ownsClient,
      @Nullable CallbackUrlProvider callbackUrlProvider) {
    this.exchange = exchange;
    this.baseUrl = baseUrl;
    this.client = client;
    this.ownsClient = ownsClient;
    // The declarative transport adapter rides the SAME exchange as the SagaHttpClient, so both
    // front-ends share one client/policy/status-classification path (the "one engine per endpoint"
    // invariant).
    this.transportAdapter = new HttpTransportAdapter(baseUrl, exchange, callbackUrlProvider);
  }

  /**
   * Builds an endpoint from {@code config}, creating a {@link HttpClient.Redirect#NEVER} client
   * with a bounded {@link #CONNECT_TIMEOUT connect timeout} if none was supplied. A caller-supplied
   * client is used as-is (the caller owns its timeout configuration) — except that, when an {@code
   * allowedHosts} allowlist is configured, it must use {@link HttpClient.Redirect#NEVER}: a
   * redirect-following client could follow a 3xx from an allowed host to a disallowed one,
   * bypassing the allowlist (which is only checked on the initial URI).
   *
   * @throws IllegalArgumentException if a supplied client follows redirects while an allowlist is
   *     set
   */
  public static HttpEndpoint create(HttpServiceConfig config) {
    return create(config, null);
  }

  /**
   * Builds an endpoint from {@code config} (see {@link #create(HttpServiceConfig)}), wiring {@code
   * callbackUrlProvider} into the declarative transport so an async step's outgoing request carries
   * a callback URL. A {@code null} provider disables async-callback provisioning.
   */
  public static HttpEndpoint create(
      HttpServiceConfig config, @Nullable CallbackUrlProvider callbackUrlProvider) {
    HttpClient client;
    boolean ownsClient;
    @Nullable HttpClient supplied = config.httpClient();
    if (supplied != null) {
      // A redirect-following supplied client could follow a 3xx from an allowed host to a
      // disallowed one, defeating the allowlist (HttpExchange only checks the initial URI). The JDK
      // default is NEVER, so this only rejects a client that explicitly opted into redirects.
      if (!config.allowedHosts().isEmpty()
          && supplied.followRedirects() != HttpClient.Redirect.NEVER) {
        throw new IllegalArgumentException(
            "A supplied HttpClient used with an allowedHosts allowlist must use Redirect.NEVER"
                + " (got "
                + supplied.followRedirects()
                + "): a redirect-following client can bypass the allowlist (SSRF).");
      }
      client = supplied;
      ownsClient = false;
    } else {
      // Redirect.NEVER: an allowed host could otherwise 302 to a metadata/internal host that the
      // JDK would follow without re-checking the allowlist (SSRF-via-redirect). connectTimeout
      // bounds the connect phase so a black-holed host fails fast.
      client =
          HttpClient.newBuilder()
              .connectTimeout(CONNECT_TIMEOUT)
              .followRedirects(HttpClient.Redirect.NEVER)
              .build();
      ownsClient = true;
    }
    HttpExchange exchange = new HttpExchange(client, policyOf(config), config.defaultHeaders());
    return new HttpEndpoint(exchange, config.baseUrl(), client, ownsClient, callbackUrlProvider);
  }

  private static OutboundHttpPolicy policyOf(HttpServiceConfig config) {
    if (config.allowedHosts().isEmpty() && config.maxBodyBytes() <= 0) {
      return OutboundHttpPolicy.allowAll();
    }
    OutboundHttpPolicy.Builder builder = OutboundHttpPolicy.newBuilder();
    if (!config.allowedHosts().isEmpty()) {
      builder.allowedHosts(config.allowedHosts().toArray(new String[0]));
    }
    if (config.maxBodyBytes() > 0) {
      builder.maxBodyBytes(config.maxBodyBytes());
    }
    return builder.build();
  }

  /** The {@link SagaHttpClient} for this endpoint, built on its shared {@link HttpExchange}. */
  public SagaHttpClient sagaHttpClient() {
    return new SagaHttpClientImpl(exchange, baseUrl);
  }

  /** The shared {@link HttpExchange} both front-ends ride (package-private; for identity tests). */
  HttpExchange exchange() {
    return exchange;
  }

  /**
   * The declarative {@link TransportAdapter} riding this endpoint's shared {@link HttpExchange} —
   * what a {@link TransportResolver} resolution returns.
   */
  TransportAdapter transportAdapter() {
    return transportAdapter;
  }

  /**
   * Replaces the endpoint default headers applied to every subsequent request through this endpoint
   * (both front-ends): secret rotation as a value swap, no client or connection churn.
   */
  void updateDefaultHeaders(Map<String, String> headers) {
    exchange.updateDefaultHeaders(headers);
  }

  /**
   * Initiates graceful retirement and never blocks: exchanges already in flight complete, and every
   * subsequent request through this endpoint fails pre-send as retryable (to be re-resolved against
   * the endpoint set that replaced it). A caller-supplied client is not shut down — the caller owns
   * its lifecycle — but the retirement flag still makes this endpoint's requests fail fast. Never
   * {@code HttpClient.close()}: its Javadoc permits blocking indefinitely on an abandoned streaming
   * body.
   */
  void shutdown() {
    exchange.markRetired();
    if (ownsClient) {
      client.shutdown();
    }
  }

  /**
   * Whether retirement has completed — the owned client has terminated, or the client was
   * caller-supplied (never shut down here, so there is nothing to wait for).
   */
  boolean isTerminated() {
    return !ownsClient || client.isTerminated();
  }

  /** Waits up to {@code duration} for an owned client to terminate after {@link #shutdown()}. */
  boolean awaitTermination(Duration duration) throws InterruptedException {
    return !ownsClient || client.awaitTermination(duration);
  }

  /** Force-stops an owned client, interrupting whatever {@link #shutdown()} left in flight. */
  void shutdownNow() {
    if (ownsClient) {
      try {
        client.shutdownNow();
      } catch (RuntimeException e) {
        logger.warn("Failed to force-stop endpoint HTTP client", e);
      }
    }
  }

  /**
   * Retires this endpoint and waits briefly for in-flight exchanges before force-stopping. A
   * caller-supplied client is left running. For standalone use (tests); the engine's endpoint
   * manager drains all endpoints under one shared deadline instead.
   */
  @Override
  public void close() {
    shutdown();
    try {
      awaitTermination(CLOSE_GRACE);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    shutdownNow();
  }
}
