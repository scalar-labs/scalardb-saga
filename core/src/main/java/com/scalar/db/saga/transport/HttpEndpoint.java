package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single per-endpoint owner of the shared HTTP machinery for one {@code httpEndpoint(name,
 * baseUrl)}: it owns ONE {@link HttpExchange} + {@link OutboundHttpPolicy} + {@link HttpClient} and
 * produces BOTH the {@link SagaHttpClient} for code steps (via {@link #sagaHttpClient()}) and the
 * declarative steps (via {@link #toStep}/{@link #toTccStep}). Both ride the same {@link
 * HttpExchange}, so a code step and a declarative step against the same endpoint share one client,
 * one policy, and one status-classification path. One owner, one {@link #close()} — so the "one
 * engine per endpoint" invariant never passes through a two-client state.
 *
 * <p>A framework-created {@link HttpClient} uses {@link HttpClient.Redirect#NEVER} (an allowed host
 * must not 302 to a disallowed one, bypassing the SSRF allowlist) and is closed by {@link
 * #close()}; a caller-supplied client is left open (the caller owns its lifecycle).
 *
 * <p>This lives in the {@code transport} package (not {@code engine}) so it can construct the
 * package-private {@link HttpExchange}/{@link OutboundHttpPolicy}/{@link SagaHttpClientImpl} and
 * the package-private {@link HttpTransportAdapter}/{@link DeclarativeBindingStep}/{@link
 * DeclarativeBindingTccStep}; the engine-side {@code HttpEndpointRegistry} holds one of these per
 * endpoint name.
 */
public final class HttpEndpoint implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(HttpEndpoint.class);

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
      HttpExchange exchange, String baseUrl, HttpClient client, boolean ownsClient) {
    this.exchange = exchange;
    this.baseUrl = baseUrl;
    this.client = client;
    this.ownsClient = ownsClient;
    // The declarative transport adapter rides the SAME exchange as the SagaHttpClient, so both
    // front-ends share one client/policy/status-classification path (the "one engine per endpoint"
    // invariant).
    this.transportAdapter = new HttpTransportAdapter(baseUrl, exchange);
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
    return new HttpEndpoint(exchange, config.baseUrl(), client, ownsClient);
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

  /**
   * Wraps a declaratively-defined service step's phases as a {@link Step} (SAGA) named {@code
   * stepName}, riding this endpoint's shared {@link HttpExchange}.
   */
  public Step toStep(String stepName, Map<Phase, CallSpec> phases) {
    return new DeclarativeBindingStep(stepName, transportAdapter, phases);
  }

  /**
   * Wraps a declaratively-defined service step's phases as a {@link TccStep} (TCC) named {@code
   * stepName}, riding this endpoint's shared {@link HttpExchange}.
   */
  public TccStep toTccStep(String stepName, Map<Phase, CallSpec> phases) {
    return new DeclarativeBindingTccStep(stepName, transportAdapter, phases);
  }

  /** The shared {@link HttpExchange} both front-ends ride (package-private; for identity tests). */
  HttpExchange exchange() {
    return exchange;
  }

  /** The declarative {@link TransportAdapter} (package-private; for identity tests). */
  TransportAdapter transportAdapter() {
    return transportAdapter;
  }

  /**
   * Closes the framework-created {@link HttpClient}, if this endpoint created it. A caller-supplied
   * client is left open. Best-effort: a failure is logged.
   */
  @Override
  public void close() {
    if (ownsClient) {
      try {
        client.close();
      } catch (RuntimeException e) {
        logger.warn("Failed to close endpoint HTTP client", e);
      }
    }
  }
}
