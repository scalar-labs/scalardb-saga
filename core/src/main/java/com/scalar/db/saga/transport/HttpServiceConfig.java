package com.scalar.db.saga.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Per-endpoint configuration for an HTTP transport endpoint, collected by {@code
 * DefaultSagaOrchestrator.Builder#httpEndpoint(...)} or constructed directly by a {@link
 * HttpEndpointRegistrar#swapHttpEndpoints} caller, and consumed by {@code
 * HttpEndpointManager#create}/{@code HttpEndpoint#create}. It carries the base URL plus the
 * outbound-policy knobs (SSRF allowlist, max body size) and an optional caller-supplied {@link
 * HttpClient} (e.g. for a proxy or custom TLS); the endpoint turns these into the package-private
 * {@code OutboundHttpPolicy} and {@code HttpExchange}.
 *
 * <p>This type lives in this package (rather than {@code api}) so it can be the wire between its
 * callers and the package-private HTTP machinery without exposing {@code OutboundHttpPolicy}. It is
 * user-facing on one path only: a configuration hot-reload constructs these directly and applies
 * them through the {@link HttpEndpointRegistrar}.
 *
 * @param baseUrl the service base URL (e.g. {@code http://account-svc:8080})
 * @param allowedHosts the SSRF allowlist; empty = allow all (defensively copied)
 * @param maxBodyBytes the max request/response body size in bytes, or {@code <= 0} to use the
 *     default
 * @param httpClient a caller-owned client to use instead of the framework-created shared one, or
 *     {@code null} to use the shared default
 * @param defaultHeaders headers applied to every request to this endpoint (declarative and
 *     code-step paths) — the channel for auth/secrets; never persisted in a definition (defensively
 *     copied; default empty)
 */
public record HttpServiceConfig(
    String baseUrl,
    List<String> allowedHosts,
    long maxBodyBytes,
    @Nullable HttpClient httpClient,
    Map<String, String> defaultHeaders) {

  public HttpServiceConfig {
    // Every construction path flows through here — the orchestrator builder and every
    // configuration swap — so a malformed or misleading base URL can never reach an endpoint,
    // no matter which route configured it.
    validateBaseUrl(baseUrl);
    allowedHosts = List.copyOf(allowedHosts);
    defaultHeaders = Map.copyOf(defaultHeaders);
  }

  /**
   * Fails on a malformed or misleading {@code baseUrl}: it must be a valid absolute {@code
   * http}/{@code https} URL with a host and no user-info component (a {@code user@host} authority
   * silently retargets the host — e.g. {@code http://svc@evil.com} resolves to {@code evil.com}).
   *
   * <p>Messages deliberately do not echo the value: on the server's reload path a base URL may have
   * been resolved from a secret reference, and these messages reach logs. Callers that know the
   * value is safe to name add their own context.
   *
   * @throws IllegalArgumentException naming the violated rule
   */
  public static void validateBaseUrl(String baseUrl) {
    URI uri;
    try {
      uri = URI.create(baseUrl);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("baseUrl is not a valid URI", e);
    }
    String scheme = uri.getScheme();
    if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
      throw new IllegalArgumentException("baseUrl must use the http or https scheme");
    }
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("baseUrl must have a host");
    }
    if (uri.getUserInfo() != null) {
      throw new IllegalArgumentException(
          "baseUrl must not contain a user-info component (it silently retargets the host)");
    }
  }
}
