package com.scalar.db.saga.transport;

import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Per-endpoint configuration for an HTTP transport endpoint, collected by {@code
 * SagaManager.Builder#httpEndpoint(...)} and consumed by {@code HttpEndpointRegistry#create}/{@code
 * HttpEndpoint#create}. It carries the base URL plus the outbound-policy knobs (SSRF allowlist, max
 * body size) and an optional caller-supplied {@link HttpClient} (e.g. for a proxy or custom TLS);
 * the endpoint turns these into the package-private {@code OutboundHttpPolicy} and {@code
 * HttpExchange}.
 *
 * <p>This type lives in this package (rather than {@code api}) so it can be the wire between the
 * {@code engine} builder and the package-private HTTP machinery without exposing {@code
 * OutboundHttpPolicy}; it is not part of the user-facing API surface.
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
    allowedHosts = List.copyOf(allowedHosts);
    defaultHeaders = Map.copyOf(defaultHeaders);
  }
}
