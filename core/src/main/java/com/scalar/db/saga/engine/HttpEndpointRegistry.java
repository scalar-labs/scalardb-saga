package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.SagaHttpClientProvider;
import com.scalar.db.saga.api.StepResolver.ResolutionContext;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.transport.HttpEndpoint;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lookup of {@link HttpEndpoint}s by the name they were registered under via {@code
 * httpEndpoint(name, baseUrl)}. Each endpoint owns ONE {@code HttpExchange} + policy + {@code
 * HttpClient}, and produces the per-endpoint {@link SagaHttpClient}.
 *
 * <p>It exposes a narrow {@link SagaHttpClientProvider} view (the {@code ResolutionContext} handed
 * to a custom {@code StepResolver}) for code steps. A fail-fast {@link #httpClient(String)} names a
 * missing endpoint. {@link AutoCloseable}: framework-created clients are closed here;
 * caller-supplied ones are left open.
 */
final class HttpEndpointRegistry implements ResolutionContext, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(HttpEndpointRegistry.class);

  private final Map<String, HttpEndpoint> endpoints;

  private HttpEndpointRegistry(Map<String, HttpEndpoint> endpoints) {
    this.endpoints = Map.copyOf(endpoints);
  }

  /** Creates a registry with one {@link HttpEndpoint} per {@code name → config} entry. */
  static HttpEndpointRegistry create(Map<String, HttpServiceConfig> endpointConfigs) {
    Map<String, HttpEndpoint> endpoints = new HashMap<>();
    for (Map.Entry<String, HttpServiceConfig> entry : endpointConfigs.entrySet()) {
      endpoints.put(entry.getKey(), HttpEndpoint.create(entry.getValue()));
    }
    return new HttpEndpointRegistry(endpoints);
  }

  /**
   * Returns the {@link SagaHttpClient} for the endpoint named {@code name}.
   *
   * @throws SagaDefinitionException if no endpoint was registered under {@code name}
   */
  @Override
  public SagaHttpClient httpClient(String name) {
    HttpEndpoint endpoint = endpoints.get(name);
    if (endpoint == null) {
      throw new SagaDefinitionException("No HTTP endpoint registered under name '" + name + "'");
    }
    return endpoint.sagaHttpClient();
  }

  /**
   * Returns the sole registered endpoint's {@link SagaHttpClient}.
   *
   * @throws SagaDefinitionException if zero endpoints are registered, or if more than one is
   *     registered (then {@code @Named}/{@link #httpClient(String)} must select one)
   */
  @Override
  public SagaHttpClient httpClient() {
    if (endpoints.isEmpty()) {
      throw new SagaDefinitionException(
          "No HTTP endpoint is registered; call httpEndpoint(name, baseUrl) on the builder");
    }
    if (endpoints.size() > 1) {
      throw new SagaDefinitionException(
          "Multiple HTTP endpoints are registered "
              + endpoints.keySet()
              + "; annotate the SagaHttpClient parameter with @Named(\"<endpoint>\") to select one");
    }
    return endpoints.values().iterator().next().sagaHttpClient();
  }

  /**
   * Closes every owned {@link HttpEndpoint} (releasing framework-created clients). Best-effort: a
   * failure to close one is logged and does not prevent the others from being closed.
   */
  @Override
  public void close() {
    for (HttpEndpoint endpoint : endpoints.values()) {
      try {
        endpoint.close();
      } catch (RuntimeException e) {
        logger.warn("Failed to close HTTP endpoint", e);
      }
    }
  }
}
