package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.exception.SagaDefinitionException;

/**
 * A narrow lookup of {@link SagaHttpClient}s by the {@code httpEndpoint(name, baseUrl)} name they
 * were registered under. Each returned client is the live, framework-built client for that endpoint
 * (shared correlation headers, SSRF allowlist, body limits, retryable classification) — a custom
 * {@link StepResolver} must obtain its clients from here rather than constructing its own, so code
 * steps keep parity with declarative steps.
 *
 * @see ResolutionContext#httpClient(String)
 */
public interface SagaHttpClientProvider {

  /**
   * Returns the {@link SagaHttpClient} for the endpoint registered as {@code name}.
   *
   * @param name the endpoint name from {@code httpEndpoint(name, baseUrl)}
   * @return the live client for that endpoint
   * @throws SagaDefinitionException if no endpoint was registered under {@code name}
   */
  SagaHttpClient httpClient(String name);

  /**
   * Returns the sole registered HTTP endpoint's client. This is the convenience for the common
   * single-endpoint case, mirroring the {@code @Named}-is-optional-when-only-one-registered rule:
   * the name is only needed to disambiguate when two or more endpoints exist.
   *
   * @return the live client of the single registered endpoint
   * @throws SagaDefinitionException if zero endpoints are registered, or if more than one is
   *     registered (then {@code @Named}/{@link #httpClient(String)} must select one)
   */
  SagaHttpClient httpClient();
}
