package com.scalar.db.saga.transport;

import java.util.Map;

/**
 * The narrow mutator for replacing the engine's HTTP endpoint set at runtime — the seam a
 * configuration hot-reload uses to apply a validated service set without a restart. It is
 * deliberately a full-set swap rather than per-endpoint add/remove operations: the caller validates
 * a complete candidate configuration and swaps to it in one call, so the registered set always
 * corresponds to exactly one validated configuration.
 *
 * <p>Swap semantics per service name: an endpoint whose topology ({@code baseUrl}, {@code
 * allowedHosts}, {@code maxBodyBytes}, supplied client) is unchanged is reused in place — a changed
 * {@code defaultHeaders} map is applied to it as a value swap, so secret rotation causes no
 * connection churn. A topology change or removal retires the old endpoint gracefully: calls already
 * in flight complete against it, and new submissions fail pre-send as retryable, to be re-resolved
 * against the new set.
 *
 * <p><b>Embedded-mode contract:</b> only declarative steps re-resolve their endpoint per call. A
 * class step's {@code SagaHttpClient}, injected when its plan was built, is NOT rebound by a swap:
 * it keeps riding the endpoint it was built against, and if that endpoint is retired by a swap its
 * calls fail as retryable until the process restarts. Embedded users who swap endpoints must
 * therefore not rely on injected clients surviving a topology change to their service.
 *
 * <p><b>Failure guarantee:</b> the published set flips in a single atomic step at the end of a
 * successful swap, so a resolution never observes a mix of old and new entries. When a swap throws
 * partway, nothing is published and the previous set keeps serving; header rotations already
 * applied to reused endpoints stick (they are validated values, so this is benign), and retrying
 * the swap converges on the intended set — last known good keeps serving until then. Endpoints the
 * failed attempt already created are shut down and tracked until their clients terminate, not
 * leaked.
 */
public interface HttpEndpointRegistrar {

  /**
   * Replaces the full endpoint set with one endpoint per {@code name → config} entry, applying the
   * reuse / rotate / retire semantics and the failure guarantee described on this interface.
   *
   * @param services the complete new endpoint set (not a delta)
   */
  void swapHttpEndpoints(Map<String, HttpServiceConfig> services);
}
